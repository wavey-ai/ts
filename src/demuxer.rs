use access_unit::AccessUnit;
use bytes::Bytes;
use h264::{Bitstream, Decode, NALUnit, SequenceParameterSet};
use mpeg2ts_reader::packet;
use mpeg2ts_reader::pes;
use mpeg2ts_reader::psi;
use mpeg2ts_reader::{
    demultiplex::{self, FilterChangeset},
    StreamType,
};
use mse_fmp4::avc::AvcDecoderConfigurationRecord;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

pub struct TsDemuxer;

impl TsDemuxer {
    pub fn start(tx: mpsc::Sender<AccessUnit>) -> mpsc::Sender<Bytes> {
        let (input_tx, input_rx) = mpsc::channel::<Bytes>(32);

        let mut context = DemuxContext::new(tx);
        let mut demux = demultiplex::Demultiplex::new(&mut context);

        tokio::spawn(async move {
            let mut input_rx = input_rx;
            while let Some(data) = input_rx.recv().await {
                demux.push(&mut context, &data);
            }
            debug!("Input channel closed, shutting down demuxer");
        });

        input_tx
    }
}

pub struct DemuxContext {
    changeset: FilterChangeset<DumpFilterSwitch>,
    output_tx: mpsc::Sender<AccessUnit>,
}

impl DemuxContext {
    fn new(output_tx: mpsc::Sender<AccessUnit>) -> Self {
        DemuxContext {
            changeset: FilterChangeset::default(),
            output_tx,
        }
    }

    fn do_construct(&mut self, req: demultiplex::FilterRequest<'_, '_>) -> DumpFilterSwitch {
        match req {
            demultiplex::FilterRequest::ByPid(psi::pat::PAT_PID) => {
                DumpFilterSwitch::Pat(demultiplex::PatPacketFilter::default())
            }
            demultiplex::FilterRequest::ByPid(mpeg2ts_reader::STUFFING_PID) => {
                DumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::ByPid(_) => {
                DumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::ByStream {
                stream_type: StreamType::ADTS,
                pmt,
                stream_info,
                ..
            } => PtsDumpElementaryStreamConsumer::construct(
                StreamType::ADTS,
                pmt,
                stream_info,
                self.output_tx.clone(),
            ),
            demultiplex::FilterRequest::ByStream {
                stream_type: StreamType::H264,
                pmt,
                stream_info,
                ..
            } => PtsDumpElementaryStreamConsumer::construct(
                StreamType::H264,
                pmt,
                stream_info,
                self.output_tx.clone(),
            ),
            demultiplex::FilterRequest::ByStream { .. } => {
                DumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
            demultiplex::FilterRequest::Pmt {
                pid,
                program_number,
            } => DumpFilterSwitch::Pmt(demultiplex::PmtPacketFilter::new(pid, program_number)),
            demultiplex::FilterRequest::Nit { .. } => {
                DumpFilterSwitch::Null(demultiplex::NullPacketFilter::default())
            }
        }
    }
}

impl demultiplex::DemuxContext for DemuxContext {
    type F = DumpFilterSwitch;

    fn filter_changeset(&mut self) -> &mut FilterChangeset<Self::F> {
        &mut self.changeset
    }

    fn construct(&mut self, req: demultiplex::FilterRequest<'_, '_>) -> Self::F {
        self.do_construct(req)
    }
}

packet_filter_switch! {
    DumpFilterSwitch<DemuxContext> {
        Pes: pes::PesPacketFilter<DemuxContext, PtsDumpElementaryStreamConsumer>,
        Pat: demultiplex::PatPacketFilter<DemuxContext>,
        Pmt: demultiplex::PmtPacketFilter<DemuxContext>,
        Null: demultiplex::NullPacketFilter<DemuxContext>,
    }
}

struct PtsDumpElementaryStreamConsumer {
    stream_type: StreamType,
    pid: packet::Pid,
    len: Option<usize>,
    accumulated_payload: Vec<u8>,
    new_access_unit: bool,
    pts: u64,
    dts: u64,
    pps: Option<Bytes>,
    sps: Option<Bytes>,
    width: u16,
    height: u16,
    fps: f64,
    is_keyframe: bool,
    avcc: Option<AvcDecoderConfigurationRecord>,
    lp_nalus: Vec<u8>,
    output_tx: mpsc::Sender<AccessUnit>,
}

impl PtsDumpElementaryStreamConsumer {
    fn construct(
        stream_type: StreamType,
        _pmt_sect: &psi::pmt::PmtSection,
        stream_info: &psi::pmt::StreamInfo,
        output_tx: mpsc::Sender<AccessUnit>,
    ) -> DumpFilterSwitch {
        let filter = pes::PesPacketFilter::new(PtsDumpElementaryStreamConsumer {
            pid: stream_info.elementary_pid(),
            len: None,
            accumulated_payload: Vec::new(),
            stream_type,
            new_access_unit: false,
            pts: 0,
            dts: 0,
            sps: None,
            pps: None,
            width: 0,
            height: 0,
            fps: 0.0,
            avcc: None,
            is_keyframe: false,
            lp_nalus: Vec::new(),
            output_tx,
        });
        DumpFilterSwitch::Pes(filter)
    }

    fn send_access_unit(&self, au: AccessUnit) {
        match self.output_tx.try_send(au) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("Output channel full, dropping AccessUnit");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!("Output channel closed");
            }
        }
    }
}

impl pes::ElementaryStreamConsumer<DemuxContext> for PtsDumpElementaryStreamConsumer {
    fn start_stream(&mut self, _ctx: &mut DemuxContext) {}

    fn begin_packet(&mut self, _ctx: &mut DemuxContext, header: pes::PesHeader) {
        match header.contents() {
            pes::PesContents::Parsed(Some(parsed)) => {
                match parsed.pts_dts() {
                    Ok(pes::PtsDts::PtsOnly(Ok(pts))) => {
                        self.pts = pts.value();
                        self.dts = pts.value();
                    }
                    Ok(pes::PtsDts::Both {
                        pts: Ok(pts),
                        dts: Ok(dts),
                    }) => {
                        self.pts = pts.value();
                        if self.dts > dts.value() {
                            error!(
                                "DTS has gone backwards! got {} was {}",
                                dts.value(),
                                self.dts
                            );
                        }
                        self.dts = dts.value();
                    }
                    _ => (),
                }

                let payload = parsed.payload();
                self.len = Some(payload.len());
                self.accumulated_payload.extend_from_slice(payload);
            }
            pes::PesContents::Parsed(None) => (),
            pes::PesContents::Payload(payload) => {
                self.len = Some(payload.len());
                self.accumulated_payload.extend_from_slice(payload);
            }
        }
    }

    fn continue_packet(&mut self, _ctx: &mut DemuxContext, data: &[u8]) {
        self.accumulated_payload.extend_from_slice(data);
        self.len = self.len.map(|l| l + data.len());
    }

    fn end_packet(&mut self, _ctx: &mut DemuxContext) {
        match self.stream_type {
            StreamType::H264 => {
                for nalu in h264::iterate_annex_b(&self.accumulated_payload) {
                    if nalu.is_empty() {
                        continue;
                    }

                    let nalu_type = nalu[0] & h264::NAL_UNIT_TYPE_MASK;

                    match nalu_type {
                        h264::NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET => {
                            if self.sps.is_none() {
                                self.sps = Some(Bytes::copy_from_slice(nalu));
                            }
                        }
                        h264::NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {
                            if self.pps.is_none() {
                                self.pps = Some(Bytes::copy_from_slice(nalu));
                            }
                        }
                        5 => {
                            self.is_keyframe = true;
                        }
                        _ => {}
                    }

                    match nalu_type {
                        1 | 5 => {
                            let mut buffer = [0u8; 4];
                            buffer.copy_from_slice(&(nalu.len() as u32).to_be_bytes());
                            self.lp_nalus.extend_from_slice(&buffer);
                            self.lp_nalus.extend_from_slice(nalu);
                        }
                        9 => {
                            self.new_access_unit = true;
                        }
                        _ => {}
                    }
                }

                if self.new_access_unit && !self.lp_nalus.is_empty() {
                    self.new_access_unit = false;
                    if self.avcc.is_none() {
                        if let (Some(sps_b), Some(pps_b)) = (&self.sps, &self.pps) {
                            let bs = Bitstream::new(sps_b.iter().copied());
                            if let Ok(mut nalu) = NALUnit::decode(bs) {
                                let mut rbsp = Bitstream::new(&mut nalu.rbsp_byte);
                                if let Ok(sps) = SequenceParameterSet::decode(&mut rbsp) {
                                    self.width = (sps.pic_width_in_samples()
                                        - (sps.frame_crop_right_offset.0 * 2)
                                        - (sps.frame_crop_left_offset.0 * 2))
                                        as u16;
                                    self.height = ((sps.frame_height_in_mbs() * 16)
                                        - (sps.frame_crop_bottom_offset.0 * 2)
                                        - (sps.frame_crop_top_offset.0 * 2))
                                        as u16;
                                    if sps.vui_parameters_present_flag.0 != 0
                                        && sps.vui_parameters.timing_info_present_flag.0 != 0
                                    {
                                        self.fps = sps.vui_parameters.time_scale.0 as f64
                                            / sps.vui_parameters.num_units_in_tick.0 as f64;
                                    }

                                    self.avcc = Some(AvcDecoderConfigurationRecord {
                                        profile_idc: sps.profile_idc.0,
                                        constraint_set_flag: sps.constraint_set0_flag.0,
                                        level_idc: sps.level_idc.0,
                                        sequence_parameter_set: sps_b.clone(),
                                        picture_parameter_set: pps_b.clone(),
                                    })
                                }
                            }
                        }
                    }

                    let au = AccessUnit {
                        avc: true,
                        key: self.is_keyframe,
                        data: Bytes::from(std::mem::take(&mut self.lp_nalus)),
                        pts: self.pts,
                        dts: self.dts,
                    };

                    self.send_access_unit(au);
                    self.is_keyframe = false;
                }
            }
            StreamType::ADTS => {
                let au = AccessUnit {
                    avc: false,
                    key: false,
                    pts: self.pts,
                    dts: self.dts,
                    data: Bytes::from(std::mem::take(&mut self.accumulated_payload)),
                };

                self.send_access_unit(au);
            }
            _ => {}
        }
    }

    fn continuity_error(&mut self, _ctx: &mut DemuxContext) {}
}
