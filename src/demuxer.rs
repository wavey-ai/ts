use crate::boxer::{box_fmp4, ticks_to_ms};
use crate::playlist::Playlists;
use crate::AccessUnit;
use byteorder::{BigEndian, ByteOrder};
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
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use tracing::{debug, error, info};

pub fn new_demuxer(
    stream_id: u64,
    playlists: Arc<Playlists>,
    min_part_ms: u32,
) -> (watch::Receiver<()>, mpsc::Sender<Bytes>) {
    let (tx, mut rx) = mpsc::channel::<Bytes>(32);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(());
    let (fin_tx, fin_rx) = watch::channel(());

    tokio::spawn(async move {
        let mut ctx = DumpDemuxContext::new(stream_id, playlists, min_part_ms, shutdown_tx);
        let mut demux = demultiplex::Demultiplex::new(&mut ctx);

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    break;
                }
                Some(data) = rx.recv() => {
                    demux.push(&mut ctx, &data);
                }
            }
        }

        fin_tx.send(());
    });

    (fin_rx, tx)
}

pub struct DumpDemuxContext {
    changeset: FilterChangeset<DumpFilterSwitch>,
    min_part_ms: u32,
    stream_id: u64,
    playlists: Arc<Playlists>,
    adts_buf: Vec<AccessUnit>,
    h264_buf: Vec<AccessUnit>,
    shutdown_tx: watch::Sender<()>,
}

// Implement the constructor for the custom context
impl DumpDemuxContext {
    fn new(
        stream_id: u64,
        playlists: Arc<Playlists>,
        min_part_ms: u32,
        shutdown_tx: watch::Sender<()>,
    ) -> Self {
        DumpDemuxContext {
            changeset: FilterChangeset::default(),
            min_part_ms,
            stream_id,
            playlists,
            adts_buf: Vec::new(),
            h264_buf: Vec::new(),
            shutdown_tx,
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
            } => PtsDumpElementaryStreamConsumer::construct(StreamType::ADTS, pmt, stream_info),
            demultiplex::FilterRequest::ByStream {
                stream_type: StreamType::H264,
                pmt,
                stream_info,
                ..
            } => PtsDumpElementaryStreamConsumer::construct(StreamType::H264, pmt, stream_info),
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

// Manually implement the `DemuxContext` trait
impl demultiplex::DemuxContext for DumpDemuxContext {
    type F = DumpFilterSwitch;

    fn filter_changeset(&mut self) -> &mut FilterChangeset<Self::F> {
        &mut self.changeset
    }

    fn construct(&mut self, req: demultiplex::FilterRequest<'_, '_>) -> Self::F {
        self.do_construct(req)
    }
}

packet_filter_switch! {
    DumpFilterSwitch<DumpDemuxContext> {
        Pes: pes::PesPacketFilter<DumpDemuxContext, PtsDumpElementaryStreamConsumer>,
        Pat: demultiplex::PatPacketFilter<DumpDemuxContext>,
        Pmt: demultiplex::PmtPacketFilter<DumpDemuxContext>,
        Null: demultiplex::NullPacketFilter<DumpDemuxContext>,
    }
}

struct PtsDumpElementaryStreamConsumer {
    stream_type: StreamType,
    pid: packet::Pid,
    len: Option<usize>,
    accumulated_payload: Vec<u8>,
    new_access_unit: bool,
    seq: u64,
    pts: u64,
    dts: u64,
    pps: Option<Bytes>,
    sps: Option<Bytes>,
    avc_timestamps: Vec<u64>,
    seg_seq: u32,
    width: u16,
    height: u16,
    fps: f64,
    is_keyframe: bool,
    avcc: Option<AvcDecoderConfigurationRecord>,
    lp_nalus: Vec<u8>,
}

impl PtsDumpElementaryStreamConsumer {
    fn construct(
        stream_type: StreamType,
        _pmt_sect: &psi::pmt::PmtSection,
        stream_info: &psi::pmt::StreamInfo,
    ) -> DumpFilterSwitch {
        let filter = pes::PesPacketFilter::new(PtsDumpElementaryStreamConsumer {
            pid: stream_info.elementary_pid(),
            len: None,
            accumulated_payload: Vec::new(),
            stream_type,
            new_access_unit: false,
            seq: 1,
            pts: 0,
            dts: 0,
            sps: None,
            pps: None,
            avc_timestamps: Vec::new(),
            seg_seq: 1,
            width: 0,
            height: 0,
            fps: 0.0,
            avcc: None,
            is_keyframe: false,
            lp_nalus: Vec::new(),
        });
        DumpFilterSwitch::Pes(filter)
    }
}

impl pes::ElementaryStreamConsumer<DumpDemuxContext> for PtsDumpElementaryStreamConsumer {
    fn start_stream(&mut self, _ctx: &mut DumpDemuxContext) {}

    fn begin_packet(&mut self, _ctx: &mut DumpDemuxContext, header: pes::PesHeader) {
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
                self.accumulated_payload.extend_from_slice(payload); // Accumulate the payload data
            }
            pes::PesContents::Parsed(None) => (),
            pes::PesContents::Payload(payload) => {
                self.len = Some(payload.len());
                self.accumulated_payload.extend_from_slice(payload); // Accumulate the payload data
            }
        }
    }

    fn continue_packet(&mut self, _ctx: &mut DumpDemuxContext, data: &[u8]) {
        // Accumulate the payload data from the continuation of the packet
        self.accumulated_payload.extend_from_slice(data);
        self.len = self.len.map(|l| l + data.len());
    }

    fn end_packet(&mut self, _ctx: &mut DumpDemuxContext) {
        match self.stream_type {
            StreamType::H264 => {
                for nalu in h264::iterate_annex_b(&self.accumulated_payload) {
                    if nalu.len() == 0 {
                        continue;
                    }

                    let nalu_type = nalu[0] & h264::NAL_UNIT_TYPE_MASK;

                    match nalu_type {
                        h264::NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET => {
                            if self.sps.is_none() {
                                self.sps = Some(Bytes::copy_from_slice(&nalu));
                            }
                        }
                        h264::NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {
                            if self.pps.is_none() {
                                self.pps = Some(Bytes::copy_from_slice(&nalu));
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
                            BigEndian::write_u32(&mut buffer, nalu.len() as u32);
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
                        if let Some(sps_b) = &self.sps {
                            if let Some(pps_b) = &self.pps {
                                let bs = Bitstream::new(sps_b.iter().copied());
                                match NALUnit::decode(bs) {
                                    Ok(mut nalu) => {
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
                                                && sps.vui_parameters.timing_info_present_flag.0
                                                    != 0
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
                                        };
                                    }
                                    Err(e) => {
                                        dbg!(e);
                                    }
                                }
                            }
                        }
                    }

                    if self.avc_timestamps.len() > 1 {
                        let elapsed_ms = ticks_to_ms(
                            self.avc_timestamps[self.avc_timestamps.len() - 1]
                                - self.avc_timestamps[0],
                        ) as u32;

                        if elapsed_ms >= _ctx.min_part_ms || self.is_keyframe {
                            let mut avc = Vec::new();
                            let mut aac = Vec::new();
                            for a in &_ctx.h264_buf {
                                avc.push(a.clone());
                            }
                            for a in &_ctx.adts_buf {
                                aac.push(a.clone());
                            }

                            let fmp4 = box_fmp4(
                                self.seg_seq,
                                self.avcc.as_ref(),
                                avc,
                                aac,
                                self.dts,
                                self.width,
                                self.height,
                            );

                            if !_ctx.playlists.add(_ctx.stream_id, fmp4) {
                                let _ = _ctx.shutdown_tx.send(());
                            }
                            self.seg_seq += 1;
                            _ctx.h264_buf.clear();
                            _ctx.adts_buf.clear();
                            self.avc_timestamps.clear();
                        }
                    }

                    let au = AccessUnit {
                        avc: true,
                        key: self.is_keyframe,
                        data: Bytes::from(self.lp_nalus.to_vec()),
                        pts: self.pts,
                        dts: self.dts,
                    };

                    self.lp_nalus.clear();
                    self.avc_timestamps.push(self.dts);
                    _ctx.h264_buf.push(au);
                    self.seq += 1;
                    self.is_keyframe = false;
                }
            }
            StreamType::ADTS => {
                let data_slice: &[u8] = &self.accumulated_payload;
                let au = AccessUnit {
                    avc: false,
                    key: false,
                    pts: self.pts,
                    dts: self.dts,
                    data: Bytes::from(data_slice.to_vec()),
                };

                _ctx.adts_buf.push(au);

                self.seq += 1;
            }
            _ => {}
        }
        self.accumulated_payload.clear();
    }

    fn continuity_error(&mut self, _ctx: &mut DumpDemuxContext) {}
}
