use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use mpeg2ts_reader::packet;
use mpeg2ts_reader::pes;
use mpeg2ts_reader::psi;
use mpeg2ts_reader::{
    demultiplex::{self, FilterChangeset},
    StreamType,
};
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Clone)]
pub struct AccessUnit {
    pub avc: bool,
    pub key: bool,
    pub pts: u64,
    pub dts: u64,
    pub seq: u64,
    pub sps: Option<Bytes>,
    pub pps: Option<Bytes>,
    pub data: Bytes,
}

pub fn new_demuxer() -> (Sender<Bytes>, Receiver<AccessUnit>) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(32);
    let (out_tx, out_rx) = tokio::sync::mpsc::channel::<AccessUnit>(32);

    tokio::spawn(async move {
        let mut ctx = DumpDemuxContext::new(out_tx);
        let mut demux = demultiplex::Demultiplex::new(&mut ctx);

        while let Some(data) = rx.recv().await {
            demux.push(&mut ctx, &data);
        }
    });

    return (tx, out_rx);
}

pub struct DumpDemuxContext {
    changeset: FilterChangeset<DumpFilterSwitch>,
    sender: Sender<AccessUnit>,
}

// Implement the constructor for the custom context
impl DumpDemuxContext {
    fn new(sender: Sender<AccessUnit>) -> Self {
        DumpDemuxContext {
            changeset: FilterChangeset::default(),
            sender,
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
                self.sender.clone(),
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
                self.sender.clone(),
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
    maybe_start_new_access_unit: bool,
    seq: u64,
    pts: u64,
    dts: u64,
    pps: Option<Bytes>,
    sps: Option<Bytes>,
    sender: Sender<AccessUnit>,
}

impl PtsDumpElementaryStreamConsumer {
    fn construct(
        stream_type: StreamType,
        _pmt_sect: &psi::pmt::PmtSection,
        stream_info: &psi::pmt::StreamInfo,
        sender: Sender<AccessUnit>,
    ) -> DumpFilterSwitch {
        let filter = pes::PesPacketFilter::new(PtsDumpElementaryStreamConsumer {
            pid: stream_info.elementary_pid(),
            len: None,
            accumulated_payload: Vec::new(),
            stream_type,
            maybe_start_new_access_unit: false,
            seq: 1,
            pts: 0,
            dts: 0,
            sps: None,
            pps: None,
            sender,
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
                    }
                    Ok(pes::PtsDts::Both {
                        pts: Ok(pts),
                        dts: Ok(dts),
                    }) => {
                        self.pts = pts.value();
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
                // Parse the NALUs from the accumulated payload
                let mut new = false;
                let mut is_keyframe = false;
                for nalu in h264::iterate_annex_b(&self.accumulated_payload) {
                    if nalu.len() == 0 {
                        continue;
                    }

                    let nalu_type = nalu[0] & h264::NAL_UNIT_TYPE_MASK;

                    match nalu_type {
                        1 | 2 | 3 | 4 => {
                            self.maybe_start_new_access_unit = true;
                        }
                        5 => {
                            self.maybe_start_new_access_unit = true;
                            is_keyframe = true;
                        }

                        6 | 7 | 8 | 9 | 14 | 15 | 16 | 17 | 18 => {
                            if self.maybe_start_new_access_unit {
                                self.maybe_start_new_access_unit = false;
                            }

                            new = true
                        }
                        _ => {}
                    }

                    if new {
                        let mut lp_nalus = Vec::new();
                        match nalu_type {
                            h264::NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET => {
                                self.sps = Some(Bytes::copy_from_slice(&nalu));
                            }
                            h264::NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {
                                self.pps = Some(Bytes::copy_from_slice(&nalu));
                            }
                            _ => {
                                let mut buffer = [0u8; 4];
                                BigEndian::write_u32(&mut buffer, nalu.len() as u32);
                                lp_nalus.extend_from_slice(&buffer);
                                lp_nalus.extend_from_slice(nalu);
                            }
                        }

                        let data_slice: &[u8] = &self.accumulated_payload;
                        let au = AccessUnit {
                            avc: true,
                            key: is_keyframe,
                            seq: self.seq,
                            pts: self.pts,
                            dts: self.dts,
                            pps: self.pps.take(),
                            sps: self.sps.take(),
                            data: Bytes::from(data_slice.to_vec()),
                        };
                        let _ = self.sender.try_send(au);

                        self.seq += 1;
                    }
                }

                if new {
                    self.accumulated_payload.clear();
                }
            }
            StreamType::ADTS => {
                let data_slice: &[u8] = &self.accumulated_payload;
                let au = AccessUnit {
                    avc: false,
                    key: false,
                    seq: self.seq,
                    pts: self.pts,
                    dts: self.dts,
                    sps: None,
                    pps: None,
                    data: Bytes::from(data_slice.to_vec()),
                };
                let _ = self.sender.try_send(au);
                self.accumulated_payload.clear();
                self.seq += 1;
            }
            _ => {}
        }
    }

    fn continuity_error(&mut self, _ctx: &mut DumpDemuxContext) {}
}
