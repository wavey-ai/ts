use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::stream::{self, StreamExt};
use mpeg2::{pes, ts};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::broadcast;
use tokio_stream::Stream;

#[derive(Debug)]
pub enum DemuxOutput {
    AVCConfig {
        picture_parameter_set: Bytes,
        sequence_parameter_set: Bytes,
    },
    AVCAccessUnit {
        /// Each access unit is given a monotonically increasing sequence number. These sequence
        /// numbers are always unique and always increase by one for each consecutive frame.
        sequence_number: u64,
        presentation_time: Duration,
        receive_time: DateTime<Utc>,
        decode_time: Duration,
        is_keyframe: bool,
        lp_nalus: Bytes,
    },
    ADTSFrame {
        presentation_time: Duration,
        receive_time: DateTime<Utc>,
        data: Bytes,
    },
    Discontinuity,
}

impl DemuxOutput {
    /// The presentation time of the output, if it has one. Roll-overs in the source stream are
    /// removed so the output presentation times are only ever out of order if the stream contains
    /// B-frames.
    pub fn presentation_time(&self) -> Option<Duration> {
        match self {
            Self::AVCConfig { .. } => None,
            Self::AVCAccessUnit {
                presentation_time, ..
            } => Some(*presentation_time),
            Self::ADTSFrame {
                presentation_time, ..
            } => Some(*presentation_time),
            Self::Discontinuity => None,
        }
    }
}

#[derive(Clone)]
enum PESStream {
    ADTSAudio {
        pes: pes::Stream,
        rollover_detector: RolloverDetector,
    },
    AVCVideo {
        pes: pes::Stream,
        next_sequence_number: u64,
        maybe_start_new_access_unit: bool,
        rollover_detector: RolloverDetector,
        sequence_parameter_set: Bytes,
        picture_parameter_set: Bytes,
        lp_nalus: Vec<u8>,
        is_keyframe: bool,
    },
    Other(u8),
}

/// RolloverDetector detects backward jumps in timestamps, and outputs corrected timestamps.
#[derive(Clone)]
struct RolloverDetector {
    last_dts: u64,
    offset: u64,
}

impl RolloverDetector {
    fn new() -> Self {
        Self {
            last_dts: 0,
            offset: 0,
        }
    }

    fn process(&mut self, pts: u64, dts: u64) -> (u64, u64) {
        if dts < self.last_dts {
            // rollover
            self.offset += self.last_dts - dts;
        }
        self.last_dts = dts;
        (self.offset + pts, self.offset + dts)
    }
}

impl PESStream {
    fn pes(&mut self) -> Option<&mut pes::Stream> {
        match self {
            Self::ADTSAudio { pes, .. } => Some(pes),
            Self::AVCVideo { pes, .. } => Some(pes),
            Self::Other(_) => None,
        }
    }

    pub fn write(&mut self, packet: &ts::Packet, out: &mut Vec<DemuxOutput>) -> Result<()> {
        if let Some(pes) = self.pes() {
            for packet in pes.write(packet).map_err(|e| anyhow!(e))? {
                match self {
                    Self::ADTSAudio {
                        rollover_detector, ..
                    } => {
                        if let Some(pts) =
                            packet.header.optional_header.as_ref().and_then(|h| h.pts)
                        {
                            let (pts, _) = rollover_detector.process(pts, pts);
                            let mut presentation_time =
                                Duration::nanoseconds((pts as f64 / 0.00009) as _);
                            let data_slice: &[u8] = &packet.data;
                            let mut data: Bytes = Bytes::from(data_slice.to_vec());
                            while data.len() >= 7 {
                                if data[0] != 0xff || (data[1] & 0xf0) != 0xf0 {
                                    bail!("invalid adts syncword")
                                }
                                let len = (((data[3] & 3) as usize) << 11)
                                    | ((data[4] as usize) << 3)
                                    | ((data[5] as usize) >> 5);
                                if len < 7 || len > data.len() {
                                    bail!("invalid adts frame length")
                                }
                                let sample_rate = match (data[2] >> 2) & 0x0f {
                                    0 => 96_000,
                                    1 => 88_200,
                                    2 => 64_000,
                                    3 => 48_000,
                                    4 => 44_100,
                                    5 => 32_000,
                                    6 => 24_000,
                                    7 => 22_050,
                                    8 => 16_000,
                                    9 => 12_000,
                                    10 => 11_025,
                                    11 => 8_000,
                                    12 => 7_350,
                                    _ => 0,
                                };

                                let frame_data = data.split_to(len);
                                out.push(DemuxOutput::ADTSFrame {
                                    presentation_time,
                                    receive_time: Utc::now(),
                                    data: frame_data,
                                });
                                presentation_time = presentation_time
                                    + Duration::nanoseconds(1024 * 1000000000 / sample_rate);
                            }
                        }
                    }
                    Self::AVCVideo {
                        next_sequence_number,
                        maybe_start_new_access_unit,
                        is_keyframe,
                        rollover_detector,
                        sequence_parameter_set,
                        picture_parameter_set,
                        lp_nalus,
                        ..
                    } => {
                        for nalu in h264::iterate_annex_b(&packet.data) {
                            if nalu.len() == 0 {
                                continue;
                            }

                            let nalu_type = nalu[0] & h264::NAL_UNIT_TYPE_MASK;

                            // ITU-T H.264, 04/2017, 7.4.1.2.3
                            // TODO: implement 7.4.1.2.4?
                            match nalu_type {
                                1 | 2 | 3 | 4 => {
                                    *maybe_start_new_access_unit = true;
                                }
                                5 => {
                                    *maybe_start_new_access_unit = true;
                                    *is_keyframe = true;
                                }
                                6 | 7 | 8 | 9 | 14 | 15 | 16 | 17 | 18 => {
                                    if *maybe_start_new_access_unit {
                                        *maybe_start_new_access_unit = false;
                                        if lp_nalus.len() > 0 {
                                            if let Some(pts) = packet
                                                .header
                                                .optional_header
                                                .as_ref()
                                                .and_then(|h| h.pts)
                                            {
                                                let dts = packet
                                                    .header
                                                    .optional_header
                                                    .as_ref()
                                                    .and_then(|h| h.dts)
                                                    .unwrap_or(pts);
                                                let (pts, dts) =
                                                    rollover_detector.process(pts, dts);
                                                out.push(DemuxOutput::AVCAccessUnit {
                                                    sequence_number: *next_sequence_number,
                                                    presentation_time: Duration::nanoseconds(
                                                        (pts as f64 / 0.00009) as _,
                                                    ),
                                                    receive_time: Utc::now(),
                                                    decode_time: Duration::nanoseconds(
                                                        (dts as f64 / 0.00009) as _,
                                                    ),
                                                    lp_nalus: std::mem::take(lp_nalus).into(),
                                                    is_keyframe: *is_keyframe,
                                                });
                                                *next_sequence_number += 1;
                                            }
                                            lp_nalus.clear();
                                            *is_keyframe = false;
                                        }
                                    }
                                }
                                _ => {}
                            }

                            match nalu_type {
                                h264::NAL_UNIT_TYPE_SEQUENCE_PARAMETER_SET => {
                                    *sequence_parameter_set = Bytes::copy_from_slice(&nalu);
                                }
                                h264::NAL_UNIT_TYPE_PICTURE_PARAMETER_SET => {
                                    *picture_parameter_set = Bytes::copy_from_slice(&nalu);
                                }
                                _ => {
                                    let mut buffer = [0u8; 4];
                                    BigEndian::write_u32(&mut buffer, nalu.len() as u32);
                                    lp_nalus.extend_from_slice(&buffer);
                                    lp_nalus.extend_from_slice(nalu);
                                }
                            }

                            if !sequence_parameter_set.is_empty()
                                && !picture_parameter_set.is_empty()
                            {
                                out.push(DemuxOutput::AVCConfig {
                                    sequence_parameter_set: std::mem::take(sequence_parameter_set),
                                    picture_parameter_set: std::mem::take(picture_parameter_set),
                                })
                            }
                        }
                    }
                    Self::Other(_) => {}
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
enum PIDState {
    Unused,
    PAT,
    PMT,
    PES { stream: PESStream },
}

struct State<R> {
    r: R,
    is_done: bool,
    pids: Vec<PIDState>,
    read_buf: [u8; mpeg2::ts::PACKET_LENGTH * 20],
    tx: Option<broadcast::Sender<Bytes>>,
}

impl<R: AsyncRead + Unpin> State<R> {
    async fn demux(&mut self) -> Result<Vec<DemuxOutput>> {
        let bytes_read = self.r.read(&mut self.read_buf).await?;
        if bytes_read == 0 {
            self.is_done = true;
        }

        let buf = &self.read_buf[..bytes_read];
        if let Some(tx) = &self.tx {
            tx.send(Bytes::from(buf.to_vec()));
        }
        if buf.len() % ts::PACKET_LENGTH != 0 {
            return Err(anyhow!("read length not divisible by packet length"));
        }

        let mut out = Vec::new();
        for buf in buf.chunks(ts::PACKET_LENGTH) {
            let p = ts::Packet::decode(&buf).map_err(|e| anyhow!(e))?;
            if p.adaptation_field
                .as_ref()
                .and_then(|af| af.discontinuity_indicator)
                == Some(true)
            {
                out.push(DemuxOutput::Discontinuity);
            }
            match &mut self.pids[p.packet_id as usize] {
                PIDState::PAT => {
                    let table_sections = p.decode_table_sections().map_err(|e| anyhow!(e))?;
                    let syntax_section = table_sections[0]
                        .decode_syntax_section()
                        .map_err(|e| anyhow!(e))?;
                    let pat = ts::PATData::decode(syntax_section.data).map_err(|e| anyhow!(e))?;
                    for entry in pat.entries {
                        self.pids[entry.program_map_pid as usize] = PIDState::PMT;
                    }
                }
                PIDState::PMT => {
                    let table_sections = p.decode_table_sections().map_err(|e| anyhow!(e))?;
                    let syntax_section = table_sections[0]
                        .decode_syntax_section()
                        .map_err(|e| anyhow!(e))?;
                    let pmt = ts::PMTData::decode(syntax_section.data).map_err(|e| anyhow!(e))?;
                    for pes in pmt.elementary_stream_info {
                        match &mut self.pids[pes.elementary_pid as usize] {
                            PIDState::PES { .. } => {}
                            state @ _ => {
                                *state = PIDState::PES {
                                    stream: match pes.stream_type {
                                        0x0f => PESStream::ADTSAudio {
                                            pes: pes::Stream::new(),
                                            rollover_detector: RolloverDetector::new(),
                                        },
                                        0x1b => PESStream::AVCVideo {
                                            pes: pes::Stream::new(),
                                            next_sequence_number: 0,
                                            maybe_start_new_access_unit: true,
                                            rollover_detector: RolloverDetector::new(),
                                            sequence_parameter_set: Bytes::new(),
                                            picture_parameter_set: Bytes::new(),
                                            lp_nalus: vec![],
                                            is_keyframe: false,
                                        },
                                        t @ _ => PESStream::Other(t),
                                    },
                                }
                            }
                        };
                    }
                }
                PIDState::PES { ref mut stream } => stream.write(&p, &mut out)?,
                PIDState::Unused => {}
            }
        }
        Ok(out)
    }
}

pub fn demux<R: AsyncRead + Unpin>(
    r: R,
    tx: Option<broadcast::Sender<Bytes>>,
) -> impl Stream<Item = Result<DemuxOutput>> + Unpin {
    let state = State {
        r,
        is_done: false,
        pids: {
            let mut v = vec![PIDState::Unused; 0x10000];
            v[ts::PID_PAT as usize] = PIDState::PAT;
            v
        },
        read_buf: [0u8; mpeg2::ts::PACKET_LENGTH * 20],
        tx,
    };
    Box::pin(
        stream::unfold(state, |mut state| async {
            if state.is_done {
                None
            } else {
                let out: Vec<Result<DemuxOutput>> = match state.demux().await {
                    Ok(v) => v.into_iter().map(|v| Ok(v)).collect(),
                    Err(e) => {
                        state.is_done = true;
                        vec![Err(e)]
                    }
                };
                Some((stream::iter(out.into_iter()), state))
            }
        })
        .flatten(),
    )
}
