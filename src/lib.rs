mod demuxer;
mod muxer;

use crate::demuxer::{demux, DemuxOutput};
use au::{
    AuKind::{AAC, AVC},
    AuPayload,
};
use bytes::Bytes;
use futures::StreamExt;
use h264::{Bitstream, Decode, NALUnit, SequenceParameterSet};
use std::cell::OnceCell;
use tokio::sync::broadcast::Sender;

pub struct Demuxer {
    tx: Option<Sender<Bytes>>,
}

impl Demuxer {
    pub fn new(tx: Option<Sender<Bytes>>) -> Self {
        Self { tx }
    }

    pub async fn start(&self, srt_stream: srt::AsyncStream, tx: Sender<AuPayload>) {
        let mut demuxed_stream = demux(srt_stream, self.tx.clone());

        let sps = OnceCell::new();
        let pps = OnceCell::new();
        let mut width = 0;
        let mut height = 0;
        let mut fps = 0.0;

        let tx_clone = tx.clone();
        while let Some(result) = demuxed_stream.next().await {
            match result {
                Ok(demuxed) => match demuxed {
                    DemuxOutput::AVCConfig {
                        picture_parameter_set,
                        sequence_parameter_set,
                    } => {
                        if sps.get().is_none() {
                            let bs = Bitstream::new(sequence_parameter_set.iter().copied());
                            match NALUnit::decode(bs) {
                                Ok(mut nalu) => {
                                    let mut rbsp = Bitstream::new(&mut nalu.rbsp_byte);
                                    if let Ok(sps) = SequenceParameterSet::decode(&mut rbsp) {
                                        width = (sps.pic_width_in_samples()
                                            - (sps.frame_crop_right_offset.0 * 2)
                                            - (sps.frame_crop_left_offset.0 * 2))
                                            as u16;
                                        height = ((sps.frame_height_in_mbs() * 16)
                                            - (sps.frame_crop_bottom_offset.0 * 2)
                                            - (sps.frame_crop_top_offset.0 * 2))
                                            as u16;
                                        if sps.vui_parameters_present_flag.0 != 0
                                            && sps.vui_parameters.timing_info_present_flag.0 != 0
                                        {
                                            fps = sps.vui_parameters.time_scale.0 as f64
                                                / sps.vui_parameters.num_units_in_tick.0 as f64;
                                        }
                                    };
                                }
                                Err(_) => {}
                            }
                            sps.set(sequence_parameter_set).unwrap_or_else(|_| {});
                            pps.set(picture_parameter_set).unwrap_or_else(|_| {});
                        }
                    }
                    DemuxOutput::ADTSFrame {
                        presentation_time,
                        receive_time,
                        data,
                    } => {
                        match tx_clone.send(AuPayload {
                            kind: AAC,
                            data: Some(data),
                            sps: None,
                            pps: None,
                            dts: Some(presentation_time),
                            audio_codec_id: None,
                            audio_channels: None,
                            audio_sample_rate: None,
                            audio_bitrate_kbps: None,
                            pts: Some(presentation_time),
                            path: None,
                            key: false,
                            seq: 0,
                            track_id: 0,
                            width: None,
                            height: None,
                            fps: None,
                        }) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("error sending: {}", e);
                            }
                        }
                    }
                    DemuxOutput::Discontinuity {} => {}
                    DemuxOutput::AVCAccessUnit {
                        sequence_number,
                        receive_time,
                        presentation_time,
                        decode_time,
                        is_keyframe,
                        lp_nalus,
                    } => {
                        match tx_clone.send(AuPayload {
                            kind: AVC,
                            data: Some(lp_nalus),
                            sps: sps.get().cloned(),
                            pps: pps.get().cloned(),
                            dts: Some(decode_time),
                            pts: Some(presentation_time),
                            path: None,
                            key: is_keyframe,
                            audio_codec_id: None,
                            audio_channels: None,
                            audio_sample_rate: None,
                            audio_bitrate_kbps: None,
                            seq: sequence_number,
                            track_id: 0,
                            width: Some(width),
                            height: Some(height),
                            fps: Some(fps),
                        }) {
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("error sending: {}", e);
                            }
                        }
                    }
                },
                Err(e) => {}
            }
        }
    }
}
