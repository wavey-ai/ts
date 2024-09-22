use crate::AccessUnit;
use std::sync::Arc;

use tracing::{debug, error};

pub fn ticks_to_ms(ticks: u64) -> u64 {
    // Convert ticks to seconds as f64
    let seconds = ticks as f64 / 90000.0;

    // Convert seconds to milliseconds
    (seconds * 1000.0) as u64
}

pub struct AccessUnitAccumulator {
    min_part_ms: u32,
    stream_id: u64,
    playlists: Arc<Playlists>,
    h264_buf: Vec<AccessUnit>,
    adts_buf: Vec<AccessUnit>,
    avc_timestamps: Vec<u64>,
    seg_seq: u32,
}

impl AccessUnitAccumulator {
    pub fn new(stream_id: u64, playlists: Arc<Playlists>, min_part_ms: u32) -> Self {
        AccessUnitAccumulator {
            min_part_ms,
            stream_id,
            playlists,
            h264_buf: Vec::new(),
            adts_buf: Vec::new(),
            avc_timestamps: Vec::new(),
            seg_seq: 1,
        }
    }

    pub fn process_access_unit(&mut self, au: AccessUnit) -> bool {
        if au.avc {
            self.h264_buf.push(au.clone());
            self.avc_timestamps.push(au.dts);
        } else {
            self.adts_buf.push(au);
        }

        if self.avc_timestamps.len() > 1 {
            let elapsed_ms = ticks_to_ms(
                self.avc_timestamps[self.avc_timestamps.len() - 1] - self.avc_timestamps[0],
            ) as u32;

            if elapsed_ms >= self.min_part_ms || au.key {
                let fmp4 = box_fmp4(
                    self.seg_seq,
                    self.avcc.as_ref(),
                    self.h264_buf.clone(),
                    self.adts_buf.clone(),
                    au.dts,
                    self.width,
                    self.height,
                );

                if !self.playlists.add(self.stream_id, fmp4) {
                    return false;
                }
                self.seg_seq += 1;
                self.h264_buf.clear();
                self.adts_buf.clear();
                self.avc_timestamps.clear();
            }
        }

        true
    }
}
