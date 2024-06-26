use au::{AuKind, AuPayload};
use bytes::{Bytes, BytesMut};
use tokio::sync::{broadcast, mpsc};
use xmpegts::{
    define::{epsi_stream_type, MPEG_FLAG_IDR_FRAME},
    ts::TsMuxer,
};

pub async fn mux_stream(
    mut rx: mpsc::Receiver<AuPayload>,
    mut tx: broadcast::Sender<Bytes>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ts_muxer = TsMuxer::new();
    let audio_pid = ts_muxer
        .add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new())
        .unwrap();
    let video_pid = ts_muxer
        .add_stream(epsi_stream_type::PSI_STREAM_H264, BytesMut::new())
        .unwrap();

    while let Some(au) = rx.recv().await {
        let pts = au.pts();
        let dts = au.dts();

        if let Some(data) = au.data {
            let mut flags: u16 = 0;

            if au.key {
                flags = MPEG_FLAG_IDR_FRAME;
            }

            match au.kind {
                AuKind::AAC => {
                    let mut payload = bytes::BytesMut::from_iter(&data);

                    if extract_aac_data(&data).is_none() {
                        if let (Some(chans), Some(sr)) = (au.audio_channels, au.audio_sample_rate) {
                            let mut adts = create_adts_header(0x66, chans, sr, data.len(), false);
                            adts.extend_from_slice(&data);
                            payload = bytes::BytesMut::from_iter(&adts);
                        }
                    }

                    match ts_muxer.write(audio_pid, dts * 90, dts * 90, flags, payload) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("error writing audio: {}", e)
                        }
                    }
                }
                AuKind::AVC => {
                    let mut payload = if has_annex_b_prefix(&data) {
                        bytes::BytesMut::from_iter(&data)
                    } else {
                        bytes::BytesMut::from_iter(lp_to_nal_start_code(&data).iter().cloned())
                    };

                    if au.key {
                        if let (Some(sps), Some(pps)) = (au.sps, au.pps) {
                            payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
                            payload.extend_from_slice(&sps);
                            payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
                            payload.extend_from_slice(&pps);
                        }
                    }

                    match ts_muxer.write(video_pid, pts * 90, dts * 90, flags, payload) {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("error writing video: {}", e)
                        }
                    }
                }
                _ => {}
            }
        }

        let data = ts_muxer.get_data();

        for d in chunk_bytes_mut(&data, 1316) {
            let b = d.freeze();
            tx.send(b);
        }
    }

    Ok(())
}

fn chunk_bytes_mut(data: &bytes::BytesMut, chunk_size: usize) -> Vec<bytes::BytesMut> {
    let mut chunks = Vec::new();
    let len = data.len();
    let mut pos = 0;

    while pos < len {
        let next_pos = std::cmp::min(pos + chunk_size, len);
        // Create a chunk by cloning the slice of data
        let chunk = bytes::BytesMut::from(&data[pos..next_pos]);
        chunks.push(chunk);
        pos = next_pos;
    }

    chunks
}

pub fn lp_to_nal_start_code(flv_data: &[u8]) -> Vec<u8> {
    let mut nal_units = Vec::new();
    let mut offset = 0;

    while offset < flv_data.len() {
        if offset + 4 > flv_data.len() {
            break;
        }

        let nalu_length =
            u32::from_be_bytes(flv_data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + nalu_length > flv_data.len() {
            break;
        }

        nal_units.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);

        nal_units.extend_from_slice(&flv_data[offset..offset + nalu_length]);
        offset += nalu_length;
    }

    nal_units
}

fn has_annex_b_prefix(data: &Bytes) -> bool {
    data.starts_with(&[0x00, 0x00, 0x01]) || data.starts_with(&[0x00, 0x00, 0x00, 0x01])
}

fn create_adts_header(
    codec_id: u8,
    channels: u8,
    sample_rate: u32,
    aac_frame_length: usize,
    has_crc: bool,
) -> Vec<u8> {
    let profile_object_type = match codec_id {
        0x66 => 1, // AAC LC
        0x67 => 2, // AAC HEV1
        0x68 => 3, // AAC HEV2
        _ => 1,    // Default to AAC LC
    };

    let sample_rate_index = sample_rate_index(sample_rate);
    let channel_config = channels.min(7);

    let header_length = if has_crc { 9 } else { 7 };
    let frame_length = aac_frame_length + header_length;

    if frame_length >= 1 << 13 {
        panic!("Frame length exceeds ADTS limit");
    }

    let mut header = Vec::with_capacity(header_length);
    let protection_absent = if has_crc { 0 } else { 1 };

    header.push(0xFF);
    header.push(0xF0 | protection_absent);

    // Fix for mixing types in bitwise operations: Ensure all operations are on the same type, and correctly cast frame_length to u8 where necessary
    let profile_and_sampling =
        ((profile_object_type - 1) << 6) | (sample_rate_index << 2) | (channel_config >> 2);
    header.push(profile_and_sampling as u8);

    // Correct handling for frame_length being a usize
    let frame_length_high = ((frame_length >> 11) & 0x03) as u8;
    let frame_length_mid = ((frame_length >> 3) & 0xFF) as u8;
    header.push((channel_config & 3) << 6 | frame_length_high);
    header.push(frame_length_mid);

    // Correct casting and operations for the last part of the frame length and buffer fullness
    let frame_length_low = ((frame_length & 0x07) << 5) | 0x1F;
    header.push(frame_length_low as u8);
    header.push(0xFC);

    if has_crc {
        // Placeholder for CRC
        header.extend_from_slice(&[0x00, 0x00]);
    }

    header
}

fn sample_rate_index(sample_rate: u32) -> u8 {
    match sample_rate {
        96000 => 0x0,
        88200 => 0x1,
        64000 => 0x2,
        48000 => 0x3,
        44100 => 0x4,
        32000 => 0x5,
        24000 => 0x6,
        22050 => 0x7,
        16000 => 0x8,
        12000 => 0x9,
        11025 => 0xA,
        8000 => 0xB,
        7350 => 0xC,
        _ => 0xF, // Invalid sample rate
    }
}

fn extract_aac_data(sound_data: &[u8]) -> Option<&[u8]> {
    if sound_data.len() < 7 {
        return None;
    }

    // Check for the ADTS sync word
    if sound_data[0] != 0xFF || (sound_data[1] & 0xF0) != 0xF0 {
        return None;
    }

    // Parse the ADTS header
    let protection_absent = (sound_data[1] & 0x01) == 0x01;
    let header_size = if protection_absent { 7 } else { 9 };

    if sound_data.len() < header_size {
        return None;
    }

    let frame_length = (((sound_data[3] as usize & 0x03) << 11)
        | ((sound_data[4] as usize) << 3)
        | ((sound_data[5] as usize) >> 5)) as usize;

    if sound_data.len() < frame_length {
        return None;
    }

    Some(&sound_data[header_size..frame_length])
}
