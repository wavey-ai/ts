use crate::AccessUnit;
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use xmpegts::{
    define::{epsi_stream_type, MPEG_FLAG_IDR_FRAME},
    ts::TsMuxer,
};

pub async fn mux_stream(
    mut rx: mpsc::Receiver<AccessUnit>,
    tx: mpsc::Sender<Bytes>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut ts_muxer = TsMuxer::new();
    let audio_pid = ts_muxer
        .add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new())
        .unwrap();
    let video_pid = ts_muxer
        .add_stream(epsi_stream_type::PSI_STREAM_H264, BytesMut::new())
        .unwrap();

    while let Some(au) = rx.recv().await {
        let mut flags: u16 = 0;

        if au.key {
            flags = MPEG_FLAG_IDR_FRAME;
        }

        match au.avc {
            // TODO: Bytes to BytesMut :(
            false => match ts_muxer.write(
                audio_pid,
                au.dts as i64 * 90,
                au.dts as i64 * 90,
                flags,
                BytesMut::from_iter(au.data),
            ) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("error writing audio: {}", e)
                }
            },
            true => {
                match ts_muxer.write(
                    video_pid,
                    au.pts as i64 * 90,
                    au.dts as i64 * 90,
                    flags,
                    BytesMut::from_iter(au.data),
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("error writing video: {}", e)
                    }
                }
            }
        }

        let data = ts_muxer.get_data();
        let _ = tx.try_send(data.freeze());
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

pub fn extract_aac_data(sound_data: &[u8]) -> Option<&[u8]> {
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
