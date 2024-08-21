use bytes::Bytes;

pub fn extract_aac_data(sound_data: &Bytes) -> Option<Bytes> {
    if sound_data.len() < 7 {
        return None;
    }

    // Check for the ADTS sync word
    if sound_data[0] != 0xFF || (sound_data[1] & 0xF0) != 0xF0 {
        return None;
    }

    // Parse the ADTS header
    let protection_absent: bool = (sound_data[1] & 0x01) == 0x01;
    let header_size: usize = if protection_absent { 7 } else { 9 };

    if sound_data.len() < header_size {
        return None;
    }

    let frame_length: usize = (((sound_data[3] as usize & 0x03) << 11)
        | ((sound_data[4] as usize) << 3)
        | ((sound_data[5] as usize) >> 5)) as usize;

    if sound_data.len() < frame_length {
        return None;
    }

    Some(sound_data.slice(header_size..frame_length))
}
