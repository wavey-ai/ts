use crate::demuxer::AccessUnit;
use bytes::Bytes;
use mse_fmp4::io::WriteTo;
use mse_fmp4::{
    aac::{AacProfile, AdtsHeader, ChannelConfiguration, SamplingFrequency},
    avc::AvcDecoderConfigurationRecord,
    fmp4::{
        AacSampleEntry, AvcConfigurationBox, AvcSampleEntry, InitializationSegment, MediaSegment,
        MovieExtendsHeaderBox, Mpeg4EsDescriptorBox, Sample, SampleEntry, SampleFlags, TrackBox,
        TrackExtendsBox, TrackFragmentBox,
    },
};

#[derive(Debug, Clone)]
pub struct Fmp4 {
    pub init: Option<Bytes>,
    pub key: bool,
    pub data: Bytes,
    pub duration: u32,
}

pub fn ticks_to_hz(ticks: u64, clock_hz: u32) -> u32 {
    // Avoid division by zero
    if ticks == 0 {
        return 0;
    }

    // Calculate the frequency in Hz as u64
    clock_hz / ticks as u32
}

pub fn ticks_to_ms(ticks: u64) -> u64 {
    // Convert ticks to seconds as f64
    let seconds = ticks as f64 / 90000.0;

    // Convert seconds to milliseconds
    (seconds * 1000.0) as u64
}

pub fn box_fmp4(
    seq: u32,
    avcc: Option<&AvcDecoderConfigurationRecord>,
    avcs: Vec<AccessUnit>,
    aacs: Vec<AccessUnit>,
    next_dts: u64,
    width: u16,
    height: u16,
) -> Fmp4 {
    let mut segment = MediaSegment::new(seq);
    let mut fmp4_data: Vec<u8> = Vec::new();
    let mut init_data: Vec<u8> = Vec::new();
    let mut total_ticks = 0;
    let mut is_key = false;
    let mut avc_data = Vec::new();
    let mut aac_data = Vec::new();

    let mut avc_samples = Vec::new();
    let mut aac_samples = Vec::new();

    if let Some(a) = &avcc {
        let mut avc_timestamps = Vec::new();

        for a in avcs.iter() {
            if a.key {
                is_key = true;
            }

            let prev_data_len = &avc_data.len();
            avc_data.extend_from_slice(&a.data);
            let sample_size = (avc_data.len() - prev_data_len) as u32;
            let sample_composition_time_offset = (a.pts - a.dts) as i32;

            avc_timestamps.push(a.dts);

            let flags = if a.key {
                Some(SampleFlags {
                    is_leading: 0,
                    sample_depends_on: 0,
                    sample_is_depdended_on: 0,
                    sample_has_redundancy: 0,
                    sample_padding_value: 0,
                    sample_is_non_sync_sample: false,
                    sample_degradation_priority: 0,
                })
            } else {
                Some(SampleFlags {
                    is_leading: 0,
                    sample_depends_on: 1,
                    sample_is_depdended_on: 0,
                    sample_has_redundancy: 0,
                    sample_padding_value: 0,
                    sample_is_non_sync_sample: true,
                    sample_degradation_priority: 0,
                })
            };

            avc_samples.push(Sample {
                duration: None,
                size: Some(sample_size),
                flags,
                composition_time_offset: Some(sample_composition_time_offset),
            });
        }

        avc_timestamps.push(next_dts);
        for i in 0..avc_samples.len() {
            let duration = avc_timestamps[i + 1] - avc_timestamps[i];
            total_ticks += duration;
            avc_samples[i].duration = Some(duration as u32);
        }

        let mut traf = TrackFragmentBox::new(true);
        traf.trun_box.first_sample_flags = None;
        traf.tfhd_box.default_sample_flags = None;
        traf.trun_box.data_offset = Some(0);
        traf.trun_box.samples = avc_samples;
        traf.tfdt_box.base_media_decode_time = avcs[0].dts as u32;
        segment.moof_box.traf_boxes.push(traf);
    }

    let mut frame_duration = 0;
    let mut sampling_frequency =
        SamplingFrequency::from_frequency(0).unwrap_or_else(|_| SamplingFrequency::Hz48000);
    let mut channel_configuration =
        ChannelConfiguration::from_u8(0).unwrap_or_else(|_| ChannelConfiguration::TwoChannels);
    let mut profile = AacProfile::Main;

    for a in aacs.iter() {
        let mut bytes = &a.data[..];
        while !bytes.is_empty() {
            if let Ok(header) = AdtsHeader::read_from(&mut bytes) {
                let sample_size = header.raw_data_blocks_len();
                sampling_frequency = header.sampling_frequency;
                channel_configuration = header.channel_configuration;
                profile = header.profile;
                frame_duration =
                    ((1024 as f32 / sampling_frequency.as_u32() as f32) * 1000.0).round() as u64;
                aac_samples.push(Sample {
                    duration: Some(ticks_to_hz(frame_duration, sampling_frequency.as_u32())),
                    size: Some(u32::from(sample_size)),
                    flags: None,
                    composition_time_offset: None,
                });
                aac_data.extend_from_slice(&bytes[..sample_size as usize]);
                bytes = &bytes[sample_size as usize..];
            } else {
                dbg!("error");
            }
        }
    }

    if avc_data.len() > 0 {
        segment.add_track_data(0, &avc_data);
    }

    let mut audio_track = TrackBox::new(false);
    if !aacs.is_empty() {
        let mut traf = TrackFragmentBox::new(false);
        traf.tfhd_box.default_sample_duration = None;
        traf.trun_box.data_offset = Some(0);
        traf.trun_box.samples = aac_samples;
        traf.tfdt_box.base_media_decode_time =
            ticks_to_hz(aacs[0].pts, sampling_frequency.as_u32());
        segment.moof_box.traf_boxes.push(traf);

        segment.add_track_data(1, &aac_data);

        audio_track.tkhd_box.duration = 0;
        audio_track.mdia_box.mdhd_box.timescale = sampling_frequency.as_u32();
        audio_track.mdia_box.mdhd_box.duration = 0;

        let aac_sample_entry = AacSampleEntry {
            esds_box: Mpeg4EsDescriptorBox {
                profile,
                frequency: sampling_frequency,
                channel_configuration,
            },
        };
        audio_track
            .mdia_box
            .minf_box
            .stbl_box
            .stsd_box
            .sample_entries
            .push(SampleEntry::Aac(aac_sample_entry));
    }
    segment.update_offsets();
    segment.write_to(&mut fmp4_data).unwrap();

    // create init.mp4
    let mut segment = InitializationSegment::default();
    segment.moov_box.mvhd_box.timescale = 1000;
    segment.moov_box.mvhd_box.duration = 0;
    segment.moov_box.mvex_box.mehd_box = Some(MovieExtendsHeaderBox {
        fragment_duration: 0,
    });

    if let Some(c) = avcc {
        let mut track = TrackBox::new(true);
        track.tkhd_box.width = (width as u32) << 16;
        track.tkhd_box.height = (height as u32) << 16;
        track.tkhd_box.duration = 0;
        //track.edts_box.elst_box.media_time = start_time;
        track.mdia_box.mdhd_box.timescale = 90000;
        track.mdia_box.mdhd_box.duration = 0;

        let avc_sample_entry = AvcSampleEntry {
            width,
            height,
            avcc_box: AvcConfigurationBox {
                configuration: c.clone(),
            },
        };
        track
            .mdia_box
            .minf_box
            .stbl_box
            .stsd_box
            .sample_entries
            .push(SampleEntry::Avc(avc_sample_entry));
        segment.moov_box.trak_boxes.push(track);
        segment
            .moov_box
            .mvex_box
            .trex_boxes
            .push(TrackExtendsBox::new(true));
    }

    if aacs.len() > 0 {
        // audio track
        segment.moov_box.trak_boxes.push(audio_track);
        segment
            .moov_box
            .mvex_box
            .trex_boxes
            .push(TrackExtendsBox::new(false));
    }

    let _ = segment.write_to(&mut init_data);

    let mut init: Option<Bytes> = None;
    if !init_data.is_empty() {
        init = Some(Bytes::from(init_data))
    }

    Fmp4 {
        init,
        duration: ticks_to_ms(total_ticks) as u32,
        key: is_key,
        data: Bytes::from(fmp4_data),
    }
}
