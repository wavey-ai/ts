use bytes::Bytes;

#[macro_use]
extern crate mpeg2ts_reader;

pub mod ip;

// SRT listener
// pub mod listener;
// mpeg ts PES stream demuxing
// pub mod demuxer;
// m3u8 playlist ringbuffers
//pub mod accumulator;
pub mod demuxer;
pub mod muxer;
pub mod playlist;
pub mod streamkey;

#[derive(Debug, Clone)]
pub struct AccessUnit {
    pub key: bool,
    pub pts: u64,
    pub dts: u64,
    pub data: Bytes,
    pub avc: bool,
}
