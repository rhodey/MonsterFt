use crate::error::{Code, Error, Result};
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, Cursor, Write};

pub const HASH_BYTES: usize = 16;
pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_DEPTH: usize = 128;

pub fn checksum(payload: &[u8]) -> [u8; HASH_BYTES] {
    xxhash_rust::xxh3::xxh3_128(payload).to_be_bytes()
}

// Enforce the frame allocation bound while serializing, including opaque app
// results. Checking the final size alone would permit an oversized allocation.
struct Encoder {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Encoder {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        if value.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "message exceeds frame limit",
            ));
        }
        self.bytes.extend_from_slice(value);
        Ok(value.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_payload<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let mut writer = Encoder {
        bytes: Vec::new(),
        limit: MAX_FRAME_BYTES - HASH_BYTES,
    };
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut writer).with_struct_map())
        .map_err(|error| Error::new(Code::FrameCorrupt, format!("message encode {error}")))?;
    Ok(writer.bytes)
}

pub fn encode_frame<T: Serialize + ?Sized>(value: &T) -> Result<Bytes> {
    let mut writer = Encoder {
        bytes: vec![0; HASH_BYTES],
        limit: MAX_FRAME_BYTES,
    };
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut writer).with_struct_map())
        .map_err(|error| Error::new(Code::FrameCorrupt, format!("message encode {error}")))?;
    let hash = checksum(&writer.bytes[HASH_BYTES..]);
    writer.bytes[..HASH_BYTES].copy_from_slice(&hash);
    Ok(writer.bytes.into())
}

/// Preserve the independently computed B/C checksum when Gate forwards A's
/// encoded output; this function deliberately does not replace it with a hash.
pub fn frame_payload(payload: &[u8], hash: &[u8; HASH_BYTES]) -> Result<Bytes> {
    if payload.is_empty() || payload.len() > MAX_FRAME_BYTES - HASH_BYTES {
        return Err(Error::new(Code::FrameCorrupt, "frame length is illegal"));
    }
    let mut frame = Vec::with_capacity(HASH_BYTES + payload.len());
    frame.extend_from_slice(hash);
    frame.extend_from_slice(payload);
    Ok(frame.into())
}

pub fn verify_frame(frame: &[u8]) -> Result<&[u8]> {
    if frame.len() <= HASH_BYTES || frame.len() > MAX_FRAME_BYTES {
        return Err(Error::new(Code::FrameCorrupt, "frame length is illegal"));
    }
    let payload = &frame[HASH_BYTES..];
    if checksum(payload) != frame[..HASH_BYTES] {
        return Err(Error::new(Code::FrameCorrupt, "frame checksum differs"));
    }
    Ok(payload)
}

pub fn decode_payload<T: DeserializeOwned>(payload: &[u8]) -> Result<T> {
    if payload.is_empty() || payload.len() > MAX_FRAME_BYTES - HASH_BYTES {
        return Err(Error::new(Code::FrameCorrupt, "payload length is illegal"));
    }
    let mut decoder = rmp_serde::Deserializer::new(Cursor::new(payload));
    decoder.set_max_depth(MAX_DEPTH);
    let value = T::deserialize(&mut decoder)
        .map_err(|error| Error::new(Code::FrameCorrupt, format!("message decode {error}")))?;
    if decoder.position() != payload.len() as u64 {
        return Err(Error::new(
            Code::FrameCorrupt,
            "message decode trailing bytes",
        ));
    }
    Ok(value)
}

pub fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T> {
    decode_payload(verify_frame(frame)?)
}
