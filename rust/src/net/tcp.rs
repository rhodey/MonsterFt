//! Four-byte big-endian framing over ordinary plaintext TCP.
use bytes::Bytes;
use futures_util::StreamExt;
use std::{
    io::{self, IoSlice},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub const MAX_FRAME_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_QUEUED_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct FrameOptions {
    pub max_frame_bytes: usize,
    pub max_queued_bytes: usize,
    pub max_queued_messages: usize,
}
impl Default for FrameOptions {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_queued_bytes: MAX_QUEUED_BYTES,
            max_queued_messages: 64,
        }
    }
}
impl FrameOptions {
    pub fn validate(&self) -> io::Result<()> {
        if self.max_frame_bytes > u32::MAX as usize
            || self.max_queued_bytes < 4
            || self.max_queued_messages == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP frame limits are illegal",
            ));
        }
        Ok(())
    }
}

/// Stateful framing keeps partial input intact when a receive future is cancelled.
pub struct FrameReader<R>(FramedRead<R, LengthDelimitedCodec>);
impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(reader: R, max_frame_bytes: usize) -> io::Result<Self> {
        if max_frame_bytes > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP maximum frame length is illegal",
            ));
        }
        let codec = LengthDelimitedCodec::builder()
            .big_endian()
            .length_field_length(4)
            .max_frame_length(max_frame_bytes)
            .new_codec();
        Ok(Self(FramedRead::new(reader, codec)))
    }
    pub async fn recv(&mut self) -> io::Result<Option<Bytes>> {
        self.0
            .next()
            .await
            .transpose()
            .map(|value| value.map(|bytes| bytes.freeze()))
    }
}

/// Send one frame without concatenating its payload into a second allocation.
/// If this future is cancelled, discard the connection; its frame may be partial.
pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    frame: &[u8],
    max_frame_bytes: usize,
) -> io::Result<()> {
    if frame.len() > max_frame_bytes || frame.len() > u32::MAX as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TCP frame is too large",
        ));
    }
    let header = (frame.len() as u32).to_be_bytes();
    let mut header_offset = 0;
    let mut payload_offset = 0;
    while header_offset < header.len() || payload_offset < frame.len() {
        let count = if header_offset < header.len() {
            writer
                .write_vectored(&[
                    IoSlice::new(&header[header_offset..]),
                    IoSlice::new(&frame[payload_offset..]),
                ])
                .await?
        } else {
            writer.write(&frame[payload_offset..]).await?
        };
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let header_count = count.min(header.len() - header_offset);
        header_offset += header_count;
        payload_offset += count - header_count;
    }
    Ok(())
}

pub async fn connect(address: impl ToSocketAddrs, timeout: Duration) -> io::Result<TcpStream> {
    let stream = tokio::time::timeout(timeout, TcpStream::connect(address))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP connect timed out"))??;
    stream.set_nodelay(true)?;
    Ok(stream)
}
