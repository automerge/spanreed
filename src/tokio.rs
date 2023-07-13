use std::fmt::Debug;

use bytes::{Buf, BytesMut};
use futures::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder};

use crate::{repo::RepoHandle, ConnDirection};
use crate::{Message, NetworkError};

mod fs_storage;
pub use fs_storage::FsStorage;

impl RepoHandle {
    /// Connect a tokio io object
    pub async fn connect_tokio_io<Io, Source>(
        &self,
        _source: Source,
        io: Io,
        direction: ConnDirection,
    ) -> Result<(), CodecError>
    where
        Io: AsyncRead + AsyncWrite + Send + 'static,
        Source: Debug,
    {
        let codec = Codec::new();
        let framed = tokio_util::codec::Framed::new(io, codec);

        self.connect_stream(framed, direction).await?;

        Ok(())
    }

    #[cfg(feature = "tokio-tungstenite")]
    pub async fn connect_tungstenite<S>(
        &self,
        stream: S,
        direction: ConnDirection,
    ) -> Result<(), CodecError>
    where
        S: Sink<tungstenite::Message, Error = tungstenite::Error>
            + Stream<Item = Result<tungstenite::Message, tungstenite::Error>>
            + Send
            + 'static,
    {
        let msg_stream = stream
            .map::<Result<Message, NetworkError>, _>(|msg| {
                let msg = msg.map_err(|_| NetworkError::Error)?;
                match msg {
                    tungstenite::Message::Binary(data) => Message::decode(&data).map_err(|e| {
                        tracing::error!(err=?e, "error decoding message");
                        NetworkError::Error
                    }),
                    _ => Err(NetworkError::Error),
                }
            })
            .sink_map_err(|_| NetworkError::Error)
            .with(|msg: Message| {
                futures::future::ready(Ok::<_, NetworkError>(tungstenite::Message::Binary(
                    msg.encode(),
                )))
            });

        self.connect_stream(msg_stream, direction).await?;

        Ok(())
    }
}

/// A simple length prefixed codec over `crate::Message` for use over stream oriented transports
pub(crate) struct Codec;

impl Codec {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Decode(#[from] crate::message::DecodeError),
    #[error(transparent)]
    Network(#[from] NetworkError),
}

impl From<CodecError> for NetworkError {
    fn from(_err: CodecError) -> Self {
        NetworkError::Error
    }
}

impl Decoder for Codec {
    type Item = Message;

    type Error = CodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }
        // Read the length prefix
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Check if we have enough data for this message
        if src.len() < len + 4 {
            src.reserve(len + 4 - src.len());
            return Ok(None);
        }

        // Parse the message
        let data = src[4..len + 4].to_vec();
        src.advance(len + 4);
        Message::decode(&data).map(Some).map_err(Into::into)
    }
}

impl Encoder<Message> for Codec {
    type Error = CodecError;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = msg.encode();
        let len = encoded.len() as u32;
        let len_slice = len.to_be_bytes();
        dst.reserve(4 + len as usize);
        dst.extend_from_slice(&len_slice);
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}
