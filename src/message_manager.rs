use std::collections::BTreeMap;
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::ops::{Deref, DerefMut};
use std::task::{Context, Poll, Waker};

use bytes::{Buf as _, BufMut as _, BytesMut};
use futures_util::{SinkExt as _, StreamExt as _};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio_util::codec;

use crate::{
    cast::{
        cast_channel,
        cast_channel::cast_message::{PayloadType, ProtocolVersion},
    },
    errors::Error,
};

#[derive(Debug, Serialize, Deserialize)]
struct Request<T> {
    #[serde(rename = "requestId")]
    request_id: u32,
    #[serde(flatten)]
    inner: T,
}

struct Lock<T>(
    #[cfg(feature = "thread_safe")] std::sync::Mutex<T>,
    #[cfg(not(feature = "thread_safe"))] std::cell::RefCell<T>,
);

struct LockGuard<'a, T>(
    #[cfg(feature = "thread_safe")] std::sync::MutexGuard<'a, T>,
    #[cfg(not(feature = "thread_safe"))] std::cell::Ref<'a, T>,
);

impl<'a, T> Deref for LockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

struct LockGuardMut<'a, T>(
    #[cfg(feature = "thread_safe")] std::sync::MutexGuard<'a, T>,
    #[cfg(not(feature = "thread_safe"))] std::cell::RefMut<'a, T>,
);

impl<'a, T> Deref for LockGuardMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T> DerefMut for LockGuardMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

impl<T> Lock<T> {
    fn new(data: T) -> Self {
        Lock({
            #[cfg(feature = "thread_safe")]
            let lock = std::sync::Mutex::new(data);
            #[cfg(not(feature = "thread_safe"))]
            let lock = std::cell::RefCell::new(data);
            lock
        })
    }

    fn borrow_mut(&self) -> LockGuardMut<'_, T> {
        LockGuardMut({
            #[cfg(feature = "thread_safe")]
            let guard = self.0.lock().unwrap();
            #[cfg(not(feature = "thread_safe"))]
            let guard = self.0.borrow_mut();
            guard
        })
    }
}

/// A message that will be serialized as JSON
#[derive(Debug, Clone)]
pub struct JsonMessage<'a, T> {
    /// A namespace is a labeled protocol. That is, messages that are exchanged throughout the
    /// Cast ecosystem utilize namespaces to identify the protocol of the message being sent.
    pub namespace: &'a str,
    /// Unique identifier of the `sender` application.
    pub source: &'a str,
    /// Unique identifier of the `receiver` application.
    pub destination: &'a str,
    /// Payload data attached to the message (either string or binary).
    pub payload: T,
}

/// Type of the payload that `CastMessage` can have.
#[derive(Debug, Clone)]
pub enum CastMessagePayload {
    /// Payload represented by UTF-8 string (usually it's just a JSON string).
    String(String),
    /// Payload represented by binary data.
    Binary(Vec<u8>),
}

/// Base structure that represents messages that are exchanged between Receiver and Sender.
#[derive(Debug, Clone)]
pub struct CastMessage {
    /// A namespace is a labeled protocol. That is, messages that are exchanged throughout the
    /// Cast ecosystem utilize namespaces to identify the protocol of the message being sent.
    pub namespace: String,
    /// Unique identifier of the `sender` application.
    pub source: String,
    /// Unique identifier of the `receiver` application.
    pub destination: String,
    /// Payload data attached to the message (either string or binary).
    pub payload: CastMessagePayload,
}

#[derive(Default)]
struct MessageCodec;

// Limit message size to 8MiB
const MAX: u32 = 8 * 1024 * 1024;

// Basically tokio_util's default LengthDelimitedCodec, with
// protobuf deserialisation on top
impl codec::Decoder for MessageCodec {
    type Item = CastMessage;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Error> {
        let mut peek: &[u8] = &*src;
        if peek.remaining() < 4 {
            return Ok(None);
        }
        let length = peek.get_u32();
        if length > MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", length),
            )
            .into());
        }
        let length: usize = length.try_into().unwrap();
        if peek.remaining() < length {
            // Enough space for this message and the length of the next
            src.reserve(length - peek.remaining() + 4);
            return Ok(None);
        }
        // We have our frame, start consuming the original buffer
        // Skip length:
        src.advance(4);
        let frame = src.split_to(length);
        // parse_from_tokio_bytes could save a few allocs,
        // if combined with codegen changes: https://lib.rs/crates/protobuf
        //let raw_message = cast_channel::CastMessage::parse_from_tokio_bytes(frame)?;
        let raw_message = cast_channel::CastMessage::parse_from_bytes(&frame)?;
        log::debug!("Message received: {:?}", raw_message);
        Ok(Some(CastMessage {
            namespace: raw_message.namespace().to_string(),
            source: raw_message.source_id().to_string(),
            destination: raw_message.destination_id().to_string(),
            payload: match raw_message.payload_type() {
                PayloadType::STRING => {
                    CastMessagePayload::String(raw_message.payload_utf8().to_string())
                }
                PayloadType::BINARY => {
                    CastMessagePayload::Binary(raw_message.payload_binary().to_owned())
                }
            },
        }))
    }
}

impl codec::Encoder<CastMessage> for MessageCodec {
    type Error = Error;

    fn encode(&mut self, message: CastMessage, dst: &mut BytesMut) -> Result<(), Error> {
        let mut raw_message = cast_channel::CastMessage::new();

        raw_message.set_protocol_version(ProtocolVersion::CASTV2_1_0);

        raw_message.set_namespace(message.namespace);
        raw_message.set_source_id(message.source);
        raw_message.set_destination_id(message.destination);

        match message.payload {
            CastMessagePayload::String(payload) => {
                raw_message.set_payload_type(PayloadType::STRING);
                raw_message.set_payload_utf8(payload);
            }

            CastMessagePayload::Binary(payload) => {
                raw_message.set_payload_type(PayloadType::BINARY);
                raw_message.set_payload_binary(payload);
            }
        };

        let message_content_buffer = raw_message.write_to_bytes()?;
        dst.put_u32(message_content_buffer.len().try_into().unwrap());
        dst.put_slice(&message_content_buffer);
        log::debug!("Message encoded: {:?}", raw_message);

        Ok(())
    }
}

#[derive(Debug)]
struct ResponseState {
    waker: Option<Waker>,
    payload: Poll<Option<String>>,
}

impl Default for ResponseState {
    fn default() -> Self {
        Self {
            waker: None,
            payload: Poll::Pending,
        }
    }
}

#[derive(Debug, Default)]
struct PendingRequests {
    /// How many requests are pending in a namespace
    by_namespace: BTreeMap<String, usize>,
    /// Request id to request state
    by_request_id: BTreeMap<NonZeroU32, ResponseState>,
}

impl PendingRequests {
    fn register(&mut self, namespace: String, request_id: NonZeroU32) {
        let by_ns = self.by_namespace.entry(namespace).or_default();
        *by_ns = by_ns.checked_add(1).unwrap();
        assert!(self
            .by_request_id
            .insert(request_id, ResponseState::default())
            .is_none());
    }
}

/// Static structure that is responsible for (de)serializing and sending/receiving Cast protocol
/// messages.
pub struct MessageManager<S>
where
    S: AsyncWrite + AsyncRead,
{
    message_buffer: Lock<Vec<CastMessage>>,
    // Using async mutexes to prevent interleaving; we keep
    // the guard across await points until a frame is entirely handled
    sender: tokio::sync::Mutex<codec::FramedWrite<WriteHalf<S>, MessageCodec>>,
    // This is an independent mutex so reads and writes can be interleaved
    receiver: tokio::sync::Mutex<codec::FramedRead<ReadHalf<S>, MessageCodec>>,
    request_counter: Lock<NonZeroU32>,
    // From namespace to request_id to response state
    pending_requests: Lock<PendingRequests>,
}

impl<S> MessageManager<S>
where
    S: AsyncWrite + AsyncRead,
{
    pub fn new(stream: S) -> Self {
        // Would like to use BiLock for splitting,
        // but https://github.com/rust-lang/futures-rs/pull/2384
        // was left unmerged.  Also, it may not be as useful
        // if poll_lock takes mut self.
        let (read, write) = tokio::io::split(stream);
        let receiver = codec::FramedRead::new(read, MessageCodec).into();
        let sender = codec::FramedWrite::new(write, MessageCodec).into();
        MessageManager {
            sender,
            receiver,
            message_buffer: Lock::new(vec![]),
            request_counter: Lock::new(NonZeroU32::MIN),
            pending_requests: Lock::new(PendingRequests::default()),
        }
    }

    /// Sends `message` to the Cast Device.
    ///
    /// # Arguments
    ///
    /// * `message` - `JsonMessage` instance to be sent to the Cast Device.
    pub async fn send<T: serde::Serialize>(
        &self,
        message: JsonMessage<'_, T>,
    ) -> Result<(), Error> {
        let message = CastMessage {
            namespace: message.namespace.to_owned(),
            source: message.source.to_owned(),
            destination: message.destination.to_owned(),
            payload: CastMessagePayload::String(serde_json::to_string(&message.payload)?),
        };
        self.sender.lock().await.send(message).await?;
        Ok(())
    }

    pub async fn send_get_reply<T: serde::Serialize, U: serde::de::DeserializeOwned>(
        &self,
        message: JsonMessage<'_, T>,
    ) -> Result<U, Error> {
        let request_id = self.generate_request_id();
        let cast_message = CastMessage {
            namespace: message.namespace.to_owned(),
            source: message.source.to_owned(),
            destination: message.destination.to_owned(),
            payload: CastMessagePayload::String(serde_json::to_string(&Request {
                request_id: request_id.get(),
                inner: message.payload,
            })?),
        };
        self.pending_requests
            .borrow_mut()
            .register(message.namespace.to_owned(), request_id);
        self.sender.lock().await.send(cast_message).await?;
        let payload = poll_fn(|cx| self.poll_reply(cx, request_id)).await.unwrap();
        let resp: Request<U> = serde_json::from_str(&payload)?;
        Ok(resp.inner)
    }

    fn poll_reply(
        &self,
        context: &mut Context<'_>,
        request_id: NonZeroU32,
    ) -> Poll<Option<String>> {
        let mut pending_requests = self.pending_requests.borrow_mut();
        let Some(state) = pending_requests.by_request_id.get_mut(&request_id) else {
            return Poll::Ready(None);
        };
        match state.payload {
            Poll::Pending => {
                if let Some(ref mut waker) = state.waker {
                    waker.clone_from(context.waker())
                } else {
                    state.waker = Some(context.waker().clone())
                }
                Poll::Pending
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(ref mut opt) => Poll::Ready(opt.take()),
        }
    }

    /// Waits for the next `CastMessage` available. Can also return existing message from the
    /// internal message buffer containing messages that have been received previously, but haven't
    /// been consumed for some reason (e.g. during `receive_find_map` call).
    ///
    /// # Return value
    ///
    /// `Result` containing parsed `CastMessage` or `Error`.
    pub async fn receive(&self) -> Result<CastMessage, Error> {
        let mut message_buffer = self.message_buffer.borrow_mut();

        // If we have messages in the buffer, let's return them from it.
        if message_buffer.is_empty() {
            self.read().await
        } else {
            Ok(message_buffer.remove(0))
        }
    }

    /// Waits for the next `CastMessage` for which `f` returns valid mapped value. Messages in which
    /// `f` is not interested are placed into internal message buffer and can be later retrieved
    /// with `receive`. This method always reads from the stream.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use tokio::net::TcpStream;
    /// # use rust_cast::message_manager::{CastMessage, MessageManager};
    /// # use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
    /// # use rustls::pki_types::ServerName;
    /// # use tokio_rustls::TlsConnector;
    /// # let config = ClientConfig::builder()
    /// #   .with_root_certificates(RootCertStore::empty())
    /// #   .with_no_client_auth();
    /// # tokio_test::block_on(async {
    /// # let server_name = ServerName::try_from("0")?.to_owned();
    /// # let connor = TlsConnector::from(Arc::new(config));
    /// # let tcp_stream = TcpStream::connect(("0", 8009)).await?;
    /// # let tls_stream = connor.connect(server_name, tcp_stream).await?;
    /// # let message_manager = MessageManager::new(tls_stream);
    /// # fn can_handle(message: &CastMessage) -> bool { unimplemented!() }
    /// # fn parse(message: &CastMessage) { unimplemented!() }
    /// message_manager.receive_find_map(|message| {
    ///   if !can_handle(message) {
    ///     return Ok(None);
    ///   }
    ///
    ///   parse(message);
    ///
    ///   Ok(Some(()))
    /// }).await?;
    /// # Ok::<(), rust_cast::errors::Error>(())
    /// # });
    /// ```
    ///
    /// # Arguments
    ///
    /// * `f` - Function that analyzes and maps `CastMessage` to any other type. If message doesn't
    /// look like something `f` is looking for, then `Ok(None)` should be returned so that message
    /// is not lost and placed into internal message buffer for later retrieval.
    ///
    /// # Return value
    ///
    /// `Result` containing parsed `CastMessage` or `Error`.
    pub async fn receive_find_map<F, B>(&self, f: F) -> Result<B, Error>
    where
        F: Fn(&CastMessage) -> Result<Option<B>, Error>,
    {
        loop {
            let message = self.read().await?;

            // If message is found, just return mapped result, otherwise keep unprocessed message
            // in the buffer, it can be later retrieved with `receive`.
            match f(&message)? {
                Some(r) => return Ok(r),
                None => self.message_buffer.borrow_mut().push(message),
            }
        }
    }

    /// Generates unique integer number that is used in some requests to map them with the response.
    ///
    /// # Return value
    ///
    /// Unique (in the scope of this particular `MessageManager` instance) integer number.
    pub fn generate_request_id(&self) -> NonZeroU32 {
        let mut counter = self.request_counter.borrow_mut();
        let request_id = *counter;
        *counter = counter.checked_add(1).unwrap();
        request_id
    }

    /// Reads next `CastMessage` from the stream.
    ///
    /// # Return value
    ///
    /// `Result` containing parsed `CastMessage` or `Error`.
    async fn read(&self) -> Result<CastMessage, Error> {
        while let Some(maybe_msg) = self.receiver.lock().await.next().await {
            let mut msg = maybe_msg?;
            let mut pending_requests = self.pending_requests.borrow_mut();
            if pending_requests
                .by_namespace
                .get(&msg.namespace)
                .cloned()
                .unwrap_or_default()
                > 0
            {
                if let CastMessagePayload::String(payload) = msg.payload {
                    let resp: Request<serde::de::IgnoredAny> = serde_json::from_str(&payload)?;
                    if let Some(request_id) = NonZeroU32::new(resp.request_id) {
                        if let Some(pending) = pending_requests.by_request_id.get_mut(&request_id) {
                            pending.payload = Poll::Ready(Some(payload));
                            if let Some(ref waker) = pending.waker {
                                waker.wake_by_ref();
                            }
                            continue;
                        }
                    }
                    msg.payload = CastMessagePayload::String(payload);
                }
            }
            return Ok(msg);
        }
        // Stream has ended cleanly; Ok(None) would be better
        // but requires updating callers
        Err(Error::Io(std::io::Error::from(
            std::io::ErrorKind::UnexpectedEof,
        )))
    }
}
