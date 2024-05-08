use std::collections::btree_map::{BTreeMap, Entry};
use std::future::poll_fn;
use std::num::NonZeroU32;
use std::ops::{Deref, DerefMut};
use std::task::{ready, Context, Poll, Waker};

use bytes::{Buf as _, BufMut as _, BytesMut};
use futures_util::{SinkExt as _, StreamExt as _};
use protobuf::Message as _;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
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

impl<T> From<T> for Lock<T> {
    fn from(data: T) -> Self {
        Self::new(data)
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
    payload: Poll<String>,
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

    fn handle_read(&mut self, mut msg: CastMessage) -> Result<Option<CastMessage>, Error> {
        let Some(by_ns) = self.by_namespace.get_mut(&msg.namespace) else {
            return Ok(Some(msg));
        };
        if *by_ns == 0 {
            return Ok(Some(msg));
        }
        if let CastMessagePayload::String(payload) = msg.payload {
            let resp: Request<serde::de::IgnoredAny> = serde_json::from_str(&payload)?;
            if let Some(request_id) = NonZeroU32::new(resp.request_id) {
                if let Some(pending) = self.by_request_id.get_mut(&request_id) {
                    pending.payload = Poll::Ready(payload);
                    if let Some(waker) = pending.waker.take() {
                        waker.wake();
                    }
                    *by_ns -= 1;
                    return Ok(None);
                }
            }
            msg.payload = CastMessagePayload::String(payload);
        }
        Ok(Some(msg))
    }
}

enum Read {
    /// We read a reply, passed it into pending_requests
    Reply,
    /// A message that's not a reply
    NonReply(CastMessage),
    /// Stream has ended cleanly
    EndOfStream,
}

/// Static structure that is responsible for (de)serializing and sending/receiving Cast protocol
/// messages.
// Implementation note about locks:
// For sending, we get a request_id from request_counter,
// store that in pending_requests, and send the message through sender.
// That last sender lock can be held across an await, should the sending
// future be suspended.
// For receiving, we can be polled from two places.
// First possibility is receive:
// message_buffer_recv > (receiver | pending_requests)
// receive will lock message_buffer_recv,
// select from the buffer (preferrably) and otherwise return from read.
// read will get a message from the receiver, drop the receiver lock
// explicitly, look up the message in pending_requests, and either store it there
// or return it.
// Second possibility:
// pending_requests > receiver
// When send_get_reply polls for a reply, we look for it in pending_requests
// and keep the lock.  If it's not there, we want to ensure the receiver
// is being polled, and poll it ourselves.  We store every ready non-reply message
// in message_buffer, then once we have one that's a reply, we process it in
// pending_requests and yield.
pub struct MessageManager<S>
where
    S: AsyncWrite + AsyncRead,
{
    message_buffer: UnboundedSender<Result<CastMessage, Error>>,
    message_buffer_recv: Lock<UnboundedReceiver<Result<CastMessage, Error>>>,
    // Using async mutexes to prevent interleaving; we keep
    // the guard across await points until a frame is entirely handled
    sender: tokio::sync::Mutex<codec::FramedWrite<WriteHalf<S>, MessageCodec>>,
    // This is an independent mutex so reads and writes can be interleaved
    receiver: Lock<codec::FramedRead<ReadHalf<S>, MessageCodec>>,
    request_counter: Lock<NonZeroU32>,
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
        let (message_buffer, message_buffer_recv) = unbounded_channel();
        MessageManager {
            sender,
            receiver,
            message_buffer,
            message_buffer_recv: message_buffer_recv.into(),
            request_counter: NonZeroU32::MIN.into(),
            pending_requests: PendingRequests::default().into(),
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
        let payload = poll_fn(|cx| self.poll_reply(cx, request_id)).await;
        let resp: Request<U> = serde_json::from_str(&payload)?;
        Ok(resp.inner)
    }

    fn poll_reply(&self, cx: &mut Context<'_>, request_id: NonZeroU32) -> Poll<String> {
        let mut pending_requests = self.pending_requests.borrow_mut();
        let Entry::Occupied(mut entry) = pending_requests.by_request_id.entry(request_id) else {
            // poll_reply is private, called only from send_get_reply
            // poll_reply consumes the entry once ready, and
            // send_get_reply drops the future at that point
            // request_id is unique, so no confusion between multiple send_get_reply calls
            unreachable!("poll_reply unexpected request_id {request_id}");
        };
        let state = entry.get_mut();
        match state.payload {
            Poll::Pending => {
                if let Some(ref mut waker) = state.waker {
                    waker.clone_from(cx.waker())
                } else {
                    state.waker = Some(cx.waker().clone())
                }
                while let Some(msgres) = ready!(self.receiver.borrow_mut().poll_next_unpin(cx)) {
                    if let Some(msgres) = msgres
                        .and_then(|msg| pending_requests.handle_read(msg))
                        .transpose()
                    {
                        self.message_buffer.send(msgres).unwrap();
                    } else {
                        break;
                    }
                }
                Poll::Pending
            }
            Poll::Ready(_) => entry.remove().payload,
        }
    }

    /// Waits for the next `CastMessage` available.
    ///
    /// # Return value
    ///
    /// `Result` containing parsed `CastMessage` or `Error`.
    pub async fn receive(&self) -> Result<CastMessage, Error> {
        let mut message_buffer = self.message_buffer_recv.borrow_mut();
        loop {
            tokio::select! {
                    biased;
                    msgresopt = message_buffer.recv() =>
                        return msgresopt.unwrap(),
                    readres = self.read() => {
                        let read = readres?;
                        match read {
                            Read::Reply => (),
                            Read::NonReply(message) => return Ok(message),
                            Read::EndOfStream =>
                                return Err(Error::Disconnected),
                        }
                    }
            }
        }
    }

    /// Generates unique integer number that is used in some requests to map them with the response.
    ///
    /// # Return value
    ///
    /// Unique (in the scope of this particular `MessageManager` instance) integer number.
    fn generate_request_id(&self) -> NonZeroU32 {
        let mut counter = self.request_counter.borrow_mut();
        let request_id = *counter;
        *counter = counter.checked_add(1).unwrap();
        request_id
    }

    /// Reads next message from the stream.
    ///
    /// # Return value
    ///
    /// `Result` containing `Read` value or `Error`.
    async fn read(&self) -> Result<Read, Error> {
        let mut guard = self.receiver.borrow_mut();
        if let Some(msg) = guard.next().await.transpose()? {
            // Explicit drop, otherwise we'd have
            // receiver > pending_request locks
            // when poll_reply nests them the other
            // way, and bad ordering brings deadlocks
            drop(guard);
            if let Some(msg) = self.pending_requests.borrow_mut().handle_read(msg)? {
                Ok(Read::NonReply(msg))
            } else {
                Ok(Read::Reply)
            }
        } else {
            Ok(Read::EndOfStream)
        }
    }
}
