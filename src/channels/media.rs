use std::borrow::Cow;
use std::cell::RefCell;
use std::io::Write;
use std::rc::Rc;

use serde_json::Value;
use serde_json::value::from_value;

use cast::cast_channel;
use errors::Error;
use message_manager::MessageManager;

const CHANNEL_NAMESPACE: &'static str = "urn:x-cast:com.google.cast.media";

const MESSAGE_TYPE_LOAD: &'static str = "LOAD";

const REPLY_TYPE_MEDIA_STATUS: &'static str = "MEDIA_STATUS";

pub enum StreamType {
    Unknown,
    Buffered,
    Live,
}

#[derive(Serialize, Debug)]
pub struct MediaRequest<'a> {
    #[serde(rename="requestId")]
    pub request_id: i32,

    #[serde(rename="sessionId")]
    pub session_id: String,

    #[serde(rename="type")]
    pub typ: String,

    pub media: Media<'a>,

    #[serde(rename="currentTime")]
    pub current_time: f64,

    #[serde(rename="customData")]
    pub custom_data: CustomData,

    pub autoplay: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Media<'a> {
    #[serde(rename="contentId")]
    pub content_id: Cow<'a, str>,

    #[serde(rename="streamType")]
    pub stream_type: Cow<'a, str>,

    #[serde(rename="contentType")]
    pub content_type: Cow<'a, str>,
}

#[derive(Serialize, Debug)]
pub struct CustomData {
    #[serde(skip_serializing)]
    private: (),
}

impl CustomData {
    pub fn new() -> CustomData {
        CustomData { private: () }
    }
}

#[derive(Deserialize, Debug)]
pub struct MediaStatus<'a> {
    #[serde(default)]
    media: Option<Media<'a>>,
}

#[derive(Deserialize, Debug)]
pub struct MediaStatusReply<'a> {
    #[serde(rename="requestId")]
    pub request_id: i32,

    #[serde(rename="type")]
    pub typ: String,

    pub status: MediaStatus<'a>,
}

#[derive(Debug)]
pub enum Reply<'a> {
    MediaStatus(MediaStatusReply<'a>),
    Unknown,
}

pub struct MediaChannel<W>
    where W: Write
{
    sender: String,
    receiver: String,
    session_id: String,
    writer: Rc<RefCell<W>>,
}

impl<W> MediaChannel<W>
    where W: Write
{
    pub fn new(sender: String,
               receiver: String,
               session_id: String,
               writer: Rc<RefCell<W>>)
               -> MediaChannel<W> {
        MediaChannel {
            sender: sender,
            receiver: receiver,
            session_id: session_id,
            writer: writer,
        }
    }

    pub fn stream<'a, S>(&self, content_id: S, content_type: S, stream_type: StreamType)
        -> Result<(), Error> where S: Into<Cow<'a, str>> {

        let stream_type_string = match stream_type {
            StreamType::Unknown => "UNKNOWN",
            StreamType::Buffered => "BUFFERED",
            StreamType::Live => "LIVE",
        };

        let payload = MediaRequest {
            request_id: 1,
            session_id: self.session_id.clone(),
            typ: MESSAGE_TYPE_LOAD.to_owned(),

            media: Media {
                content_id: content_id.into(),
                stream_type: stream_type_string.into(),
                content_type: content_type.into(),
            },

            current_time: 0_f64,
            autoplay: true,
            custom_data: CustomData::new(),
        };

        let message = try!(MessageManager::create(CHANNEL_NAMESPACE.to_owned(),
                                                  self.sender.clone(),
                                                  self.receiver.clone(),
                                                  Some(payload)));

        MessageManager::send(&mut *self.writer.borrow_mut(), message)
    }

    pub fn try_handle(&self, message: &cast_channel::CastMessage) -> Result<Reply, Error> {
        if message.get_namespace() != CHANNEL_NAMESPACE {
            return Err(Error::Internal("Channel does not support provided message.".to_owned()));
        }

        let reply: Value = try!(MessageManager::parse_payload(message));

        let message_type = {
            let reply_object_value = reply.as_object().unwrap();
            reply_object_value.get("type").unwrap().as_string().unwrap().to_owned()
        };

        let reply = match &message_type as &str {
            REPLY_TYPE_MEDIA_STATUS => Reply::MediaStatus(from_value(reply).unwrap()),
            _ => Reply::Unknown,
        };

        Ok(reply)
    }
}