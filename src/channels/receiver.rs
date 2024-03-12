use std::{borrow::Cow, convert::Into, str::FromStr, string::ToString};

use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    cast::proxies::receiver as proxies,
    errors::Error,
    message_manager::{CastMessage, CastMessagePayload, JsonMessage, MessageManager},
    Lrc,
};

use proxies::ReceiverReply;

const CHANNEL_NAMESPACE: &str = "urn:x-cast:com.google.cast.receiver";

const MESSAGE_TYPE_LAUNCH: &str = "LAUNCH";
const MESSAGE_TYPE_STOP: &str = "STOP";
const MESSAGE_TYPE_GET_STATUS: &str = "GET_STATUS";
const MESSAGE_TYPE_SET_VOLUME: &str = "SET_VOLUME";

const MESSAGE_TYPE_RECEIVER_STATUS: &str = "RECEIVER_STATUS";
const MESSAGE_TYPE_LAUNCH_ERROR: &str = "LAUNCH_ERROR";
const MESSAGE_TYPE_INVALID_REQUEST: &str = "INVALID_REQUEST";

const APP_DEFAULT_MEDIA_RECEIVER_ID: &str = "CC1AD845";
const APP_BACKDROP_ID: &str = "E8C28D3C";
const APP_YOUTUBE_ID: &str = "233637DE";

/// Structure that describes possible cast device volume options.
#[derive(Copy, Clone, Debug)]
pub struct Volume {
    /// Volume level.
    pub level: Option<f32>,
    /// Mute/unmute state.
    pub muted: Option<bool>,
}

// The structs are actually identical
impl From<proxies::Volume> for Volume {
    fn from(value: proxies::Volume) -> Self {
        Self {
            level: value.level,
            muted: value.muted,
        }
    }
}

impl From<Volume> for proxies::Volume {
    fn from(value: Volume) -> Self {
        Self {
            level: value.level,
            muted: value.muted,
        }
    }
}

/// This `From<f32>` implementation is useful when only volume level is needed.
impl From<f32> for Volume {
    fn from(level: f32) -> Self {
        Self {
            level: Some(level),
            muted: None,
        }
    }
}

/// This `From<bool>` implementation is useful when only mute/unmute state is needed.
impl From<bool> for Volume {
    fn from(muted: bool) -> Self {
        Self {
            level: None,
            muted: Some(muted),
        }
    }
}

/// This `From<(f32, bool)>` implementation is useful when both volume level and mute/unmute state are
/// needed.
impl From<(f32, bool)> for Volume {
    fn from((level, muted): (f32, bool)) -> Self {
        Self {
            level: Some(level),
            muted: Some(muted),
        }
    }
}

/// Structure that describes currently run Cast Device application.
#[derive(Clone, Debug)]
pub struct Application {
    /// The identifier of the Cast application. Not for display.
    pub app_id: String,
    /// Session id of the currently active application.
    pub session_id: String,
    /// Name of the `pipe` to talk to the application.
    pub transport_id: String,
    /// A list of the namespaces supported by the receiver application.
    pub namespaces: Vec<String>,
    /// The human-readable name of the Cast application, for example, "YouTube".
    pub display_name: String,
    /// Descriptive text for the current application content, for example “My vacations”.
    pub status_text: String,
}

// The structs are almost identical, except this public struct
// removes one level of nesting along amespaces[] -> name
impl From<proxies::Application> for Application {
    fn from(app: proxies::Application) -> Self {
        Self {
            app_id: app.app_id,
            session_id: app.session_id,
            transport_id: app.transport_id,
            namespaces: app.namespaces.into_iter().map(|ns| ns.name).collect(),
            display_name: app.display_name,
            status_text: app.status_text,
        }
    }
}

/// Describes the current status of the receiver cast device.
#[derive(Clone, Debug)]
pub struct Status {
    /// Contains the list of applications that are currently run.
    pub applications: Vec<Application>,
    /// Determines whether the Cast device is the active input or not.
    pub is_active_input: bool,
    /// Determines whether the Cast device is in stand by mode.
    pub is_stand_by: bool,
    /// Volume parameters of the currently active cast device.
    pub volume: Volume,
}

impl From<proxies::Status> for Status {
    fn from(status: proxies::Status) -> Self {
        Self {
            applications: status
                .applications
                .into_iter()
                .map(Into::into)
                .collect::<Vec<Application>>(),
            is_active_input: status.is_active_input,
            is_stand_by: status.is_stand_by,
            volume: status.volume.into(),
        }
    }
}

/// Describes the application launch error.
#[derive(Clone, Debug)]
pub struct LaunchError {
    /// Description of the launch error reason if available.
    pub reason: Option<String>,
}

/// Describes the invalid request error.
#[derive(Clone, Debug)]
pub struct InvalidRequest {
    /// Description of the invalid request reason if available.
    pub reason: Option<String>,
}

/// Represents all currently supported incoming messages that receiver channel can handle.
#[derive(Clone, Debug)]
pub enum ReceiverResponse {
    /// Status of the currently active receiver.
    Status(Status),
    /// Error indicating that receiver failed to launch application.
    LaunchError(LaunchError),
    /// Error indicating that request is not valid.
    InvalidRequest(InvalidRequest),
    /// Used every time when channel can't parse the message. Associated data contains `type` string
    /// field and raw JSON data returned from cast device.
    NotImplemented(String, serde_json::Value),
}

#[derive(Clone, Debug, PartialEq)]
pub enum CastDeviceApp {
    DefaultMediaReceiver,
    Backdrop,
    YouTube,
    Custom(String),
}

impl FromStr for CastDeviceApp {
    type Err = ();

    fn from_str(s: &str) -> Result<CastDeviceApp, ()> {
        let app = match s {
            APP_DEFAULT_MEDIA_RECEIVER_ID | "default" => CastDeviceApp::DefaultMediaReceiver,
            APP_BACKDROP_ID | "backdrop" => CastDeviceApp::Backdrop,
            APP_YOUTUBE_ID | "youtube" => CastDeviceApp::YouTube,
            custom => CastDeviceApp::Custom(custom.to_string()),
        };

        Ok(app)
    }
}

impl ToString for CastDeviceApp {
    fn to_string(&self) -> String {
        match *self {
            CastDeviceApp::DefaultMediaReceiver => APP_DEFAULT_MEDIA_RECEIVER_ID.to_string(),
            CastDeviceApp::Backdrop => APP_BACKDROP_ID.to_string(),
            CastDeviceApp::YouTube => APP_YOUTUBE_ID.to_string(),
            CastDeviceApp::Custom(ref app_id) => app_id.to_string(),
        }
    }
}

pub struct ReceiverChannel<'a, W>
where
    W: AsyncRead + AsyncWrite,
{
    sender: Cow<'a, str>,
    receiver: Cow<'a, str>,
    message_manager: Lrc<MessageManager<W>>,
}

impl<'a, W> ReceiverChannel<'a, W>
where
    W: AsyncRead + AsyncWrite,
{
    pub fn new<S>(
        sender: S,
        receiver: S,
        message_manager: Lrc<MessageManager<W>>,
    ) -> ReceiverChannel<'a, W>
    where
        S: Into<Cow<'a, str>>,
    {
        ReceiverChannel {
            sender: sender.into(),
            receiver: receiver.into(),
            message_manager,
        }
    }

    /// Launches the specified receiver's application.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::str::FromStr;
    /// use rust_cast::{CastDevice, channels::receiver::CastDeviceApp};
    ///
    /// # tokio_test::block_on(async {
    /// # let cast_device = CastDevice::connect_without_host_verification("host", 1234).await.unwrap();
    /// cast_device.receiver.launch_app(&CastDeviceApp::from_str("youtube").unwrap()).await;
    /// # })
    /// ```
    ///
    /// # Arguments
    ///
    /// * `app` - `CastDeviceApp` instance reference to run.
    pub async fn launch_app(&self, app: &CastDeviceApp) -> Result<Application, Error> {
        let reply: ReceiverReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &self.receiver,
                payload: proxies::AppLaunchRequest {
                    typ: MESSAGE_TYPE_LAUNCH.to_string(),
                    app_id: app.to_string(),
                },
            })
            .await?;

        // Once application is run cast receiver device should emit status update event, or launch
        // error event if something went wrong.
        match reply {
            ReceiverReply::Status(mut status) => Ok(status.status.applications.remove(0).into()),
            ReceiverReply::LaunchError(error) => Err(Error::Internal(format!(
                "Could not run application ({}).",
                error.reason.unwrap_or_else(|| "Unknown".to_string())
            ))),
            _ => Err(Error::Internal(
                "Other error running application.".to_string(),
            )),
        }
    }

    /// Broadcasts a message over a cast device's message bus.
    ///
    /// Receiver can observe messages using `context.addCustomMessageListener` with custom namespace.
    ///
    /// ```javascript, no_run
    /// context.addCustomMessageListener('urn:x-cast:com.example.castdata', function(customEvent) {
    ///   // do something with message
    /// });
    /// ```
    ///
    /// Namespace should start with `urn:x-cast:`
    ///
    /// # Arguments
    ///
    /// * `namespace` - Message namespace that should start with `urn:x-cast:`.
    /// * `message` - Message instance to send.
    pub async fn broadcast_message<M: Serialize>(
        &self,
        namespace: &str,
        payload: &M,
    ) -> Result<(), Error> {
        if !namespace.starts_with("urn:x-cast:") {
            return Err(Error::Namespace(format!(
                "'{}' should start with 'urn:x-cast:' prefix",
                namespace
            )));
        }
        self.message_manager
            .send(JsonMessage {
                namespace,
                source: &self.sender,
                destination: "*",
                payload,
            })
            .await?;

        Ok(())
    }

    /// Stops currently active app using corresponding `session_id`.
    ///
    /// # Arguments
    /// * `session_id` - identifier of the active application session from `Application` instance.
    pub async fn stop_app<S>(&self, session_id: S) -> Result<(), Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: ReceiverReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &self.receiver,
                payload: proxies::AppStopRequest {
                    typ: MESSAGE_TYPE_STOP.to_string(),
                    session_id: session_id.into(),
                },
            })
            .await?;

        // Once application is stopped cast receiver device should emit status update event, or
        // invalid request event if provided session id is not valid.
        match reply {
            ReceiverReply::Status(_) => Ok(()),
            ReceiverReply::InvalidRequest(error) => Err(Error::Internal(format!(
                "Invalid request ({}).",
                error.reason.unwrap_or_else(|| "Unknown".to_string())
            ))),

            _ => Err(Error::Internal("Error stopping.".to_string())),
        }
    }

    /// Retrieves status of the cast device receiver.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn get_status(&self) -> Result<Status, Error> {
        let reply: ReceiverReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &self.receiver,
                payload: proxies::GetStatusRequest {
                    typ: MESSAGE_TYPE_GET_STATUS.to_string(),
                },
            })
            .await?;

        if let ReceiverReply::Status(status) = reply {
            Ok(status.status.into())
        } else {
            Err(Error::Internal("Couldn't get status".to_string()))
        }
    }

    /// Sets volume for the active cast device.
    ///
    /// # Arguments
    ///
    /// * `volume` - anything that can be converted to a valid `Volume` structure. It's possible to
    ///              set volume level, mute/unmute state or both altogether.
    ///
    /// # Return value
    ///
    /// Actual `Volume` instance returned by receiver.
    ///
    /// # Errors
    ///
    /// Usually method can fail only if network connection with cast device is lost for some reason.
    pub async fn set_volume<T>(&self, volume: T) -> Result<Volume, Error>
    where
        T: Into<Volume>,
    {
        let volume = volume.into();

        let reply: ReceiverReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &self.receiver,
                payload: proxies::SetVolumeRequest {
                    typ: MESSAGE_TYPE_SET_VOLUME.to_string(),
                    volume: proxies::Volume {
                        level: volume.level,
                        muted: volume.muted,
                    },
                },
            })
            .await?;

        if let ReceiverReply::Status(status) = reply {
            Ok(status.status.volume.into())
        } else {
            Err(Error::Internal("Couldn't set volume".to_string()))
        }
    }

    pub fn can_handle(&self, message: &CastMessage) -> bool {
        message.namespace == CHANNEL_NAMESPACE
    }

    pub fn parse(&self, message: &CastMessage) -> Result<ReceiverResponse, Error> {
        let reply = match message.payload {
            CastMessagePayload::String(ref payload) => {
                serde_json::from_str::<serde_json::Value>(payload)?
            }
            _ => {
                return Err(Error::Internal(
                    "Binary payload is not supported!".to_string(),
                ))
            }
        };

        let message_type = reply
            .as_object()
            .and_then(|object| object.get("type"))
            .and_then(|property| property.as_str())
            .unwrap_or("")
            .to_string();

        let response = match message_type.as_ref() {
            MESSAGE_TYPE_RECEIVER_STATUS => {
                let status_reply: proxies::StatusReply = serde_json::value::from_value(reply)?;

                ReceiverResponse::Status(status_reply.status.into())
            }
            MESSAGE_TYPE_LAUNCH_ERROR => {
                let reply: proxies::LaunchErrorReply = serde_json::value::from_value(reply)?;

                ReceiverResponse::LaunchError(LaunchError {
                    reason: reply.reason,
                })
            }
            MESSAGE_TYPE_INVALID_REQUEST => {
                let reply: proxies::InvalidRequestReply = serde_json::value::from_value(reply)?;

                ReceiverResponse::InvalidRequest(InvalidRequest {
                    reason: reply.reason,
                })
            }
            _ => ReceiverResponse::NotImplemented(message_type.to_string(), reply),
        };

        Ok(response)
    }
}
