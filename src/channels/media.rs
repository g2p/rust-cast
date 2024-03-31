use std::{borrow::Cow, str::FromStr, string::ToString};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    cast::proxies::media as proxies,
    errors::Error,
    message_manager::{CastMessage, CastMessagePayload, JsonMessage, MessageManager},
    Lrc,
};

use proxies::MediaReply;

const CHANNEL_NAMESPACE: &str = "urn:x-cast:com.google.cast.media";

const MESSAGE_TYPE_GET_STATUS: &str = "GET_STATUS";
const MESSAGE_TYPE_LOAD: &str = "LOAD";
const MESSAGE_TYPE_QUEUE_LOAD: &str = "QUEUE_LOAD";
const MESSAGE_TYPE_QUEUE_PREV: &str = "QUEUE_PREV";
const MESSAGE_TYPE_QUEUE_NEXT: &str = "QUEUE_NEXT";
const MESSAGE_TYPE_QUEUE_UPDATE: &str = "QUEUE_UPDATE";
const MESSAGE_TYPE_PLAY: &str = "PLAY";
const MESSAGE_TYPE_PAUSE: &str = "PAUSE";
const MESSAGE_TYPE_STOP: &str = "STOP";
const MESSAGE_TYPE_SEEK: &str = "SEEK";

/// Describes the way cast device should stream content.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StreamType {
    /// This variant allows cast device to automatically choose whatever way it's most comfortable
    /// with.
    None,
    /// Cast device should buffer some portion of the content and only then start streaming.
    Buffered,
    /// Cast device should display content as soon as it gets any portion of it.
    Live,
}

impl FromStr for StreamType {
    type Err = Error;

    fn from_str(s: &str) -> Result<StreamType, Error> {
        match s {
            "BUFFERED" | "buffered" => Ok(StreamType::Buffered),
            "LIVE" | "live" => Ok(StreamType::Live),
            _ => Ok(StreamType::None),
        }
    }
}

impl ToString for StreamType {
    fn to_string(&self) -> String {
        let stream_type = match *self {
            StreamType::None => "NONE",
            StreamType::Buffered => "BUFFERED",
            StreamType::Live => "LIVE",
        };

        stream_type.to_string()
    }
}

/// Generic, movie, TV show, music track, or photo metadata.
#[derive(Clone, Debug)]
pub enum Metadata {
    Generic(GenericMediaMetadata),
    Movie(MovieMediaMetadata),
    TvShow(TvShowMediaMetadata),
    MusicTrack(MusicTrackMediaMetadata),
    Photo(PhotoMediaMetadata),
}

impl Metadata {
    fn encode(&self) -> proxies::Metadata {
        match self {
            Metadata::Generic(ref x) => proxies::Metadata {
                title: x.title.clone(),
                subtitle: x.subtitle.clone(),
                images: x.images.iter().map(|i| i.encode()).collect(),
                release_date: x.release_date.clone(),
                ..proxies::Metadata::new(0)
            },
            Metadata::Movie(ref x) => proxies::Metadata {
                title: x.title.clone(),
                subtitle: x.subtitle.clone(),
                studio: x.studio.clone(),
                images: x.images.iter().map(|i| i.encode()).collect(),
                release_date: x.release_date.clone(),
                ..proxies::Metadata::new(1)
            },
            Metadata::TvShow(ref x) => proxies::Metadata {
                series_title: x.series_title.clone(),
                subtitle: x.episode_title.clone(),
                season: x.season,
                episode: x.episode,
                images: x.images.iter().map(|i| i.encode()).collect(),
                original_air_date: x.original_air_date.clone(),
                ..proxies::Metadata::new(2)
            },
            Metadata::MusicTrack(ref x) => proxies::Metadata {
                album_name: x.album_name.clone(),
                title: x.title.clone(),
                album_artist: x.album_artist.clone(),
                artist: x.artist.clone(),
                composer: x.composer.clone(),
                track_number: x.track_number,
                disc_number: x.disc_number,
                images: x.images.iter().map(|i| i.encode()).collect(),
                release_date: x.release_date.clone(),
                ..proxies::Metadata::new(3)
            },
            Metadata::Photo(ref x) => proxies::Metadata {
                title: x.title.clone(),
                artist: x.artist.clone(),
                location: x.location.clone(),
                latitude: x.latitude_longitude.map(|coord| coord.0),
                longitude: x.latitude_longitude.map(|coord| coord.1),
                width: x.dimensions.map(|dims| dims.0),
                height: x.dimensions.map(|dims| dims.1),
                creation_date_time: x.creation_date_time.clone(),
                ..proxies::Metadata::new(4)
            },
        }
    }
}

impl TryFrom<&proxies::Metadata> for Metadata {
    type Error = Error;

    fn try_from(m: &proxies::Metadata) -> Result<Self, Error> {
        Ok(match m.metadata_type {
            0 => Self::Generic(GenericMediaMetadata {
                title: m.title.clone(),
                subtitle: m.subtitle.clone(),
                images: m.images.iter().map(Image::from).collect(),
                release_date: m.release_date.clone(),
            }),
            1 => Self::Movie(MovieMediaMetadata {
                title: m.title.clone(),
                subtitle: m.subtitle.clone(),
                studio: m.studio.clone(),
                images: m.images.iter().map(Image::from).collect(),
                release_date: m.release_date.clone(),
            }),
            2 => Self::TvShow(TvShowMediaMetadata {
                series_title: m.series_title.clone(),
                episode_title: m.subtitle.clone(),
                season: m.season,
                episode: m.episode,
                images: m.images.iter().map(Image::from).collect(),
                original_air_date: m.original_air_date.clone(),
            }),
            3 => Self::MusicTrack(MusicTrackMediaMetadata {
                album_name: m.album_name.clone(),
                title: m.title.clone(),
                album_artist: m.album_artist.clone(),
                artist: m.artist.clone(),
                composer: m.composer.clone(),
                track_number: m.track_number,
                disc_number: m.disc_number,
                images: m.images.iter().map(Image::from).collect(),
                release_date: m.release_date.clone(),
            }),
            4 => {
                let mut dimensions = None;
                let mut latitude_longitude = None;
                if let Some(width) = m.width {
                    if let Some(height) = m.height {
                        dimensions = Some((width, height))
                    }
                }
                if let Some(lat) = m.latitude {
                    if let Some(long) = m.longitude {
                        latitude_longitude = Some((lat, long))
                    }
                }
                Self::Photo(PhotoMediaMetadata {
                    title: m.title.clone(),
                    artist: m.artist.clone(),
                    location: m.location.clone(),
                    latitude_longitude,
                    dimensions,
                    creation_date_time: m.creation_date_time.clone(),
                })
            }
            _ => {
                return Err(Error::Parsing(format!(
                    "Bad metadataType {}",
                    m.metadata_type
                )))
            }
        })
    }
}

/// Generic media metadata.
///
/// See also the [`GenericMediaMetadata` Cast reference](https://developers.google.com/cast/docs/reference/messages#GenericMediaMetadata).
#[derive(Clone, Debug, Default)]
pub struct GenericMediaMetadata {
    /// Descriptive title of the content.
    pub title: Option<String>,
    /// Descriptive subtitle of the content.
    pub subtitle: Option<String>,
    /// Zero or more URLs to an image associated with the content.
    pub images: Vec<Image>,
    /// Date and time the content was released, formatted as ISO 8601.
    pub release_date: Option<String>,
}

/// Movie media metadata.
///
/// See also the [`MovieMediaMetadata` Cast reference](https://developers.google.com/cast/docs/reference/messages#MovieMediaMetadata).
#[derive(Clone, Debug, Default)]
pub struct MovieMediaMetadata {
    /// Title of the movie.
    pub title: Option<String>,
    /// Subtitle of the movie.
    pub subtitle: Option<String>,
    /// Studio which released the movie.
    pub studio: Option<String>,
    /// Zero or more URLs to an image associated with the content.
    pub images: Vec<Image>,
    /// Date and time the movie was released, formatted as ISO 8601.
    pub release_date: Option<String>,
}

/// TV show media metadata.
///
/// See also the [`TvShowMediaMetadata` Cast reference](https://developers.google.com/cast/docs/reference/messages#TvShowMediaMetadata).
#[derive(Clone, Debug, Default)]
pub struct TvShowMediaMetadata {
    /// Title of the TV series.
    pub series_title: Option<String>,
    /// Title of the episode.
    pub episode_title: Option<String>,
    /// Season number of the TV show.
    pub season: Option<u32>,
    /// Episode number (in the season) of the episode.
    pub episode: Option<u32>,
    /// Zero or more URLs to an image associated with the content.
    pub images: Vec<Image>,
    /// Date and time this episode was released, formatted as ISO 8601.
    pub original_air_date: Option<String>,
}

/// Music track media metadata.
///
/// See also the [`MusicTrackMediaMetadata` Cast reference](https://developers.google.com/cast/docs/reference/messages#MusicTrackMediaMetadata).
#[derive(Clone, Debug, Default)]
pub struct MusicTrackMediaMetadata {
    /// Album or collection from which the track is taken.
    pub album_name: Option<String>,
    /// Name of the track (for example, song title).
    pub title: Option<String>,
    /// Name of the artist associated with the album featuring this track.
    pub album_artist: Option<String>,
    /// Name of the artist associated with the track.
    pub artist: Option<String>,
    /// Name of the composer associated with the track.
    pub composer: Option<String>,
    /// Number of the track on the album.
    pub track_number: Option<u32>,
    /// Number of the volume (for example, a disc) of the album.
    pub disc_number: Option<u32>,
    /// Zero or more URLs to an image associated with the content.
    pub images: Vec<Image>,
    /// Date and time the content was released, formatted as ISO 8601.
    pub release_date: Option<String>,
}

/// Photo media metadata.
///
/// See also the [`PhotoMediaMetadata` Cast reference](https://developers.google.com/cast/docs/reference/messages#PhotoMediaMetadata).
#[derive(Clone, Debug, Default)]
pub struct PhotoMediaMetadata {
    /// Title of the photograph.
    pub title: Option<String>,
    /// Name of the photographer.
    pub artist: Option<String>,
    /// Verbal location where the photograph was taken, for example “Madrid, Spain”.
    pub location: Option<String>,
    /// Latitude and longitude of the location where the photograph was taken.
    pub latitude_longitude: Option<(f64, f64)>,
    /// Width and height of the photograph in pixels.
    pub dimensions: Option<(u32, u32)>,
    /// Date and time the photograph was taken, formatted as ISO 8601.
    pub creation_date_time: Option<String>,
}

/// Image URL and optionally size metadata.
///
/// This is the description of an image, including a small amount of metadata to
/// allow the sender application a choice of images, depending on how it will
/// render them. The height and width are optional on only one item in an array
/// of images.
///
/// See also the [`Image` Cast reference](https://developers.google.com/cast/docs/reference/messages#Image).
#[derive(Clone, Debug)]
pub struct Image {
    /// URL of the image.
    pub url: String,
    /// Width and height of the image.
    pub dimensions: Option<(u32, u32)>,
}

impl Image {
    pub fn new(url: String) -> Image {
        Image {
            url,
            dimensions: None,
        }
    }

    fn encode(&self) -> proxies::Image {
        proxies::Image {
            url: self.url.clone(),
            width: self.dimensions.map(|d| d.0),
            height: self.dimensions.map(|d| d.1),
        }
    }
}

impl From<&proxies::Image> for Image {
    fn from(i: &proxies::Image) -> Self {
        let mut dimensions = None;
        if let Some(width) = i.width {
            if let Some(height) = i.height {
                dimensions = Some((width, height));
            }
        };
        Self {
            url: i.url.clone(),
            dimensions,
        }
    }
}

/// Queue repeat modes
/// <https://developers.google.com/cast/docs/reference/web_sender/chrome.cast.media#.RepeatMode>
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RepeatMode {
    /// Items are played in order, and when the queue is completed (the last
    /// item has ended) the media session is terminated.
    Off,
    /// The items in the queue will be played indefinitely. When the last item
    /// has ended, the first item will be played again.
    All,
    ///The current item will be repeated indefinitely.
    Single,
    /// The items in the queue will be played indefinitely. When the last item
    /// has ended, the list of items will be randomly shuffled by the receiver,
    /// and the queue will continue to play starting from the first item of the
    /// shuffled items.
    AllAndShuffle,
}

impl FromStr for RepeatMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "REPEAT_OFF" => Ok(Self::Off),
            "REPEAT_ALL" => Ok(Self::All),
            "REPEAT_SINGLE" => Ok(Self::Single),
            "REPEAT_ALL_AND_SHUFFLE" => Ok(Self::AllAndShuffle),
            _ => Err(Error::Internal(format!("Unknown queue repeat mode {s}"))),
        }
    }
}

impl ToString for RepeatMode {
    fn to_string(&self) -> String {
        match *self {
            Self::Off => "REPEAT_OFF",
            Self::All => "REPEAT_ALL",
            Self::Single => "REPEAT_SINGLE",
            Self::AllAndShuffle => "REPEAT_ALL_AND_SHUFFLE",
        }
        .to_string()
    }
}

/// Describes possible player states.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PlayerState {
    /// Player has not been loaded yet.
    Idle,
    /// Player is actively playing content.
    Playing,
    /// Player is in PLAY mode but not actively playing content (currentTime is not changing).
    Buffering,
    /// Player is paused.
    Paused,
}

impl FromStr for PlayerState {
    type Err = Error;

    fn from_str(s: &str) -> Result<PlayerState, Error> {
        match s {
            "IDLE" => Ok(PlayerState::Idle),
            "PLAYING" => Ok(PlayerState::Playing),
            "BUFFERING" => Ok(PlayerState::Buffering),
            "PAUSED" => Ok(PlayerState::Paused),
            _ => Err(Error::Internal(format!("Unknown player state {}", s))),
        }
    }
}

impl ToString for PlayerState {
    fn to_string(&self) -> String {
        let player_state = match *self {
            PlayerState::Idle => "IDLE",
            PlayerState::Playing => "PLAYING",
            PlayerState::Buffering => "BUFFERING",
            PlayerState::Paused => "PAUSED",
        };

        player_state.to_string()
    }
}

/// Describes possible player states.
/// Can appear when the base state is PlayerState::Idle
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ExtendedPlayerState {
    /// Player is loading the next media
    Loading,
}

impl FromStr for ExtendedPlayerState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "LOADING" => Ok(Self::Loading),
            _ => Err(Error::Internal(format!(
                "Unknown extended player state {}",
                s
            ))),
        }
    }
}

impl ToString for ExtendedPlayerState {
    fn to_string(&self) -> String {
        let player_state = match *self {
            Self::Loading => "LOADING",
        };

        player_state.to_string()
    }
}

/// Describes possible player idle reasons.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IdleReason {
    /// A sender requested to stop playback using the STOP command.
    Cancelled,
    /// A sender requested playing a different media using the LOAD command.
    Interrupted,
    /// The media playback completed.
    Finished,
    /// The media was interrupted due to an error; For example, if the player could not download the
    /// media due to network issues.
    Error,
}

impl FromStr for IdleReason {
    type Err = Error;

    fn from_str(s: &str) -> Result<IdleReason, Error> {
        match s {
            "CANCELLED" => Ok(IdleReason::Cancelled),
            "INTERRUPTED" => Ok(IdleReason::Interrupted),
            "FINISHED" => Ok(IdleReason::Finished),
            "ERROR" => Ok(IdleReason::Error),
            _ => Err(Error::Internal(format!("Unknown idle reason {}", s))),
        }
    }
}

/// <https://developers.google.com/cast/docs/reference/web_sender/chrome.cast.media#.QueueType>
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum QueueType {
    Album,
    Playlist,
    Audiobook,
    RadioStation,
    PodcastSeries,
    TvSeries,
    VideoPlaylist,
    LiveTv,
    Movie,
}

impl FromStr for QueueType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "ALBUM" => Ok(Self::Album),
            "PLAYLIST" => Ok(Self::Playlist),
            "AUDIOBOOK" => Ok(Self::Audiobook),
            "RADIO_STATION" => Ok(Self::RadioStation),
            "PODCAST_SERIES" => Ok(Self::PodcastSeries),
            "TV_SERIES" => Ok(Self::TvSeries),
            "VIDEO_PLAYLIST" => Ok(Self::VideoPlaylist),
            "LIVE_TV" => Ok(Self::LiveTv),
            "MOVIE" => Ok(Self::Movie),
            _ => Err(Error::Internal(format!("Unknown queue type {}", s))),
        }
    }
}

impl ToString for QueueType {
    fn to_string(&self) -> String {
        match self {
            QueueType::Album => "ALBUM",
            QueueType::Playlist => "PLAYLIST",
            QueueType::Audiobook => "AUDIOBOOK",
            QueueType::RadioStation => "RADIO_STATION",
            QueueType::PodcastSeries => "PODCAST_SERIES",
            QueueType::TvSeries => "TV_SERIES",
            QueueType::VideoPlaylist => "VIDEO_PLAYLIST",
            QueueType::LiveTv => "LIVE_TV",
            QueueType::Movie => "MOVIE",
        }
        .to_string()
    }
}

/// Describes the operation to perform with playback while seeking.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ResumeState {
    /// Forces media to start.
    PlaybackStart,
    /// Forces media to pause.
    PlaybackPause,
}

impl FromStr for ResumeState {
    type Err = Error;

    fn from_str(s: &str) -> Result<ResumeState, Error> {
        match s {
            "PLAYBACK_START" | "start" => Ok(ResumeState::PlaybackStart),
            "PLAYBACK_PAUSE" | "pause" => Ok(ResumeState::PlaybackPause),
            _ => Err(Error::Internal(format!("Unknown resume state {}", s))),
        }
    }
}

impl ToString for ResumeState {
    fn to_string(&self) -> String {
        let resume_state = match *self {
            ResumeState::PlaybackStart => "PLAYBACK_START",
            ResumeState::PlaybackPause => "PLAYBACK_PAUSE",
        };

        resume_state.to_string()
    }
}

/// This data structure describes a media stream.
#[derive(Clone, Debug)]
pub struct Media {
    /// Service-specific identifier of the content currently loaded by the media player. This is a
    /// free form string and is specific to the application. In most cases, this will be the URL to
    /// the media, but the sender can choose to pass a string that the receiver can interpret
    /// properly. Max length: 1k.
    pub content_id: String,
    /// Describes the type of media artifact.
    pub stream_type: StreamType,
    /// MIME content type of the media being played.
    pub content_type: String,
    /// Generic, movie, TV show, music track, or photo metadata.
    pub metadata: Option<Metadata>,
    /// Duration of the currently playing stream in seconds.
    pub duration: Option<f32>,
}

impl Media {
    fn encode(&self) -> proxies::Media {
        let metadata = self.metadata.as_ref().map(|m| m.encode());

        proxies::Media {
            content_id: self.content_id.clone(),
            stream_type: self.stream_type.to_string(),
            content_type: self.content_type.clone(),
            metadata,
            duration: self.duration,
        }
    }
}

impl TryFrom<proxies::Media> for Media {
    type Error = Error;

    fn try_from(m: proxies::Media) -> Result<Self, Error> {
        Ok(Self {
            content_id: m.content_id,
            stream_type: StreamType::from_str(m.stream_type.as_ref())?,
            content_type: m.content_type,
            metadata: m.metadata.as_ref().map(TryInto::try_into).transpose()?,
            duration: m.duration,
        })
    }
}

/// One item in a queue
#[derive(Clone, Debug)]
pub struct QueueItem {
    /// The item as media
    pub media: Media,
    /// Receciver-assigned identifier
    pub item_id: Option<u16>,
}

impl QueueItem {
    fn encode(&self) -> proxies::QueueItem {
        proxies::QueueItem {
            active_track_ids: None,
            autoplay: true,
            custom_data: None,
            item_id: None, // Mustn't be set by the sender
            media: self.media.encode(),
            playback_duration: None,
            preload_time: 20.,
            start_time: 0.,
        }
    }
}

impl TryFrom<proxies::QueueItem> for QueueItem {
    type Error = Error;

    fn try_from(value: proxies::QueueItem) -> Result<Self, Error> {
        Ok(Self {
            media: Media::try_from(value.media)?,
            item_id: value.item_id,
        })
    }
}

/// A queue of items to play in sequence
#[derive(Clone, Debug)]
pub struct MediaQueue {
    /// Every item in the queue, in order
    pub items: Vec<QueueItem>,
    /// Array index of the first item to be played
    /// Starts at zero.
    pub start_index: u16,
    /// What the queue represents
    pub queue_type: QueueType,
    /// Will the queue or a track be repeated in a loop
    pub repeat_mode: RepeatMode,
}

impl MediaQueue {
    fn encode(&self) -> proxies::QueueData {
        proxies::QueueData {
            items: self.items.iter().map(|qi| qi.encode()).collect(),
            queue_type: Some(self.queue_type.to_string()),
            repeat_mode: self.repeat_mode.to_string(),
            start_index: self.start_index,
        }
    }
}

/// Describes the current status of the media artifact with respect to the session.
#[derive(Clone, Debug)]
pub struct Status {
    /// Detailed status of every media status entry.
    pub entries: Vec<StatusEntry>,
}

impl Status {
    fn into_entry_for_media_session(self, media_session_id: i32) -> Result<StatusEntry, Error> {
        self.entries
            .into_iter()
            .find(|entry| entry.media_session_id == media_session_id)
            .ok_or_else(|| {
                Error::Internal("Status reply didn't include our media session id".to_owned())
            })
    }
}

impl MediaReply {
    fn into_entry_for_media_session(self, media_session_id: i32) -> Result<StatusEntry, Error> {
        let status: Status = self.try_into()?;
        status.into_entry_for_media_session(media_session_id)
    }
}

impl TryFrom<proxies::StatusReply> for Status {
    type Error = Error;

    fn try_from(status: proxies::StatusReply) -> Result<Self, Error> {
        Ok(Status {
            entries: status
                .status
                .into_iter()
                .map(StatusEntry::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<proxies::MediaReply> for Status {
    type Error = Error;

    fn try_from(repl: proxies::MediaReply) -> Result<Self, Self::Error> {
        match repl {
            MediaReply::Status(status) => status.try_into(),
            MediaReply::LoadCancelled(_) => Err(Error::Internal(
                "Load cancelled by another request".to_owned(),
            )),
            MediaReply::LoadFailed(_) => Err(Error::Internal("Load failed".to_owned())),
            MediaReply::InvalidPlayerState(_) => {
                Err(Error::Internal("Invalid player state".to_owned()))
            }
            MediaReply::InvalidRequest(err) => Err(Error::Internal(format!(
                "Invalid media request ({})",
                err.reason.as_deref().unwrap_or("Unknown")
            ))),
        }
    }
}

/// Status of loading the next media
#[derive(Clone, Debug)]
pub struct ExtendedStatus {
    /// Describes the state of the player.
    pub player_state: ExtendedPlayerState,
    /// Unique ID for the playback of this specific session. This ID is set by the receiver at LOAD
    /// and can be used to identify a specific instance of a playback. For example, two playbacks of
    /// "Wish you were here" within the same session would each have a unique mediaSessionId.
    pub media_session_id: Option<i32>,
    /// Full description of the content that is being played back. Only be returned in a status
    /// messages if the Media has changed.
    pub media: Option<Media>,
}

impl TryFrom<proxies::ExtendedStatus> for ExtendedStatus {
    type Error = Error;

    fn try_from(es: proxies::ExtendedStatus) -> Result<Self, Error> {
        Ok(Self {
            player_state: ExtendedPlayerState::from_str(&es.player_state)?,
            media_session_id: es.media_session_id,
            media: es.media.map(Media::try_from).transpose()?,
        })
    }
}

/// Detailed status of the media artifact with respect to the session.
#[derive(Clone, Debug)]
pub struct StatusEntry {
    /// Unique ID for the playback of this specific session. This ID is set by the receiver at LOAD
    /// and can be used to identify a specific instance of a playback. For example, two playbacks of
    /// "Wish you were here" within the same session would each have a unique mediaSessionId.
    pub media_session_id: i32,
    /// Full description of the content that is being played back. Only be returned in a status
    /// messages if the Media has changed.
    pub media: Option<Media>,
    /// Indicates whether the media time is progressing, and at what rate. This is independent of
    /// the player state since the media time can stop in any state. 1.0 is regular time, 0.5 is
    /// slow motion.
    pub playback_rate: f32,
    /// Describes the state of the player.
    pub player_state: PlayerState,
    /// Id of the current queue item
    pub current_item_id: Option<u16>,
    /// Id of the item currently loading
    pub loading_item_id: Option<u16>,
    /// Id of the item currently preloaded
    pub preloaded_item_id: Option<u16>,
    /// If the player_state is IDLE and the reason it became IDLE is known, this property is
    /// provided. If the player is IDLE because it just started, this property will not be provided.
    /// If the player is in any other state this property should not be provided.
    pub idle_reason: Option<IdleReason>,
    /// An extended status can be used when the player is idle (no playback) but also loading
    /// another media.
    pub extended_status: Option<ExtendedStatus>,
    /// The current position of the media player since the beginning of the content, in seconds.
    /// If this a live stream content, then this field represents the time in seconds from the
    /// beginning of the event that should be known to the player.
    pub current_time: Option<f32>,
    /// Flags describing which media commands the media player supports:
    /// * `1` `Pause`;
    /// * `2` `Seek`;
    /// * `4` `Stream volume`;
    /// * `8` `Stream mute`;
    /// * `16` `Skip forward`;
    /// * `32` `Skip backward`;
    /// * `1 << 12` `Unknown`;
    /// * `1 << 13` `Unknown`;
    /// * `1 << 18` `Unknown`.
    /// Combinations are described as summations; for example, Pause+Seek+StreamVolume+Mute == 15.
    pub supported_media_commands: u32,
    /// Will the queue or a track be repeated in a loop
    pub repeat_mode: Option<RepeatMode>,
    /// List of media queue items
    pub items: Option<Vec<QueueItem>>,
}

impl TryFrom<proxies::Status> for StatusEntry {
    type Error = Error;

    fn try_from(x: proxies::Status) -> Result<Self, Error> {
        Ok(Self {
            media_session_id: x.media_session_id,
            media: x.media.map(TryInto::try_into).transpose()?,
            playback_rate: x.playback_rate,
            player_state: PlayerState::from_str(&x.player_state)?,
            current_item_id: x.current_item_id,
            loading_item_id: x.loading_item_id,
            preloaded_item_id: x.preloaded_item_id,
            idle_reason: x
                .idle_reason
                .as_ref()
                .map(|reason| IdleReason::from_str(reason))
                .transpose()?,
            extended_status: x
                .extended_status
                .map(ExtendedStatus::try_from)
                .transpose()?,
            current_time: x.current_time,
            supported_media_commands: x.supported_media_commands,
            repeat_mode: x
                .repeat_mode
                .as_deref()
                .map(RepeatMode::from_str)
                .transpose()?,
            items: x
                .items
                .map(|items| items.into_iter().map(QueueItem::try_from).collect())
                .transpose()?,
        })
    }
}

/// Describes the load cancelled error.
#[derive(Copy, Clone, Debug)]
pub struct LoadCancelled;

/// Describes the load failed error.
#[derive(Copy, Clone, Debug)]
pub struct LoadFailed;

/// Describes the invalid player state error.
#[derive(Copy, Clone, Debug)]
pub struct InvalidPlayerState;

/// Describes the invalid request error.
#[derive(Clone, Debug)]
pub struct InvalidRequest {
    /// Description of the invalid request reason if available.
    pub reason: Option<String>,
}

/// Represents all currently supported incoming messages that media channel can handle.
#[derive(Clone, Debug)]
pub enum MediaResponse {
    /// Statuses of the currently active media.
    Status(Status),
    /// Sent when the load request was cancelled (a second load request was received).
    LoadCancelled(LoadCancelled),
    /// Sent when the load request failed. The player state will be IDLE.
    LoadFailed(LoadFailed),
    /// Sent when the request by the sender can not be fulfilled because the player is not in a
    /// valid state. For example, if the application has not created a media element yet.
    InvalidPlayerState(InvalidPlayerState),
    /// Error indicating that request is not valid.
    InvalidRequest(InvalidRequest),
    /// Used every time when channel can't parse the message. Associated data contains `type` string
    /// field and raw JSON data returned from cast device.
    NotImplemented(String, serde_json::Value),
}

impl TryFrom<proxies::MediaReply> for MediaResponse {
    type Error = Error;

    fn try_from(repl: proxies::MediaReply) -> Result<Self, Error> {
        match repl {
            MediaReply::Status(status) => Ok(MediaResponse::Status(status.try_into()?)),
            MediaReply::LoadCancelled(proxies::LoadCancelledReply) => {
                Ok(Self::LoadCancelled(LoadCancelled))
            }
            MediaReply::LoadFailed(proxies::LoadFailedReply) => Ok(Self::LoadFailed(LoadFailed)),
            MediaReply::InvalidPlayerState(proxies::InvalidPlayerStateReply) => {
                Ok(Self::InvalidPlayerState(InvalidPlayerState))
            }
            MediaReply::InvalidRequest(err) => {
                Ok(Self::InvalidRequest(InvalidRequest { reason: err.reason }))
            }
        }
    }
}

pub struct MediaChannel<'a, W>
where
    W: AsyncRead + AsyncWrite,
{
    sender: Cow<'a, str>,
    message_manager: Lrc<MessageManager<W>>,
}

impl<'a, W> MediaChannel<'a, W>
where
    W: AsyncRead + AsyncWrite,
{
    pub fn new<S>(sender: S, message_manager: Lrc<MessageManager<W>>) -> MediaChannel<'a, W>
    where
        S: Into<Cow<'a, str>>,
    {
        MediaChannel {
            sender: sender.into(),
            message_manager,
        }
    }

    /// Retrieves status of the cast device media session.
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` identifier of specific app media session;
    /// * `media_session_id` - Media session ID of the media for which the media status should be
    /// returned. If none is provided, then the status for all media session IDs will be provided.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn get_status<S>(
        &self,
        destination: S,
        media_session_id: Option<i32>,
    ) -> Result<Status, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::GetStatusRequest {
                    typ: MESSAGE_TYPE_GET_STATUS.to_string(),

                    media_session_id,
                },
            })
            .await?;

        reply.try_into()
    }

    /// Loads provided media to the application.
    ///
    /// # Arguments
    /// * `destination` - `protocol` of the application to load media with (e.g. `web-1`);
    /// * `session_id` - Current session identifier of the player application;
    /// * `media` - `Media` instance that describes the media we'd like to load.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn load<S>(
        &self,
        destination: S,
        session_id: S,
        media: &Media,
    ) -> Result<Status, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        self.load_with_queue(destination, session_id, media, None)
            .await
    }

    /// Loads provided media to the application.
    ///
    /// # Arguments
    /// * `destination` - `protocol` of the application to load media with (e.g. `web-1`);
    /// * `session_id` - Current session identifier of the player application;
    /// * `media` - `Media` instance that describes the media we'd like to load.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn load_with_queue<S>(
        &self,
        destination: S,
        session_id: S,
        media: &Media,
        queue: Option<&MediaQueue>,
    ) -> Result<Status, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::MediaRequest {
                    session_id: session_id.into().to_string(),
                    typ: MESSAGE_TYPE_LOAD.to_string(),

                    media: media.encode(),

                    current_time: 0_f64,
                    autoplay: true,
                    custom_data: proxies::CustomData::new(),
                    queue_data: queue.map(|qd| qd.encode()),
                },
            })
            .await?;

        // Once media is loaded cast receiver device should emit status update event, or load failed
        // event if something went wrong.
        reply.try_into()
    }

    pub async fn load_queue<S>(
        &self,
        destination: S,
        _session_id: S,
        queue: &MediaQueue,
    ) -> Result<Status, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::QueueLoadRequest {
                    typ: MESSAGE_TYPE_QUEUE_LOAD.to_string(),

                    custom_data: None,
                    items: queue.items.iter().map(|qi| qi.encode()).collect(),
                    queue_type: Some(queue.queue_type.to_string()),
                    repeat_mode: queue.repeat_mode.to_string(),
                    start_index: queue.start_index,
                },
            })
            .await?;

        // Once media is loaded cast receiver device should emit status update event, or load failed
        // event if something went wrong.
        reply.try_into()
    }

    /// Pauses playback of the current content. Triggers a STATUS event notification to all sender
    /// applications.
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to be paused.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn pause<S>(
        &self,
        destination: S,
        media_session_id: i32,
    ) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::PlaybackGenericRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_PAUSE.to_string(),
                    custom_data: proxies::CustomData::new(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    /// Begins playback of the content that was loaded with the load call, playback is continued
    /// from the current time position.
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to be played.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn play<S>(&self, destination: S, media_session_id: i32) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::PlaybackGenericRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_PLAY.to_string(),
                    custom_data: proxies::CustomData::new(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    /// Go to next item in queue
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to be played.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn next<S>(&self, destination: S, media_session_id: i32) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::PlaybackGenericRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_QUEUE_NEXT.to_string(),
                    custom_data: proxies::CustomData::new(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    /// Go to previous item in queue
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to be played.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn prev<S>(&self, destination: S, media_session_id: i32) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::PlaybackGenericRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_QUEUE_PREV.to_string(),
                    custom_data: proxies::CustomData::new(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    /// Update queue parameters (repeat, shuffle…)
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to be played.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn update_queue<S>(
        &self,
        destination: S,
        media_session_id: i32,
        repeat_mode: Option<RepeatMode>,
    ) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::QueueUpdateRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_QUEUE_UPDATE.to_string(),
                    custom_data: proxies::CustomData::new(),
                    sequence_number: None,
                    repeat_mode: repeat_mode.map(|e| e.to_string()).unwrap_or_default(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    /// Stops playback of the current content. Triggers a STATUS event notification to all sender
    /// applications. After this command the content will no longer be loaded and the
    /// media_session_id is invalidated.
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to be stopped.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn stop<S>(&self, destination: S, media_session_id: i32) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::PlaybackGenericRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_STOP.to_string(),
                    custom_data: proxies::CustomData::new(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    /// Sets the current position in the stream. Triggers a STATUS event notification to all sender
    /// applications. If the position provided is outside the range of valid positions for the
    /// current content, then the player should pick a valid position as close to the requested
    /// position as possible.
    ///
    /// # Arguments
    ///
    /// * `destination` - `protocol` of the media application (e.g. `web-1`);
    /// * `media_session_id` - ID of the media session to seek in;
    /// * `current_time` - Time in seconds to seek to.
    ///
    /// # Return value
    ///
    /// Returned `Result` should consist of either `Status` instance or an `Error`.
    pub async fn seek<S>(
        &self,
        destination: S,
        media_session_id: i32,
        current_time: Option<f32>,
        relative_time: Option<f32>,
        resume_state: Option<ResumeState>,
    ) -> Result<StatusEntry, Error>
    where
        S: Into<Cow<'a, str>>,
    {
        let reply: MediaReply = self
            .message_manager
            .send_get_reply(JsonMessage {
                namespace: CHANNEL_NAMESPACE,
                source: &self.sender,
                destination: &destination.into(),
                payload: proxies::PlaybackSeekRequest {
                    media_session_id,
                    typ: MESSAGE_TYPE_SEEK.to_string(),
                    current_time,
                    relative_time,
                    resume_state: resume_state.map(|s| s.to_string()),
                    custom_data: proxies::CustomData::new(),
                },
            })
            .await?;

        reply.into_entry_for_media_session(media_session_id)
    }

    pub fn can_handle(&self, message: &CastMessage) -> bool {
        message.namespace == CHANNEL_NAMESPACE
    }

    pub fn parse(&self, message: &CastMessage) -> Result<MediaResponse, Error> {
        let CastMessagePayload::String(ref payload) = message.payload else {
            return Err(Error::Internal(
                "Binary payload is not supported!".to_string(),
            ));
        };

        let repl: proxies::MediaReply = serde_json::from_str(payload)?;

        repl.try_into()
    }
}
