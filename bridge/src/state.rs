use bytes::Bytes;
use std::collections::VecDeque;
use std::task::Waker;
use std::time::Instant;

/// Track metadata
#[derive(Debug, Clone)]
pub struct TrackInfo {
    pub name: String,
    pub artist: String,
    pub album: String,
    pub cover: String,
    pub uri: String,
    pub duration_ms: u32,
}

/// MP3 encoding constants
pub const MP3_BITRATE: u32 = 192; // kbps
pub const MP3_BYTES_PER_SEC: usize = (MP3_BITRATE as usize * 1000) / 8; // 24000
pub const SAMPLE_RATE: u32 = 44100;
pub const CHANNELS: u32 = 2;

/// Shared state between BridgeSink (sync player thread) and async tasks (HTTP, Glagol, events)
pub struct BridgeShared {
    // Stream state
    pub mp3_buffer: VecDeque<Bytes>,
    pub stream_waker: Option<Waker>,
    pub stream_token: u64,
    pub expected_bytes: usize,
    pub bytes_produced: usize, // MP3 bytes produced by encoder for current track
    pub bytes_delivered: usize, // MP3 bytes delivered to HTTP client

    // Track state
    pub current_track: Option<TrackInfo>,
    pub pending_track: Option<TrackInfo>,
    pub track_ended: bool, // EndOfTrack received for current track

    // Control
    pub streaming: bool,
    pub paused: bool,

    // Timing — for progress bar sync
    pub track_start_wall: Option<Instant>, // when Sink::start() was called
    pub station_connected_wall: Option<Instant>, // when Station HTTP connected
    pub station_offset_ms: u64, // estimated delay: progress_bar - actual_playback
    pub internal_seek: bool, // reserved for future position correction

    // Auto-pause: true when WE disconnect the stream (skip/seek/pause), false otherwise
    pub internal_disconnect: bool,
    // BT detection: true when radio_play was deferred due to BT active on Station
    pub bt_deferred: bool,

    // Health
    pub spirc_alive: bool,

    // Diagnostics
    pub tracks_played: u64,
    pub total_bytes_served: u64,
    pub glagol_commands_sent: u64,
    pub last_transition_ms: u64,

    // Silence buffer (pre-generated MP3 silence)
    pub silence: Bytes,
}

impl BridgeShared {
    pub fn new(silence: Bytes) -> Self {
        Self {
            mp3_buffer: VecDeque::with_capacity(64),
            stream_waker: None,
            stream_token: 0,
            expected_bytes: 0,
            bytes_produced: 0,
            bytes_delivered: 0,

            current_track: None,
            pending_track: None,
            track_ended: false,

            streaming: false,
            paused: false,

            track_start_wall: None,
            station_connected_wall: None,
            station_offset_ms: 0,
            internal_seek: false,

            internal_disconnect: false,
            bt_deferred: false,

            spirc_alive: false,

            tracks_played: 0,
            total_bytes_served: 0,
            glagol_commands_sent: 0,
            last_transition_ms: 0,

            silence,
        }
    }

    /// Push MP3 data into buffer and wake HTTP stream if waiting
    pub fn push_mp3(&mut self, data: Vec<u8>) {
        if data.is_empty() {
            return;
        }
        self.bytes_produced += data.len();
        self.mp3_buffer.push_back(Bytes::from(data));
        if let Some(waker) = self.stream_waker.take() {
            waker.wake();
        }
    }

    /// Calculate expected Content-Length for a track
    pub fn calc_expected_bytes(duration_ms: u32, position_ms: u32) -> usize {
        let remaining_ms = duration_ms.saturating_sub(position_ms) as usize;
        // Add 1 second padding to avoid truncation, ceil to whole seconds
        let seconds = (remaining_ms + 999) / 1000 + 1;
        seconds * MP3_BYTES_PER_SEC
    }

    /// Start a new track stream
    pub fn start_new_track(&mut self, track: TrackInfo, position_ms: u32) {
        self.internal_disconnect = true;
        self.stream_token += 1;
        self.mp3_buffer.clear();
        self.bytes_produced = 0;
        self.bytes_delivered = 0;
        self.expected_bytes = Self::calc_expected_bytes(track.duration_ms, position_ms);
        self.current_track = Some(track);
        self.track_ended = false;
        self.streaming = true;
        self.paused = false;
        self.tracks_played += 1;
        self.track_start_wall = Some(std::time::Instant::now());
        self.station_connected_wall = None;

        // Wake any pending stream (it will see token mismatch and close)
        if let Some(waker) = self.stream_waker.take() {
            waker.wake();
        }
    }

    /// Reset stream for seek
    pub fn reset_stream(&mut self, position_ms: u32) {
        self.internal_disconnect = true;
        self.stream_token += 1;
        self.mp3_buffer.clear();
        self.bytes_produced = 0;
        self.bytes_delivered = 0;
        if let Some(ref track) = self.current_track {
            self.expected_bytes = Self::calc_expected_bytes(track.duration_ms, position_ms);
        }
        self.track_ended = false;
        self.station_connected_wall = None;
        self.track_start_wall = Some(std::time::Instant::now());

        if let Some(waker) = self.stream_waker.take() {
            waker.wake();
        }
    }
}

/// Commands from EventHandler to async command executor
#[derive(Debug)]
pub enum BridgeCommand {
    SendRadioPlay,
    Stop,
    SetVolume(f64),
    AutoPause(u64),
    #[allow(dead_code)]
    ForceStop,
}

/// Glagol Station state (updated from WebSocket messages)
pub struct GlagolState {
    pub player_type: Option<String>,
    pub bt_playing: bool,
    pub has_pause: bool,
    pub volume: f64,
    pub last_message: Option<Instant>,
    // Station playback progress (from playerState)
    pub progress_ms: f64,
    pub duration_ms: f64,
    pub progress_updated: Option<Instant>,
}

impl GlagolState {
    pub fn new() -> Self {
        Self {
            player_type: None,
            bt_playing: false,
            has_pause: false,
            volume: 0.0,
            last_message: None,
            progress_ms: 0.0,
            duration_ms: 0.0,
            progress_updated: None,
        }
    }
}

/// Bridge configuration
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub station_ip: String,
    pub sbc_ip: String,
    pub bridge_port: u16,
    pub session_id: String,
    pub device_id: String,
    pub platform: String,
    pub socks_proxy: String,
    pub mp3_bitrate: u32,
}
