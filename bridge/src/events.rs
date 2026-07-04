use std::sync::Arc;

use librespot_connect::Spirc;
use librespot_metadata::audio::item::AudioItem;
use librespot_metadata::audio::item::UniqueFields;
use librespot_playback::player::PlayerEvent;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;

use crate::glagol::GlagolClient;
use crate::state::{BridgeCommand, BridgeConfig, BridgeShared, GlagolState, TrackInfo};

/// Extract TrackInfo from AudioItem
fn track_info_from(item: &AudioItem) -> TrackInfo {
    let (artist, album) = match &item.unique_fields {
        UniqueFields::Track {
            artists, album, ..
        } => {
            let artist_name = artists
                .0
                .first()
                .map(|a| a.name.clone())
                .unwrap_or_default();
            (artist_name, album.clone())
        }
        UniqueFields::Episode { show_name, .. } => (show_name.clone(), String::new()),
        _ => (String::new(), String::new()),
    };

    let cover_url = item
        .covers
        .first()
        .map(|c| c.url.clone())
        .unwrap_or_default();

    TrackInfo {
        name: item.name.clone(),
        duration_ms: item.duration_ms,
        uri: item.uri.clone(),
        artist,
        album,
        cover: cover_url,
    }
}

/// Spawn a background task that waits for Station to finish playing,
/// then sends radio_play for the next track.
/// Returns a JoinHandle that can be aborted to cancel the wait.
fn spawn_wait_and_transition(
    shared: Arc<Mutex<BridgeShared>>,
    glagol: Arc<TokioMutex<GlagolClient>>,
    config: Arc<BridgeConfig>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let (expected, track_name) = {
            let s = shared.lock();
            (
                s.expected_bytes,
                s.current_track
                    .as_ref()
                    .map(|t| t.name.clone())
                    .unwrap_or_default(),
            )
        };

        log::info!(
            "TRANSITION: waiting for Station to finish {} (expected {}B)",
            track_name,
            expected
        );

        // Poll bytes_delivered until Station has received all data
        let start = std::time::Instant::now();
        loop {
            let (delivered, token_changed) = {
                let s = shared.lock();
                // If streaming stopped or token changed, abort wait
                (s.bytes_delivered, !s.streaming || s.paused)
            };

            if delivered >= expected.saturating_sub(1000) {
                break; // Station got all data
            }
            if token_changed {
                log::info!("TRANSITION: cancelled (stream state changed)");
                return;
            }
            if start.elapsed().as_secs() > 30 {
                log::warn!(
                    "TRANSITION: timeout ({}B / {}B), forcing switch",
                    delivered,
                    expected
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }

        let elapsed = start.elapsed();
        log::info!(
            "TRANSITION: stream delivered in {:.1}s",
            elapsed.as_secs_f64()
        );

        // Switch to next track
        let has_next = {
            let mut s = shared.lock();
            if let Some(next) = s.pending_track.take() {
                log::info!("TRANSITION: switching to {}", next.name);
                s.start_new_track(next, 0);
                s.track_ended = false;
                true
            } else {
                log::info!("TRANSITION: no pending track");
                s.track_ended = false;
                false
            }
        };

        if !has_next {
            return;
        }

        // Wake old stream to close
        {
            let s = shared.lock();
            if let Some(waker) = s.stream_waker.as_ref() {
                waker.wake_by_ref();
            }
        }

        let (token, title, artist) = {
            let s = shared.lock();
            (
                s.stream_token,
                s.current_track
                    .as_ref()
                    .map(|t| t.name.clone())
                    .unwrap_or_default(),
                s.current_track
                    .as_ref()
                    .map(|t| t.artist.clone())
                    .unwrap_or_default(),
            )
        };

        let url = format!(
            "http://{}:{}/stream.mp3?t={}",
            config.sbc_ip, config.bridge_port, token
        );

        let mut g = glagol.lock().await;
        if let Err(e) = g.send_stop().await {
            log::warn!("Glagol stop failed: {}", e);
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if let Err(e) = g.send_radio_play(&url, &title, &artist).await {
            log::error!("Glagol radio_play failed: {}", e);
        }
        log::info!("TRANSITION: radio_play sent for {}", title);
    })
}

/// Main event loop
pub async fn event_loop(
    mut player_events: mpsc::UnboundedReceiver<PlayerEvent>,
    shared: Arc<Mutex<BridgeShared>>,
    bridge_tx: mpsc::Sender<BridgeCommand>,
    glagol: Arc<TokioMutex<GlagolClient>>,
    config: Arc<BridgeConfig>,
) {
    log::info!("Event loop started");

    // Handle for the current WaitAndTransition task (if any)
    let mut transition_handle: Option<tokio::task::JoinHandle<()>> = None;

    while let Some(event) = player_events.recv().await {
        match event {
            PlayerEvent::TrackChanged { audio_item } => {
                let info = track_info_from(&audio_item);

                log::info!(
                    "EVENT: TrackChanged — {} by {} ({}ms)",
                    info.name,
                    info.artist,
                    info.duration_ms
                );

                let action = {
                    let mut s = shared.lock();
                    if s.streaming && !s.track_ended {
                        // Currently playing, not ended — this is preload or skip
                        s.pending_track = Some(info);
                        log::info!("EVENT: Saving as pending track");
                        0 // no action
                    } else if s.track_ended {
                        // EndOfTrack already fired — natural transition
                        s.pending_track = Some(info);
                        log::info!("EVENT: TrackChanged after EndOfTrack — queued for transition");
                        1 // start/continue wait-and-transition
                    } else {
                        // First track or explicit play
                        s.start_new_track(info, 0);
                        2 // immediate radio_play
                    }
                };

                match action {
                    1 => {
                        // Start WaitAndTransition if not already running
                        if transition_handle.as_ref().map_or(true, |h| h.is_finished()) {
                            transition_handle = Some(spawn_wait_and_transition(
                                shared.clone(),
                                glagol.clone(),
                                config.clone(),
                            ));
                        }
                    }
                    2 => {
                        // Cancel any pending transition
                        if let Some(h) = transition_handle.take() {
                            h.abort();
                        }
                        let _ = bridge_tx.send(BridgeCommand::SendRadioPlay).await;
                        log::info!("EVENT: radio_play sent");
                    }
                    _ => {}
                }
            }

            PlayerEvent::EndOfTrack { .. } => {
                let start_transition = {
                    let mut s = shared.lock();
                    s.track_ended = true;
                    if s.pending_track.is_some() {
                        log::info!("EVENT: EndOfTrack + pending track — starting transition");
                        true
                    } else {
                        log::info!("EVENT: EndOfTrack — waiting for TrackChanged");
                        false
                    }
                };

                if start_transition {
                    // Cancel old transition if any
                    if let Some(h) = transition_handle.take() {
                        h.abort();
                    }
                    transition_handle = Some(spawn_wait_and_transition(
                        shared.clone(),
                        glagol.clone(),
                        config.clone(),
                    ));
                }
            }

            PlayerEvent::Paused { position_ms, .. } => {
                log::info!("EVENT: Paused at {}ms", position_ms);

                // Cancel pending transition
                if let Some(h) = transition_handle.take() {
                    h.abort();
                    log::info!("EVENT: Cancelled pending transition due to pause");
                }

                {
                    let mut s = shared.lock();
                    s.paused = true;
                    s.track_ended = false;
                    s.internal_disconnect = true;
                    s.stream_token += 1;
                    s.mp3_buffer.clear();
                    if let Some(waker) = s.stream_waker.take() {
                        waker.wake();
                    }
                }
                let _ = bridge_tx.send(BridgeCommand::Stop).await;
            }

            PlayerEvent::Playing { position_ms, .. } => {
                let send = {
                    let mut s = shared.lock();
                    if s.paused {
                        s.paused = false;
                        s.track_ended = false;

                        if let Some(pending) = s.pending_track.take() {
                            log::info!(
                                "EVENT: Playing after skip — switching to {}",
                                pending.name
                            );
                            s.start_new_track(pending, position_ms);
                        } else {
                            log::info!("EVENT: Resumed at {}ms", position_ms);
                            if let Some(ref track) = s.current_track {
                                s.expected_bytes = BridgeShared::calc_expected_bytes(
                                    track.duration_ms,
                                    position_ms,
                                );
                            }
                            s.internal_disconnect = true;
                            s.stream_token += 1;
                            s.mp3_buffer.clear();
                            s.bytes_produced = 0;
                            s.bytes_delivered = 0;
                        }
                        true
                    } else {
                        false
                    }
                };
                if send {
                    let _ = bridge_tx.send(BridgeCommand::SendRadioPlay).await;
                }
            }

            PlayerEvent::VolumeChanged { volume } => {
                let station_vol = volume as f64 / 65535.0;
                log::info!("EVENT: Volume {} ({:.0}%)", volume, station_vol * 100.0);
                let _ = bridge_tx.send(BridgeCommand::SetVolume(station_vol)).await;
            }

            PlayerEvent::Seeked { position_ms, .. } => {
                let is_internal = {
                    let mut s = shared.lock();
                    if s.internal_seek {
                        s.internal_seek = false;
                        true
                    } else {
                        false
                    }
                };

                if is_internal {
                    log::info!("EVENT: Seeked to {}ms (internal position correction — ignored)", position_ms);
                } else {
                    log::info!("EVENT: Seeked to {}ms", position_ms);
                    if let Some(h) = transition_handle.take() {
                        h.abort();
                    }
                    {
                        let mut s = shared.lock();
                        s.track_ended = false;
                        s.reset_stream(position_ms);
                    }
                    let _ = bridge_tx.send(BridgeCommand::SendRadioPlay).await;
                }
            }

            PlayerEvent::Stopped { .. } => {
                let mut s = shared.lock();
                if s.pending_track.is_some() {
                    log::info!("EVENT: Stopped (skip in progress, keeping pending)");
                    s.paused = true;
                } else {
                    log::info!("EVENT: Stopped");
                    // Cancel transition
                    if let Some(h) = transition_handle.take() {
                        h.abort();
                    }
                    s.streaming = false;
                    s.paused = false;
                    s.track_ended = false;
                    s.current_track = None;
                    s.mp3_buffer.clear();
                    if let Some(waker) = s.stream_waker.take() {
                        waker.wake();
                    }
                }
            }

            PlayerEvent::Unavailable { .. } => {
                log::warn!("EVENT: Track unavailable");
            }

            _ => {}
        }
    }

    log::info!("Event loop ended");
}

/// Command executor — handles bridge commands (radio_play, stop, volume, auto-pause)
pub async fn command_executor(
    mut rx: mpsc::Receiver<BridgeCommand>,
    shared: Arc<Mutex<BridgeShared>>,
    glagol: Arc<TokioMutex<GlagolClient>>,
    config: Arc<BridgeConfig>,
    glagol_state: Arc<Mutex<GlagolState>>,
    spirc: Arc<Spirc>,
) {
    log::info!("Command executor started");

    while let Some(cmd) = rx.recv().await {
        match cmd {
            BridgeCommand::SendRadioPlay => {
                // Check if BT is active on Station — defer radio_play
                if glagol_state.lock().bt_playing {
                    log::info!("BT active on Station, deferring radio_play");
                    shared.lock().bt_deferred = true;
                    continue;
                }

                // Wake old stream
                {
                    let s = shared.lock();
                    if let Some(waker) = s.stream_waker.as_ref() {
                        waker.wake_by_ref();
                    }
                }

                let (token, title, artist) = {
                    let s = shared.lock();
                    (
                        s.stream_token,
                        s.current_track
                            .as_ref()
                            .map(|t| t.name.clone())
                            .unwrap_or_default(),
                        s.current_track
                            .as_ref()
                            .map(|t| t.artist.clone())
                            .unwrap_or_default(),
                    )
                };

                let url = format!(
                    "http://{}:{}/stream.mp3?t={}",
                    config.sbc_ip, config.bridge_port, token
                );

                let mut g = glagol.lock().await;
                if let Err(e) = g.send_stop().await {
                    log::warn!("Glagol stop failed: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if let Err(e) = g.send_radio_play(&url, &title, &artist).await {
                    log::error!("Glagol radio_play failed: {}", e);
                }

                shared.lock().glagol_commands_sent = g.commands_sent;
            }

            BridgeCommand::Stop => {
                let mut g = glagol.lock().await;
                if let Err(e) = g.send_stop().await {
                    log::warn!("Glagol stop failed: {}", e);
                }
            }

            BridgeCommand::SetVolume(vol) => {
                let mut g = glagol.lock().await;
                if let Err(e) = g.send_set_volume(vol).await {
                    log::warn!("Glagol setVolume failed: {}", e);
                }
            }

            BridgeCommand::AutoPause(pause_token) => {
                // Debounce: wait 2s, then check if Station is still disconnected
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                let should_pause = {
                    let s = shared.lock();
                    s.streaming
                        && !s.paused
                        && !s.internal_disconnect
                        && s.stream_token == pause_token
                };

                if should_pause {
                    if glagol_state.lock().bt_playing {
                        log::info!("Auto-pause: cancelled (BT active on Station, token={})", pause_token);
                    } else {
                        log::info!("Auto-pause: Station disconnected, pausing Spotify (token={})", pause_token);
                        if let Err(e) = spirc.pause() {
                            log::warn!("Auto-pause: spirc.pause() failed: {}", e);
                        }
                        let mut g = glagol.lock().await;
                        let _ = g.send_stop().await;
                    }
                } else {
                    log::debug!("Auto-pause: cancelled (state changed during debounce)");
                }
            }

            BridgeCommand::ForceStop => {
                {
                    let mut s = shared.lock();
                    s.streaming = false;
                    s.paused = false;
                    s.mp3_buffer.clear();
                    if let Some(waker) = s.stream_waker.take() {
                        waker.wake();
                    }
                }
                let mut g = glagol.lock().await;
                let _ = g.send_stop().await;
            }
        }
    }
}
