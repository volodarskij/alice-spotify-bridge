use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::Bytes;
use http_body::Body as HttpBody;
use http_body::Frame;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use tokio::net::TcpListener;

use crate::state::{BridgeCommand, BridgeConfig, BridgeShared, GlagolState};

/// Streaming MP3 body — polls shared.mp3_buffer for new data
pub struct StreamBody {
    shared: Arc<Mutex<BridgeShared>>,
    token: u64,
    bytes_delivered: usize,
    expected_bytes: usize,
    done: bool,
}

impl StreamBody {
    pub fn new(shared: Arc<Mutex<BridgeShared>>, token: u64, expected_bytes: usize) -> Self {
        Self {
            shared,
            token,
            bytes_delivered: 0,
            expected_bytes,
            done: false,
        }
    }
}

impl HttpBody for StreamBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let me = self.get_mut();
        if me.done {
            return Poll::Ready(None);
        }

        let mut shared = me.shared.lock();

        // Check if our stream is still valid
        if me.token != shared.stream_token {
            me.done = true;
            return Poll::Ready(None);
        }

        if let Some(chunk) = shared.mp3_buffer.pop_front() {
            let len = chunk.len();
            me.bytes_delivered += len;
            shared.bytes_delivered += len;
            shared.total_bytes_served += len as u64;
            drop(shared);

            if me.bytes_delivered >= me.expected_bytes {
                me.done = true;
            }

            Poll::Ready(Some(Ok(Frame::data(chunk))))
        } else if me.bytes_delivered >= me.expected_bytes {
            me.done = true;
            Poll::Ready(None)
        } else {
            // No data yet — register waker
            shared.stream_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Full response body type (either streaming or static)
pub enum ResponseBody {
    Stream(StreamBody),
    Static(Option<Bytes>),
}

impl HttpBody for ResponseBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.get_mut() {
            ResponseBody::Stream(body) => Pin::new(body).poll_frame(cx),
            ResponseBody::Static(data) => {
                if let Some(bytes) = data.take() {
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

/// Handle incoming HTTP requests
fn handle_request(
    req: Request<Incoming>,
    shared: Arc<Mutex<BridgeShared>>,
    config: Arc<BridgeConfig>,
    start_time: Instant,
    glagol_connected: Arc<AtomicBool>,
    session_id_valid: Arc<AtomicBool>,
    glagol_state: Arc<Mutex<GlagolState>>,
) -> Response<ResponseBody> {
    let path = req.uri().path();

    match path {
        p if p.starts_with("/stream.mp3") => handle_stream(req, shared),
        "/status" => handle_status(shared, config, start_time, &session_id_valid),
        "/debug" => handle_debug(shared, config, start_time, &session_id_valid, &glagol_state),
        "/health" => handle_health(shared, glagol_connected, session_id_valid),
        "/metrics" => handle_metrics(shared, start_time, &glagol_connected, &session_id_valid),
        "/stop" => handle_stop(shared),
        _ => {
            let body = ResponseBody::Static(Some(Bytes::from("Not Found")));
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(body)
                .unwrap()
        }
    }
}

/// GET /stream.mp3?t={token}
fn handle_stream(
    req: Request<Incoming>,
    shared: Arc<Mutex<BridgeShared>>,
) -> Response<ResponseBody> {
    let token: u64 = req
        .uri()
        .query()
        .and_then(|q| {
            q.split('&')
                .find_map(|p| p.strip_prefix("t="))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0);

    let (valid, expected_bytes, silence, track_name) = {
        let mut s = shared.lock();
        let is_valid = token == s.stream_token && s.streaming;
        if is_valid && s.station_connected_wall.is_none() {
            s.station_connected_wall = Some(std::time::Instant::now());
            s.internal_disconnect = false; // Station connected — external disconnect now triggers auto-pause
            if let Some(start) = s.track_start_wall {
                let connect_delay = start.elapsed().as_millis() as u64;
                // Total offset = time to connect + Station's internal buffer (~2.5s)
                // Station buffers ~2-3s of MP3 before starting playback
                s.station_offset_ms = connect_delay + 2500;
                log::info!("HTTP: Station offset = {}ms (connect={}ms + buffer=2500ms)", s.station_offset_ms, connect_delay);
            }
        }
        (
            is_valid,
            s.expected_bytes,
            s.silence.clone(),
            s.current_track
                .as_ref()
                .map(|t| t.name.clone())
                .unwrap_or_default(),
        )
    };

    if !valid {
        log::info!(
            "HTTP: rejected stale token {} (current: {})",
            token,
            shared.lock().stream_token
        );
        let len = silence.len();
        let body = ResponseBody::Static(Some(silence));
        return Response::builder()
            .header("Content-Type", "audio/mpeg")
            .header("Content-Length", len)
            .header("Connection", "close")
            .body(body)
            .unwrap();
    }

    log::info!(
        "HTTP: Station connected, token={}, track={}, content-length={}",
        token,
        track_name,
        expected_bytes
    );

    let body = ResponseBody::Stream(StreamBody::new(shared, token, expected_bytes));
    Response::builder()
        .header("Content-Type", "audio/mpeg")
        .header("Content-Length", expected_bytes)
        .header("Accept-Ranges", "none")
        .header("Connection", "close")
        .body(body)
        .unwrap()
}

/// GET /status
fn handle_status(
    shared: Arc<Mutex<BridgeShared>>,
    config: Arc<BridgeConfig>,
    start_time: Instant,
    session_id_valid: &Arc<AtomicBool>,
) -> Response<ResponseBody> {
    let s = shared.lock();
    let status = serde_json::json!({
        "service": "librespot-bridge",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_sec": start_time.elapsed().as_secs(),
        "streaming": s.streaming,
        "paused": s.paused,
        "track": s.current_track.as_ref().map(|t| &t.name),
        "artist": s.current_track.as_ref().map(|t| &t.artist),
        "album": s.current_track.as_ref().map(|t| &t.album),
        "cover": s.current_track.as_ref().map(|t| &t.cover),
        "duration_ms": s.current_track.as_ref().map(|t| t.duration_ms),
        "stream_token": s.stream_token,
        "tracks_played": s.tracks_played,
        "total_bytes_served": s.total_bytes_served,
        "station_ip": &config.station_ip,
        "session_id_valid": session_id_valid.load(Ordering::Relaxed),
    });
    drop(s);

    let json = serde_json::to_string_pretty(&status).unwrap();
    let body = ResponseBody::Static(Some(Bytes::from(json)));
    Response::builder()
        .header("Content-Type", "application/json")
        .body(body)
        .unwrap()
}

/// GET /debug
fn handle_debug(
    shared: Arc<Mutex<BridgeShared>>,
    config: Arc<BridgeConfig>,
    start_time: Instant,
    session_id_valid: &Arc<AtomicBool>,
    glagol_state: &Arc<Mutex<GlagolState>>,
) -> Response<ResponseBody> {
    let s = shared.lock();
    let gs = glagol_state.lock();
    let station_progress_ms = gs.progress_ms;
    let station_duration_ms = gs.duration_ms;
    let progress_age_ms = gs.progress_updated
        .map(|t| t.elapsed().as_millis() as u64)
        .unwrap_or(0);
    // Calculate position offset: how far ahead librespot is vs Station
    let position_offset_ms = s.track_start_wall
        .map(|start| {
            let librespot_pos = start.elapsed().as_millis() as f64;
            (librespot_pos - station_progress_ms).max(0.0) as u64
        })
        .unwrap_or(s.station_offset_ms);
    drop(gs);
    let debug = serde_json::json!({
        "service": "librespot-bridge",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_sec": start_time.elapsed().as_secs(),
        "streaming": s.streaming,
        "paused": s.paused,
        "spirc_alive": s.spirc_alive,
        "track": s.current_track.as_ref().map(|t| &t.name),
        "artist": s.current_track.as_ref().map(|t| &t.artist),
        "album": s.current_track.as_ref().map(|t| &t.album),
        "uri": s.current_track.as_ref().map(|t| &t.uri),
        "duration_ms": s.current_track.as_ref().map(|t| t.duration_ms),
        "stream_token": s.stream_token,
        "expected_bytes": s.expected_bytes,
        "bytes_produced": s.bytes_produced,
        "bytes_delivered": s.bytes_delivered,
        "mp3_buffer_chunks": s.mp3_buffer.len(),
        "track_ended": s.track_ended,
        "tracks_played": s.tracks_played,
        "total_bytes_served": s.total_bytes_served,
        "glagol_commands_sent": s.glagol_commands_sent,
        "last_transition_ms": s.last_transition_ms,
        "station_progress_ms": station_progress_ms as u64,
        "station_duration_ms": station_duration_ms as u64,
        "progress_age_ms": progress_age_ms,
        "position_offset_ms": position_offset_ms,
        "station_ip": &config.station_ip,
        "bridge_port": config.bridge_port,
        "mp3_bitrate": config.mp3_bitrate,
        "session_id_valid": session_id_valid.load(Ordering::Relaxed),
    });
    drop(s);

    let json = serde_json::to_string_pretty(&debug).unwrap();
    let body = ResponseBody::Static(Some(Bytes::from(json)));
    Response::builder()
        .header("Content-Type", "application/json")
        .body(body)
        .unwrap()
}

/// GET /health — watchdog-friendly endpoint
fn handle_health(
    shared: Arc<Mutex<BridgeShared>>,
    glagol_connected: Arc<AtomicBool>,
    session_id_valid: Arc<AtomicBool>,
) -> Response<ResponseBody> {
    let spirc_alive = shared.lock().spirc_alive;
    let glagol_ok = glagol_connected.load(Ordering::Relaxed);
    let session_ok = session_id_valid.load(Ordering::Relaxed);
    let healthy = spirc_alive && glagol_ok && session_ok;

    let json = serde_json::json!({
        "healthy": healthy,
        "spirc_alive": spirc_alive,
        "glagol_connected": glagol_ok,
        "session_id_valid": session_ok,
    });

    let status = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    let body = ResponseBody::Static(Some(Bytes::from(
        serde_json::to_string_pretty(&json).unwrap(),
    )));
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(body)
        .unwrap()
}

/// GET /metrics — Prometheus text format
/// Exposes counters that already exist in BridgeShared. No new counters introduced.
/// TODO: spirc_reconnects_total — `reconnect_count` is a local var in main.rs reconnect loop,
///       not stored in shared state. Need to expose via Arc<AtomicU64> to export it.
/// TODO: glagol_reconnects_total — not currently tracked anywhere; glagol.rs reconnects
///       lazily in send_command() on send failure but does not increment a counter.
fn handle_metrics(
    shared: Arc<Mutex<BridgeShared>>,
    start_time: Instant,
    glagol_connected: &Arc<AtomicBool>,
    session_id_valid: &Arc<AtomicBool>,
) -> Response<ResponseBody> {
    let (
        total_bytes_served,
        tracks_played,
        glagol_commands_sent,
        mp3_buffer_chunks,
        bytes_produced,
        bytes_delivered,
        streaming,
        paused,
        spirc_alive,
    ) = {
        let s = shared.lock();
        (
            s.total_bytes_served,
            s.tracks_played,
            s.glagol_commands_sent,
            s.mp3_buffer.len(),
            s.bytes_produced,
            s.bytes_delivered,
            s.streaming,
            s.paused,
            s.spirc_alive,
        )
    };
    let glagol_ok = glagol_connected.load(Ordering::Relaxed);
    let session_ok = session_id_valid.load(Ordering::Relaxed);
    let uptime = start_time.elapsed().as_secs();

    let mut out = String::with_capacity(1024);

    out.push_str("# HELP librespot_bytes_delivered_total Total MP3 bytes sent over /stream.mp3 since process start\n");
    out.push_str("# TYPE librespot_bytes_delivered_total counter\n");
    out.push_str(&format!("librespot_bytes_delivered_total {}\n", total_bytes_served));

    out.push_str("# HELP librespot_tracks_played_total Total tracks started since process start\n");
    out.push_str("# TYPE librespot_tracks_played_total counter\n");
    out.push_str(&format!("librespot_tracks_played_total {}\n", tracks_played));

    out.push_str("# HELP librespot_glagol_commands_sent_total Total Glagol WSS commands sent to Station\n");
    out.push_str("# TYPE librespot_glagol_commands_sent_total counter\n");
    out.push_str(&format!("librespot_glagol_commands_sent_total {}\n", glagol_commands_sent));

    out.push_str("# HELP librespot_buffer_chunks Current MP3 buffer chunk count (queued, not yet sent)\n");
    out.push_str("# TYPE librespot_buffer_chunks gauge\n");
    out.push_str(&format!("librespot_buffer_chunks {}\n", mp3_buffer_chunks));

    out.push_str("# HELP librespot_bytes_produced_current Bytes encoded by MP3 encoder for current track\n");
    out.push_str("# TYPE librespot_bytes_produced_current gauge\n");
    out.push_str(&format!("librespot_bytes_produced_current {}\n", bytes_produced));

    out.push_str("# HELP librespot_bytes_delivered_current Bytes delivered to HTTP client for current track\n");
    out.push_str("# TYPE librespot_bytes_delivered_current gauge\n");
    out.push_str(&format!("librespot_bytes_delivered_current {}\n", bytes_delivered));

    out.push_str("# HELP librespot_uptime_seconds Process uptime in seconds\n");
    out.push_str("# TYPE librespot_uptime_seconds counter\n");
    out.push_str(&format!("librespot_uptime_seconds {}\n", uptime));

    out.push_str("# HELP librespot_streaming Whether bridge is currently streaming a track (1) or idle (0)\n");
    out.push_str("# TYPE librespot_streaming gauge\n");
    out.push_str(&format!("librespot_streaming {}\n", streaming as u8));

    out.push_str("# HELP librespot_paused Whether playback is paused (1) or playing (0)\n");
    out.push_str("# TYPE librespot_paused gauge\n");
    out.push_str(&format!("librespot_paused {}\n", paused as u8));

    out.push_str("# HELP librespot_spirc_alive Whether Spotify Connect (Spirc) session is alive\n");
    out.push_str("# TYPE librespot_spirc_alive gauge\n");
    out.push_str(&format!("librespot_spirc_alive {}\n", spirc_alive as u8));

    out.push_str("# HELP librespot_glagol_connected Whether Glagol WSS connection to Station is up\n");
    out.push_str("# TYPE librespot_glagol_connected gauge\n");
    out.push_str(&format!("librespot_glagol_connected {}\n", glagol_ok as u8));

    out.push_str("# HELP librespot_session_id_valid Whether Yandex session_id is valid for Glagol auth\n");
    out.push_str("# TYPE librespot_session_id_valid gauge\n");
    out.push_str(&format!("librespot_session_id_valid {}\n", session_ok as u8));

    let body = ResponseBody::Static(Some(Bytes::from(out)));
    Response::builder()
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(body)
        .unwrap()
}

/// GET /stop
fn handle_stop(shared: Arc<Mutex<BridgeShared>>) -> Response<ResponseBody> {
    let mut s = shared.lock();
    s.streaming = false;
    s.paused = false;
    s.mp3_buffer.clear();
    if let Some(waker) = s.stream_waker.take() {
        waker.wake();
    }
    drop(s);

    log::info!("HTTP: Force stop via /stop endpoint");
    let body = ResponseBody::Static(Some(Bytes::from("{\"ok\":true}")));
    Response::builder()
        .header("Content-Type", "application/json")
        .body(body)
        .unwrap()
}

/// Shared command sender — updated when Spirc reconnects
pub type SharedCmdSender = Arc<Mutex<Option<tokio::sync::mpsc::Sender<BridgeCommand>>>>;

/// Start the HTTP server
pub async fn run_http_server(
    shared: Arc<Mutex<BridgeShared>>,
    config: Arc<BridgeConfig>,
    cmd_sender: SharedCmdSender,
    glagol_connected: Arc<AtomicBool>,
    session_id_valid: Arc<AtomicBool>,
    glagol_state: Arc<Mutex<GlagolState>>,
) {
    let addr = SocketAddr::from(([0, 0, 0, 0], config.bridge_port));
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind HTTP server");
    let start_time = Instant::now();

    log::info!("HTTP server listening on :{}", config.bridge_port);

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                log::error!("HTTP accept error: {}", e);
                continue;
            }
        };

        let shared = shared.clone();
        let config = config.clone();
        let cmd_sender = cmd_sender.clone();
        let glagol_connected = glagol_connected.clone();
        let session_id_valid = session_id_valid.clone();
        let glagol_state = glagol_state.clone();

        tokio::spawn(async move {
            // Track whether this connection served a /stream.mp3 request
            let served_stream = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let stream_token_at_connect = Arc::new(std::sync::atomic::AtomicU64::new(0));

            let io = TokioIo::new(stream);
            let ss = served_stream.clone();
            let st = stream_token_at_connect.clone();
            let shared_clone = shared.clone();
            let gc = glagol_connected.clone();
            let sv = session_id_valid.clone();
            let gs = glagol_state.clone();
            let service = service_fn(move |req: Request<Incoming>| {
                // Detect /stream.mp3 requests for auto-pause tracking
                if req.uri().path().starts_with("/stream.mp3") {
                    ss.store(true, std::sync::atomic::Ordering::Relaxed);
                    st.store(
                        shared_clone.lock().stream_token,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                let resp = handle_request(
                    req,
                    shared_clone.clone(),
                    config.clone(),
                    start_time,
                    gc.clone(),
                    sv.clone(),
                    gs.clone(),
                );
                async move { Ok::<_, Infallible>(resp) }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                if !e.to_string().contains("connection closed") {
                    log::debug!("HTTP connection error from {}: {}", peer, e);
                }
            }

            // Connection closed — check if we should auto-pause
            if served_stream.load(std::sync::atomic::Ordering::Relaxed) {
                let should_notify = {
                    let s = shared.lock();
                    let token = stream_token_at_connect
                        .load(std::sync::atomic::Ordering::Relaxed);
                    s.streaming
                        && !s.paused
                        && !s.internal_disconnect
                        && token == s.stream_token
                        && !glagol_state.lock().bt_playing
                };
                if should_notify {
                    let token = stream_token_at_connect
                        .load(std::sync::atomic::Ordering::Relaxed);
                    log::info!("HTTP: Station disconnected from stream, sending AutoPause (token={})", token);
                    let tx = cmd_sender.lock().clone();
                    if let Some(tx) = tx {
                        let _ = tx.send(BridgeCommand::AutoPause(token)).await;
                    }
                }
            }
        });
    }
}
