mod encoder;
mod events;
mod glagol;
mod http;
mod sink;
mod state;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use librespot_core::authentication::Credentials;
use librespot_core::cache::Cache;
use librespot_core::config::SessionConfig;
use librespot_core::session::Session;
use librespot_core::SpotifyUri;
use librespot_playback::config::{AudioFormat, Bitrate, PlayerConfig};
use librespot_playback::mixer::{Mixer, MixerConfig, NoOpVolume};
use librespot_playback::mixer::softmixer::SoftMixer;
use librespot_playback::player::Player;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};

use crate::glagol::GlagolClient;
use crate::sink::BridgeSink;
use crate::state::{BridgeCommand, BridgeConfig, BridgeShared, GlagolState, SAMPLE_RATE, CHANNELS};

#[derive(Parser, Debug)]
#[command(name = "librespot-bridge")]
#[command(about = "Spotify Connect bridge for Yandex Station")]
struct Args {
    /// Spotify Connect device name
    #[arg(long, default_value = "Yandex Station")]
    name: String,

    /// Spotify username (for password auth)
    #[arg(long)]
    username: Option<String>,

    /// Spotify password (for password auth)
    #[arg(long)]
    password: Option<String>,

    /// Spotify OAuth access token (alternative to username/password)
    #[arg(long)]
    access_token: Option<String>,

    /// Spotify audio bitrate (96, 160, 320)
    #[arg(long, default_value = "160")]
    bitrate: u32,

    /// Yandex Station IP
    #[arg(long, default_value = "192.168.1.21")]
    station_ip: String,

    /// SBC IP (this device)
    #[arg(long, default_value = "192.168.1.19")]
    sbc_ip: String,

    /// HTTP server port
    #[arg(long, default_value = "8888")]
    bridge_port: u16,

    /// Yandex Session_id cookie for Glagol
    #[arg(long)]
    session_id: String,

    /// Yandex Device ID
    #[arg(long)]
    device_id: String,

    /// Yandex Platform
    #[arg(long, default_value = "cucumber")]
    platform: String,

    /// SOCKS proxy for API calls
    #[arg(long, default_value = "socks5h://127.0.0.1:1070")]
    socks_proxy: String,

    /// MP3 output bitrate (kbps)
    #[arg(long, default_value = "192")]
    mp3_bitrate: u32,

    /// Cache directory
    #[arg(long, default_value = "/tmp/librespot-cache")]
    cache_dir: String,
}

/// Load credentials: prefer cached auth blob, fallback to CLI args
fn load_credentials(args: &Args) -> Credentials {
    // Cached credentials (reusable auth blob saved by librespot after first auth)
    let cred_path = std::path::Path::new(&args.cache_dir).join("credentials.json");
    if cred_path.exists() {
        if let Ok(data) = std::fs::read_to_string(&cred_path) {
            if let Ok(creds) = serde_json::from_str(&data) {
                log::info!("Using cached credentials (reusable auth blob)");
                return creds;
            }
        }
    }

    // Fallback to CLI args
    if let Some(ref token) = args.access_token {
        log::info!("Using access token from CLI (first-time auth)");
        Credentials::with_access_token(token)
    } else if let (Some(ref user), Some(ref pass)) = (&args.username, &args.password) {
        log::info!("Using username/password from CLI");
        Credentials::with_password(user, pass)
    } else {
        panic!("Either --access-token, --username + --password, or cached credentials required");
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    // Install rustls crypto provider (required by librespot's TLS)
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = Args::parse();

    log::info!("librespot-bridge v{}", env!("CARGO_PKG_VERSION"));
    log::info!(
        "Station: {}, Bridge port: {}",
        args.station_ip,
        args.bridge_port
    );

    // Bridge config
    let config = Arc::new(BridgeConfig {
        station_ip: args.station_ip.clone(),
        sbc_ip: args.sbc_ip.clone(),
        bridge_port: args.bridge_port,
        session_id: args.session_id.clone(),
        device_id: args.device_id.clone(),
        platform: args.platform.clone(),
        socks_proxy: args.socks_proxy.clone(),
        mp3_bitrate: args.mp3_bitrate,
    });

    // Generate silence buffer (1.5 seconds)
    let silence_data =
        encoder::generate_silence(1500, SAMPLE_RATE, CHANNELS, args.mp3_bitrate);
    let silence = Bytes::from(silence_data);
    log::info!("Generated {}B MP3 silence buffer", silence.len());

    // Shared state
    let shared = Arc::new(Mutex::new(BridgeShared::new(silence)));

    // Glagol state (updated by WS read task)
    let glagol_state = Arc::new(Mutex::new(GlagolState::new()));

    // Session_id validity tracking
    let session_id_valid = Arc::new(AtomicBool::new(true));

    // BT stopped notification channel
    let (bt_stopped_tx, mut bt_stopped_rx) = mpsc::channel::<()>(4);

    // Glagol client
    let glagol = Arc::new(TokioMutex::new(
        GlagolClient::new(config.clone(), glagol_state.clone(), bt_stopped_tx, session_id_valid.clone()),
    ));

    // Shared command sender — updated on each Spirc session
    let cmd_sender: http::SharedCmdSender = Arc::new(Mutex::new(None));

    // Start HTTP server (persistent — survives Spirc reconnects)
    let http_shared = shared.clone();
    let http_config = config.clone();
    let http_cmd_sender = cmd_sender.clone();
    let http_glagol_connected = glagol.lock().await.connected.clone();
    let http_session_id_valid = session_id_valid.clone();
    let http_glagol_state = glagol_state.clone();
    tokio::spawn(async move {
        http::run_http_server(http_shared, http_config, http_cmd_sender, http_glagol_connected, http_session_id_valid, http_glagol_state).await;
    });

    // BT stopped listener (persistent — survives Spirc reconnects)
    let bt_shared = shared.clone();
    let bt_cmd_sender = cmd_sender.clone();
    tokio::spawn(async move {
        while bt_stopped_rx.recv().await.is_some() {
            let deferred = bt_shared.lock().bt_deferred;
            if deferred {
                bt_shared.lock().bt_deferred = false;
                log::info!("BT stopped, sending deferred radio_play");
                tokio::time::sleep(Duration::from_millis(500)).await;
                let tx = bt_cmd_sender.lock().clone();
                if let Some(tx) = tx {
                    let _ = tx.send(BridgeCommand::SendRadioPlay).await;
                }
            }
        }
    });

    // Connect Glagol and stop any lingering Station playback
    {
        let mut g = glagol.lock().await;
        match g.connect().await {
            Ok(_) => {
                log::info!("Glagol connected");
                if let Err(e) = g.send_stop().await {
                    log::warn!("Glagol initial stop failed: {}", e);
                } else {
                    log::info!("Glagol: sent initial stop to clear stale playback");
                }
            }
            Err(e) => log::error!(
                "Glagol initial connect failed: {} (will retry on first command)",
                e
            ),
        }
    }

    // Player config (reused across reconnects)
    let player_config = PlayerConfig {
        bitrate: match args.bitrate {
            96 => Bitrate::Bitrate96,
            320 => Bitrate::Bitrate320,
            _ => Bitrate::Bitrate160,
        },
        gapless: true,
        ..Default::default()
    };

    let cache_path: std::path::PathBuf = args.cache_dir.clone().into();
    let format = AudioFormat::S16;

    // Spirc reconnect loop
    let mut backoff = Duration::from_secs(2);
    let mut reconnect_count: u64 = 0;

    loop {
        // Load credentials (prefer cached auth blob from previous session)
        let credentials = load_credentials(&args);

        // Create fresh Session + Cache
        let session_config = SessionConfig::default();
        let cache = Cache::new(
            Some(cache_path.clone()),
            None,
            Some(cache_path.clone()),
            None,
        )
        .ok();
        let session = Session::new(session_config, cache);

        // Create Player with BridgeSink (reuses shared state)
        let sink_shared = shared.clone();
        let volume_getter: Box<dyn librespot_playback::mixer::VolumeGetter + Send> = Box::new(NoOpVolume);
        let player = Player::new(
            player_config.clone(),
            session.clone(),
            volume_getter,
            move || Box::new(BridgeSink::new(sink_shared.clone(), format)),
        );

        // Create Mixer
        let mixer = Arc::new(SoftMixer::open(MixerConfig::default()).expect("Failed to create mixer"));

        // Create Spirc
        let connect_config = librespot_connect::ConnectConfig {
            name: args.name.clone(),
            ..Default::default()
        };

        let spirc_result = librespot_connect::Spirc::new(
            connect_config,
            session.clone(),
            credentials,
            player.clone(),
            mixer,
        )
        .await;

        let (spirc_raw, spirc_task) = match spirc_result {
            Ok(v) => v,
            Err(e) => {
                log::error!("Failed to create Spirc: {}, retrying in {:?}...", e, backoff);
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
                continue;
            }
        };
        let spirc = Arc::new(spirc_raw);

        // Position offset and play delay disabled — Spotify server tracks position
        // independently from play command timestamp, ignoring device-reported position
        // and Loading/Buffering state. ~4s offset is architectural (same as BT/Chromecast).

        // Mark Spirc as alive
        shared.lock().spirc_alive = true;

        // L3: session-supervisor — detect a dead/zombie Spotify session and force a
        // reconnect. librespot's spirc_task consumes only the dealer and has no timer
        // branch, so a silently-dead dealer/AP leaves it parked forever (is_invalid()
        // never flips for dealer death, and the parked task never re-checks it). We probe
        // liveness with an UNCACHED spclient metadata round-trip (token probes are cached
        // and useless), and on sustained failure signal a oneshot the reconnect select!
        // watches — which drops the parked spirc_task and recreates Session+Spirc.
        let (death_tx, mut death_rx) = oneshot::channel::<()>();
        let sup_session = session.clone();
        let supervisor_handle = tokio::spawn(async move {
            // A stable public track; the metadata fetch always round-trips (uncached) over
            // the AP/HTTPS path, so it times out when the transparent-proxy path is dead.
            const PROBE_TRACK: &str = "spotify:track:4cOdK2wGLETKBW3PvgPWqT";
            let track = match SpotifyUri::from_uri(PROBE_TRACK) {
                Ok(t) => t,
                Err(e) => {
                    log::warn!("supervisor: bad probe track id: {}", e);
                    return;
                }
            };
            let mut fails: u32 = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if sup_session.is_invalid() {
                    log::warn!("supervisor: session.is_invalid() -> forcing reconnect");
                    let _ = death_tx.send(());
                    return;
                }
                let probe = tokio::time::timeout(
                    Duration::from_secs(10),
                    sup_session.spclient().get_track_metadata(&track),
                )
                .await;
                match probe {
                    Ok(Ok(_)) => {
                        if fails > 0 {
                            log::info!("supervisor: probe recovered");
                        }
                        fails = 0;
                    }
                    _ => {
                        fails += 1;
                        log::warn!("supervisor: liveness probe failed ({}/3)", fails);
                        if fails >= 3 {
                            log::warn!("supervisor: 3 consecutive failures -> forcing reconnect");
                            let _ = death_tx.send(());
                            return;
                        }
                    }
                }
            }
        });

        if reconnect_count > 0 {
            log::info!("Spirc reconnected (attempt #{})", reconnect_count);
            // Send Glagol stop to clear stale playback after reconnect
            let mut g = glagol.lock().await;
            let _ = g.send_stop().await;
        } else {
            log::info!("Spotify Connect ready. Waiting for connections...");
        }

        // Create per-session command channel
        let (cmd_tx, cmd_rx) = mpsc::channel::<BridgeCommand>(32);
        *cmd_sender.lock() = Some(cmd_tx.clone());

        // Get player event channel AFTER Spirc is created
        let player_events = player.get_player_event_channel();

        // Spawn command executor (per-session — needs current spirc)
        let cmd_shared = shared.clone();
        let cmd_glagol = glagol.clone();
        let cmd_config = config.clone();
        let cmd_glagol_state = glagol_state.clone();
        let cmd_spirc = spirc.clone();
        let cmd_handle = tokio::spawn(async move {
            events::command_executor(
                cmd_rx, cmd_shared, cmd_glagol, cmd_config, cmd_glagol_state, cmd_spirc,
            )
            .await;
        });

        // Spawn event loop (per-session — consumes player event channel)
        let event_shared = shared.clone();
        let event_tx = cmd_tx.clone();
        let event_glagol = glagol.clone();
        let event_config = config.clone();
        let event_handle = tokio::spawn(async move {
            events::event_loop(player_events, event_shared, event_tx, event_glagol, event_config).await;
        });

        // Track session start for backoff reset
        let session_start = std::time::Instant::now();

        // Wait for spirc_task to end, supervisor-signalled death, or shutdown signal
        tokio::select! {
            _ = spirc_task => {
                log::warn!("Spirc task ended, preparing to reconnect...");
            }
            _ = &mut death_rx => {
                // Supervisor detected a dead/zombie session. The spirc_task future is
                // dropped by this select! completing (safe: no Drop side effects we rely
                // on); the loop recreates Session+Spirc below.
                log::warn!("Session supervisor signalled death — forcing reconnect");
                let _ = spirc.as_ref().shutdown();
            }
            _ = tokio::signal::ctrl_c() => {
                log::info!("Shutting down...");
                let _ = spirc.as_ref().shutdown();
                break;
            }
        }

        // Spirc disconnected — clean up per-session tasks
        shared.lock().spirc_alive = false;
        supervisor_handle.abort();
        event_handle.abort();
        cmd_handle.abort();
        *cmd_sender.lock() = None;

        // Explicitly close the old session's dealer WebSocket before reconnecting.
        // On a supervisor-forced reconnect (death_rx arm) the select! completes and the
        // `task.run()` future is dropped mid-flight, so SpircTask's own end-of-run cleanup
        // (`session.dealer().close()`, see librespot connect/src/spirc.rs) never executes.
        // The dealer task is detached, so dropping the Session does NOT stop it: the old
        // dealer stays connected to Spotify and keeps receiving routed play/transfer
        // commands, but its Spirc request receiver is gone — every command then logs
        // `failed sending dealer request channel closed` and is silently dropped. The
        // device still shows in Spotify (a fresh Spirc keeps publishing PutStateRequest via
        // spclient), so the user sees the device but cannot start playback, and no detector
        // (spirc_task end / spclient liveness probe / /health) catches it. Closing the old
        // dealer here prevents the orphan. close() is idempotent — a no-op on the normal
        // path where run() already closed it. Spawned (not awaited) so a hung close cannot
        // stall the reconnect; it acts on the old Session only and never touches the new one.
        {
            let old_session = session.clone();
            tokio::spawn(async move {
                old_session.dealer().close().await;
            });
        }

        // Clear streaming state
        {
            let mut s = shared.lock();
            s.streaming = false;
            s.paused = false;
            s.mp3_buffer.clear();
            s.current_track = None;
            s.pending_track = None;
            s.track_ended = false;
            if let Some(waker) = s.stream_waker.take() {
                waker.wake();
            }
        }

        // Reset backoff if session lasted >30s (was a healthy connection)
        if session_start.elapsed() > Duration::from_secs(30) {
            backoff = Duration::from_secs(2);
        }

        reconnect_count += 1;
        log::info!("Reconnecting in {:?}...", backoff);
        tokio::time::sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, Duration::from_secs(60));
    }
}
