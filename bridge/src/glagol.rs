use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex as SyncMutex;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::state::{BridgeConfig, GlagolState};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsTx = futures::stream::SplitSink<WsStream, Message>;

/// Glagol WebSocket client for Yandex Station control
pub struct GlagolClient {
    config: Arc<BridgeConfig>,
    ws_tx: Option<Arc<TokioMutex<WsTx>>>,
    cached_token: Option<(String, Instant)>,
    http_client: reqwest::Client,
    pub connected: Arc<AtomicBool>,
    pub session_id_valid: Arc<AtomicBool>,
    pub commands_sent: u64,
    glagol_state: Arc<SyncMutex<GlagolState>>,
    bt_stopped_tx: mpsc::Sender<()>,
}

impl GlagolClient {
    pub fn new(
        config: Arc<BridgeConfig>,
        glagol_state: Arc<SyncMutex<GlagolState>>,
        bt_stopped_tx: mpsc::Sender<()>,
        session_id_valid: Arc<AtomicBool>,
    ) -> Self {
        // Build HTTP client with SOCKS proxy for Yandex API calls
        let mut client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .danger_accept_invalid_certs(true);

        if !config.socks_proxy.is_empty() {
            if let Ok(proxy) = reqwest::Proxy::all(&config.socks_proxy) {
                client_builder = client_builder.proxy(proxy);
            }
        }

        let http_client = client_builder.build().unwrap_or_default();

        Self {
            config,
            ws_tx: None,
            cached_token: None,
            http_client,
            connected: Arc::new(AtomicBool::new(false)),
            session_id_valid,
            commands_sent: 0,
            glagol_state,
            bt_stopped_tx,
        }
    }

    /// Get Glagol token (cached for 25s)
    async fn get_token(&mut self) -> Result<String, String> {
        if let Some((ref token, expiry)) = self.cached_token {
            if Instant::now() < expiry {
                return Ok(token.clone());
            }
        }

        let url = format!(
            "https://quasar.yandex.ru/glagol/token?device_id={}&platform={}",
            self.config.device_id, self.config.platform
        );

        let resp = self
            .http_client
            .get(&url)
            .header("Cookie", format!("Session_id={}", self.config.session_id))
            .send()
            .await
            .map_err(|e| format!("token request failed: {}", e))?;

        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            log::error!("==================================================");
            log::error!("YANDEX SESSION_ID EXPIRED! Token request returned 401.");
            log::error!("Update --session-id and restart the bridge.");
            log::error!("==================================================");
            self.session_id_valid.store(false, Ordering::Relaxed);
            return Err("session_id expired (401)".into());
        }
        if !status.is_success() {
            return Err(format!("token request failed: HTTP {}", status));
        }
        self.session_id_valid.store(true, Ordering::Relaxed);

        let body: Value = resp
            .json()
            .await
            .map_err(|e| format!("token parse failed: {}", e))?;

        let token = body["token"]
            .as_str()
            .ok_or("no token in response")?
            .to_string();

        self.cached_token = Some((token.clone(), Instant::now() + Duration::from_secs(25)));
        log::debug!("Glagol token refreshed");

        Ok(token)
    }

    /// Connect to Yandex Station WebSocket
    pub async fn connect(&mut self) -> Result<(), String> {
        // Clean up previous connection
        self.ws_tx = None;
        self.connected.store(false, Ordering::Relaxed);

        let url = format!("wss://{}:1961", self.config.station_ip);

        // Use native-tls with disabled cert verification for self-signed Station cert
        let tls_connector = native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|e| format!("TLS error: {}", e))?;

        let connector =
            tokio_tungstenite::Connector::NativeTls(tls_connector);

        let (ws_stream, _) = tokio_tungstenite::connect_async_tls_with_config(
            &url,
            None,
            false,
            Some(connector),
        )
        .await
        .map_err(|e| format!("WebSocket connect failed: {}", e))?;

        let (tx, rx) = ws_stream.split();
        let ws_tx = Arc::new(TokioMutex::new(tx));
        self.ws_tx = Some(ws_tx.clone());
        self.connected.store(true, Ordering::Relaxed);

        // Spawn read task — processes incoming Station state messages
        let read_connected = self.connected.clone();
        let read_state = self.glagol_state.clone();
        let read_bt_tx = self.bt_stopped_tx.clone();
        tokio::spawn(async move {
            glagol_read_task(rx, read_connected, read_state, read_bt_tx).await;
        });

        // Spawn ping task — keeps WebSocket alive
        let ping_connected = self.connected.clone();
        let ping_tx = ws_tx.clone();
        tokio::spawn(async move {
            glagol_ping_task(ping_tx, ping_connected).await;
        });

        log::info!("Glagol: connected to {}", self.config.station_ip);
        Ok(())
    }

    /// Send a command via WebSocket
    pub async fn send_command(&mut self, payload: Value) -> Result<(), String> {
        if !self.connected.load(Ordering::Relaxed) || self.ws_tx.is_none() {
            self.connect().await?;
        }

        let token = self.get_token().await?;

        let msg = serde_json::json!({
            "conversationToken": token,
            "id": uuid_simple(),
            "sentTime": epoch_ms(),
            "payload": payload,
        });

        let text = serde_json::to_string(&msg).map_err(|e| e.to_string())?;

        if let Some(ref ws_tx) = self.ws_tx {
            let mut tx = ws_tx.lock().await;
            match tx.send(Message::Text(text.clone().into())).await {
                Ok(_) => {
                    self.commands_sent += 1;
                    Ok(())
                }
                Err(e) => {
                    log::error!("Glagol send failed: {}, reconnecting", e);
                    drop(tx);
                    self.connect().await?;
                    if let Some(ref ws_tx) = self.ws_tx {
                        let mut tx = ws_tx.lock().await;
                        tx.send(Message::Text(text.into()))
                            .await
                            .map_err(|e| format!("retry failed: {}", e))?;
                        self.commands_sent += 1;
                        Ok(())
                    } else {
                        Err("reconnect failed".into())
                    }
                }
            }
        } else {
            Err("not connected".into())
        }
    }

    /// Send radio_play command to Station
    pub async fn send_radio_play(
        &mut self,
        stream_url: &str,
        title: &str,
        subtitle: &str,
    ) -> Result<(), String> {
        let radio_data = serde_json::json!({
            "streamUrl": stream_url,
            "force_restart_player": true,
            "title": title,
            "subtitle": subtitle,
        });

        let proto = protobuf_dumps(&[
            (1, "radio_play"),
            (2, &serde_json::to_string(&radio_data).unwrap()),
        ]);

        let b64 = BASE64.encode(&proto);

        self.send_command(serde_json::json!({
            "command": "externalCommandBypass",
            "data": b64,
        }))
        .await?;

        log::info!("Glagol: radio_play sent, title={}", title);
        Ok(())
    }

    /// Send stop command
    pub async fn send_stop(&mut self) -> Result<(), String> {
        self.send_command(serde_json::json!({ "command": "stop" }))
            .await
    }

    /// Send setVolume command
    pub async fn send_set_volume(&mut self, volume: f64) -> Result<(), String> {
        self.send_command(serde_json::json!({
            "command": "setVolume",
            "volume": volume,
        }))
        .await
    }
}

/// Background task: read WebSocket messages from Station, update GlagolState
async fn glagol_read_task(
    mut rx: futures::stream::SplitStream<WsStream>,
    connected: Arc<AtomicBool>,
    state: Arc<SyncMutex<GlagolState>>,
    bt_stopped_tx: mpsc::Sender<()>,
) {
    while let Some(result) = rx.next().await {
        match result {
            Ok(Message::Text(text)) => {
                if let Ok(msg) = serde_json::from_str::<Value>(&text) {
                    parse_station_state(&msg, &state, &bt_stopped_tx);
                }
            }
            Ok(Message::Close(_)) => {
                log::info!("Glagol: Station closed WebSocket");
                break;
            }
            Ok(_) => {} // Ping/Pong/Binary — ignore
            Err(e) => {
                log::warn!("Glagol read error: {}", e);
                break;
            }
        }
    }

    connected.store(false, Ordering::Relaxed);
    log::info!("Glagol: read task ended, will reconnect on next command");
}

/// Parse Station state from Glagol WebSocket message
fn parse_station_state(
    msg: &Value,
    state: &Arc<SyncMutex<GlagolState>>,
    bt_stopped_tx: &mpsc::Sender<()>,
) {
    let ps = match msg.get("state").and_then(|s| s.get("playerState")) {
        Some(ps) => ps,
        None => return,
    };

    let player_type = ps.get("playerType").and_then(|v| v.as_str()).map(String::from);
    let has_pause = ps.get("hasPause").and_then(|v| v.as_bool()).unwrap_or(false);
    let volume = msg
        .get("state")
        .and_then(|s| s.get("volume"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    // Playback progress (Glagol reports in seconds as f64)
    let progress = ps.get("progress").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let duration = ps.get("duration").and_then(|v| v.as_f64()).unwrap_or(0.0);

    let new_bt = player_type.as_deref() == Some("bluetooth") && has_pause;

    let was_bt = {
        let mut s = state.lock();
        let was = s.bt_playing;
        s.player_type = player_type;
        s.has_pause = has_pause;
        s.bt_playing = new_bt;
        s.volume = volume;
        s.last_message = Some(Instant::now());
        s.progress_ms = progress * 1000.0;
        s.duration_ms = duration * 1000.0;
        s.progress_updated = Some(Instant::now());
        was
    };

    // BT just stopped — notify listeners
    if was_bt && !new_bt {
        log::info!("Glagol: BT playback stopped");
        let _ = bt_stopped_tx.try_send(());
    }
}

/// Background task: send WebSocket pings every 30s to keep connection alive
async fn glagol_ping_task(
    ws_tx: Arc<TokioMutex<WsTx>>,
    connected: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.tick().await; // skip first immediate tick

    loop {
        interval.tick().await;
        if !connected.load(Ordering::Relaxed) {
            break;
        }

        let mut tx = ws_tx.lock().await;
        if tx.send(Message::Ping(vec![].into())).await.is_err() {
            log::warn!("Glagol: ping failed, connection lost");
            drop(tx);
            connected.store(false, Ordering::Relaxed);
            break;
        }
    }

    log::debug!("Glagol: ping task ended");
}

/// Protobuf wire format encoding
fn protobuf_dumps(entries: &[(u32, &str)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for &(tag, value) in entries {
        let encoded = value.as_bytes();
        buf.push(((tag << 3) | 2) as u8);
        let mut len = encoded.len();
        while len > 127 {
            buf.push((len as u8 & 0x7F) | 0x80);
            len >>= 7;
        }
        buf.push(len as u8);
        buf.extend_from_slice(encoded);
    }
    buf
}

fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:032x}", t)
}

fn epoch_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
