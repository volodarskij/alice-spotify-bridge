# librespot-bridge — Technical Specification

> **При доработках обязательно искать недостающую информацию в сети** — документация librespot, Glagol протокол, Spotify Connect API часто обновляются. Не полагаться только на этот документ.

## Overview

Single Rust binary replacing the 4-process Node.js bridge (librespot → pv → lame → Node.js).
Streams Spotify audio to Yandex Station via Glagol protocol.

**Binary**: `/usr/local/bin/librespot-bridge` (14MB, aarch64-musl, statically linked)
**Replaces**: `spotify_hls.js` + external `librespot` + `pv` + `lame`

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│              librespot-bridge (one process)       │
│                                                   │
│  Spotify CDN → decrypt → decode → PCM (f64)       │
│                                    ↓              │
│  BridgeSink (impl Sink)                           │
│    ├─ Converter: f64 → i16                        │
│    ├─ Rate limiter: 500ms pre-buffer              │
│    └─ Mp3Encoder: i16 → MP3 192kbps CBR           │
│                        ↓                          │
│  BridgeShared (Arc<Mutex>)                        │
│    └─ mp3_buffer: VecDeque<Bytes>                 │
│                        ↓                          │
│  HTTP Server (hyper, :8888)                       │
│    └─ StreamBody polls mp3_buffer                 │
│                        ↓                          │
│  Glagol WSS Client → radio_play → Station         │
│                                                   │
│  PlayerEvent channel → EventHandler               │
│    └─ spawns WaitAndTransition tasks              │
│                                                   │
│  BridgeCommand channel → CommandExecutor          │
│    └─ SendRadioPlay, Stop, SetVolume, AutoPause   │
│                                                   │
│  Glagol read task → parses playerState → GlagolState│
│  Glagol ping task → WS ping every 30s             │
│  BT stopped listener → deferred radio_play        │
└──────────────────────────────────────────────────┘
```

### Thread Model

| Thread/Task | Type | Role |
|-------------|------|------|
| Player thread | OS thread (librespot) | Calls `Sink::write()` synchronously |
| tokio runtime | Async tasks | HTTP server, Glagol, events, transitions |
| Event loop | tokio task | Processes `PlayerEvent`, dispatches commands |
| Command executor | tokio task | Executes Glagol commands (stop, radio_play, volume, auto-pause) |
| WaitAndTransition | spawned tokio task | Polls bytes_delivered, triggers next track |
| HTTP connections | spawned per-connection | Serves `/stream.mp3`, `/status`, `/debug`, `/stop`; detects Station disconnect |
| Glagol read task | spawned tokio task | Reads WS messages from Station, updates `GlagolState` |
| Glagol ping task | spawned tokio task | Sends WS ping every 30s to keep connection alive |
| BT stopped listener | spawned tokio task | Sends deferred `radio_play` when BT playback ends on Station |

### Shared State (BridgeShared)

Protected by `parking_lot::Mutex` (sync, works across sync Sink + async tasks).

Key fields:
- `mp3_buffer: VecDeque<Bytes>` — MP3 data queue (Sink writes, HTTP reads)
- `stream_waker: Option<Waker>` — wakes HTTP StreamBody when new data arrives
- `stream_token: u64` — incremented on track change/seek/pause; old HTTP clients see mismatch and close
- `expected_bytes: usize` — Content-Length for current track
- `bytes_delivered: usize` — bytes sent to Station (for WaitAndTransition)
- `pending_track: Option<TrackInfo>` — next track metadata (from preload or skip)
- `track_ended: bool` — EndOfTrack received
- `internal_disconnect: bool` — true when WE disconnect the stream (skip/seek/pause); prevents false auto-pause triggers
- `bt_deferred: bool` — true when radio_play was deferred due to BT active on Station

### Glagol State (GlagolState)

Separate `parking_lot::Mutex<GlagolState>`, updated by the Glagol read task from WebSocket messages.

Key fields:
- `player_type: Option<String>` — Station's current player ("bluetooth", "radio", etc.)
- `bt_playing: bool` — `player_type == "bluetooth" && has_pause == true`
- `has_pause: bool` — Station is currently playing
- `volume: f64` — Station hardware volume
- `last_message: Option<Instant>` — last WS message timestamp

---

## Audio Pipeline

### PCM Source
- librespot decodes Spotify OGG/Vorbis → PCM f64 samples
- `Sink::write(packet: AudioPacket, converter: &mut Converter)` called per packet
- `converter.f64_to_s16()` converts to 16-bit signed interleaved stereo

### Rate Limiter (in Sink::write)
```
samples_written tracks total stereo samples since Sink::start()
audio_ms = samples_written * 1000 / 44100
wall_ms = elapsed since start_time

if audio_ms > wall_ms + 500ms:
    sleep(min(audio_ms - wall_ms - 200, 200))
```

Without rate limiting, librespot decodes 100x faster than real-time.
EndOfTrack would fire in seconds instead of minutes.

**Pre-buffer**: 500ms ahead of wall clock.
**Progress bar offset**: ~4s (architectural — Spotify server tracks position from play command timestamp, not device-reported position. Same as BT/Chromecast).

### MP3 Encoder
- `mp3lame-encoder` crate (libmp3lame bindings)
- 192 kbps CBR, 44100 Hz, stereo
- `InterleavedPcm(&[i16])` input, `encode_to_vec()` output
- `flush_to_vec::<FlushNoGap>()` on Sink::stop()

### HTTP Streaming
- `GET /stream.mp3?t={token}` — StreamBody (impl HttpBody)
- `Content-Length = ceil((duration_ms - position_ms) / 1000 + 1) * 24000`
- +1 second padding to prevent truncation
- StreamBody polls `mp3_buffer`, registers Waker when empty
- Token mismatch → stream closes (returns `Poll::Ready(None)`)
- Stale token requests get silence + close

---

## Track Transitions

### Natural (gapless)

```
librespot preloads Track B ~28s before Track A ends
→ TrackChanged(B) → saved as pending_track (streaming && !track_ended)

Track A PCM ends, Track B PCM starts flowing (NO Sink::stop/start!)
→ EndOfTrack → track_ended = true → spawn WaitAndTransition

WaitAndTransition:
  polls bytes_delivered every 200ms until >= expected_bytes
  timeout: 30s → force switch
  on cancel (pause/seek/skip): returns immediately

  when delivered:
    pending_track → start_new_track() → stream_token++
    Glagol: stop → 100ms delay → radio_play(new URL)
    Station connects → new StreamBody → Track B audio flows
```

### Skip (explicit)

```
TrackChanged(B) → pending_track saved
Sink::stop() → flush encoder
PlayerEvent::Paused → paused=true, stream_token++, buffer cleared
Sink::start() → new encoder
PlayerEvent::Playing → paused was true → start_new_track(pending) → SendRadioPlay
```

### Seek

```
PlayerEvent::Seeked → cancel transition, reset_stream(position_ms)
  stream_token++, buffer cleared, expected_bytes recalculated
  → SendRadioPlay
```

### Pause / Resume

```
Pause:
  Sink::stop() → flush encoder
  PlayerEvent::Paused → paused=true, internal_disconnect=true, stream_token++, buffer cleared
  → Glagol stop

Resume:
  Sink::start() → new encoder, start_time reset
  PlayerEvent::Playing → paused=false, internal_disconnect=true, stream_token++
  → SendRadioPlay (Station reconnects)
```

### Auto-pause (Station disconnect)

```
Station user says "Алиса, стоп" or Station closes HTTP stream:
  HTTP serve_connection() completes → check:
    served /stream.mp3? && token matches current? && streaming && !paused && !internal_disconnect?
  → send BridgeCommand::AutoPause

CommandExecutor handles AutoPause:
  sleep 2s (debounce)
  re-check: streaming && !paused && !internal_disconnect
  → spirc.pause() + Glagol stop
```

`internal_disconnect` prevents false triggers: set to `true` before every programmatic `stream_token++` (skip, seek, pause, new track), reset to `false` when Station connects to a new stream.

### BT Detection

```
Glagol read task receives playerState:
  playerType == "bluetooth" && hasPause == true → bt_playing = true

CommandExecutor receives SendRadioPlay:
  if bt_playing → set bt_deferred=true, skip radio_play

Glagol read task detects bt_playing: true→false:
  → send notification via bt_stopped_tx channel

BT stopped listener:
  if bt_deferred → clear flag, sleep 500ms, send SendRadioPlay
```

---

## Glagol Protocol

### Connection
- WebSocket: `wss://{station_ip}:1961`
- TLS: native-tls with `danger_accept_invalid_certs(true)` (self-signed cert)
- Bidirectional: write half (`Arc<TokioMutex<WsTx>>`) shared between send_command and ping task; read half consumed by read task
- **Keep-alive**: ping task sends `Message::Ping` every 30s to prevent idle disconnects
- **Read task**: reads incoming Station state messages, parses `playerState` (playerType, hasPause, volume), updates `GlagolState`
- **Auto-reconnect**: read/ping task sets `connected = false` on WS close/error; next `send_command()` triggers reconnect
- Initial stop on startup (clears stale playback from previous session)

### Token
- `GET https://quasar.yandex.ru/glagol/token?device_id={}&platform={}`
- Header: `Cookie: Session_id={session_id}`
- Cached 25s (tokens last ~30s)
- Fetched via reqwest + SOCKS proxy

### Commands
Message format:
```json
{
  "conversationToken": "<glagol_token>",
  "id": "<uuid>",
  "sentTime": <epoch_ms>,
  "payload": { "command": "..." }
}
```

**radio_play**:
```json
{
  "command": "externalCommandBypass",
  "data": "<base64 protobuf>"
}
```
Protobuf: `{1: "radio_play", 2: JSON.stringify({streamUrl, force_restart_player: true, title, subtitle})}`

**stop**: `{"command": "stop"}`
**setVolume**: `{"command": "setVolume", "volume": 0.0..1.0}`

### Volume Control

Volume is controlled exclusively via Glagol `setVolume` (Station hardware volume).
`NoOpVolume` is used as the Player's `VolumeGetter` — PCM is always at 100%, no software attenuation.
`SoftMixer` is kept only for Spirc (Spotify Connect protocol needs a mixer to report volume capabilities).
`VolumeChanged` events from Spirc are forwarded to Glagol `setVolume`.

### Protobuf Wire Format
Tag encoding: `(tag << 3) | 2` (LEN-delimited), varint length, UTF-8 bytes.

---

## HTTP Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/stream.mp3?t={token}` | GET | MP3 audio stream (Content-Length based) |
| `/status` | GET | JSON: track, artist, album, cover, streaming, paused, uptime, session_id_valid |
| `/debug` | GET | JSON: all /status fields + bytes, buffer, offset, station_progress, config |
| `/health` | GET | JSON: healthy, spirc_alive, glagol_connected, session_id_valid (200/503) |
| `/stop` | GET | Force stop playback |

---

## Authentication

Priority order:
1. `--access-token` — Spotify OAuth access token (from token refresh)
2. `--username` + `--password` — Spotify credentials
3. Cached credentials — `{cache_dir}/credentials.json` (saved by librespot after first auth)

Access tokens expire (~1h). Cached credentials persist across restarts and reboots.

**Cache directory**: `/root/.librespot-cache/` (persistent overlayfs, survives reboot).
First auth requires `--access-token`; after that, cached `credentials.json` is used automatically.

---

## Build & Deploy

### Build (WSL, cross-compile)
```bash
# Requires: Rust 1.88+, aarch64-linux-musl-gcc
source ~/.cargo/env
cd spotify/librespot-bridge
CC_aarch64_unknown_linux_musl=aarch64-linux-musl-gcc \
  cargo +1.88.0 build --release --target aarch64-unknown-linux-musl

# Binary: target/aarch64-unknown-linux-musl/release/librespot-bridge (14MB)
```

### Dependency note
- `vergen-gitcl` must be pinned to 1.0.0 and `vergen` to 9.0.6 to avoid vergen-lib version conflict
- After `cargo update`, run: `cargo update vergen-gitcl --precise 1.0.0 && cargo update vergen --precise 9.0.6`

### Deploy
```bash
scp target/aarch64-unknown-linux-musl/release/librespot-bridge root@<sbc>:/usr/local/bin/
ssh root@<sbc> 'chmod +x /usr/local/bin/librespot-bridge && /etc/init.d/librespot_bridge restart'
```

### Run
```bash
librespot-bridge \
  --session-id "YANDEX_SESSION_ID" \
  --bitrate 320 \
  --bridge-port 8888 \
  --cache-dir /root/.librespot-cache
```

Default device name: "Yandex Station" (shown in Spotify device picker).

### Service (procd)

`/etc/init.d/librespot_bridge` — procd script, waits for SOCKS proxy (up to 60s), starts binary with `respawn 60 10 0`.
Single process — no child cleanup needed (unlike Node.js version).
Boot sequence: passwall2 starts SOCKS (~60s) → librespot_bridge connects to Spotify AP → ready.

---

## File Structure

```
librespot-bridge/
├── Cargo.toml           # Dependencies: librespot v0.8.0 (git tag), mp3lame-encoder,
│                        #   hyper 1, tokio-tungstenite, reqwest, rustls, clap
├── Cargo.lock           # Pinned dependency versions
├── Cross.toml           # cross-rs config for aarch64-musl
├── .cargo/config.toml   # Linker: aarch64-linux-musl-gcc
├── SPEC.md              # This file
└── src/
    ├── main.rs          # CLI args, Session, Player, Spirc, task spawning, BT listener
    ├── sink.rs          # BridgeSink: impl Sink, PCM→MP3, rate limiting
    ├── encoder.rs       # Mp3Encoder wrapper, silence generation
    ├── http.rs          # hyper HTTP server, StreamBody, auto-pause detection, /status, /debug, /stop
    ├── glagol.rs        # Glagol WSS client, read/ping tasks, state monitoring, protobuf, token refresh
    ├── events.rs        # PlayerEvent handler, WaitAndTransition, CommandExecutor (incl. AutoPause, BT defer)
    └── state.rs         # BridgeShared, GlagolState, TrackInfo, BridgeCommand, BridgeConfig
```

---

## Key Constants

```
Sample rate:      44100 Hz
Channels:         2 (stereo)
MP3 bitrate:      192 kbps CBR
MP3 bytes/sec:    24000
Pre-buffer:       500ms (rate limiter)
Content-Length:    ceil(remaining_sec) + 1 second padding
Glagol token TTL: 25s cache (30s server TTL)
WaitAndTransition timeout: 30s
Glagol stop→radio_play delay: 100ms
Glagol ping interval: 30s
Auto-pause debounce: 2s
BT resume delay: 500ms
Progress bar offset: ~4s (architectural, server-side)
```

---

## Known Limitations

| Issue | Cause | Status |
|-------|-------|--------|
| Progress bar ~4s ahead | Spotify server tracks position from play command timestamp, ignoring device-reported position and Loading state | Architectural, same as BT/Chromecast. Extensively tested: position offset, loading delay, combined — none work. |
| No SOCKS for Spotify AP | librespot v0.8.0 doesn't support SOCKS in SessionConfig | Spotify routes via passwall2 transparent proxy (port 443) |
| `context is not available` warnings | Spirc can't load playlist context for some tracks | Harmless, doesn't affect playback |

---

## Interaction with alice_spotify.js

Alice skill stays on Node.js. Communicates with bridge via HTTP:
- `GET http://127.0.0.1:8888/stop` — pause/stop playback
- `GET http://127.0.0.1:8888/status` — current track info
- Playback control (play, next, prev) goes via Spotify API → Spirc receives commands

---

## Implemented Improvements

### Resilience

- **Spirc auto-reconnect** — main loop wraps Spirc lifecycle in a reconnect loop. When Spirc task ends (Spotify AP disconnect), Session/Player/Spirc are recreated with exponential backoff (2s→4s→8s...max 60s, reset after 30s of healthy connection). HTTP server and Glagol client persist across reconnects. Per-session tasks (event_loop, command_executor) are respawned with new channels. Shared command sender (`Arc<Mutex<Option<Sender>>>`) is updated on each reconnect.
- **Orphaned-dealer cleanup on reconnect (L4)** — on a supervisor-forced reconnect the `select!` completes and the `task.run()` future is *dropped mid-flight*, so SpircTask's own end-of-run cleanup (`session.dealer().close()`) never runs. The dealer task is detached — dropping the Session does **not** stop it. The old dealer WebSocket then stays connected to Spotify and keeps receiving routed play/transfer commands, but its Spirc request consumer is gone: every command logs `failed sending dealer request channel closed` and is silently dropped. Symptom: **device still appears in Spotify (a fresh Spirc keeps publishing `PutStateRequest` via spclient) but cannot start playback**, and no existing detector catches it (`spirc_task` never ends, the supervisor's liveness probe rides the *spclient* path which stays healthy, `/health` reports healthy). Fix: the cleanup block after `select!` spawns `old_session.dealer().close()` before the next iteration. `close()` is idempotent (no-op on the normal path where `run()` already closed it). Belt-and-suspenders: `bridge_watchdog.sh` restarts the service if it ever sees the `dealer request channel closed` signature for the current PID.
- **Session_id 401 detection** — `get_token()` checks HTTP status code. On 401, logs prominent error banner and sets `session_id_valid = false`. Visible in `/status`, `/debug`, and `/health` endpoints.
- **Health endpoint** — `GET /health` returns `{"healthy", "spirc_alive", "glagol_connected", "session_id_valid"}` with HTTP 200 (healthy) or 503 (unhealthy). Suitable for external watchdog.
- **Spotify token auto-refresh** — not needed. librespot caches reusable auth blob to `credentials.json` after first auth. Reconnect loop prefers cached credentials over CLI access_token.

### Progress bar monitoring (passive)

Station playback progress is parsed from Glagol `playerState` WebSocket messages (`progress` and `duration` fields, reported in seconds). Available in `/debug` as `station_progress_ms`, `station_duration_ms`, `progress_age_ms`, and `position_offset_ms` (how far ahead librespot is vs actual Station playback).

### Progress bar position offset (investigated, not feasible)

Extensively tested approaches to reduce ~4s progress bar offset:

1. **position_as_of_timestamp offset** — forked librespot-connect, subtracted offset from position in `send_state()` (covering all code paths: `notify()`, `notify_volume_changed()`, `notify_new_device_appeared()`). Confirmed offset applied via logging. **Result: Spotify app ignores device-reported position during active playback.** Server tracks position independently from play command timestamp.

2. **Loading state delay** — delayed `SpircPlayStatus::Playing` transition by N ms, keeping device in `LoadingPlay` (buffering) state. **Result: Spotify app shows no loading indicator, progress bar starts immediately from play command. 4s delay = partial 1.8s effect, 6s delay = no effect, 8s delay = device timeout.**

3. **Position + Loading combined** — no additional effect.

**Conclusion**: ~4s offset is architectural. Spotify server starts position tracking at the moment it processes the play command, independent of device-reported state. Same behavior as Bluetooth speakers and Chromecast. Cannot be fixed through Spotify Connect protocol (PutStateRequest).

## Ideas for Future Work

### Instant resume from pause

**Current behavior**: pause increments `stream_token` and clears `mp3_buffer`. On resume, a fresh stream is created with new token → Glagol `stop` + `radio_play` → Station reconnects and buffers ~2.5s before playback. Total delay: ~2.6s.

**Attempted: keep buffer + same token (reverted)**

Idea: on pause, don't clear `mp3_buffer` and don't increment `stream_token`. On resume, send `radio_play` with same URL — Station reconnects and gets cached data immediately.

Why it failed:
- On pause, `StreamBody` must stop serving (otherwise Station keeps playing). Setting `paused=true` and checking it in `poll_frame()` → returns `None` → HTTP response ends mid-stream.
- Station receives fewer bytes than `Content-Length` promised → treats it as a broken connection → retries the same URL every ~1.6s.
- During pause, these retries are rejected (paused check in `handle_stream`). Station accumulates failed retry state.
- On resume, `radio_play` sends the same URL. Station may ignore it or behave unpredictably because it already has a failed/cached connection for that URL.
- Even when resume works, buffer is typically 0B — Station consumed all data before pause took effect (StreamBody drains buffer in real-time). No actual speedup.

**Why the buffer is always empty**: the rate limiter in `Sink::write()` keeps librespot only ~500ms ahead of wall clock. StreamBody delivers data to Station as fast as it arrives. By the time pause fires, the Station has consumed everything. `Sink::stop()` flushes a few encoder frames, but these are consumed by StreamBody before the `Paused` event handler runs (async race).

**Approaches NOT tried**:
- Keep HTTP connection alive during pause (don't end StreamBody, just stop sending data). Problem: Station may timeout the connection, and `Content-Length` header was already sent — can't extend it.
- Use chunked transfer encoding instead of Content-Length. Problem: Station rejects chunked/infinite streams (documented in "What doesn't work" section).
- Pre-buffer extra audio during pause. Problem: librespot doesn't produce PCM while paused (`Sink::stop()` is called).

**Possible approaches**:
- Accept the ~2.6s delay as architectural (same as Bluetooth/Chromecast handoff delay).
- Investigate whether Glagol supports a "pause/resume" command pair that keeps the Station's player alive without reconnecting HTTP.


