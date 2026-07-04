# Архитектура

## Общая схема

```
Spotify App / Spotify Connect
    ↓ (Spotify Connect protocol)
librespot-bridge (один Rust-бинарник)
    ├─ librespot: Spotify CDN → decrypt → decode → PCM f64
    ├─ BridgeSink: f64 → i16 → MP3 192kbps CBR (mp3lame)
    ├─ Rate limiter: 500 мс пре-буфер
    ├─ HTTP-сервер :8888 → /stream.mp3?t={streamToken}
    └─ Glagol WSS → radio_play → Яндекс.Станция
```

Голосовое управление:

```
Пользователь → «Алиса, попроси Спотик включить Coldplay»
    ↓
Яндекс.Диалоги → HTTPS POST :8889/alice (webhook)
    ↓
alice_spotify.js → Groq LLM (парсинг) → Spotify Search API → Play API
    ↓
librespot получает команду через Spotify Connect → аудио течёт по цепочке выше
```

Детальная спецификация Rust-моста (пайплайн, переходы треков, auto-pause, BT-детект, реконнекты): [../bridge/SPEC.md](../bridge/SPEC.md).

## Ключевой механизм: Direct HTTP Stream

Станция не принимает бесконечные HTTP-потоки (chunked, ICY, огромный Content-Length — всё отвергает). Поэтому каждый трек — отдельный конечный HTTP-ответ:

- `Content-Length = ceil(остаток_трека_в_сек + 1) * 24000` (24000 байт/с при MP3 192 kbps CBR)
- При смене трека/seek/паузе: `stop` → `streamToken++` → новый `radio_play` с новым URL
- Станция подключается к `/stream.mp3?t={token}`; запросы со старым токеном получают тишину и закрытие соединения

## Протокол Glagol

Локальный WebSocket-протокол Яндекс.Станции (недокументированный, реверс-инжиниринг сообщества):

- `wss://{station_ip}:1961`, самоподписанный сертификат (проверку отключаем)
- Аутентификация: краткоживущий токен (~30 с) c `https://quasar.yandex.ru/glagol/token?device_id=...&platform=...`, заголовок `Cookie: Session_id=<кука>`
- Воспроизведение произвольного URL — команда `externalCommandBypass` с base64-protobuf `{1: "radio_play", 2: JSON({streamUrl, force_restart_player: true, title, subtitle})}`
- Также: `stop`, `setVolume`, а входящие сообщения несут `playerState` (тип плеера, громкость, прогресс) — используется для детекта Bluetooth и авто-паузы

Protobuf wire format собирается вручную (это единственное protobuf-сообщение): `(tag << 3) | 2`, varint-длина, UTF-8.

## Компоненты

| Компонент | Порт | Роль |
|-----------|------|------|
| `bridge/` (librespot-bridge) | :8888 HTTP | Spotify Connect устройство + MP3-стрим + Glagol-клиент |
| `alice-skill/alice_spotify.js` | :8889 HTTPS | Вебхук Алисы: парсинг команд → Spotify Web API |
| `alice-skill/spotify_auth.js` | — | Одноразовый OAuth2 (refresh_token) |
| `alice-skill/logger.js` | — | Структурированный логгер: ring buffer 200 записей, ротация файла, таймеры, `/debug`-диагностика |

Навык общается с мостом по HTTP (`/stop`, `/status`), а управление воспроизведением (play/next/prev) идёт через Spotify Web API — команды прилетают мосту по протоколу Spotify Connect, как обычному устройству.

## Ключевые параметры

```
Spotify PCM:     44100 Hz, 16 bit, stereo = 176400 байт/с
MP3 CBR:         192 kbps = 24000 байт/с
Rate limiter:    500 мс пре-буфер (иначе librespot декодирует в 100x реального времени)
Glagol:          порт 1961 (WSS), токен кешируется 25 с
Битрейт Spotify: 320 kbps (--bitrate 320, нужен Premium)
```

## Что НЕ работает (сэкономьте себе время)

| Подход | Проблема |
|--------|----------|
| Бесконечный HTTP-стрим | Станция отключается (Content-Length: 999999999, chunked, ICY — всё проверено) |
| HLS live stream | Работает, но Direct HTTP проще и надёжнее |
| Уменьшить ~4 с опережения прогресс-бара | Архитектурно невозможно: сервер Spotify считает позицию от момента команды play и игнорирует позицию, репортуемую устройством. Проверено форком librespot-connect (position offset, loading delay, комбинация) |
| Мгновенный resume после паузы | Станция не принимает недокачанный Content-Length и не переподключается к тому же URL; буфер к моменту паузы всегда пуст. ~2.5 с на переподключение — архитектурный минимум |
| Держать паузу через тот же streamToken | Станция ретраит оборванный URL каждые ~1.6 с и копит битое состояние |

## История версий

- **v1–v3 (Node.js)**: librespot → pv → lame → Node.js HTTP (4 процесса). Работало, но хрупко: ручные «ворота» вместо pipe (`readable.pause()` на piped-стримах ненадёжен), drain-пайплайн для смены треков, гонки при skip.
- **v4 (Rust, текущая)**: всё в одном процессе — PCM забирается прямо из librespot через `Sink` trait, никаких пайпов и гонок. Плюс авто-реконнект Spirc с экспоненциальным бэкоффом, health-эндпоинт, детект протухшей Session_id, отложенный radio_play при активном Bluetooth.
