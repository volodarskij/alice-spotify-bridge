# alice-spotify-bridge

**Spotify Connect → Яндекс.Станция**: локальный стриминг Spotify на Станцию + голосовое управление через навык Алисы.

Станция появляется в списке устройств Spotify Connect как обычная колонка, а голосом можно сказать: *«Алиса, попроси Спотик включить Coldplay»* — и музыка заиграет из вашего Spotify-аккаунта.

```
Spotify App / Spotify Connect
    ↓ (Spotify Connect protocol)
librespot-bridge (один Rust-бинарник, ~14 МБ)
    ├─ librespot: Spotify CDN → decrypt → decode → PCM
    ├─ PCM → MP3 192 kbps CBR (mp3lame)
    ├─ HTTP-сервер :8888 → /stream.mp3
    └─ Glagol WSS → radio_play → Яндекс.Станция

«Алиса, попроси Спотик включить …»
    ↓
Яндекс.Диалоги → HTTPS POST :8889/alice (webhook)
    ↓
alice_spotify.js → Groq LLM (парсинг команды) → Spotify Search → Play API
    ↓
librespot-bridge получает команду → аудио течёт по цепочке выше
```

## Почему так

Яндекс.Станция не умеет Spotify. Bluetooth — с потерями и без управления из приложения. Этот проект использует недокументированный локальный протокол Станции (**Glagol**, WSS на порту 1961) и команду `radio_play`: Станции передаётся URL локального HTTP-потока, и она проигрывает его сама — по Wi-Fi, без BT.

## Компоненты

| Компонент | Порт | Описание |
|-----------|------|----------|
| [`bridge/`](bridge/) | :8888 | Rust: Spotify Connect + MP3-энкодер + HTTP-стрим + Glagol-клиент |
| [`alice-skill/alice_spotify.js`](alice-skill/alice_spotify.js) | :8889 | Навык Алисы (HTTPS webhook): LLM-парсинг команд, поиск и запуск в Spotify |
| [`alice-skill/spotify_auth.js`](alice-skill/spotify_auth.js) | — | Одноразовая OAuth2-авторизация Spotify (получение refresh_token) |
| [`alice-skill/logger.js`](alice-skill/logger.js) | — | Структурированный логгер (ring buffer, ротация, таймеры) |
| [`init.d/`](init.d/) | — | Шаблоны procd-сервисов для OpenWrt |

Подробная архитектура: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) и [bridge/SPEC.md](bridge/SPEC.md).
Гайд по созданию навыка Алисы: [docs/ALICE_SKILL_GUIDE.md](docs/ALICE_SKILL_GUIDE.md).
Сервис-независимая спецификация стриминга на Станцию (Glagol, HTTP-поток, тайминги): [yandex-station-local-streaming](https://github.com/volodarskij/yandex-station-local-streaming).

## Что понадобится

- **Яндекс.Станция** (протестировано на Станции 1; должен работать любой Quasar-девайс с Glagol)
- **Linux-хост в той же сети** (протестировано: NanoPi R7S на OpenWrt; подойдёт любой SBC/сервер, aarch64 или x86)
- **Spotify Premium** (требование Spotify Connect / librespot)
- Аккаунт Яндекса (тот же, к которому привязана Станция)
- Для голосового навыка: домен с HTTPS-сертификатом (вебхук Диалогов работает только по HTTPS)
- Если Spotify в вашей стране не работает — SOCKS5-прокси (код умеет ходить через `socks5h://`)

## Быстрый старт

### 1. Собрать мост

```bash
cd bridge
# вариант А: cross (Docker), для aarch64-роутера/SBC
cross build --release --target aarch64-unknown-linux-musl
# вариант Б: нативно на целевой машине
cargo build --release
```

### 2. Узнать device_id Станции

Залогиньтесь в браузере в Яндекс и откройте:
`https://quasar.iot.yandex.ru/glagol/device_list` — в ответе будут `id` (device_id) и `platform` вашей Станции.

### 3. Получить Session_id

Кука `Session_id` нужна мосту для получения локального Glagol-токена. Как её достать из браузера — в [docs/ALICE_SKILL_GUIDE.md](docs/ALICE_SKILL_GUIDE.md#session_id). Живёт долго (месяцы), но протухает — мост сообщит об этом в `/health`.

### 4. Запустить

```bash
librespot-bridge \
  --session-id "<Yandex Session_id>" \
  --device-id "<device_id Станции>" \
  --station-ip 192.168.1.21 \
  --sbc-ip 192.168.1.19 \
  --access-token "<одноразовый Spotify OAuth token>" \
  --bitrate 320 --cache-dir /root/.librespot-cache
```

`--access-token` нужен только при первом запуске: librespot сохранит многоразовые креды в `--cache-dir`. Токен можно взять, например, из веб-плеера Spotify (DevTools → запросы к `api.spotify.com` → заголовок `Authorization`).

Откройте Spotify на телефоне → значок устройств → выберите «Yandex Station». Музыка заиграет на Станции.

### 5. Голосовой навык (опционально)

Полная инструкция — [docs/ALICE_SKILL_GUIDE.md](docs/ALICE_SKILL_GUIDE.md). Кратко:

1. Создайте приложение на [developer.spotify.com/dashboard](https://developer.spotify.com/dashboard), впишите redirect URI.
2. `cp alice-skill/.env.example .env`, заполните.
3. Одноразово: `node spotify_auth.js` → авторизация в браузере → появится `spotify_tokens.json`.
4. Запустите `node alice_spotify.js` (или через `init.d/alice_spotify`).
5. Создайте навык в [Яндекс.Диалогах](https://dialogs.yandex.ru/developer), укажите webhook `https://ваш-домен:8889/alice`.

## Эндпоинты для диагностики

| Сервис | Эндпоинт | Что показывает |
|--------|----------|----------------|
| bridge :8888 | `/status` | трек, исполнитель, стриминг, валидность Session_id |
| bridge :8888 | `/debug` | полная диагностика (байты, буфер, конфиг) |
| bridge :8888 | `/health` | healthcheck 200/503 — для watchdog |
| bridge :8888 | `/stop` | принудительный стоп |
| skill :8889 | `/status`, `/debug` | состояние токенов, метрики запросов |

## Безопасность

В репозитории нет ни одного секрета — всё передаётся через переменные окружения или CLI-флаги:

- `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` — ваше приложение Spotify
- `GROQ_API_KEY` — опционально, для LLM-парсинга команд
- `--session-id` — кука Яндекса (даёт доступ к аккаунту — **никому не показывайте**)
- `spotify_tokens.json`, `credentials.json`, `*.pem` — в `.gitignore`

Мост слушает только локальную сеть; навык Алисы — единственное, что должно быть доступно снаружи (HTTPS-вебхук).

## Ограничения

- Прогресс-бар в приложении Spotify опережает реальное воспроизведение на ~4 с — архитектурная особенность Spotify Connect (так же ведут себя BT-колонки и Chromecast), подробности в [bridge/SPEC.md](bridge/SPEC.md).
- Пауза → продолжение занимает ~2.5 с (Станция заново подключается к потоку и буферизует).
- Кука `Session_id` со временем протухает — следите за `/health`.

## Лицензия

MIT. Проект не аффилирован ни со Spotify, ни с Яндексом. Использует [librespot](https://github.com/librespot-org/librespot) (MIT) и реверс-инжиниринг протокола Glagol, задокументированный сообществом.
