#!/usr/bin/env node
// Alice Skill webhook server for voice-controlled Spotify playback.
// Receives commands from Yandex Alice, searches Spotify, starts playback
// on librespot device. Audio flows through existing spotify_hls.js bridge.

const http = require('http');
const { execSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const createLogger = require('./logger');

const log = createLogger({
  name: 'alice-spotify',
  logFile: '/tmp/alice_spotify.log',
  maxFileBytes: 2 * 1024 * 1024,
  defaultLevel: process.env.LOG_LEVEL || 'info',
});

// --- Config ---
const CLIENT_ID     = process.env.SPOTIFY_CLIENT_ID     || '';
const CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET  || '';
const PROXY         = process.env.SOCKS_PROXY || 'socks5h://127.0.0.1:1070';
const PORT          = 8889;
const TOKEN_FILE    = path.join(__dirname, 'spotify_tokens.json');
const DEVICE_NAME   = 'Yandex Station';
const GROQ_API_KEY  = process.env.GROQ_API_KEY || '';
const GROQ_MODEL    = 'llama-3.1-8b-instant';

const LLM_SYSTEM_PROMPT = `You are a Spotify voice command parser. Extract structured JSON from user commands.

Output format: {"type":"track|album|artist|playlist", "query":"optimized Spotify search query", "artist":"artist name or null", "album":"album name or null"}

Rules:
- Transliterate Russian artist names to original spelling: мак миллер=Mac Miller, оксимирон=Oxxxymiron, битлз=The Beatles, скриптонит=Scriptonite, моргенштерн=MORGENSHTERN
- Keep Russian song/album titles in Russian (Горгород stays Горгород)
- If only artist name mentioned with no specific track/album → type:"artist"
- If "альбом" keyword present → type:"album"
- If specific song mentioned → type:"track"
- Strip filler words (включи, поставь, песню, от)
- For type:"track" with known artist, set query to: "artist:ARTIST track:TRACK"
- For type:"album" with known artist, set query to: "artist:ARTIST album:ALBUM"
- Reply ONLY valid JSON, no markdown.`;

// --- Spotify Token State ---
let refreshToken = '';
let accessToken  = '';
let tokenExpiry  = 0;
let cachedDeviceId = '';

// --- Request metrics ---
let totalRequests = 0;
let lastRequestTs = null;
let totalApiCalls = 0;
let apiErrorCount = 0;
let lastApiErrorTs = null;

// --- Register state snapshot ---
log.setSnapshotFn(() => ({
  hasRefreshToken: !!refreshToken,
  hasAccessToken: !!accessToken && Date.now() < tokenExpiry,
  tokenExpiresInSec: Math.max(0, Math.round((tokenExpiry - Date.now()) / 1000)),
  cachedDeviceId: cachedDeviceId || null,
  totalRequests,
  lastRequestTs,
  totalApiCalls,
  apiErrorCount,
  memRssMB: +(process.memoryUsage().rss / 1048576).toFixed(1),
  uptimeSec: Math.floor(process.uptime()),
}));

// Load refresh token from file
function loadTokens() {
  try {
    const data = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf-8'));
    refreshToken = data.refresh_token;
    log.info('AUTH', 'Loaded refresh_token', { file: TOKEN_FILE });
  } catch (e) {
    log.error('AUTH', 'Failed to load tokens', { error: e.message });
    log.error('AUTH', 'Run spotify_auth.js first to obtain refresh_token');
  }
}

// Refresh access token via Spotify API
function getAccessToken(reqLog) {
  if (accessToken && Date.now() < tokenExpiry) return accessToken;
  if (!refreshToken) return null;

  const timer = (reqLog || log).startTimer('AUTH', 'Token refresh');
  try {
    const auth = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
    const resp = execSync(
      `curl -s -x ${PROXY} -X POST "https://accounts.spotify.com/api/token" ` +
      `-H "Authorization: Basic ${auth}" ` +
      `-H "Content-Type: application/x-www-form-urlencoded" ` +
      `-d "grant_type=refresh_token&refresh_token=${refreshToken}"`,
      { timeout: 10000 }
    ).toString();
    const json = JSON.parse(resp);
    if (json.error) { (reqLog || log).error('AUTH', 'Refresh error', json); timer.end({ status: 'error' }); return null; }
    accessToken = json.access_token;
    tokenExpiry = Date.now() + (json.expires_in - 60) * 1000;
    // Spotify may rotate refresh_token
    if (json.refresh_token) {
      refreshToken = json.refresh_token;
      const stored = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf-8'));
      stored.refresh_token = refreshToken;
      fs.writeFileSync(TOKEN_FILE, JSON.stringify(stored, null, 2));
      (reqLog || log).debug('AUTH', 'Refresh token rotated');
    }
    timer.end({ expiresIn: json.expires_in });
    (reqLog || log).info('AUTH', 'Token refreshed', { expiresIn: json.expires_in });
    return accessToken;
  } catch (e) {
    (reqLog || log).error('AUTH', 'Token refresh failed', { error: e.message, stack: e.stack });
    timer.end({ status: 'error' });
    return null;
  }
}

// --- Spotify API helpers (curl through SOCKS5) ---

// The SOCKS path rides the leastLoad balancer: each curl may land on a
// different exit node, and a Spotify-dead member yields a hard curl failure.
// One retry re-rolls the member — only for idempotent requests (GET/PUT).
const API_RETRY_DELAY_MS = 1500;
function sleepMs(ms) { try { execSync(`sleep ${ms / 1000}`); } catch (e) { /* ignore */ } }

function spotifyGet(endpoint, reqLog) {
  const token = getAccessToken(reqLog);
  if (!token) return null;
  totalApiCalls++;
  const timer = (reqLog || log).startTimer('SPOTIFY', `GET ${endpoint}`);
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      const resp = execSync(
        `curl -s -x ${PROXY} -H "Authorization: Bearer $SPOTIFY_TOKEN" "https://api.spotify.com/v1${endpoint}"`,
        { timeout: 10000, env: { ...process.env, SPOTIFY_TOKEN: token } }
      ).toString();
      timer.end({ status: 'ok', attempt });
      return JSON.parse(resp);
    } catch (e) {
      if (attempt < 2) {
        (reqLog || log).warn('SPOTIFY', 'GET failed, retrying', { endpoint, error: e.message });
        sleepMs(API_RETRY_DELAY_MS);
        continue;
      }
      apiErrorCount++;
      lastApiErrorTs = new Date().toISOString();
      (reqLog || log).error('SPOTIFY', 'GET error', { endpoint, error: e.message });
      timer.end({ status: 'error' });
      return null;
    }
  }
  return null;
}

function spotifyApi(method, endpoint, body, reqLog) {
  const token = getAccessToken(reqLog);
  if (!token) { (reqLog || log).warn('SPOTIFY', 'No token', { method, endpoint }); return null; }
  totalApiCalls++;
  const timer = (reqLog || log).startTimer('SPOTIFY', `${method} ${endpoint}`);
  // PUT (pause/play/seek/volume) is idempotent — retry re-rolls the balancer
  // member. POST (next/previous) is NOT: a retry could double-skip.
  const attempts = method === 'PUT' ? 2 : 1;
  // Write body to a unique temp file and use --data-binary @file — avoids any
  // shell-quoting of JSON payload (single quotes, Cyrillic, control chars).
  let bodyFile = null;
  try {
    let cmd = `curl -s -x ${PROXY} -X ${method}` +
      ` "https://api.spotify.com/v1${endpoint}"` +
      ` -H "Authorization: Bearer $SPOTIFY_TOKEN"`;
    if (body !== undefined) {
      const bodyStr = JSON.stringify(body);
      bodyFile = path.join(os.tmpdir(), `spotify_req_${process.pid}_${crypto.randomBytes(4).toString('hex')}.json`);
      fs.writeFileSync(bodyFile, bodyStr);
      cmd += ` -H "Content-Type: application/json" --data-binary @${bodyFile}`;
    }
    for (let attempt = 1; attempt <= attempts; attempt++) {
      try {
        (reqLog || log).debug('SPOTIFY', 'API call', { method, endpoint, attempt });
        const resp = execSync(cmd, { timeout: 10000, env: { ...process.env, SPOTIFY_TOKEN: token } }).toString().trim();
        (reqLog || log).debug('SPOTIFY', 'API response', { method, endpoint, preview: (resp || '(empty)').slice(0, 200) });
        timer.end({ status: 'ok', attempt });
        if (!resp) return {};
        try { return JSON.parse(resp); } catch (e) { return {}; }
      } catch (e) {
        if (attempt < attempts) {
          (reqLog || log).warn('SPOTIFY', 'API failed, retrying', { method, endpoint, error: e.message });
          sleepMs(API_RETRY_DELAY_MS);
          continue;
        }
        apiErrorCount++;
        lastApiErrorTs = new Date().toISOString();
        (reqLog || log).error('SPOTIFY', 'API error', { method, endpoint, error: e.message });
        timer.end({ status: 'error' });
        return null;
      }
    }
    return null;
  } finally {
    if (bodyFile) {
      try { fs.unlinkSync(bodyFile); } catch (e) { /* ignore */ }
    }
  }
}

function spotifyPut(endpoint, body, reqLog) { return spotifyApi('PUT', endpoint, body, reqLog); }
function spotifyPost(endpoint, reqLog) { return spotifyApi('POST', endpoint, undefined, reqLog); }

// --- Device discovery ---

function getDeviceId(reqLog) {
  if (cachedDeviceId) return cachedDeviceId;
  const data = spotifyGet('/me/player/devices', reqLog);
  if (!data || !data.devices) return null;
  const dev = data.devices.find(d => d.name === DEVICE_NAME);
  if (dev) {
    cachedDeviceId = dev.id;
    (reqLog || log).info('SPOTIFY', 'Found device', { name: dev.name, id: dev.id });
    return dev.id;
  }
  (reqLog || log).warn('SPOTIFY', 'Device not found', { available: data.devices.map(d => d.name) });
  return null;
}

// --- Search ---

function spotifySearch(query, type, reqLog) {
  const q = encodeURIComponent(query);
  const qLower = query.toLowerCase();

  // For explicit type requests (user said "альбом", "плейлист", "исполнитель"), search that type only
  if (type === 'playlist') {
    // "мой плейлист X" means the USER'S playlist — public /search can't see it.
    // Try the user's library first, fall back to public search.
    const name = query.replace(/^playlist:/i, '').trim();
    const own = findMyPlaylist(name, reqLog);
    if (own) return own;
    return spotifySearchSingle(encodeURIComponent(name), type, reqLog);
  }
  if (type === 'album') {
    return spotifySearchSingle(q, type, reqLog);
  }

  // For artist request or default (track) — do multi-type search
  const searchTypes = type === 'artist' ? 'artist' : 'track,artist';
  const data = spotifyGet(`/search?q=${q}&type=${searchTypes}&limit=5&market=RU`, reqLog);
  if (!data) return null;

  const artists = data.artists?.items || [];
  const tracks = data.tracks?.items || [];

  // Check if query closely matches an artist name
  const artistMatch = artists.find(a => {
    const aLower = a.name.toLowerCase();
    return aLower === qLower || qLower.includes(aLower) || aLower.includes(qLower);
  });

  // If user explicitly asked for artist, or query matches an artist — play artist
  if (type === 'artist' || artistMatch) {
    const artist = artistMatch || artists[0];
    if (artist) {
      (reqLog || log).debug('SEARCH', 'Artist match', { query, artist: artist.name, explicit: type === 'artist' });
      return { uri: artist.uri, name: artist.name, artist: artist.name, type: 'artist' };
    }
  }

  // Otherwise find best track — prefer tracks where artist or track name matches query
  if (tracks.length > 0) {
    // Score tracks: bonus if artist name is in query or track name matches well
    let best = tracks[0];
    let bestScore = 0;

    for (const t of tracks) {
      let score = 0;
      const tName = t.name.toLowerCase();
      const tArtists = (t.artists || []).map(a => a.name.toLowerCase());

      // Exact track name match
      if (tName === qLower) score += 10;
      // Track name contained in query
      else if (qLower.includes(tName)) score += 5;
      // Query contained in track name
      else if (tName.includes(qLower)) score += 3;

      // Artist name matches part of query
      for (const a of tArtists) {
        if (qLower.includes(a)) score += 7;
        else if (a.includes(qLower)) score += 4;
      }

      // Popularity bonus (Spotify returns popularity 0-100)
      score += (t.popularity || 0) / 50;

      if (score > bestScore) {
        bestScore = score;
        best = t;
      }
    }

    (reqLog || log).debug('SEARCH', 'Track selected', { query, track: best.name, artist: best.artists?.[0]?.name, score: bestScore.toFixed(1) });
    return {
      uri: best.uri,
      albumUri: best.album?.uri || '',
      name: best.name,
      artist: best.artists?.map(a => a.name).join(', ') || '',
      type: 'track',
    };
  }

  return null;
}

// Find a playlist in the user's own library by (fuzzy) name.
function findMyPlaylist(name, reqLog) {
  const data = spotifyGet('/me/playlists?limit=50', reqLog);
  const items = (data?.items || []).filter(p => p && p.uri);
  if (items.length === 0) return null;
  const nLower = name.toLowerCase();
  const match =
    items.find(p => p.name.toLowerCase() === nLower) ||
    items.find(p => p.name.toLowerCase().includes(nLower)) ||
    items.find(p => nLower.includes(p.name.toLowerCase()));
  if (!match) return null;
  (reqLog || log).info('SEARCH', 'Own playlist match', { query: name, playlist: match.name });
  return { uri: match.uri, name: match.name, artist: '', type: 'playlist' };
}

// Single-type search for album/playlist
function spotifySearchSingle(q, type, reqLog) {
  const data = spotifyGet(`/search?q=${q}&type=${type}&limit=3&market=RU`, reqLog);
  if (!data) return null;

  const key = type + 's';
  // Spotify search quirk: items[] regularly contains literal nulls — skip them.
  const items = (data[key]?.items || []).filter(i => i && i.uri);
  if (items.length === 0) return null;

  const item = items[0];

  if (type === 'album') {
    return {
      uri: item.uri,
      name: item.name,
      artist: item.artists?.map(a => a.name).join(', ') || '',
      type: 'album',
    };
  }
  if (type === 'playlist') {
    return {
      uri: item.uri,
      name: item.name,
      artist: item.owner?.display_name || '',
      type: 'playlist',
    };
  }
  return null;
}

// --- Playback control ---

function startPlayback(result, reqLog) {
  let body;
  if (result.type === 'track') {
    // Play track in album context so next/previous work
    if (result.albumUri) {
      body = { context_uri: result.albumUri, offset: { uri: result.uri } };
    } else {
      body = { uris: [result.uri] };
    }
  } else if (result.type === 'artist') {
    // Playing an artist context on librespot fails with 403 "Restriction
    // violated" — play the artist's top tracks as a plain uris list instead.
    const artistId = result.uri.split(':').pop();
    const top = spotifyGet(`/artists/${artistId}/top-tracks?market=RU`, reqLog);
    const uris = (top?.tracks || []).filter(t => t && t.uri).map(t => t.uri).slice(0, 10);
    if (uris.length > 0) {
      (reqLog || log).info('SPOTIFY', 'Artist -> top tracks', { artist: result.name, count: uris.length });
      body = { uris };
    } else {
      body = { context_uri: result.uri };  // fallback: let Spotify try the context
    }
  } else {
    body = { context_uri: result.uri };
  }

  // Try with cached device first, then without (last active device)
  const deviceId = getDeviceId(reqLog);
  const endpoint = deviceId
    ? `/me/player/play?device_id=${deviceId}`
    : '/me/player/play';

  (reqLog || log).info('SPOTIFY', 'Starting playback', { endpoint, body });
  const resp = spotifyPut(endpoint, body, reqLog);
  (reqLog || log).debug('SPOTIFY', 'Play response', { resp });
  if (!resp) return false;              // curl never delivered the command
  if (resp.error) {
    (reqLog || log).error('SPOTIFY', 'Play error', resp.error);
    // If device not found, retry without device_id (last active)
    if (resp.error.reason === 'NO_ACTIVE_DEVICE' || resp.error.status === 404) {
      cachedDeviceId = '';
      (reqLog || log).warn('SPOTIFY', 'Retrying without device_id');
      const resp2 = spotifyPut('/me/player/play', body, reqLog);
      if (resp2 && !resp2.error) return true;   // retry succeeded
      (reqLog || log).error('SPOTIFY', 'Retry also failed', resp2 ? resp2.error : 'no response');
      // TODO: send short TTS to Station ("Станция не отвечает") via Glagol.
      // librespot-bridge has Glagol client but no exposed TTS endpoint yet —
      // need to add a `/say?text=...` HTTP handler in src/http.rs that calls
      // glagol.send_tts(text). Skipping until bridge exposes it (zone D).
    }
    return false;
  }
  return true;
}

// --- LLM-powered command parsing (Groq) with regex fallback ---

function parseWithLLM(command, reqLog) {
  if (!GROQ_API_KEY) return null;

  const cleaned = stripAlicePrefix(command);
  if (!cleaned) return null;

  const timer = (reqLog || log).startTimer('LLM', 'Parse');
  // Unique temp file — multiple concurrent Alice requests would otherwise stomp
  // the shared /tmp/groq_req.json. Cleaned up in finally.
  const groqFile = path.join(os.tmpdir(), `groq_req_${process.pid}_${crypto.randomBytes(4).toString('hex')}.json`);
  try {
    const body = JSON.stringify({
      model: GROQ_MODEL,
      messages: [
        { role: 'system', content: LLM_SYSTEM_PROMPT },
        { role: 'user', content: cleaned }
      ],
      max_tokens: 150,
      temperature: 0,
      response_format: { type: 'json_object' }
    });

    // Write body to temp file to avoid shell escaping issues with Cyrillic
    fs.writeFileSync(groqFile, body);

    const resp = execSync(
      `curl -s --max-time 1.5 -x ${PROXY} ` +
      `-H "Authorization: Bearer $GROQ_KEY" ` +
      `-H "Content-Type: application/json" ` +
      `--data-binary @${groqFile} ` +
      `https://api.groq.com/openai/v1/chat/completions`,
      { timeout: 2000, env: { ...process.env, GROQ_KEY: GROQ_API_KEY } }
    ).toString();

    const data = JSON.parse(resp);
    if (data.error) {
      (reqLog || log).warn('LLM', 'API error', data.error);
      timer.end({ status: 'api_error' });
      return null;
    }

    const content = data.choices[0].message.content;
    const result = JSON.parse(content);
    const elapsed = timer.end({ status: 'ok', type: result.type, query: result.query });

    (reqLog || log).info('LLM', 'Parsed', { type: result.type, query: result.query, artist: result.artist, album: result.album, elapsed_ms: elapsed });
    return result;
  } catch (e) {
    (reqLog || log).warn('LLM', 'Fallback to regex', { error: e.message });
    timer.end({ status: 'error' });
    return null;
  } finally {
    try { fs.unlinkSync(groqFile); } catch (e) { /* ignore */ }
  }
}

// --- Strip Alice invocation prefix ---

function stripAlicePrefix(text) {
  let q = (text || '').toLowerCase().trim();
  q = q.replace(/^(алиса\s+)?/i, '');
  q = q.replace(/^(попроси|спроси|открой|запусти)\s+(навык\s+)?(скилл?\s+)?\S+\s+/i, '');
  return q.trim();
}

// --- Regex command parsing (fallback) ---

function parseCommand(command) {
  let q = stripAlicePrefix(command);
  let type = 'track';

  // Detect content type from keywords (no \b — doesn't work with Cyrillic in JS)
  if (/(?:^|\s)альбом(?:\s|$)/.test(q))                               type = 'album';
  else if (/(?:^|\s)плейлист(?:\s|$)/.test(q))                        type = 'playlist';
  else if (/(?:^|\s)(исполнител\S*|групп[уыа]?)(?:\s|$)/.test(q))     type = 'artist';
  else if (/(?:^|\s)(музык[уа]|песни)(?:\s|$)/.test(q))               type = 'artist';

  // Strip action verbs (all forms)
  q = q.replace(/^(включи\S*|поставь\S*|поставить|запусти\S*|играй\S*|играть|найди\S*|найти|play|put on|воспроизвед\S*|послушать|слушать|хочу)\s+/i, '');

  // For albums: detect "ARTIST альбом ALBUM" or "альбом ALBUM от ARTIST" patterns
  // and build Spotify query syntax: artist:X album:Y
  if (type === 'album') {
    // Pattern: "X альбом Y" → artist:X album:Y
    let m = q.match(/^(.+?)\s+альбом\s+(.+)$/i);
    if (m) {
      const artist = m[1].replace(/\b(от|группы|группу|исполнителя)\b/gi, '').trim();
      const album = m[2].replace(/\b(от|группы|группу|исполнителя)\b/gi, '').trim();
      if (artist && album) {
        q = `artist:${artist} album:${album}`;
        return { query: q, type };
      }
    }
    // Pattern: "альбом Y от X" → artist:X album:Y
    m = q.match(/^альбом\s+(.+?)\s+от\s+(.+)$/i);
    if (m) {
      q = `artist:${m[2].trim()} album:${m[1].trim()}`;
      return { query: q, type };
    }
  }

  // Strip filler words (use (?:^|\s) instead of \b for Cyrillic compatibility)
  q = q.replace(/(?:^|\s)(песню|песня|песни|трек|альбом|плейлист|музык[уа]|от|группы|группу|группа|исполнителя|исполнитель|со\s*спотика|из\s*спотифай|из\s*спотика|в\s*спотифай|на\s*спотифай|пожалуйста|мне|что-нибудь|что нибудь|какую-нибудь|какую нибудь|немного)(?=\s|$)/gi, '');

  // Collapse whitespace
  q = q.replace(/\s+/g, ' ').trim();

  return { query: q, type };
}

// --- Build Alice response ---

function aliceResponse(text, endSession) {
  return {
    response: {
      text: text,
      tts: text,
      end_session: endSession !== false,
    },
    version: '1.0',
  };
}

// --- Handle Alice webhook ---

function handleAlice(body, reqLog) {
  // New session greeting
  if (body.session?.new && (!body.request?.command || body.request.command === '')) {
    reqLog.info('ALICE', 'New session greeting');
    return aliceResponse('Скажи что включить. Например: включи Coldplay, или включи альбом OK Computer.', false);
  }

  const command = body.request?.command || '';
  reqLog.info('ALICE', 'Command', { command });

  // Exit skill
  if (/(выход|выйди|хватит|закрой|закройся)/i.test(command)) {
    reqLog.info('ALICE', 'Exit command');
    return aliceResponse('Пока!', true);
  }

  // Pause/stop commands
  if (/(пауз[аеу]|стоп|останови|выключи)/i.test(command)) {
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPut(`/me/player/pause${qs}`, undefined, reqLog);
    // Direct stop via bridge — reliable even if Spotify API fails
    try { execSync('curl -s --max-time 1 "http://127.0.0.1:8888/stop"', { timeout: 2000 }); } catch(e) {}
    reqLog.info('ALICE', 'Pause sent (API + bridge)');
    return aliceResponse('Пауза.', false);
  }

  // Resume (продолжи, играй, давай, го, верни — without extra words = resume, not search)
  if (/(продолж|resume|возобнов)/i.test(command) || /^(играй|play|го|давай|верни|запусти|старт)$/i.test(command.trim())) {
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPut(`/me/player/play${qs}`, {}, reqLog);
    reqLog.info('ALICE', 'Resume sent');
    return aliceResponse('Играю.', false);
  }

  // Skip
  if (/(следующ|скип|skip|next|дальше)/i.test(command)) {
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPost(`/me/player/next${qs}`, reqLog);
    spotifyPut(`/me/player/play${qs}`, {}, reqLog); // force resume (librespot stays paused after next)
    reqLog.info('ALICE', 'Skip sent');
    return aliceResponse('Следующий.', false);
  }

  // Previous
  if (/(предыдущ|назад|previous|back)/i.test(command)) {
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPost(`/me/player/previous${qs}`, reqLog);
    spotifyPut(`/me/player/play${qs}`, {}, reqLog); // force resume (librespot stays paused after previous)
    reqLog.info('ALICE', 'Previous sent');
    return aliceResponse('Предыдущий.', false);
  }

  // What's playing?
  if (/(что\s*(сейчас\s*)?игра|что\s*за\s*(трек|песн)|current)/i.test(command)) {
    const data = spotifyGet('/me/player/currently-playing', reqLog);
    if (data?.item) {
      const name = data.item.name;
      const artist = data.item.artists?.map(a => a.name).join(', ') || '';
      reqLog.info('ALICE', 'Currently playing', { name, artist });
      return aliceResponse(`Сейчас играет ${name} от ${artist}.`, false);
    }
    reqLog.info('ALICE', 'Nothing playing');
    return aliceResponse('Сейчас ничего не играет.', false);
  }

  // Search and play — try LLM first, fallback to regex
  const llmResult = parseWithLLM(command, reqLog);
  let query, type;

  if (llmResult && llmResult.query) {
    query = llmResult.query;
    type = llmResult.type || 'track';
  } else {
    const parsed = parseCommand(command);
    query = parsed.query;
    type = parsed.type;
  }

  if (!query) {
    reqLog.warn('ALICE', 'Empty query after parsing', { originalCommand: command });
    return aliceResponse('Не поняла что включить. Скажи название трека или исполнителя.', false);
  }

  reqLog.info('ALICE', 'Search', { query, type, llm: !!llmResult });
  const result = spotifySearch(query, type, reqLog);

  if (!result) {
    reqLog.warn('ALICE', 'Not found', { query, type });
    return aliceResponse(`Не нашла "${query}" на Spotify.`, true);
  }

  reqLog.info('ALICE', 'Found', { name: result.name, artist: result.artist, uri: result.uri, type: result.type });

  // Start playback synchronously (need device to register before response)
  let played = false;
  try { played = startPlayback(result, reqLog); } catch (e) { reqLog.error('PLAY', 'Playback error', { error: e.message, stack: e.stack }); }

  // Be honest when the play command failed — "Включаю X" followed by silence
  // reads as a dead system and hides the real error.
  if (!played) {
    reqLog.warn('ALICE', 'Play failed', { name: result.name, type: result.type });
    return aliceResponse(`Нашла ${result.name}, но включить не получилось. Попробуй ещё раз.`, true);
  }

  // Build response text
  let text;
  if (result.type === 'track') {
    text = result.artist
      ? `Включаю ${result.name} от ${result.artist}.`
      : `Включаю ${result.name}.`;
  } else if (result.type === 'album') {
    text = result.artist
      ? `Включаю альбом ${result.name} от ${result.artist}.`
      : `Включаю альбом ${result.name}.`;
  } else if (result.type === 'playlist') {
    text = `Включаю плейлист ${result.name}.`;
  } else if (result.type === 'artist') {
    text = `Включаю ${result.name}.`;
  }

  return aliceResponse(text, false);
}

// --- HTTPS Server ---

const tlsOptions = {
  key: fs.readFileSync(process.env.SSL_KEY_FILE || '/root/ssl_key.pem'),
  cert: fs.readFileSync(process.env.SSL_CERT_FILE || '/root/ssl_cert.pem'),
};

const server = require('https').createServer(tlsOptions, (req, res) => {
  // Direct play endpoint (called by spotify_hls.js for auto-fallback)
  if (req.method === 'GET' && req.url.startsWith('/play?')) {
    const url = new URL(req.url, 'https://localhost');
    const q = url.searchParams.get('q') || '';
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });
    const timer = reqLog.startTimer('PLAY', 'Auto-fallback');

    if (!q) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'missing q' }));
      return;
    }

    reqLog.info('PLAY', 'Auto-fallback request', { query: q });

    // LLM parse + search + play
    const llmResult = parseWithLLM(q, reqLog);
    let query = llmResult?.query || q;
    let type = llmResult?.type || 'track';

    const result = spotifySearch(query, type, reqLog);
    if (result) {
      try { startPlayback(result, reqLog); } catch (e) { reqLog.error('PLAY', 'Playback error', { error: e.message }); }
      timer.end({ status: 'ok', name: result.name });
      reqLog.info('PLAY', 'Auto-fallback success', { name: result.name, artist: result.artist });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, name: result.name, artist: result.artist, type: result.type }));
    } else {
      timer.end({ status: 'not_found' });
      reqLog.warn('PLAY', 'Auto-fallback: not found', { query });
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, query }));
    }
    return;
  }

  // Playback control endpoints (dashboard)
  if (req.method === 'GET' && req.url === '/pause') {
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPut(`/me/player/pause${qs}`, undefined, reqLog);
    try { execSync('curl -s --max-time 1 "http://127.0.0.1:8888/stop"', { timeout: 2000 }); } catch(e) {}
    reqLog.info('HTTP', 'Pause');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === 'GET' && req.url === '/resume') {
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPut(`/me/player/play${qs}`, {}, reqLog);
    reqLog.info('HTTP', 'Resume');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === 'GET' && req.url === '/next') {
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPost(`/me/player/next${qs}`, reqLog);
    spotifyPut(`/me/player/play${qs}`, {}, reqLog);
    reqLog.info('HTTP', 'Next');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  // Fresh (uncached) Spotify Connect registration check for the watchdog:
  // the bridge can silently de-register from the Spotify cloud while its
  // /health stays green (dead dealer, stale session). found:false is the
  // ONLY definitive negative; API failure returns 503 (unknown — SOCKS flake
  // must not count as de-registration).
  if (req.method === 'GET' && req.url === '/devcheck') {
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });
    const data = spotifyGet('/me/player/devices', reqLog);
    if (!data || !Array.isArray(data.devices)) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, found: null, error: 'devices query failed' }));
      return;
    }
    const dev = data.devices.find(d => d.name === DEVICE_NAME);
    if (dev) { cachedDeviceId = dev.id; } else { cachedDeviceId = null; }
    reqLog.info('HTTP', 'Devcheck', { found: !!dev, devices: data.devices.map(d => d.name) });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, found: !!dev, devices: data.devices.map(d => d.name) }));
    return;
  }

  if (req.method === 'GET' && req.url === '/previous') {
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });
    const deviceId = getDeviceId(reqLog);
    const qs = deviceId ? `?device_id=${deviceId}` : '';
    spotifyPost(`/me/player/previous${qs}`, reqLog);
    spotifyPut(`/me/player/play${qs}`, {}, reqLog);
    reqLog.info('HTTP', 'Previous');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }

  if (req.method === 'GET' && req.url.startsWith('/search?')) {
    const url = new URL(req.url, 'https://localhost');
    const q = url.searchParams.get('q') || '';
    const reqLog = log.child({ requestId: crypto.randomBytes(4).toString('hex') });

    if (!q) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'missing q' }));
      return;
    }

    const encoded = encodeURIComponent(q);
    const data = spotifyGet(`/search?q=${encoded}&type=track,artist,album&limit=6&market=RU`, reqLog);
    if (!data) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: false, error: 'spotify api error' }));
      return;
    }

    const tracks = (data.tracks?.items || []).map(t => ({
      name: t.name,
      artist: t.artists?.map(a => a.name).join(', ') || '',
      album: t.album?.name || '',
      cover: t.album?.images?.[t.album.images.length - 1]?.url || '',
      uri: t.uri,
      duration_ms: t.duration_ms,
      type: 'track',
    }));

    reqLog.info('HTTP', 'Search', { query: q, results: tracks.length });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, tracks }));
    return;
  }

  // Debug endpoint
  if (req.method === 'GET' && req.url === '/debug') {
    const diag = log.getDiagnostics();
    diag.api = {
      totalCalls: totalApiCalls,
      errorCount: apiErrorCount,
      lastErrorTs: lastApiErrorTs,
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(diag, null, 2));
    return;
  }

  // Status endpoint
  if (req.method === 'GET' && (req.url === '/status' || req.url === '/')) {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      service: 'alice-spotify',
      hasRefreshToken: !!refreshToken,
      hasAccessToken: !!accessToken && Date.now() < tokenExpiry,
      cachedDeviceId: cachedDeviceId || null,
      totalRequests,
    }));
    return;
  }

  // Alice webhook
  if (req.method === 'POST' && req.url === '/alice') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      totalRequests++;
      lastRequestTs = new Date().toISOString();
      const requestId = crypto.randomBytes(4).toString('hex');
      const reqLog = log.child({ requestId });
      const timer = reqLog.startTimer('ALICE', 'Request');

      try {
        const json = JSON.parse(body);
        const response = handleAlice(json, reqLog);
        timer.end({ command: json.request?.command || '(none)', responseText: response.response?.text?.slice(0, 100) });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(response));
      } catch (e) {
        reqLog.error('HTTP', 'Parse/handle error', { error: e.message, stack: e.stack });
        timer.end({ status: 'error' });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(aliceResponse('Произошла ошибка.', true)));
      }
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// --- Start ---
loadTokens();
server.listen(PORT, '0.0.0.0', () => {
  log.info('SERVER', 'Alice Spotify skill ready', { port: PORT, webhook: 'POST /alice', status: 'GET /status', debug: 'GET /debug' });
});
