#!/usr/bin/env node
// One-time Spotify OAuth helper.
// 1. Set CLIENT_ID and CLIENT_SECRET below
// 2. Run: node spotify_auth.js
// 3. Open the printed URL in a browser
// 4. After redirect, refresh_token is saved to spotify_tokens.json

const http = require('http');
const https = require('https');
const { URL } = require('url');
const fs = require('fs');
const path = require('path');

// --- Fill these in from https://developer.spotify.com/dashboard ---
const CLIENT_ID     = process.env.SPOTIFY_CLIENT_ID     || '';
const CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET  || '';

const PORT         = 8889;
const REDIRECT_URI = process.env.SPOTIFY_REDIRECT_URI || 'https://your-domain.example.com/callback';
const SCOPES       = 'user-modify-playback-state user-read-playback-state streaming';
const TOKEN_FILE   = path.join(__dirname, 'spotify_tokens.json');

const authUrl = `https://accounts.spotify.com/authorize?` +
  `client_id=${CLIENT_ID}` +
  `&response_type=code` +
  `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
  `&scope=${encodeURIComponent(SCOPES)}`;

console.log('\n=== Spotify OAuth ===');
console.log('Open this URL in your browser:\n');
console.log(authUrl);
console.log('\nWaiting for callback on port', PORT, '...\n');

const tlsOptions = {
  key: fs.readFileSync(process.env.SSL_KEY_FILE || '/root/ssl_key.pem'),
  cert: fs.readFileSync(process.env.SSL_CERT_FILE || '/root/ssl_cert.pem'),
};

const server = https.createServer(tlsOptions, async (req, res) => {
  const url = new URL(req.url, `https://localhost:${PORT}`);
  if (url.pathname !== '/callback') {
    res.writeHead(404); res.end('Not found'); return;
  }

  console.log('Callback URL:', req.url);
  const error = url.searchParams.get('error');
  if (error) {
    res.writeHead(400); res.end('Spotify error: ' + error);
    console.error('Spotify denied:', error);
    return;
  }
  const code = url.searchParams.get('code');
  if (!code) {
    res.writeHead(400); res.end('No code in callback. Full URL: ' + req.url);
    return;
  }

  // Exchange code for tokens
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: REDIRECT_URI,
  }).toString();

  const authHeader = 'Basic ' + Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');

  const tokenReq = https.request('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': authHeader,
    },
  }, (tokenRes) => {
    let data = '';
    tokenRes.on('data', c => data += c);
    tokenRes.on('end', () => {
      try {
        const json = JSON.parse(data);
        if (json.error) {
          res.writeHead(500); res.end('Error: ' + json.error_description);
          console.error('Token error:', json);
          return;
        }
        const tokens = {
          access_token: json.access_token,
          refresh_token: json.refresh_token,
          expires_in: json.expires_in,
          obtained_at: new Date().toISOString(),
        };
        fs.writeFileSync(TOKEN_FILE, JSON.stringify(tokens, null, 2));
        console.log('Tokens saved to', TOKEN_FILE);
        console.log('refresh_token:', json.refresh_token.slice(0, 20) + '...');
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end('<h1>Готово! Refresh token сохранён. Можно закрыть вкладку.</h1>');
        setTimeout(() => process.exit(0), 1000);
      } catch (e) {
        res.writeHead(500); res.end('Parse error');
        console.error('Parse error:', e, data);
      }
    });
  });
  tokenReq.on('error', e => { res.writeHead(500); res.end(e.message); });
  tokenReq.write(body);
  tokenReq.end();
});

server.listen(PORT, '0.0.0.0');
