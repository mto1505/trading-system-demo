// Static server cho ui/ + proxy same-origin toi backend (tranh CORS).
// Chay: node ui/serve.js  -> http://localhost:8080
// Chi tiet: xem DEPLOY-LOCAL.md, muc "Giao dien UI local".
const http = require('http');
const fs = require('fs');
const path = require('path');

const UI_DIR = __dirname;
const PORT = 8080;
const PORTS = ['8081', '8082', '8083', '8084'];

const server = http.createServer((req, res) => {
  const m = req.url.match(/^\/p\/(\d{4})(\/.*)$/);
  if (m && PORTS.includes(m[1])) {
    const pr = http.request(
      { host: '127.0.0.1', port: +m[1], path: m[2], method: req.method,
        headers: { ...req.headers, host: `127.0.0.1:${m[1]}` } },
      (pres) => { res.writeHead(pres.statusCode, pres.headers); pres.pipe(res); }
    );
    pr.on('error', (e) => { res.writeHead(502); res.end('proxy error: ' + e.message); });
    req.pipe(pr);
    return;
  }

  let file = req.url.split('?')[0];
  if (file === '/' || file === '') file = '/index.html';
  const full = path.join(UI_DIR, path.normalize(file).replace(/^[\/]+/, ''));
  fs.readFile(full, (err, buf) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    if (full.endsWith('.html')) {
      let html = buf.toString('utf8');
      for (const p of PORTS) html = html.split(`http://localhost:${p}/api/v1`).join(`/p/${p}/api/v1`);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } else { res.writeHead(200); res.end(buf); }
  });
});

server.listen(PORT, () => console.log(`UI running at http://localhost:${PORT}`));
