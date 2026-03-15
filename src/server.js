import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { TokenMonitor } from './monitor.js';
import { config } from './config.js';

const __dirname = fileURLToPath(new URL('.', import.meta.url));
const root = join(__dirname, '..');

const monitor = new TokenMonitor();
monitor.start();

const mime = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
};

const server = createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host}`);

  if (url.pathname === '/api/state') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(monitor.getState()));
    return;
  }

  if (url.pathname === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    monitor.subscribeClient(res);
    req.on('close', () => monitor.unsubscribeClient(res));
    return;
  }

  const path = url.pathname === '/' ? '/public/index.html' : `/public${url.pathname}`;
  try {
    const content = await readFile(join(root, path));
    res.writeHead(200, { 'Content-Type': mime[extname(path)] || 'application/octet-stream' });
    res.end(content);
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  }
});

server.listen(config.port, () => {
  console.log(`Dashboard running at http://localhost:${config.port}`);
});

process.on('SIGINT', () => {
  monitor.stop();
  server.close(() => process.exit(0));
});
