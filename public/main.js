const whitelistBody = document.getElementById('whitelist');
const blacklistBody = document.getElementById('blacklist');
const meta = document.getElementById('meta');

function fmtAge(ms) {
  const sec = Math.floor(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m`;
  const h = Math.floor(min / 60);
  const rm = min % 60;
  return `${h}h ${rm}m`;
}

function fmtNum(v) {
  if (v === null || v === undefined) return '-';
  return Number(v).toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function tokenLink(address) {
  return `https://gmgn.ai/sol/token/${address}`;
}

function render(state) {
  const now = Date.now();
  meta.textContent = `总收录: ${state.total} | 观察池: ${state.pool} | 最近刷新: ${new Date().toLocaleTimeString()}`;

  whitelistBody.innerHTML = state.whitelist
    .map((t) => {
      const age = fmtAge(now - t.discoveredAt);
      return `<tr>
        <td>${t.symbol || 'UNKNOWN'}</td>
        <td><a href="${tokenLink(t.address)}" target="_blank" rel="noreferrer">${t.address}</a></td>
        <td>${age}</td>
        <td>${fmtNum(t.stats.holders)}</td>
        <td>${fmtNum(t.stats.liquidity)}</td>
        <td>${fmtNum(t.stats.fdvOrMcap)}</td>
        <td>${fmtNum(t.stats.lpOverFdv)}</td>
        <td>${fmtNum(t.stats.top10Percent)}%</td>
        <td>${fmtNum(t.stats.txCount)}</td>
        <td>${fmtNum(t.stats.buyCount)} / ${fmtNum(t.stats.sellCount)}</td>
      </tr>`;
    })
    .join('');

  blacklistBody.innerHTML = state.blacklist
    .map(
      (t) => `<tr>
        <td>${t.symbol || 'UNKNOWN'}</td>
        <td><a href="${tokenLink(t.address)}" target="_blank" rel="noreferrer">${t.address}</a></td>
        <td>${new Date(t.discoveredAt).toLocaleTimeString()}</td>
        <td>${(t.reasons || []).join(', ') || '-'}</td>
      </tr>`,
    )
    .join('');
}

async function bootstrap() {
  const state = await fetch('/api/state').then((r) => r.json());
  render(state);

  const es = new EventSource('/events');
  es.addEventListener('update', (evt) => render(JSON.parse(evt.data)));
  es.addEventListener('snapshot', (evt) => render(JSON.parse(evt.data)));
}

bootstrap();
