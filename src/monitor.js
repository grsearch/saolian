import { config } from './config.js';

const now = () => Date.now();

async function requestJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.json();
}

function toNullish(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).toLowerCase();
  if (s === 'null' || s === 'none' || s === '') return null;
  return value;
}

function normalizeAddress(event) {
  return event?.address || event?.tokenAddress || event?.baseAddress || event?.mint || event?.baseMint || event?.token || null;
}

function normalizePair(event) {
  return event?.pairAddress || event?.pair || event?.lpAddress || null;
}

export class TokenMonitor {
  constructor() {
    this.tokens = new Map();
    this.pairs = new Map();
    this.clients = new Set();
    this.birdeyeSocket = null;
    this.startedAt = now();
  }

  start() {
    this.connectBirdeye();
    this.cron = setInterval(() => this.tick().catch((e) => console.error(e)), config.refreshSeconds * 1000);
  }

  stop() {
    if (this.cron) clearInterval(this.cron);
    this.birdeyeSocket?.close();
  }

  subscribeClient(res) {
    this.clients.add(res);
    res.write(`event: snapshot\ndata: ${JSON.stringify(this.getState())}\n\n`);
  }

  unsubscribeClient(res) {
    this.clients.delete(res);
  }

  publish(event, payload) {
    const message = `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const client of this.clients) client.write(message);
  }

  getState() {
    const list = [...this.tokens.values()];
    return {
      startedAt: this.startedAt,
      whitelist: list.filter((t) => t.status === 'whitelist').sort((a, b) => b.discoveredAt - a.discoveredAt),
      blacklist: list.filter((t) => t.status === 'blacklist').sort((a, b) => b.discoveredAt - a.discoveredAt),
      pool: list.filter((t) => t.status === 'watching').length,
      total: list.length,
    };
  }

  connectBirdeye() {
    try {
      const wsUrl = config.birdeyeApiKey
        ? `${config.birdeyeWsUrl}${config.birdeyeWsUrl.includes('?') ? '&' : '?'}x-api-key=${config.birdeyeApiKey}`
        : config.birdeyeWsUrl;
      this.birdeyeSocket = new WebSocket(wsUrl);
    } catch (error) {
      console.error('Birdeye websocket init failed:', error.message);
      return;
    }

    this.birdeyeSocket.addEventListener('open', () => {
      this.sendWs({ type: 'SUBSCRIBE_TOKEN_NEW_LISTING' });
      this.sendWs({ type: 'SUBSCRIBE_NEW_PAIR' });
      console.log('Connected Birdeye websocket.');
    });

    this.birdeyeSocket.addEventListener('message', (event) => {
      try {
        this.handleWsMessage(typeof event.data === 'string' ? event.data : String(event.data));
      } catch (error) {
        console.error('WS parse error:', error.message);
      }
    });

    this.birdeyeSocket.addEventListener('close', () => {
      console.warn('Birdeye websocket closed, reconnecting in 5s...');
      setTimeout(() => this.connectBirdeye(), 5000);
    });

    this.birdeyeSocket.addEventListener('error', (err) => {
      console.error('Birdeye websocket error', err.message || err);
    });
  }

  sendWs(obj) {
    if (this.birdeyeSocket?.readyState === WebSocket.OPEN) {
      this.birdeyeSocket.send(JSON.stringify(obj));
    }
  }

  handleWsMessage(raw) {
    const parsed = JSON.parse(raw);
    const type = parsed?.type || parsed?.channel || parsed?.topic;
    const payload = parsed?.data || parsed?.payload || parsed;

    if (String(type).includes('NEW_LISTING') || payload?.event === 'new_token') {
      this.onNewToken(payload);
      return;
    }

    if (String(type).includes('NEW_PAIR') || payload?.event === 'new_pair') {
      this.onNewPair(payload);
      return;
    }

    if (String(type).includes('TRADE')) {
      this.onTrade(payload);
    }
  }

  async onNewToken(event) {
    const address = normalizeAddress(event);
    if (!address || this.tokens.has(address)) return;

    this.tokens.set(address, {
      address,
      symbol: event?.symbol || 'UNKNOWN',
      discoveredAt: now(),
      pairAddress: normalizePair(event),
      status: 'watching',
      reasons: [],
      stats: {
        holders: null,
        liquidity: null,
        fdvOrMcap: null,
        lpOverFdv: null,
        top10Percent: null,
        txCount: 0,
        buyCount: 0,
        sellCount: 0,
      },
      security: {},
    });

    await this.enrichAndClassify(address);
  }

  async onNewPair(event) {
    const pair = normalizePair(event);
    if (!pair || this.pairs.has(pair)) return;
    this.pairs.set(pair, { ...event, discoveredAt: now() });

    const tokenAddress = normalizeAddress(event);
    if (tokenAddress && !this.tokens.has(tokenAddress)) {
      await this.onNewToken({ ...event, address: tokenAddress, pairAddress: pair });
    }
  }

  onTrade(event) {
    const address = normalizeAddress(event);
    if (!address) return;
    const token = this.tokens.get(address);
    if (!token || token.status !== 'whitelist') return;

    token.stats.txCount += 1;
    const side = String(event?.side || event?.type || '').toLowerCase();
    if (side.includes('buy')) token.stats.buyCount += 1;
    if (side.includes('sell')) token.stats.sellCount += 1;
    token.lastUpdateAt = now();

    this.publish('update', this.getState());
  }

  async enrichAndClassify(address) {
    const token = this.tokens.get(address);
    if (!token) return;

    const [birdeye, helius, rugcheck] = await Promise.allSettled([
      this.fetchBirdeyeToken(address),
      this.fetchHeliusAsset(address),
      this.fetchRugcheck(address),
    ]);

    const data = {
      birdeye: birdeye.status === 'fulfilled' ? birdeye.value : null,
      helius: helius.status === 'fulfilled' ? helius.value : null,
      rugcheck: rugcheck.status === 'fulfilled' ? rugcheck.value : null,
    };

    this.mergeData(token, data);

    const rules = this.evaluateRules(token);
    token.reasons = rules.failed;
    token.status = rules.ok ? 'whitelist' : 'blacklist';

    if (token.status === 'whitelist') {
      this.sendWs({ type: 'SUBSCRIBE_TOKEN_TRADES', data: { address } });
      if (token.pairAddress) this.sendWs({ type: 'SUBSCRIBE_PAIR_TRADES', data: { pairAddress: token.pairAddress } });
      await this.refreshMarketStats(token);
    }

    this.publish('update', this.getState());
  }

  mergeData(token, data) {
    token.symbol = data.birdeye?.data?.symbol || data.rugcheck?.tokenMeta?.symbol || token.symbol;

    token.creationInfo = {
      source: data.helius ? 'helius' : data.birdeye ? 'birdeye' : data.rugcheck ? 'rugcheck' : 'unknown',
      createdAt: data.birdeye?.data?.createdAt || data.rugcheck?.tokenMeta?.mintTime || token.discoveredAt,
    };

    const mintAuthority = toNullish(data.helius?.result?.content?.metadata?.mintAuthority ?? data.rugcheck?.token?.mintAuthority);
    const freezeAuthority = toNullish(data.helius?.result?.content?.metadata?.freezeAuthority ?? data.rugcheck?.token?.freezeAuthority);
    const updateAuthority = toNullish(data.helius?.result?.authorities?.[0]?.address ?? data.rugcheck?.token?.updateAuthority);
    const lpBurnedPct = Number(
      data.rugcheck?.markets?.[0]?.lp?.burnPct ??
      data.rugcheck?.liquidityDetails?.lpBurnPct ??
      data.birdeye?.data?.lpBurnedPct ??
      0,
    );

    token.security = {
      mintAuthority,
      freezeAuthority,
      updateAuthority,
      lpBurnedPct,
    };

    const fdv = Number(data.birdeye?.data?.fdv || data.birdeye?.data?.marketCap || data.rugcheck?.tokenMeta?.marketCap || 0);
    const liquidity = Number(data.birdeye?.data?.liquidity || data.rugcheck?.markets?.[0]?.liquidity || 0);

    token.stats.fdvOrMcap = fdv || null;
    token.stats.liquidity = liquidity || null;
    token.stats.lpOverFdv = fdv > 0 && liquidity > 0 ? Number((liquidity / fdv).toFixed(4)) : null;
    token.stats.top10Percent = Number(data.rugcheck?.token?.topHoldersPct || 0) || null;
    token.stats.holders = Number(data.birdeye?.data?.holder || data.rugcheck?.token?.holderCount || 0) || null;
    token.lastUpdateAt = now();
  }

  evaluateRules(token) {
    const failed = [];

    if ((token.security.lpBurnedPct ?? 0) <= 95) failed.push('LP Burned <= 95%');
    if (token.security.mintAuthority !== null) failed.push('Mint Authority not null');
    if (token.security.freezeAuthority !== null) failed.push('Freeze Authority not null');
    if (token.security.updateAuthority !== null) failed.push('Update Authority not null');

    return { ok: failed.length === 0, failed };
  }

  async tick() {
    const stamp = now();
    for (const token of this.tokens.values()) {
      if (token.status === 'whitelist') {
        await this.refreshMarketStats(token);
        this.applyWhitelistExit(token, stamp);
      }
      if (token.status === 'blacklist') {
        const ageMs = stamp - token.discoveredAt;
        if (ageMs > config.blacklistTtlMinutes * 60 * 1000) {
          this.tokens.delete(token.address);
        }
      }
    }

    this.publish('update', this.getState());
  }

  applyWhitelistExit(token, stamp) {
    const ageMs = stamp - token.discoveredAt;
    const fdv = token.stats.fdvOrMcap || 0;

    if (ageMs > config.whitelistExitHours * 3600 * 1000) {
      this.tokens.delete(token.address);
      return;
    }

    if (ageMs > config.staleLowCapHours * 3600 * 1000 && fdv < config.lowCapThreshold) {
      this.tokens.delete(token.address);
    }
  }

  async refreshMarketStats(token) {
    try {
      const data = await this.fetchBirdeyeToken(token.address);
      this.mergeData(token, { birdeye: data, helius: null, rugcheck: null });
    } catch {
      // ignore transient refresh errors
    }
  }

  async fetchBirdeyeToken(address) {
    if (!config.birdeyeApiKey) return null;
    const url = `${config.birdeyeApiUrl}/defi/token_overview?address=${address}`;
    return requestJson(url, { headers: { 'x-api-key': config.birdeyeApiKey } });
  }

  async fetchHeliusAsset(address) {
    if (!config.heliusApiKey) return null;
    const url = `${config.heliusApiUrl}/?api-key=${config.heliusApiKey}`;
    return requestJson(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '1',
        method: 'getAsset',
        params: { id: address },
      }),
    });
  }

  async fetchRugcheck(address) {
    const url = `${config.rugcheckApiUrl}/v1/tokens/${address}/report`;
    return requestJson(url, { headers: { Accept: 'application/json' } });
  }
}
