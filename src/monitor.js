import { config } from './config.js';

const now = () => Date.now();

async function requestJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

function toNullish(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim().toLowerCase();
  if (!s || s === 'null' || s === 'none') return null;
  return value;
}

function normalizeArrayResponse(payload) {
  if (!payload) return [];
  if (Array.isArray(payload?.data?.items)) return payload.data.items;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.items)) return payload.items;
  return [];
}

function parseNumber(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string') {
    const n = Number(v.replaceAll(',', '').trim());
    return Number.isFinite(n) ? n : null;
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parsePercent(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'string') {
    const n = Number(v.replace('%', '').trim());
    if (!Number.isFinite(n)) return null;
    return n >= 0 && n <= 1 ? n * 100 : n;
  }
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return n >= 0 && n <= 1 ? n * 100 : n;
}

function normalizeEpochMaybe(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return null;
  return n < 10_000_000_000 ? n * 1000 : n;
}

export class TokenMonitor {
  constructor() {
    this.tokens = new Map();
    this.clients = new Set();
    this.seenAddresses = new Set();
    this.startedAt = now();
    this.discoveryRunning = false;
  }

  start() {
    this.discoveryLoop().catch((e) => console.error('Initial discovery failed:', e.message));
    this.discoveryTimer = setInterval(() => this.discoveryLoop().catch((e) => console.error('Discovery failed:', e.message)), config.discoverySeconds * 1000);
    this.refreshTimer = setInterval(() => this.tick().catch((e) => console.error('Tick failed:', e.message)), config.refreshSeconds * 1000);
  }

  stop() {
    if (this.discoveryTimer) clearInterval(this.discoveryTimer);
    if (this.refreshTimer) clearInterval(this.refreshTimer);
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
      seenCount: this.seenAddresses.size,
    };
  }

  async discoveryLoop() {
    if (this.discoveryRunning) return;
    this.discoveryRunning = true;
    try {
      const listed = await this.fetchBirdeyeNewListing();
      for (const item of listed) {
        const address = item?.address;
        if (!address || this.seenAddresses.has(address)) continue;
        this.seenAddresses.add(address);
        await this.onNewToken(item);
      }
      this.publish('update', this.getState());
    } finally {
      this.discoveryRunning = false;
    }
  }

  async onNewToken(event) {
    const address = event?.address;
    if (!address || this.tokens.has(address)) return;

    this.tokens.set(address, {
      address,
      symbol: event?.symbol || 'UNKNOWN',
      name: event?.name || '',
      source: event?.source || '',
      discoveredAt: now(),
      liquidityAddedAt: event?.liquidityAddedAt || null,
      status: 'watching',
      reasons: [],
      stats: {
        holders: null,
        liquidity: parseNumber(event?.liquidity),
        fdvOrMcap: null,
        lpOverFdv: null,
        top10Percent: null,
        txCount: 0,
        buyCount: 0,
        sellCount: 0,
      },
      security: {
        mintAuthority: null,
        freezeAuthority: null,
        updateAuthority: null,
        lpBurned: null,
        lpBurnedPct: null,
        lpBurnedSource: null,
        burnCheckedAt: null,
      },
    });

    await this.enrichAndClassify(address);
  }

  async enrichAndClassify(address) {
    const token = this.tokens.get(address);
    if (!token) return;

    const [creationInfo, security, overview, metadata, helius] = await Promise.allSettled([
      this.fetchBirdeyeCreationInfo(address),
      this.fetchBirdeyeSecurity(address),
      this.fetchBirdeyeTokenOverview(address),
      this.fetchBirdeyeMetadata(address),
      this.fetchHeliusAsset(address),
    ]);

    const data = {
      creationInfo: creationInfo.status === 'fulfilled' ? creationInfo.value : null,
      security: security.status === 'fulfilled' ? security.value : null,
      overview: overview.status === 'fulfilled' ? overview.value : null,
      metadata: metadata.status === 'fulfilled' ? metadata.value : null,
      helius: helius.status === 'fulfilled' ? helius.value : null,
    };

    this.mergeData(token, data);

    const rules = this.evaluateRules(token);
    token.reasons = rules.failed;
    token.status = rules.ok ? 'whitelist' : 'blacklist';

    if (token.status === 'whitelist') {
      await this.refreshWhitelistBurned(token);
    }

    this.publish('update', this.getState());
  }

  mergeData(token, data) {
    token.symbol = data.metadata?.data?.symbol || data.overview?.data?.symbol || token.symbol;
    token.name = data.metadata?.data?.name || token.name;

    token.creationInfo = {
      source: data.creationInfo ? 'birdeye' : data.helius ? 'helius' : 'unknown',
      createdAt:
        normalizeEpochMaybe(data.creationInfo?.data?.blockUnixTime) ||
        normalizeEpochMaybe(data.creationInfo?.data?.createdTime) ||
        normalizeEpochMaybe(token.liquidityAddedAt) ||
        token.discoveredAt,
    };

    token.security.mintAuthority = toNullish(data.security?.data?.mintAuthority ?? data.helius?.result?.content?.metadata?.mintAuthority);
    token.security.freezeAuthority = toNullish(data.security?.data?.freezeAuthority ?? data.helius?.result?.content?.metadata?.freezeAuthority);
    token.security.updateAuthority = toNullish(data.security?.data?.updateAuthority ?? data.helius?.result?.authorities?.[0]?.address);

    const fdv = parseNumber(data.overview?.data?.fdv) ?? parseNumber(data.overview?.data?.marketCap) ?? parseNumber(data.metadata?.data?.fdv);
    const liquidity =
      parseNumber(data.overview?.data?.liquidity) ??
      parseNumber(data.overview?.data?.liquidityUsd) ??
      parseNumber(data.overview?.data?.liquidity_usd) ??
      token.stats.liquidity;

    token.stats.fdvOrMcap = fdv;
    token.stats.liquidity = liquidity;
    token.stats.lpOverFdv = fdv && liquidity ? Number((liquidity / fdv).toFixed(4)) : null;
    token.stats.top10Percent = parseNumber(data.security?.data?.top10HolderPercent);
    token.stats.holders = parseNumber(data.overview?.data?.holder) ?? parseNumber(data.security?.data?.holder);
    token.lastUpdateAt = now();
  }

  evaluateRules(token) {
    const failed = [];
    const lpRatio = token.stats.lpOverFdv;
    if (lpRatio === null || lpRatio <= config.lpOverFdvThreshold) {
      failed.push(`LP/FDV <= ${(config.lpOverFdvThreshold * 100).toFixed(0)}%`);
    }
    if (token.security.mintAuthority !== null) failed.push('Mint Authority not null');
    if (token.security.freezeAuthority !== null) failed.push('Freeze Authority not null');
    if (token.security.updateAuthority !== null) failed.push('Update Authority not null');
    return { ok: failed.length === 0, failed };
  }

  async refreshWhitelistBurned(token) {
    try {
      const rug = await this.fetchRugcheck(token.address);
      const markets = Array.isArray(rug?.markets) ? rug.markets : [];
      let best = null;
      let bestLiq = -1;
      for (const m of markets) {
        const liq = parseNumber(m?.liquidity) ?? 0;
        if (liq > bestLiq) {
          best = m;
          bestLiq = liq;
        }
      }

      const burnPct = parsePercent(best?.lp?.burnPct ?? rug?.liquidityDetails?.lpBurnPct ?? rug?.liquidityDetails?.lpBurnedPct);
      token.security.lpBurnedPct = burnPct;
      token.security.lpBurned = burnPct !== null ? burnPct >= 99.5 : null;
      token.security.lpBurnedSource = 'rugcheck/report';
      token.security.burnCheckedAt = now();
    } catch (error) {
      token.security.lpBurnedSource = 'rugcheck_failed';
      token.security.burnCheckedAt = now();
      console.warn(`Rugcheck whitelist burn refresh failed for ${token.address}:`, error.message);
    }
  }

  async tick() {
    const stamp = now();
    for (const token of this.tokens.values()) {
      if (token.status === 'whitelist') {
        await this.refreshMarketStats(token);
        if (!token.security.burnCheckedAt || stamp - token.security.burnCheckedAt > config.rugcheckRefreshMinutes * 60 * 1000) {
          await this.refreshWhitelistBurned(token);
        }
        this.applyWhitelistExit(token, stamp);
      }
      if (token.status === 'blacklist') {
        const ageMs = stamp - token.discoveredAt;
        if (ageMs > config.blacklistTtlMinutes * 60 * 1000) this.tokens.delete(token.address);
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
    const [overview, security] = await Promise.allSettled([
      this.fetchBirdeyeTokenOverview(token.address),
      this.fetchBirdeyeSecurity(token.address),
    ]);

    this.mergeData(token, {
      overview: overview.status === 'fulfilled' ? overview.value : null,
      security: security.status === 'fulfilled' ? security.value : null,
      creationInfo: null,
      metadata: null,
      helius: null,
    });

    token.stats.txCount = token.stats.txCount ?? 0;
    token.stats.buyCount = token.stats.buyCount ?? 0;
    token.stats.sellCount = token.stats.sellCount ?? 0;
  }

  birdeyeHeaders() {
    return {
      'x-chain': 'solana',
      ...(config.birdeyeApiKey ? { 'x-api-key': config.birdeyeApiKey } : {}),
    };
  }

  async fetchBirdeyeNewListing() {
    const url = `${config.birdeyeApiUrl}/defi/v2/tokens/new_listing?limit=${config.maxNewListingPageSize}`;
    const json = await requestJson(url, { headers: this.birdeyeHeaders() });
    return normalizeArrayResponse(json);
  }

  async fetchBirdeyeCreationInfo(address) {
    const url = `${config.birdeyeApiUrl}/defi/token_creation_info?address=${address}`;
    return requestJson(url, { headers: this.birdeyeHeaders() });
  }

  async fetchBirdeyeSecurity(address) {
    const url = `${config.birdeyeApiUrl}/defi/token_security?address=${address}`;
    return requestJson(url, { headers: this.birdeyeHeaders() });
  }

  async fetchBirdeyeMetadata(address) {
    const url = `${config.birdeyeApiUrl}/defi/v3/token/meta-data/single?address=${address}`;
    return requestJson(url, { headers: this.birdeyeHeaders() });
  }

  async fetchBirdeyeTokenOverview(address) {
    const url = `${config.birdeyeApiUrl}/defi/token_overview?address=${address}`;
    return requestJson(url, { headers: this.birdeyeHeaders() });
  }

  async fetchHeliusAsset(address) {
    if (!config.heliusApiKey) return null;
    const url = `${config.heliusApiUrl}/?api-key=${config.heliusApiKey}`;
    return requestJson(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: '1', method: 'getAsset', params: { id: address } }),
    });
  }

  async fetchRugcheck(address) {
    const url = `${config.rugcheckApiUrl}/v1/tokens/${address}/report`;
    return requestJson(url, { headers: { Accept: 'application/json' } });
  }
}
