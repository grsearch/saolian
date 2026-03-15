import { config } from './config.js';

const now = () => Date.now();

async function requestJson(url, options = {}, timeoutMs = config.httpTimeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return res.json();
  } finally {
    clearTimeout(timer);
  }
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
    this.tickRunning = false;
    this.tickCursor = 0;
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
    const whitelist = list.filter((t) => t.status === 'whitelist').sort((a, b) => b.discoveredAt - a.discoveredAt);
    const blacklist = list.filter((t) => t.status === 'blacklist').sort((a, b) => b.discoveredAt - a.discoveredAt);

    return {
      startedAt: this.startedAt,
      whitelistTotal: whitelist.length,
      blacklistTotal: blacklist.length,
      whitelist: whitelist.slice(0, config.maxStateRows),
      blacklist: blacklist.slice(0, config.maxStateRows),
      pool: list.filter((t) => t.status === 'watching').length,
      total: list.length,
      seenCount: this.seenAddresses.size,
      maxStateRows: config.maxStateRows,
    };
  }

  pruneIfNeeded() {
    if (this.tokens.size <= config.maxTrackedTokens) return;
    const sorted = [...this.tokens.values()].sort((a, b) => a.discoveredAt - b.discoveredAt);
    const removeCount = this.tokens.size - config.maxTrackedTokens;
    for (let i = 0; i < removeCount; i += 1) {
      this.tokens.delete(sorted[i].address);
    }
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
        await this.onNewToken(item, { publish: false });
      }
      this.pruneIfNeeded();
      this.publish('update', this.getState());
    } finally {
      this.discoveryRunning = false;
    }
  }

  async onNewToken(event, options = { publish: true }) {
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

    await this.enrichAndClassify(address, options);
  }

  async enrichAndClassify(address, options = { publish: true }) {
    const token = this.tokens.get(address);
    if (!token) return;

    const [creationInfo, security, overview, metadata, helius] = await Promise.allSettled([
      this.fetchBirdeyeCreationInfo(address),
      this.fetchBirdeyeSecurity(address),
      this.fetchBirdeyeTokenOverview(address),
      this.fetchBirdeyeMetadata(address),
      this.fetchHeliusAsset(address),
    ]);

    this.mergeData(token, {
      creationInfo: creationInfo.status === 'fulfilled' ? creationInfo.value : null,
      security: security.status === 'fulfilled' ? security.value : null,
      overview: overview.status === 'fulfilled' ? overview.value : null,
      metadata: metadata.status === 'fulfilled' ? metadata.value : null,
      helius: helius.status === 'fulfilled' ? helius.value : null,
    });

    const rules = this.evaluateRules(token);
    token.reasons = rules.failed;
    token.status = rules.ok ? 'whitelist' : 'blacklist';

    if (token.status === 'whitelist') {
      await this.refreshWhitelistBurned(token);
    }

    if (options.publish) this.publish('update', this.getState());
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
    token.stats.top10Percent =
      parsePercent(data.security?.data?.top10HolderPercent) ??
      parsePercent(data.security?.data?.top10HolderRatio) ??
      parsePercent(data.overview?.data?.top10HolderPercent) ??
      parsePercent(data.overview?.data?.top10HolderRatio);

    token.stats.holders = parseNumber(data.overview?.data?.holder) ?? parseNumber(data.security?.data?.holder);

    token.stats.txCount =
      parseNumber(data.overview?.data?.txCount24h) ??
      parseNumber(data.overview?.data?.trade24h) ??
      parseNumber(data.overview?.data?.trade24hCount) ??
      parseNumber(data.overview?.data?.txns24h) ??
      token.stats.txCount ??
      0;

    token.stats.buyCount =
      parseNumber(data.overview?.data?.buy24h) ??
      parseNumber(data.overview?.data?.buy24hCount) ??
      parseNumber(data.overview?.data?.buyTx24h) ??
      token.stats.buyCount ??
      0;

    token.stats.sellCount =
      parseNumber(data.overview?.data?.sell24h) ??
      parseNumber(data.overview?.data?.sell24hCount) ??
      parseNumber(data.overview?.data?.sellTx24h) ??
      token.stats.sellCount ??
      0;

    token.lastUpdateAt = now();
  }

  evaluateRules(token) {
    const failed = [];
    const lpRatio = token.stats.lpOverFdv;
    if (lpRatio === null || lpRatio <= config.lpOverFdvThreshold) failed.push(`LP/FDV <= ${(config.lpOverFdvThreshold * 100).toFixed(0)}%`);
    if (lpRatio !== null && lpRatio >= config.lpOverFdvUpperThreshold) failed.push(`LP/FDV >= ${(config.lpOverFdvUpperThreshold * 100).toFixed(0)}%`);
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

      const top10FromRugcheck = parsePercent(rug?.token?.topHoldersPct);
      if (top10FromRugcheck !== null) token.stats.top10Percent = top10FromRugcheck;
    } catch (error) {
      token.security.lpBurnedSource = 'rugcheck_failed';
      token.security.burnCheckedAt = now();
      console.warn(`Rugcheck whitelist burn refresh failed for ${token.address}:`, error.message);
    }
  }

  async tick() {
    if (this.tickRunning) return;
    this.tickRunning = true;
    try {
      const stamp = now();
      const all = [...this.tokens.values()];
      const batchSize = Math.max(1, config.tickBatchSize);
      if (this.tickCursor >= all.length) this.tickCursor = 0;
      const batch = all.slice(this.tickCursor, this.tickCursor + batchSize);
      this.tickCursor += batch.length;
      if (this.tickCursor >= all.length) this.tickCursor = 0;

      for (const token of batch) {
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

      this.pruneIfNeeded();
      this.publish('update', this.getState());
    } finally {
      this.tickRunning = false;
    }
  }

  applyWhitelistExit(token, stamp) {
    const ageMs = stamp - token.discoveredAt;
    const fdv = token.stats.fdvOrMcap || 0;

    if (fdv < config.immediateMinFdv) {
      this.tokens.delete(token.address);
      return;
    }

    if (ageMs > config.whitelistExitHours * 3600 * 1000) {
      this.tokens.delete(token.address);
      return;
    }
    if (ageMs > config.staleLowCapHours * 3600 * 1000 && fdv < config.lowCapThreshold) this.tokens.delete(token.address);
  }

  async refreshMarketStats(token) {
    const [overview, security] = await Promise.allSettled([this.fetchBirdeyeTokenOverview(token.address), this.fetchBirdeyeSecurity(token.address)]);
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
    return normalizeArrayResponse(await requestJson(url, { headers: this.birdeyeHeaders() }));
  }

  async fetchBirdeyeCreationInfo(address) {
    return requestJson(`${config.birdeyeApiUrl}/defi/token_creation_info?address=${address}`, { headers: this.birdeyeHeaders() });
  }

  async fetchBirdeyeSecurity(address) {
    return requestJson(`${config.birdeyeApiUrl}/defi/token_security?address=${address}`, { headers: this.birdeyeHeaders() });
  }

  async fetchBirdeyeMetadata(address) {
    return requestJson(`${config.birdeyeApiUrl}/defi/v3/token/meta-data/single?address=${address}`, { headers: this.birdeyeHeaders() });
  }

  async fetchBirdeyeTokenOverview(address) {
    return requestJson(`${config.birdeyeApiUrl}/defi/token_overview?address=${address}`, { headers: this.birdeyeHeaders() });
  }

  async fetchHeliusAsset(address) {
    if (!config.heliusApiKey) return null;
    return requestJson(`${config.heliusApiUrl}/?api-key=${config.heliusApiKey}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: '1', method: 'getAsset', params: { id: address } }),
    });
  }

  async fetchRugcheck(address) {
    return requestJson(`${config.rugcheckApiUrl}/v1/tokens/${address}/report`, { headers: { Accept: 'application/json' } });
  }
}
