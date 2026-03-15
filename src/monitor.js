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

function parsePercent(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    const cleaned = value.replace('%', '').trim();
    const num = Number(cleaned);
    if (!Number.isFinite(num)) return null;
    if (num >= 0 && num <= 1) return num * 100;
    return num;
  }

  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (num >= 0 && num <= 1) return num * 100;
  return num;
}

function firstNonNull(values) {
  for (const value of values) {
    if (value !== null && value !== undefined) return value;
  }
  return null;
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
        liquidity: Number(event?.liquidity || 0) || null,
        fdvOrMcap: null,
        lpOverFdv: null,
        top10Percent: null,
        txCount: 0,
        buyCount: 0,
        sellCount: 0,
      },
      security: {
        lpBurnedPct: null,
        lpBurnedSource: null,
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
      rugcheck: null,
    };

    // 仅当 Birdeye 关键字段缺失时才使用 Rugcheck 兜底，减少免费 API 限频影响。
    if (this.needsRugcheckFallback(data)) {
      try {
        data.rugcheck = await this.fetchRugcheck(address);
      } catch (error) {
        console.warn(`Rugcheck fallback failed for ${address}:`, error.message);
      }
    }

    this.mergeData(token, data);
    const rules = this.evaluateRules(token);
    token.reasons = rules.failed;
    token.status = rules.ok ? 'whitelist' : 'blacklist';

    if (token.status === 'whitelist') {
      await this.refreshMarketStats(token);
    }

    this.publish('update', this.getState());
  }

  needsRugcheckFallback(data) {
    const sec = data.security?.data || {};
    const lp = firstNonNull([sec.lpBurnedPercent, sec.lpBurnedPct]);
    const missingAuthorities = sec.mintAuthority === undefined || sec.freezeAuthority === undefined || sec.updateAuthority === undefined;
    return lp === null || lp === undefined || missingAuthorities;
  }

  resolveLpBurned(data) {
    const birdeyeSec = data.security?.data || {};
    const birdeyeCandidates = [
      birdeyeSec.lpBurnedPercent,
      birdeyeSec.lpBurnedPct,
      birdeyeSec.lp_burned_percent,
      birdeyeSec.lp_burned_pct,
    ]
      .map(parsePercent)
      .filter((v) => v !== null);

    if (birdeyeCandidates.length > 0) {
      return { value: Math.max(...birdeyeCandidates), source: 'birdeye/token_security' };
    }

    const rugcheckCandidates = [
      data.rugcheck?.markets?.[0]?.lp?.burnPct,
      data.rugcheck?.liquidityDetails?.lpBurnPct,
      data.rugcheck?.liquidityDetails?.lpBurnedPct,
    ]
      .map(parsePercent)
      .filter((v) => v !== null);

    if (rugcheckCandidates.length > 0) {
      return { value: Math.max(...rugcheckCandidates), source: 'rugcheck/report' };
    }

    return { value: null, source: 'unknown' };
  }

  mergeData(token, data) {
    token.symbol = data.metadata?.data?.symbol || data.overview?.data?.symbol || data.rugcheck?.tokenMeta?.symbol || token.symbol;
    token.name = data.metadata?.data?.name || token.name;

    token.creationInfo = {
      source: data.creationInfo ? 'birdeye' : data.helius ? 'helius' : data.rugcheck ? 'rugcheck' : 'unknown',
      createdAt:
        data.creationInfo?.data?.blockUnixTime ||
        data.creationInfo?.data?.createdTime ||
        data.rugcheck?.tokenMeta?.mintTime ||
        token.liquidityAddedAt ||
        token.discoveredAt,
    };

    const birdeyeSec = data.security?.data || {};
    const mintAuthority = toNullish(
      birdeyeSec.mintAuthority ?? data.helius?.result?.content?.metadata?.mintAuthority ?? data.rugcheck?.token?.mintAuthority,
    );
    const freezeAuthority = toNullish(
      birdeyeSec.freezeAuthority ?? data.helius?.result?.content?.metadata?.freezeAuthority ?? data.rugcheck?.token?.freezeAuthority,
    );
    const updateAuthority = toNullish(
      birdeyeSec.updateAuthority ?? data.helius?.result?.authorities?.[0]?.address ?? data.rugcheck?.token?.updateAuthority,
    );
    const lp = this.resolveLpBurned(data);

    token.security = {
      mintAuthority,
      freezeAuthority,
      updateAuthority,
      lpBurnedPct: lp.value,
      lpBurnedSource: lp.source,
    };

    const fdv = Number(
      data.overview?.data?.fdv ||
        data.overview?.data?.marketCap ||
        data.metadata?.data?.fdv ||
        data.rugcheck?.tokenMeta?.marketCap ||
        0,
    );
    const liquidity = Number(data.overview?.data?.liquidity || token.stats.liquidity || data.rugcheck?.markets?.[0]?.liquidity || 0);

    token.stats.fdvOrMcap = fdv || null;
    token.stats.liquidity = liquidity || null;
    token.stats.lpOverFdv = fdv > 0 && liquidity > 0 ? Number((liquidity / fdv).toFixed(4)) : null;
    token.stats.top10Percent = Number(data.security?.data?.top10HolderPercent || data.rugcheck?.token?.topHoldersPct || 0) || null;
    token.stats.holders = Number(data.overview?.data?.holder || data.security?.data?.holder || data.rugcheck?.token?.holderCount || 0) || null;
    token.lastUpdateAt = now();
  }

  evaluateRules(token) {
    const failed = [];
    const lpBurnedPct = token.security.lpBurnedPct;

    if (lpBurnedPct === null || lpBurnedPct <= 95) failed.push('LP Burned <= 95%');
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
      rugcheck: null,
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
