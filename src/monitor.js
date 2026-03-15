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

function getPath(obj, path) {
  if (!obj) return null;
  const segs = path.split('.');
  let cur = obj;
  for (const seg of segs) {
    if (cur === null || cur === undefined) return null;
    if (/^\d+$/.test(seg)) {
      cur = cur[Number(seg)];
    } else {
      cur = cur[seg];
    }
  }
  return cur ?? null;
}

function pickFirstPath(obj, paths) {
  for (const path of paths) {
    const value = getPath(obj, path);
    if (value !== null && value !== undefined) return { value, path };
  }
  return { value: null, path: null };
}

function normalizeEpochMaybe(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n < 10_000_000_000) return n * 1000;
  return n;
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
        lpBurnedPctRaw: null,
        lpBurnedPct: null,
        lpBurnedSource: null,
        lpBurnedPath: null,
        lpLockedPctRaw: null,
        lpLockedPct: null,
        lpLockedSource: null,
        lpLockedPath: null,
        lpPassed: false,
        lpReason: null,
        mainPairAddress: null,
        mainPairLiquidityUsd: null,
        mainPairDex: null,
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
      securityFetchOk: security.status === 'fulfilled',
    };

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
    const sec = data.security;
    if (!sec) return true;

    const lpBurned = pickFirstPath(sec, [
      'data.lpBurnedPercent',
      'data.lpBurnedPct',
      'data.lp_burned_percent',
      'data.lp_burned_pct',
      'data.liquidityBurned',
      'data.liquidity_burned',
      'data.security.lpBurnedPercent',
      'data.security.lpBurnedPct',
      'data.security.liquidityBurned',
      'data.liquidityBurned.percent',
      'data.markets.0.lpBurnedPercent',
    ]);

    const mintAuthority = getPath(sec, 'data.mintAuthority');
    const freezeAuthority = getPath(sec, 'data.freezeAuthority');
    const updateAuthority = getPath(sec, 'data.updateAuthority');

    return lpBurned.value === null || mintAuthority === null || freezeAuthority === null || updateAuthority === null;
  }

  extractMainPair(data) {
    const markets = [];

    const secMarkets = getPath(data.security, 'data.markets');
    if (Array.isArray(secMarkets)) markets.push(...secMarkets);

    const ovMarkets = getPath(data.overview, 'data.markets');
    if (Array.isArray(ovMarkets)) markets.push(...ovMarkets);

    const rugMarkets = data.rugcheck?.markets;
    if (Array.isArray(rugMarkets)) markets.push(...rugMarkets);

    if (markets.length === 0) {
      return { address: null, liquidityUsd: null, dex: null, found: false };
    }

    let best = null;
    let bestLiq = -1;
    for (const m of markets) {
      const liq = Number(m?.liquidity ?? m?.liquidityUsd ?? m?.liquidity_usd ?? 0);
      if (liq > bestLiq) {
        best = m;
        bestLiq = liq;
      }
    }

    return {
      address: best?.pairAddress || best?.address || best?.pair || best?.lpAddress || null,
      liquidityUsd: bestLiq > 0 ? bestLiq : null,
      dex: best?.dex || best?.source || best?.market || null,
      found: Boolean(best),
    };
  }

  extractLpMetric(data, kind) {
    const birdeyePaths =
      kind === 'burned'
        ? [
            'data.lpBurnedPercent',
            'data.lpBurnedPct',
            'data.lp_burned_percent',
            'data.lp_burned_pct',
            'data.liquidityBurned',
            'data.liquidity_burned',
            'data.security.lpBurnedPercent',
            'data.security.lpBurnedPct',
            'data.security.liquidityBurned',
            'data.liquidityBurned.percent',
            'data.markets.0.lpBurnedPercent',
          ]
        : [
            'data.lpLockedPercent',
            'data.lpLockedPct',
            'data.lp_locked_percent',
            'data.lp_locked_pct',
            'data.liquidityLocked',
            'data.liquidity_locked',
            'data.security.lpLockedPercent',
            'data.security.lpLockedPct',
            'data.security.liquidityLocked',
            'data.liquidityLocked.percent',
            'data.markets.0.lpLockedPercent',
          ];

    const rugcheckPaths =
      kind === 'burned'
        ? ['markets.0.lp.burnPct', 'liquidityDetails.lpBurnPct', 'liquidityDetails.lpBurnedPct']
        : ['markets.0.lp.lockPct', 'liquidityDetails.lpLockPct', 'liquidityDetails.lpLockedPct'];

    const b = pickFirstPath(data.security, birdeyePaths);
    const bNorm = parsePercent(b.value);
    if (bNorm !== null) {
      return { raw: b.value, percent: bNorm, source: 'birdeye/token_security', path: b.path };
    }

    const r = pickFirstPath(data.rugcheck, rugcheckPaths);
    const rNorm = parsePercent(r.value);
    if (rNorm !== null) {
      return { raw: r.value, percent: rNorm, source: 'rugcheck/report', path: r.path };
    }

    return { raw: null, percent: null, source: null, path: null };
  }

  decideLpStatus(data, token) {
    const mainPair = this.extractMainPair(data);
    const burned = this.extractLpMetric(data, 'burned');
    const locked = this.extractLpMetric(data, 'locked');

    token.security.mainPairAddress = mainPair.address;
    token.security.mainPairLiquidityUsd = mainPair.liquidityUsd;
    token.security.mainPairDex = mainPair.dex;

    token.security.lpBurnedPctRaw = burned.raw;
    token.security.lpBurnedPct = burned.percent;
    token.security.lpBurnedSource = burned.source;
    token.security.lpBurnedPath = burned.path;

    token.security.lpLockedPctRaw = locked.raw;
    token.security.lpLockedPct = locked.percent;
    token.security.lpLockedSource = locked.source;
    token.security.lpLockedPath = locked.path;

    if (!data.securityFetchOk && burned.percent === null && locked.percent === null) {
      return { passed: false, reason: 'NO_SECURITY_DATA' };
    }

    if (!mainPair.found) {
      return { passed: false, reason: 'MAIN_PAIR_NOT_FOUND' };
    }

    if (burned.percent !== null && burned.percent >= config.lpBurnedThreshold) {
      return { passed: true, reason: 'PASS_BURNED' };
    }

    if (locked.percent !== null && locked.percent >= config.lpLockedThreshold) {
      return { passed: true, reason: 'PASS_LOCKED' };
    }

    if (burned.percent === null && locked.percent === null) {
      return { passed: false, reason: 'LP_BURNED_FIELD_MISSING' };
    }

    return { passed: false, reason: 'PERCENT_BELOW_THRESHOLD' };
  }

  mergeData(token, data) {
    token.symbol = data.metadata?.data?.symbol || data.overview?.data?.symbol || data.rugcheck?.tokenMeta?.symbol || token.symbol;
    token.name = data.metadata?.data?.name || token.name;

    token.creationInfo = {
      source: data.creationInfo ? 'birdeye' : data.helius ? 'helius' : data.rugcheck ? 'rugcheck' : 'unknown',
      createdAt:
        normalizeEpochMaybe(data.creationInfo?.data?.blockUnixTime) ||
        normalizeEpochMaybe(data.creationInfo?.data?.createdTime) ||
        normalizeEpochMaybe(data.rugcheck?.tokenMeta?.mintTime) ||
        normalizeEpochMaybe(token.liquidityAddedAt) ||
        token.discoveredAt,
    };

    const mintAuthority = toNullish(
      getPath(data.security, 'data.mintAuthority') ?? data.helius?.result?.content?.metadata?.mintAuthority ?? data.rugcheck?.token?.mintAuthority,
    );
    const freezeAuthority = toNullish(
      getPath(data.security, 'data.freezeAuthority') ?? data.helius?.result?.content?.metadata?.freezeAuthority ?? data.rugcheck?.token?.freezeAuthority,
    );
    const updateAuthority = toNullish(
      getPath(data.security, 'data.updateAuthority') ?? data.helius?.result?.authorities?.[0]?.address ?? data.rugcheck?.token?.updateAuthority,
    );

    token.security.mintAuthority = mintAuthority;
    token.security.freezeAuthority = freezeAuthority;
    token.security.updateAuthority = updateAuthority;

    const lp = this.decideLpStatus(data, token);
    token.security.lpPassed = lp.passed;
    token.security.lpReason = lp.reason;

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
    if (!token.security.lpPassed) failed.push(`LP_CHECK_FAILED:${token.security.lpReason || 'UNKNOWN'}`);
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
      securityFetchOk: security.status === 'fulfilled',
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
