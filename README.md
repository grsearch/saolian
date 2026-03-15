# SOL 新币扫描程序

基于你的最新要求实现（先 LP/FDV，再 authority，再白名单补查 burned），并增加防卡死机制：

- 每 60 秒轮询 Birdeye `defi/v2/tokens/new_listing`，发现 Solana 新上市代币。
- 对新地址做去重（`seenAddresses`），避免重复处理。
- 对每个新币补查：
  - Birdeye `token_creation_info`
  - Birdeye `token_security`
  - Birdeye `token_overview`
  - Birdeye `v3 token meta-data`
  - Helius（authority 兜底）
- **前置白名单判定（同级条件）**：
  - `LP/FDV > 10%`
  - `mintAuthority = null`
  - `freezeAuthority = null`
  - `updateAuthority = null`
- 进入白名单后，才调用 Rugcheck 获取并展示：
  - `是否 burned`
  - `burned 比例`

## 防卡死优化（OpenClaw 大量收录场景）

- `tick` 增加运行锁，防止上一轮未结束又重入。
- `tick` 改为批处理（默认每轮最多处理 50 个 token），避免单轮处理几千币阻塞。
- 所有外部 API 请求增加超时（默认 8 秒）。
- SSE 返回状态默认只保留最新 300 行（白/黑名单各自截断），避免前端渲染过大卡死。
- 内存 token 池增加上限（默认 3000），超过后自动移除最旧数据。

## 退出机制

- 白名单：AGE > 24h 或 AGE > 2h 且 FDV/MCAP < 30000
- 黑名单：仅保留最近 15 分钟

## 启动

```bash
node src/server.js
```

默认端口：`3000`

## 环境变量

- `BIRDEYE_API_KEY`（建议必填）
- `HELIUS_API_KEY`（可选）
- `PORT`（可选）
- `DISCOVERY_SECONDS`（可选，默认 60）
- `REFRESH_SECONDS`（可选，默认 30）
- `NEW_LISTING_PAGE_SIZE`（可选，默认 50）
- `LP_FDV_THRESHOLD_PERCENT`（可选，默认 10）
- `RUGCHECK_REFRESH_MINUTES`（可选，默认 5，白名单 burned 刷新间隔）
- `HTTP_TIMEOUT_MS`（可选，默认 8000）
- `TICK_BATCH_SIZE`（可选，默认 50）
- `MAX_TRACKED_TOKENS`（可选，默认 3000）
- `MAX_STATE_ROWS`（可选，默认 300）

可选覆盖：
- `BIRDEYE_API_URL`（默认 `https://public-api.birdeye.so`）
- `HELIUS_API_URL`（默认 `https://mainnet.helius-rpc.com`）
- `RUGCHECK_API_URL`（默认 `https://api.rugcheck.xyz`）

## Dashboard 字段

### 白名单
- Symbol
- 合约地址（点击跳 GMGN）
- AGE
- holders
- Liquidity
- FDV/MCAP
- LP/FDV
- TOP10占比
- TX 交易笔数
- 买卖比（buy/sell）
- LP Burned?
- Burned%

### 黑名单
- Symbol
- 合约地址
- 收录时间
- 拉黑原因
