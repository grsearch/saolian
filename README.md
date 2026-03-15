# SOL 新币扫描程序

基于你给的规则实现：

- 通过 Birdeye WebSocket 订阅 `SUBSCRIBE_TOKEN_NEW_LISTING` 与 `SUBSCRIBE_NEW_PAIR`。
- 接到新 token/pair 后立即缓存并补查：Birdeye（市场数据）、Helius（authority）、Rugcheck（安全信息兜底）。
- 四条规则用于分流白名单/黑名单：
  - LP burned > 95%
  - mintAuthority = null
  - freezeAuthority = null
  - updateAuthority = null
- 白名单实时更新交易统计与市场指标；黑名单不继续实时拉取指标。
- 退出机制：
  - 白名单：AGE > 24h 或 AGE > 2h 且 FDV/MCAP < 30000
  - 黑名单：仅保留最近 15 分钟

## 启动

```bash
node src/server.js
```

或

```bash
npm start
```

默认端口：`3000`

## 环境变量

- `BIRDEYE_API_KEY`（必填，REST + WebSocket 推荐）
- `HELIUS_API_KEY`（可选）
- `PORT`（可选）
- `REFRESH_SECONDS`（可选，默认 30）

可选覆盖：
- `BIRDEYE_WS_URL`（默认 `wss://public-api.birdeye.so/socket/solana`）
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

### 黑名单
- Symbol
- 合约地址
- 收录时间
- 拉黑原因

