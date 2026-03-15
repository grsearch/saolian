export const config = {
  port: Number(process.env.PORT || 3000),
  birdeyeWsUrl: process.env.BIRDEYE_WS_URL || 'wss://public-api.birdeye.so/socket/solana',
  birdeyeApiUrl: process.env.BIRDEYE_API_URL || 'https://public-api.birdeye.so',
  birdeyeApiKey: process.env.BIRDEYE_API_KEY || '',
  heliusApiUrl: process.env.HELIUS_API_URL || 'https://mainnet.helius-rpc.com',
  heliusApiKey: process.env.HELIUS_API_KEY || '',
  rugcheckApiUrl: process.env.RUGCHECK_API_URL || 'https://api.rugcheck.xyz',
  refreshSeconds: Number(process.env.REFRESH_SECONDS || 30),
  whitelistExitHours: 24,
  staleLowCapHours: 2,
  lowCapThreshold: 30000,
  blacklistTtlMinutes: 15,
};
