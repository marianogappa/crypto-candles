# MEXC Exchange Integration

## Documentation

- **API Documentation**: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints#klinecandlestick-data
- **Endpoint Used**: `/api/v3/klines`

## Endpoint Details

- **Base URL**: `https://api.mexc.com/`
- **Full Endpoint**: `/api/v3/klines`
- **Method**: GET

## Symbol Format

- **Format**: `BASEQUOTE` (uppercase, no separator)
- **Example**: `BTCUSDT` (for BTC/USDT)

## Available Intervals

- 1 minute (`1m`)
- 5 minutes (`5m`)
- 15 minutes (`15m`)
- 30 minutes (`30m`)
- 1 hour (`1h`)
- 4 hours (`4h`)
- 1 day (`1d`)
- 1 week (`1w`)
- 1 month (`1M`)

## Request Parameters

- `symbol`: Trading pair (e.g., `BTCUSDT`)
- `interval`: Interval code (e.g., `1m`, `1h`, `1d`)
- `limit`: Number of candlesticks (max 1000, default 500)
- `startTime`: Start time in milliseconds

## Response Format

```json
[
  [
    1763293590000,                  // openTime: Open time in milliseconds
    "96171.61",                     // open: Open price (string)
    "96500.0",                      // high: High price (string)
    "95781.35",                     // low: Low price (string)
    "96500.0",                      // close: Close price (string)
    "7.06280949",                   // volume: Volume in base asset (string)
    1763293650000,                  // closeTime: Close time in milliseconds
    "71260782.42"                   // quoteAssetVolume: Volume in quote asset (string)
  ]
]
```

## Limitations

- **Rate Limit**: Not explicitly documented in the endpoint docs
- **Maximum Limit**: 1000 candlesticks per request
- **Default Limit**: 500 candlesticks if not specified

## Error Codes

- `code: 0` - Success
- Non-zero codes indicate various API errors
- Error response format:
```json
{
  "code": 400,
  "msg": "Invalid symbol"
}
```

## Error Response Format

```json
{
  "code": 400,
  "msg": "Invalid symbol"
}
```

## Special Notes

- All price and volume values are returned as **strings**, not numbers
- The response is an array of arrays (not objects)
- The response includes both open time and close time for each candlestick
- The response includes volumes in both base asset and quote asset
- The `startTime` parameter is in milliseconds
- The response format is similar to Binance's klines format

