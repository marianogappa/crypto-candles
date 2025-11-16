# Bitget Exchange Integration

## Documentation

- **API Documentation**: https://bitgetlimited.github.io/apidoc/en/spot/#get-history-candle-data
- **Endpoint Used**: `/api/spot/v1/market/history-candles`

## Endpoint Details

- **Base URL**: `https://api.bitget.com/api/spot/v1/`
- **Full Endpoint**: `/api/spot/v1/market/history-candles`
- **Method**: GET

## Symbol Format

- **Format**: `BASEQUOTE_SPBL` (uppercase, no separator, with `_SPBL` suffix)
- **Example**: `BTCUSDT_SPBL` (for BTC/USDT)
- **Note**: The `_SPBL` suffix indicates spot trading

## Available Intervals

- 1 minute (`1min`)
- 5 minutes (`5min`)
- 15 minutes (`15min`)
- 30 minutes (`30min`)
- 1 hour (`1h`)
- 4 hours (`4h`)
- 6 hours (`6h`)
- 12 hours (`12h`)
- 1 day (`1day`)
- 3 days (`3day`)
- 1 week (`1week`)
- 1 month (`1M`)

## Request Parameters

- `symbol`: Trading pair with suffix (e.g., `BTCUSDT_SPBL`)
- `period`: Interval code (e.g., `1min`, `1h`, `1day`)
- `limit`: Number of candlesticks (max 200, default 200)
- `endTime`: End time in milliseconds

## Response Format

```json
{
  "code": "00000",
  "msg": "success",
  "requestTime": 1763293590977,
  "data": [
    {
      "ts": "1763293590977",        // System timestamp (milliseconds as string)
      "open": "96171.61",           // Opening price
      "high": "96500.0",            // Highest price
      "low": "95781.35",            // Lowest price
      "close": "96500.0",           // Closing price
      "baseVol": "7.06280949",      // Base coin volume
      "quoteVol": "71260782.42",    // Denomination coin volume
      "usdtVol": "71260782.42"      // USDT volume
    }
  ]
}
```

## Limitations

- **Rate Limit**: Not explicitly documented in the endpoint docs
- **Maximum Limit**: 200 candlesticks per request
- **Success Code**: `"00000"` indicates successful response

## Error Codes

- `code: "00000"` - Success
- Non-zero codes indicate various API errors
- Error response format:
```json
{
  "code": "40001",
  "msg": "Invalid parameter"
}
```

## Error Response Format

```json
{
  "code": "40001",
  "msg": "Invalid parameter"
}
```

## Special Notes

- All price and volume values are returned as **strings**, not numbers
- Timestamps are in milliseconds (as strings)
- The symbol format requires the `_SPBL` suffix for spot trading pairs
- The `endTime` parameter is used to specify the end time for historical data
- The response includes both base volume (`baseVol`) and quote volume (`quoteVol`), plus USDT volume (`usdtVol`)

