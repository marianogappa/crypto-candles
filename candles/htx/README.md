# HTX (formerly Huobi) Exchange Integration

## Documentation

- **API Documentation**: https://www.htx.com/en-in/opend/newApiPages/?id=7ec4a4da-7773-11ed-9966-0242ac110003
- **Endpoint Used**: `/market/history/kline`

## Endpoint Details

- **Base URL**: `https://api.huobi.pro/`
- **Full Endpoint**: `/market/history/kline`
- **Method**: GET

## Symbol Format

- **Format**: `basequote` (lowercase, no separator)
- **Example**: `btcusdt` (for BTC/USDT)

## Available Intervals

- 1 minute (`1min`)
- 5 minutes (`5min`)
- 15 minutes (`15min`)
- 30 minutes (`30min`)
- 1 hour (`60min`)
- 4 hours (`4hour`)
- 1 day (`1day`)
- 1 week (`1week`)
- 1 month (`1mon`)
- 1 year (`1year`)

## Request Parameters

- `symbol`: Trading pair (e.g., `btcusdt`)
- `period`: Interval code (e.g., `1min`, `60min`, `1day`)
- `size`: Number of candlesticks (max 2000, default 150)
- `from`: Start time in Unix seconds

## Response Format

```json
{
  "status": "ok",
  "ch": "market.btcusdt.kline.1min",
  "ts": 1763293590977,
  "data": [
    {
      "id": 1763293590,           // Timestamp in seconds (can be float64 or string)
      "open": 96171.61,            // Opening price (can be float64 or string)
      "close": 96500.0,            // Closing price (can be float64 or string)
      "low": 95781.35,             // Low price (can be float64 or string)
      "high": 96500.0,             // High price (can be float64 or string)
      "amount": 7.06280949,        // Accumulated trading volume in base currency
      "vol": 71260782.42,          // Accumulated trading value in quote currency
      "count": 1234                 // Number of completed trades
    }
  ]
}
```

## Limitations

- **Rate Limit**: Not explicitly documented
- **Maximum Limit**: 2000 candlesticks per request
- **Default Limit**: 150 candlesticks if not specified
- **Data Ordering**: Returns candlesticks in descending order (newest first), which is reversed to ascending order in the implementation

## Error Codes

- `status: "ok"` - Success
- `status: "error"` - Error occurred
- Error response format:
```json
{
  "status": "error",
  "err-code": "invalid-parameter",
  "err-msg": "invalid symbol"
}
```

## Error Response Format

```json
{
  "status": "error",
  "err-code": "invalid-parameter",
  "err-msg": "invalid symbol"
}
```

## Special Notes

- HTX (formerly Huobi) uses **lowercase** symbols with no separator
- The response format uses `status: "ok"` for success (not HTTP status codes)
- Field values can be either `float64` or `string` types - the implementation handles both
- The `id` field represents the timestamp in seconds
- The `ch` (channel) field indicates the market and interval
- The `ts` field is the server timestamp when the response was generated
- The response includes trade count (`count`) and volumes in both base (`amount`) and quote (`vol`) currencies
- 1-year intervals are documented but may have limited historical data availability

