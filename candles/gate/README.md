# Gate.io Exchange Integration

## Documentation

- **API Documentation**: https://www.gate.com/docs/developers/apiv4/en/#market-k-line-chart
- **Endpoint Used**: `/api/v4/spot/candlesticks`

## Endpoint Details

- **Base URL**: `https://api.gateio.ws/api/v4/`
- **Full Endpoint**: `/api/v4/spot/candlesticks`
- **Method**: GET

## Symbol Format

- **Format**: `BASE_QUOTE` (uppercase with underscore)
- **Example**: `BTC_USDT` (for BTC/USDT)

## Available Intervals

- 1 second (`1s`)
- 10 seconds (`10s`)
- 1 minute (`1m`)
- 5 minutes (`5m`)
- 15 minutes (`15m`)
- 30 minutes (`30m`)
- 1 hour (`1h`)
- 4 hours (`4h`)
- 8 hours (`8h`)
- 1 day (`1d`)
- 7 days (`7d`)
- 30 days (`30d`)

## Request Parameters

- `currency_pair`: Trading pair (e.g., `BTC_USDT`)
- `interval`: Interval code (e.g., `1h`, `1d`)
- `limit`: Number of candlesticks (max 1000, default 1000)
- `from`: Start time in Unix seconds

## Response Format

```json
[
  [
    "1763118000",                    // timestamp (seconds as string)
    "71260782.42422470",            // volume_quote
    "96165.1",                      // close
    "96942.2",                      // high
    "95707.1",                      // low
    "96758.9",                      // open
    "739.87607500",                 // volume_base
    "true"                          // is_closed
  ]
]
```

**Note**: The response format is `[timestamp, volume_quote, close, high, low, open, volume_base, is_closed]` - note the order differs from typical OHLCV format.

## Limitations

- **Rate Limit**: Not explicitly documented, but rate limiting is handled
- **Maximum Limit**: 1000 candlesticks per request
- **Historical Data Limit**: Maximum of 10,000 intervals back from current time (NOT documented in official API docs but enforced)
  - Example: With `interval=1h`, you can only request data up to 10,000 hours (~416 days) back
  - This limit applies to all intervals proportionally
- **Error Message**: When exceeding the limit: `"Candlestick too long ago. Maximum 10000 points ago are allowed"`

## Error Codes

- `label: "TOO_MANY_REQUESTS"` - Rate limit exceeded
- `label: "INVALID_PARAM_VALUE"` - Invalid parameter (e.g., invalid currency_pair or data too far back)

## Error Response Format

```json
{
  "label": "INVALID_PARAM_VALUE",
  "message": "Candlestick too long ago. Maximum 10000 points ago are allowed"
}
```

## Special Notes

- The historical data limit of 10,000 intervals is **not documented** in the official API documentation but is enforced by the API server
- The implementation includes a guard to check this limit before making requests
- Timestamps in the response are in seconds (as strings)
- The response array order is non-standard: `[timestamp, volume_quote, close, high, low, open, volume_base, is_closed]`

