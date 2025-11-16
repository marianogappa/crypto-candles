# Upbit Exchange Integration

## Documentation

- **API Documentation**: 
  - Seconds: https://global-docs.upbit.com/reference/list-candles-seconds
  - Minutes: https://global-docs.upbit.com/reference/list-candles-minutes
  - Days: https://global-docs.upbit.com/reference/list-candles-days
  - Weeks: https://global-docs.upbit.com/reference/list-candles-weeks
  - Months: https://global-docs.upbit.com/reference/list-candles-months
  - Years: https://global-docs.upbit.com/reference/list-candles-years

## Endpoint Details

- **Base URL**: `https://api.upbit.com/v1/`
- **Endpoints** (varies by interval):
  - `/v1/candles/seconds` - For 1 second intervals
  - `/v1/candles/minutes/{unit}` - For minute intervals (1, 3, 5, 15, 30, 60, 240)
  - `/v1/candles/days` - For daily intervals
  - `/v1/candles/weeks` - For weekly intervals
  - `/v1/candles/months` - For monthly intervals
  - `/v1/candles/years` - For yearly intervals
- **Method**: GET

## Symbol Format

- **Format**: `QUOTE-BASE` (uppercase with hyphen)
- **Example**: `USDT-BTC` (for BTC/USDT) or `KRW-BTC` (for BTC/KRW)
- **Note**: Upbit primarily uses KRW pairs, but also supports USDT pairs

## Available Intervals

- 1 second (`candles/seconds`)
- 1 minute (`candles/minutes/1`)
- 3 minutes (`candles/minutes/3`)
- 5 minutes (`candles/minutes/5`)
- 15 minutes (`candles/minutes/15`)
- 30 minutes (`candles/minutes/30`)
- 1 hour (`candles/minutes/60`)
- 4 hours (`candles/minutes/240`)
- 1 day (`candles/days`)
- 1 week (`candles/weeks`)
- 1 month (`candles/months`)
- 1 year (`candles/years`)

## Request Parameters

- `market`: Trading pair (e.g., `USDT-BTC`, `KRW-BTC`)
- `count`: Number of candlesticks (max 200, default 1)
- `to`: End time in ISO 8601 format (e.g., `2025-06-24T04:56:53Z`) - returns candles earlier than this time
- `after`: Start time in ISO 8601 format (optional) - returns candles earlier than this time

## Response Format

```json
[
  {
    "market": "USDT-BTC",
    "candle_date_time_utc": "2025-11-16T11:46:00",
    "candle_date_time_kst": "2025-11-16T20:46:00",
    "opening_price": 143628000.0,
    "high_price": 143789000.0,
    "low_price": 143628000.0,
    "trade_price": 143789000.0,
    "timestamp": 1763293590977,
    "candle_acc_trade_price": 1014603026.73324,
    "candle_acc_trade_volume": 7.06280949,
    "unit": 1
  }
]
```

## Limitations

- **Rate Limit**: 10 calls per second (IP-based, shared within 'candle' group)
- **Maximum Limit**: 200 candlesticks per request
- **Seconds Candles Data Retention**: Only 3 months from request time
  - If you request data beyond this period, the response may return an empty list or fewer items
- **Data Ordering**: Returns candlesticks in descending order (newest first), which is reversed to ascending order in the implementation
- **Gap Handling**: Candles are created only when trades occur - if no trades occur in an interval, that candle will not exist in the response

## Error Codes

- Standard HTTP status codes (200, 400, 404, 429, etc.)
- Error response format:
```json
{
  "error": {
    "name": "too_many_requests",
    "message": "Too many requests"
  }
}
```

## Error Response Format

```json
{
  "error": {
    "name": "invalid_market",
    "message": "Invalid market"
  }
}
```

## Special Notes

- Upbit uses **different endpoints** for different intervals (unlike most exchanges that use a single endpoint with a parameter)
- The `to` parameter uses ISO 8601 format (RFC3339), not Unix timestamps
- The `to` parameter returns candles **earlier than** the specified time
- For seconds candles, data is only available for the last 3 months
- The `unit` field is only present in minute interval responses
- Timestamps in the response are in milliseconds
- Upbit is primarily a Korean exchange, so KRW pairs are more common, but USDT pairs are also available

