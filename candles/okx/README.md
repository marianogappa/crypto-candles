# OKX Exchange Integration

## Documentation

- **API Documentation**: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-index-candlesticks-history
- **Endpoint Used**: `/api/v5/market/history-index-candles`

## Endpoint Details

- **Base URL**: `https://www.okx.com/api/v5/`
- **Full Endpoint**: `/api/v5/market/history-index-candles`
- **Method**: GET

## Symbol Format

- **Format**: `BASE-QUOTE` (uppercase with hyphen)
- **Example**: `BTC-USD` (for BTC/USD index)
- **Note**: OKX uses index candlesticks, so the format is `BTC-USD` rather than `BTC-USDT`

## Available Intervals

- 1 minute (`1m`)
- 3 minutes (`3m`)
- 5 minutes (`5m`)
- 15 minutes (`15m`)
- 30 minutes (`30m`)
- 1 hour (`1H`)
- 2 hours (`2H`)
- 4 hours (`4H`)
- 6 hours (`6H`)
- 12 hours (`12H`)
- 1 day (`1D`)
- 1 week (`1W`)
- 1 month (`1M`)

## Request Parameters

- `instId`: Trading pair (e.g., `BTC-USD`)
- `bar`: Interval code (e.g., `1H`, `1D`)
- `limit`: Number of candlesticks (max 100, default 100)
- `before`: Timestamp in milliseconds - returns records newer than this timestamp
- `after`: Timestamp in milliseconds - returns records earlier than this timestamp (optional)

## Response Format

```json
{
  "code": "0",
  "msg": "",
  "data": [
    [
      "1597026383085",  // ts: Opening time (milliseconds)
      "3.721",          // o: Open price
      "3.743",          // h: Highest price
      "3.677",          // l: Lowest price
      "3.708",          // c: Close price
      "1"               // confirm: State (0 = uncompleted, 1 = completed)
    ]
  ]
}
```

## Limitations

- **Rate Limit**: 10 requests per 2 seconds (IP-based)
- **Maximum Limit**: 100 candlesticks per request
- **Data Ordering**: Returns candlesticks in descending order (newest first), which is reversed to ascending order in the implementation

## Error Codes

- `code: "0"` - Success
- `code: "50903"` - Too many requests (rate limit)
- `code: "51000"` - Invalid instrument/market pair
- Other non-zero codes indicate various API errors

## Error Response Format

```json
{
  "code": "51000",
  "msg": "Invalid instrument"
}
```

## Special Notes

- OKX uses **index candlesticks** endpoint, which provides index prices rather than spot trading prices
- The `before` parameter is used to paginate data - it returns records newer than the specified timestamp
- Timestamps are in milliseconds
- The `confirm` field indicates whether the candlestick is completed (1) or uncompleted (0)

