package okx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

const (
	// MaxLimit is the maximum number of candlesticks that can be requested per API call
	// This is documented in the OKX API: "The maximum is 100; The default is 100"
	MaxLimit = 100

	// RateLimitRequestsPerWindow is the number of requests allowed per rate limit window
	// This is documented in the OKX API: "Rate Limit: 10 requests per 2 seconds"
	RateLimitRequestsPerWindow = 10

	// RateLimitWindowDuration is the duration of the rate limit window
	// This is documented in the OKX API: "Rate Limit: 10 requests per 2 seconds"
	RateLimitWindowDuration = 2 * time.Second
)

type errorResponse struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type successfulResponse struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data [][]string `json:"data"`
}

// Docs: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-index-candlesticks-history
// Response format: [ts, o, h, l, c, confirm] where:
//   - ts: Opening time of the candlestick, Unix timestamp format in milliseconds
//   - o: Open price
//   - h: Highest price
//   - l: Lowest price
//   - c: Close price
//   - confirm: The state of candlesticks (0 = uncompleted, 1 = completed)
//
// IMPORTANT LIMITATIONS:
//   - Rate Limit: 10 requests per 2 seconds (IP-based)
//   - Maximum limit: 100 candlesticks per request
//   - OKX returns candlesticks in descending order (newest first), which this function reverses to ascending order
func (e *OKX) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vmarket/history-index-candles", e.apiURL), nil)
	symbol := fmt.Sprintf("%v-%v", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset)) // OKX uses BTC-USD format for index

	q := req.URL.Query()
	q.Add("instId", symbol)

	// OKX uses interval codes: 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 1W, 1M
	var interval string
	switch candlestickInterval {
	case 1 * time.Minute:
		interval = "1m"
	case 3 * time.Minute:
		interval = "3m"
	case 5 * time.Minute:
		interval = "5m"
	case 15 * time.Minute:
		interval = "15m"
	case 30 * time.Minute:
		interval = "30m"
	case 1 * 60 * time.Minute:
		interval = "1H"
	case 2 * 60 * time.Minute:
		interval = "2H"
	case 4 * 60 * time.Minute:
		interval = "4H"
	case 6 * 60 * time.Minute:
		interval = "6H"
	case 12 * 60 * time.Minute:
		interval = "12H"
	case 1 * 60 * 24 * time.Minute:
		interval = "1D"
	case 7 * 60 * 24 * time.Minute:
		interval = "1W"
	case 30 * 60 * 24 * time.Minute:
		interval = "1M"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}
	q.Add("bar", interval)
	q.Add("limit", fmt.Sprintf("%d", MaxLimit))
	// OKX uses 'before' parameter to get candles newer than the requested timestamp (in milliseconds)
	// The docs say: "Pagination of data to return records newer than the requested ts"
	// To get data starting from startTime, we use 'before' with startTime + one interval.
	// This ensures we get records with timestamp >= startTime (since "newer than" means > timestamp).
	// The API returns data in descending order (newest first), which we reverse to ascending order.
	beforeTimestamp := startTime.Add(candlestickInterval).Unix() * 1000
	q.Add("before", fmt.Sprintf("%v", beforeTimestamp))

	req.URL.RawQuery = q.Encode()

	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: fmt.Errorf("%w: %v", common.ErrExecutingRequest, err)}
	}
	defer resp.Body.Close()

	byts, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrBrokenBodyResponse}
	}

	maybeErrorResponse := errorResponse{}
	err = json.Unmarshal(byts, &maybeErrorResponse)
	if err == nil && maybeErrorResponse.Code != "0" {
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Err:            common.ErrRateLimit,
			}
		}
		// Check for invalid market pair errors
		if strings.Contains(maybeErrorResponse.Msg, "instrument") || strings.Contains(maybeErrorResponse.Msg, "Invalid") {
			return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
		}
		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Err:            errors.New(maybeErrorResponse.Msg),
		}
	}

	maybeResponse := successfulResponse{}
	err = json.Unmarshal(byts, &maybeResponse)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if maybeResponse.Code != "0" {
		if strings.Contains(maybeResponse.Msg, "instrument") || strings.Contains(maybeResponse.Msg, "Invalid") {
			return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
		}
		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Err:            errors.New(maybeResponse.Msg),
		}
	}

	if len(maybeResponse.Data) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	candlesticks := make([]common.Candlestick, len(maybeResponse.Data))
	for i, raw := range maybeResponse.Data {
		// Response format: [ts, o, h, l, c, confirm] - we need at least 5 elements (ts, o, h, l, c)
		if len(raw) < 5 {
			return candlesticks, fmt.Errorf("candlestick %v has len < 5! Invalid syntax from OKX", i)
		}

		// Parse timestamp (milliseconds as string)
		timestampMs, err := strconv.ParseInt(raw[0], 10, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp! Invalid syntax from OKX: %v", i, err)
		}
		timestamp := int(time.Unix(0, timestampMs*int64(time.Millisecond)).Unix())

		// Parse open price
		openPrice, err := strconv.ParseFloat(raw[1], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from OKX: %v", i, raw[1], err)
		}

		// Parse high price
		highPrice, err := strconv.ParseFloat(raw[2], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from OKX: %v", i, raw[2], err)
		}

		// Parse low price
		lowPrice, err := strconv.ParseFloat(raw[3], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from OKX: %v", i, raw[3], err)
		}

		// Parse close price
		closePrice, err := strconv.ParseFloat(raw[4], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from OKX: %v", i, raw[4], err)
		}

		candlesticks[i] = common.Candlestick{
			Timestamp:    timestamp,
			OpenPrice:    common.JSONFloat64(openPrice),
			ClosePrice:   common.JSONFloat64(closePrice),
			LowestPrice:  common.JSONFloat64(lowPrice),
			HighestPrice: common.JSONFloat64(highPrice),
		}
	}

	if e.debug {
		log.Info().Str("exchange", "OKX").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	// OKX returns candlesticks in descending order (newest first), so reverse them
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}
