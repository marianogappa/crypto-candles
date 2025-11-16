package upbit

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

const (
	// MaxLimit is the maximum number of candlesticks that can be requested per API call
	// This is documented in the Upbit API: "Supports up to 200 candles"
	MaxLimit = 200

	// RateLimitRequestsPerSecond is the number of requests allowed per second
	// This is documented in the Upbit API: "Up to 10 calls per second are allowed"
	RateLimitRequestsPerSecond = 10

	// SecondsDataRetentionMonths is the data retention period for second candles
	// This is documented in the Upbit API: "The 1-second candle API provides data for up to 3 months"
	SecondsDataRetentionMonths = 3

	// SecondsDataRetentionDuration is the data retention duration for second candles
	SecondsDataRetentionDuration = SecondsDataRetentionMonths * 30 * 24 * time.Hour
)

type errorResponse struct {
	Error struct {
		Name    string `json:"name"`
		Message string `json:"message"`
	} `json:"error"`
}

type candlestickResponse struct {
	Market               string  `json:"market"`
	CandleDateTimeUTC    string  `json:"candle_date_time_utc"`
	CandleDateTimeKST    string  `json:"candle_date_time_kst"`
	OpeningPrice         float64 `json:"opening_price"`
	HighPrice            float64 `json:"high_price"`
	LowPrice             float64 `json:"low_price"`
	TradePrice           float64 `json:"trade_price"`
	Timestamp            int64   `json:"timestamp"`
	CandleAccTradePrice  float64 `json:"candle_acc_trade_price"`
	CandleAccTradeVolume float64 `json:"candle_acc_trade_volume"`
	Unit                 int     `json:"unit,omitempty"` // Only for minutes endpoint
}

// Docs: https://global-docs.upbit.com/reference/list-candles-seconds
// Upbit uses different endpoints for different intervals:
// - /v1/candles/seconds for second intervals
// - /v1/candles/minutes/{unit} for minute intervals (1, 3, 5, 15, 30, 60, 240)
// - /v1/candles/days for daily intervals
// - /v1/candles/weeks for weekly intervals
// - /v1/candles/months for monthly intervals
// - /v1/candles/years for yearly intervals
//
// IMPORTANT LIMITATIONS:
//   - Rate Limit: 10 calls per second (IP-based, shared within 'candle' group)
//   - Maximum limit: 200 candlesticks per request
//   - Seconds candles: Data retention is 3 months from request time
//   - Upbit returns candlesticks in descending order (newest first), which this function reverses to ascending order
//   - Candles are created only when trades occur - gaps may exist in the response
func (e *Upbit) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	symbol := fmt.Sprintf("%v-%v", strings.ToUpper(quoteAsset), strings.ToUpper(baseAsset)) // Upbit uses KRW-BTC format

	// Guard: Check if requesting seconds candles beyond data retention period
	if candlestickInterval == 1*time.Second {
		now := time.Now()
		if now.Sub(startTime) > SecondsDataRetentionDuration {
			return nil, common.CandleReqError{
				IsNotRetryable: true,
				Err:            fmt.Errorf("%w: seconds candles data retention is %d months", common.ErrDataTooFarBack, SecondsDataRetentionMonths),
			}
		}
	}

	var endpoint string

	// Determine endpoint based on interval
	switch candlestickInterval {
	case 1 * time.Second:
		endpoint = "candles/seconds"
	case 1 * time.Minute:
		endpoint = "candles/minutes/1"
	case 3 * time.Minute:
		endpoint = "candles/minutes/3"
	case 5 * time.Minute:
		endpoint = "candles/minutes/5"
	case 15 * time.Minute:
		endpoint = "candles/minutes/15"
	case 30 * time.Minute:
		endpoint = "candles/minutes/30"
	case 1 * 60 * time.Minute:
		endpoint = "candles/minutes/60"
	case 4 * 60 * time.Minute:
		endpoint = "candles/minutes/240"
	case 1 * 60 * 24 * time.Minute:
		endpoint = "candles/days"
	case 7 * 60 * 24 * time.Minute:
		endpoint = "candles/weeks"
	case 30 * 60 * 24 * time.Minute:
		endpoint = "candles/months"
	case 365 * 60 * 24 * time.Minute:
		endpoint = "candles/years"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("%v%v", e.apiURL, endpoint), nil)
	q := req.URL.Query()
	q.Add("market", symbol)
	q.Add("count", fmt.Sprintf("%d", MaxLimit))
	// Upbit uses 'to' parameter (end time in ISO 8601 format)
	// The docs say: "Candles earlier than the specified time will be retrieved"
	// To get data starting from startTime, we use startTime + interval to ensure we get candles that include startTime
	// Format: ISO 8601 datetime format (e.g., 2025-06-24T04:56:53Z)
	toTime := startTime.Add(candlestickInterval)
	toParam := toTime.UTC().Format(time.RFC3339)
	q.Add("to", toParam)

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

	if resp.StatusCode != http.StatusOK {
		maybeErrorResponse := errorResponse{}
		err = json.Unmarshal(byts, &maybeErrorResponse)
		if err == nil && maybeErrorResponse.Error.Name != "" {
			if resp.StatusCode == http.StatusTooManyRequests {
				return nil, common.CandleReqError{
					IsNotRetryable: false,
					Err:            common.ErrRateLimit,
				}
			}
			// Check for invalid market pair errors
			if strings.Contains(maybeErrorResponse.Error.Message, "market") || strings.Contains(maybeErrorResponse.Error.Name, "market") {
				return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
			}
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Err:            errors.New(maybeErrorResponse.Error.Message),
			}
		}
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	var responseCandlesticks []candlestickResponse
	err = json.Unmarshal(byts, &responseCandlesticks)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if len(responseCandlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	candlesticks := make([]common.Candlestick, len(responseCandlesticks))
	for i, raw := range responseCandlesticks {
		// Upbit returns candlesticks in reverse chronological order (newest first)
		// We need to reverse them and use the timestamp
		timestamp := int(raw.Timestamp / 1000) // Convert from milliseconds to seconds

		candlesticks[i] = common.Candlestick{
			Timestamp:    timestamp,
			OpenPrice:    common.JSONFloat64(raw.OpeningPrice),
			ClosePrice:   common.JSONFloat64(raw.TradePrice),
			LowestPrice:  common.JSONFloat64(raw.LowPrice),
			HighestPrice: common.JSONFloat64(raw.HighPrice),
		}
	}

	if e.debug {
		log.Info().Str("exchange", "Upbit").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	// Reverse slice, because Upbit returns candlesticks in descending order (newest first)
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}
