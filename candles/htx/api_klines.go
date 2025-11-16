package htx

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
	// This is documented in the HTX API: "The number of data returns [1-2000]"
	MaxLimit = 2000

	// StatusOK is the success status returned by HTX API
	StatusOK = "ok"

	// StatusError is the error status returned by HTX API
	StatusError = "error"
)

type errorResponse struct {
	Status  string `json:"status"`
	ErrCode string `json:"err-code"`
	ErrMsg  string `json:"err-msg"`
}

type candlestickData struct {
	ID     interface{} `json:"id"`     // Timestamp in seconds (can be float64 or string)
	Open   interface{} `json:"open"`   // Opening price (can be float64 or string)
	Close  interface{} `json:"close"`  // Closing price (can be float64 or string)
	Low    interface{} `json:"low"`    // Low price (can be float64 or string)
	High   interface{} `json:"high"`   // High price (can be float64 or string)
	Amount interface{} `json:"amount"` // Accumulated trading volume in base currency
	Vol    interface{} `json:"vol"`    // Accumulated trading value in quote currency
	Count  interface{} `json:"count"`  // Number of completed trades
}

type successfulResponse struct {
	Status string            `json:"status"`
	Ch     string            `json:"ch"`
	Ts     int64             `json:"ts"`
	Data   []candlestickData `json:"data"`
}

// Docs: https://www.htx.com/en-in/opend/newApiPages/?id=7ec4a4da-7773-11ed-9966-0242ac110003
// Response format: objects with fields: id, open, close, low, high, amount, vol, count
// id is the timestamp in seconds
func (e *HTX) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vmarket/history/kline", e.apiURL), nil)
	symbol := fmt.Sprintf("%v%v", strings.ToLower(baseAsset), strings.ToLower(quoteAsset)) // HTX uses btcusdt format (lowercase)

	q := req.URL.Query()
	q.Add("symbol", symbol)

	// HTX uses period codes: 1min, 5min, 15min, 30min, 60min, 4hour, 1day, 1week, 1mon, 1year
	// Note: 1year is documented but may have limited historical data availability
	var period string
	switch candlestickInterval {
	case 1 * time.Minute:
		period = "1min"
	case 5 * time.Minute:
		period = "5min"
	case 15 * time.Minute:
		period = "15min"
	case 30 * time.Minute:
		period = "30min"
	case 1 * 60 * time.Minute:
		period = "60min"
	case 4 * 60 * time.Minute:
		period = "4hour"
	case 1 * 60 * 24 * time.Minute:
		period = "1day"
	case 7 * 60 * 24 * time.Minute:
		period = "1week"
	case 30 * 60 * 24 * time.Minute:
		period = "1mon"
	case 365 * 60 * 24 * time.Minute:
		period = "1year"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}
	q.Add("period", period)
	q.Add("size", fmt.Sprintf("%d", MaxLimit))
	q.Add("from", fmt.Sprintf("%v", startTime.Unix())) // HTX uses 'from' parameter (start time in seconds)

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
	if err == nil && maybeErrorResponse.Status == StatusError {
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Err:            common.ErrRateLimit,
			}
		}
		// Check for invalid market pair errors
		if strings.Contains(maybeErrorResponse.ErrMsg, "symbol") || strings.Contains(maybeErrorResponse.ErrMsg, "invalid") {
			return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
		}
		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Err:            errors.New(maybeErrorResponse.ErrMsg),
		}
	}

	maybeResponse := successfulResponse{}
	err = json.Unmarshal(byts, &maybeResponse)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if maybeResponse.Status != StatusOK {
		if maybeResponse.Status == StatusError {
			return nil, common.CandleReqError{IsNotRetryable: false, Err: errors.New("HTX API returned error status")}
		}
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if len(maybeResponse.Data) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	candlesticks := make([]common.Candlestick, len(maybeResponse.Data))
	for i, raw := range maybeResponse.Data {
		// Parse timestamp (seconds) - can be float64 or string
		var timestamp int
		switch v := raw.ID.(type) {
		case string:
			ts, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp! Invalid syntax from HTX: %v", i, err)
			}
			timestamp = int(ts)
		case float64:
			timestamp = int(v)
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp type! Invalid syntax from HTX", i)
		}

		// Parse open price - can be float64 or string
		var openPrice float64
		switch v := raw.Open.(type) {
		case string:
			price, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from HTX: %v", i, v, err)
			}
			openPrice = price
		case float64:
			openPrice = v
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid open price type! Invalid syntax from HTX", i)
		}

		// Parse close price - can be float64 or string
		var closePrice float64
		switch v := raw.Close.(type) {
		case string:
			price, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from HTX: %v", i, v, err)
			}
			closePrice = price
		case float64:
			closePrice = v
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid close price type! Invalid syntax from HTX", i)
		}

		// Parse low price - can be float64 or string
		var lowPrice float64
		switch v := raw.Low.(type) {
		case string:
			price, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from HTX: %v", i, v, err)
			}
			lowPrice = price
		case float64:
			lowPrice = v
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid low price type! Invalid syntax from HTX", i)
		}

		// Parse high price - can be float64 or string
		var highPrice float64
		switch v := raw.High.(type) {
		case string:
			price, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from HTX: %v", i, v, err)
			}
			highPrice = price
		case float64:
			highPrice = v
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid high price type! Invalid syntax from HTX", i)
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
		log.Info().Str("exchange", "HTX").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	// HTX returns candlesticks in descending order (newest first), so reverse them
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}
