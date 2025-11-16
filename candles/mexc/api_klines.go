package mexc

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
	// This is documented in the MEXC API: "limit: Default 500; max 1000"
	MaxLimit = 1000

	// DefaultLimit is the default number of candlesticks returned if limit is not specified
	DefaultLimit = 500
)

type errorResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// Docs: https://www.mexc.com/api-docs/spot-v3/market-data-endpoints#klinecandlestick-data
// Response format: [[openTime, open, high, low, close, volume, closeTime, quoteAssetVolume], ...]
// - openTime: Open time in milliseconds
// - open: Open price (string)
// - high: High price (string)
// - low: Low price (string)
// - close: Close price (string)
// - volume: Volume in base asset (string)
// - closeTime: Close time in milliseconds
// - quoteAssetVolume: Volume in quote asset (string)
func (e *MEXC) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vklines", e.apiURL), nil)
	symbol := fmt.Sprintf("%v%v", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset)) // MEXC uses BTCUSDT format

	q := req.URL.Query()
	q.Add("symbol", symbol)

	// MEXC uses interval codes: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M
	var interval string
	switch candlestickInterval {
	case 1 * time.Minute:
		interval = "1m"
	case 5 * time.Minute:
		interval = "5m"
	case 15 * time.Minute:
		interval = "15m"
	case 30 * time.Minute:
		interval = "30m"
	case 1 * 60 * time.Minute:
		interval = "1h"
	case 4 * 60 * time.Minute:
		interval = "4h"
	case 1 * 60 * 24 * time.Minute:
		interval = "1d"
	case 7 * 60 * 24 * time.Minute:
		interval = "1w"
	case 30 * 60 * 24 * time.Minute:
		interval = "1M"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}
	q.Add("interval", interval)
	q.Add("limit", fmt.Sprintf("%d", MaxLimit))
	q.Add("startTime", fmt.Sprintf("%v", startTime.Unix()*1000)) // MEXC uses startTime in milliseconds

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

	// Try to parse as error response first
	maybeErrorResponse := errorResponse{}
	err = json.Unmarshal(byts, &maybeErrorResponse)
	if err == nil && maybeErrorResponse.Code != 0 {
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Err:            common.ErrRateLimit,
			}
		}
		// Check for invalid market pair errors
		if strings.Contains(maybeErrorResponse.Msg, "symbol") || strings.Contains(maybeErrorResponse.Msg, "Invalid") {
			return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
		}
		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Code:           maybeErrorResponse.Code,
			Err:            errors.New(maybeErrorResponse.Msg),
		}
	}

	var responseCandlesticks [][]interface{}
	err = json.Unmarshal(byts, &responseCandlesticks)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if len(responseCandlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	candlesticks := make([]common.Candlestick, len(responseCandlesticks))
	for i, raw := range responseCandlesticks {
		if len(raw) < 5 {
			return candlesticks, fmt.Errorf("candlestick %v has len < 5! Invalid syntax from MEXC", i)
		}

		// Parse timestamp (milliseconds)
		var timestampMs int64
		switch v := raw[0].(type) {
		case string:
			ts, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp! Invalid syntax from MEXC: %v", i, err)
			}
			timestampMs = ts
		case float64:
			timestampMs = int64(v)
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp type! Invalid syntax from MEXC", i)
		}
		timestamp := int(time.Unix(0, timestampMs*int64(time.Millisecond)).Unix())

		// Parse open price
		openStr, ok := raw[1].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string open! Invalid syntax from MEXC", i)
		}
		openPrice, err := strconv.ParseFloat(openStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from MEXC: %v", i, openStr, err)
		}

		// Parse high price
		highStr, ok := raw[2].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string high! Invalid syntax from MEXC", i)
		}
		highPrice, err := strconv.ParseFloat(highStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from MEXC: %v", i, highStr, err)
		}

		// Parse low price
		lowStr, ok := raw[3].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string low! Invalid syntax from MEXC", i)
		}
		lowPrice, err := strconv.ParseFloat(lowStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from MEXC: %v", i, lowStr, err)
		}

		// Parse close price
		closeStr, ok := raw[4].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string close! Invalid syntax from MEXC", i)
		}
		closePrice, err := strconv.ParseFloat(closeStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from MEXC: %v", i, closeStr, err)
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
		log.Info().Str("exchange", "MEXC").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	return candlesticks, nil
}


