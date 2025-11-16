package gate

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
	// This is documented in the Gate API: "Maximum of 1000 points can be returned in a query"
	MaxLimit = 1000

	// MaxPointsBack is the maximum number of intervals back from current time that can be requested
	// This limit is NOT documented in the Gate API docs but is enforced by the API.
	// The API returns error: "Candlestick too long ago. Maximum 10000 points ago are allowed"
	// This means if you request data with interval=1h, you can only go back 10000 hours (~416 days)
	MaxPointsBack = 10000

	// ErrorMessageTooFarBack is the error message pattern returned by Gate API when requesting data too far back
	ErrorMessageTooFarBack = "Maximum 10000 points ago are allowed"
)

type errorResponse struct {
	Label   string `json:"label"`
	Message string `json:"message"`
}

// Docs: https://www.gate.com/docs/developers/apiv4/en/#market-k-line-chart
// Response format: [timestamp, volume_quote, close, high, low, open, volume_base, is_closed]
//
// IMPORTANT LIMITATION: Gate API enforces a maximum of MaxPointsBack (10000) intervals back from current time.
// This limit is NOT documented in the official API docs but is enforced by the API server.
// If you request data that exceeds this limit, the API returns: "Candlestick too long ago. Maximum 10000 points ago are allowed"
// For example, with interval=1h, you can only request data up to 10000 hours (~416 days) back.
// This function guards against this by checking the startTime before making the request.
func (e *Gate) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	// Guard: Check if startTime is too far back (more than MaxPointsBack intervals ago)
	// This prevents the API error: "Candlestick too long ago. Maximum 10000 points ago are allowed"
	now := time.Now()
	intervalsBack := int(now.Sub(startTime) / candlestickInterval)
	if intervalsBack > MaxPointsBack {
		return nil, common.CandleReqError{
			IsNotRetryable: true,
			Err:            fmt.Errorf("%w: requested %d intervals back, maximum is %d", common.ErrDataTooFarBack, intervalsBack, MaxPointsBack),
		}
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("%vspot/candlesticks", e.apiURL), nil)
	symbol := fmt.Sprintf("%v_%v", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset)) // Gate uses BTC_USDT format

	q := req.URL.Query()
	q.Add("currency_pair", symbol)

	// Gate uses interval codes: 1s, 10s, 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 7d, 30d
	var interval string
	switch candlestickInterval {
	case 1 * time.Second:
		interval = "1s"
	case 10 * time.Second:
		interval = "10s"
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
	case 8 * 60 * time.Minute:
		interval = "8h"
	case 1 * 60 * 24 * time.Minute:
		interval = "1d"
	case 7 * 60 * 24 * time.Minute:
		interval = "7d"
	case 30 * 60 * 24 * time.Minute:
		interval = "30d"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}
	q.Add("interval", interval)
	q.Add("limit", fmt.Sprintf("%d", MaxLimit))
	q.Add("from", fmt.Sprintf("%v", startTime.Unix())) // Gate uses 'from' parameter (start time in seconds)

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
		if err == nil && maybeErrorResponse.Label != "" {
			if resp.StatusCode == http.StatusTooManyRequests {
				return nil, common.CandleReqError{
					IsNotRetryable: false,
					Err:            common.ErrRateLimit,
				}
			}
			// Check for data too far back error (not documented in API but enforced)
			if strings.Contains(maybeErrorResponse.Message, ErrorMessageTooFarBack) {
				return nil, common.CandleReqError{
					IsNotRetryable: true,
					Err:            fmt.Errorf("%w: %s", common.ErrDataTooFarBack, maybeErrorResponse.Message),
				}
			}
			// Check for invalid market pair errors
			if strings.Contains(maybeErrorResponse.Message, "currency_pair") || strings.Contains(maybeErrorResponse.Label, "INVALID") {
				return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
			}
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Err:            errors.New(maybeErrorResponse.Message),
			}
		}
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
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
		// Response format: [timestamp, volume_quote, close, high, low, open, volume_base, is_closed]
		// We need at least 6 elements (timestamp, volume_quote, close, high, low, open)
		if len(raw) < 6 {
			return candlesticks, fmt.Errorf("candlestick %v has len < 6! Invalid syntax from Gate", i)
		}

		// Parse timestamp (seconds as string or number)
		var timestamp int
		switch v := raw[0].(type) {
		case string:
			ts, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp! Invalid syntax from Gate: %v", i, err)
			}
			timestamp = int(ts)
		case float64:
			timestamp = int(v)
		default:
			return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp type! Invalid syntax from Gate", i)
		}

		// Parse volume (not used but present)
		// Parse close price
		closeStr, ok := raw[2].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string close! Invalid syntax from Gate", i)
		}
		closePrice, err := strconv.ParseFloat(closeStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from Gate: %v", i, closeStr, err)
		}

		// Parse high price
		highStr, ok := raw[3].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string high! Invalid syntax from Gate", i)
		}
		highPrice, err := strconv.ParseFloat(highStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from Gate: %v", i, highStr, err)
		}

		// Parse low price
		lowStr, ok := raw[4].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string low! Invalid syntax from Gate", i)
		}
		lowPrice, err := strconv.ParseFloat(lowStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from Gate: %v", i, lowStr, err)
		}

		// Parse open price
		openStr, ok := raw[5].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string open! Invalid syntax from Gate", i)
		}
		openPrice, err := strconv.ParseFloat(openStr, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from Gate: %v", i, openStr, err)
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
		log.Info().Str("exchange", "Gate").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	return candlesticks, nil
}
