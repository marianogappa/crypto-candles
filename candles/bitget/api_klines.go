package bitget

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
	MaxLimit = 200
	// SuccessCode is the response code indicating a successful API request
	SuccessCode = "00000"
)

type errorResponse struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type candlestickData struct {
	Ts       string `json:"ts"`       // System timestamp (milliseconds as string)
	Open     string `json:"open"`     // Opening price
	High     string `json:"high"`     // Highest price
	Low      string `json:"low"`      // Lowest price
	Close    string `json:"close"`    // Closing price
	BaseVol  string `json:"baseVol"`  // Base coin volume
	QuoteVol string `json:"quoteVol"` // Denomination coin volume
	UsdtVol  string `json:"usdtVol"`  // USDT volume
}

type successfulResponse struct {
	Code        string            `json:"code"`
	Msg         string            `json:"msg"`
	RequestTime int64             `json:"requestTime"`
	Data        []candlestickData `json:"data"`
}

// Docs: https://bitgetlimited.github.io/apidoc/en/spot/#get-history-candle-data
// Response format: array of objects with ts, open, high, low, close, baseVol, quoteVol, usdtVol
func (e *Bitget) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vmarket/history-candles", e.apiURL), nil)
	symbol := fmt.Sprintf("%v%v_SPBL", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset)) // Bitget uses BTCUSDT_SPBL format

	q := req.URL.Query()
	q.Add("symbol", symbol)

	// Bitget uses period codes: 1min, 5min, 15min, 30min, 1h, 4h, 6h, 12h, 1day, 3day, 1week, 1M
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
		period = "1h"
	case 4 * 60 * time.Minute:
		period = "4h"
	case 6 * 60 * time.Minute:
		period = "6h"
	case 12 * 60 * time.Minute:
		period = "12h"
	case 1 * 60 * 24 * time.Minute:
		period = "1day"
	case 3 * 60 * 24 * time.Minute:
		period = "3day"
	case 7 * 60 * 24 * time.Minute:
		period = "1week"
	case 30 * 60 * 24 * time.Minute:
		period = "1M"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}
	q.Add("period", period)
	q.Add("limit", fmt.Sprintf("%d", MaxLimit))
	// Bitget uses 'endTime' parameter (end time in milliseconds)
	// We request data ending at startTime + MaxLimit*interval to get enough historical data
	q.Add("endTime", fmt.Sprintf("%v", startTime.Add(time.Duration(MaxLimit)*candlestickInterval).Unix()*1000))

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
	if err == nil && maybeErrorResponse.Code != SuccessCode {
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
			Err:            errors.New(maybeErrorResponse.Msg),
		}
	}

	maybeResponse := successfulResponse{}
	err = json.Unmarshal(byts, &maybeResponse)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if maybeResponse.Code != SuccessCode {
		if strings.Contains(maybeResponse.Msg, "symbol") || strings.Contains(maybeResponse.Msg, "Invalid") {
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
		// Parse timestamp (milliseconds as string)
		timestampMs, err := strconv.ParseInt(raw.Ts, 10, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp! Invalid syntax from Bitget: %v", i, err)
		}
		timestamp := int(time.Unix(0, timestampMs*int64(time.Millisecond)).Unix())

		// Parse open price
		openPrice, err := strconv.ParseFloat(raw.Open, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from Bitget: %v", i, raw.Open, err)
		}

		// Parse high price
		highPrice, err := strconv.ParseFloat(raw.High, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from Bitget: %v", i, raw.High, err)
		}

		// Parse low price
		lowPrice, err := strconv.ParseFloat(raw.Low, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from Bitget: %v", i, raw.Low, err)
		}

		// Parse close price
		closePrice, err := strconv.ParseFloat(raw.Close, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from Bitget: %v", i, raw.Close, err)
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
		log.Info().Str("exchange", "Bitget").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	// Bitget returns candlesticks in descending order (newest first), so reverse them
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}
