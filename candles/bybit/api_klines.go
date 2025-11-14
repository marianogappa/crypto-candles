package bybit

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"io"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

const (
	// retCodeNotSupportedSymbols indicates the symbol/market pair is not supported
	retCodeNotSupportedSymbols = 10001
	// retCodeInvalidCategory indicates an invalid category was provided
	retCodeInvalidCategory = 10002
	// MaxLimit is the maximum number of candlesticks that can be requested per API call
	MaxLimit = 1000
)

type errorResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
}

type successfulResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Category string     `json:"category"`
		List     [][]string `json:"list"`
		Symbol   string     `json:"symbol"`
	} `json:"result"`
}

// Docs https://bybit-exchange.github.io/docs/v5/market/kline
//
// list[0]: startTime	string	Start time of the candle (ms)
// list[1]: openPrice	string	Open price
// list[2]: highPrice	string	Highest price
// list[3]: lowPrice	string	Lowest price
// list[4]: closePrice	string	Close price. Is the last traded price when the candle is not closed
func (r successfulResponse) toCandlesticks() ([]common.Candlestick, error) {
	candlesticks := make([]common.Candlestick, len(r.Result.List))
	for i := 0; i < len(r.Result.List); i++ {
		raw := r.Result.List[i]
		if len(raw) != 7 {
			return candlesticks, fmt.Errorf("candlestick %v has len != 7! Invalid syntax from Bybit", i)
		}

		// Parse timestamp (milliseconds as string)
		timestampMs, err := strconv.ParseInt(raw[0], 10, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has invalid timestamp! Invalid syntax from Bybit: %v", i, err)
		}
		timestamp := int(time.Unix(0, timestampMs*int64(time.Millisecond)).Unix())

		// Parse open price
		openPrice, err := strconv.ParseFloat(raw[1], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from Bybit: %v", i, raw[1], err)
		}

		// Parse high price
		highPrice, err := strconv.ParseFloat(raw[2], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from Bybit: %v", i, raw[2], err)
		}

		// Parse low price
		lowPrice, err := strconv.ParseFloat(raw[3], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from Bybit: %v", i, raw[3], err)
		}

		// Parse close price
		closePrice, err := strconv.ParseFloat(raw[4], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from Bybit: %v", i, raw[4], err)
		}

		candlesticks[i] = common.Candlestick{
			Timestamp:    timestamp,
			OpenPrice:    common.JSONFloat64(openPrice),
			ClosePrice:   common.JSONFloat64(closePrice),
			LowestPrice:  common.JSONFloat64(lowPrice),
			HighestPrice: common.JSONFloat64(highPrice),
		}
	}

	return candlesticks, nil
}

func (e *Bybit) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vmarket/kline", e.apiURL), nil)
	symbol := fmt.Sprintf("%v%v", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset))

	q := req.URL.Query()
	q.Add("category", "spot")
	q.Add("symbol", symbol)

	// Bybit uses numeric interval codes:
	// 1 = 1 minute, 3 = 3 minutes, 5 = 5 minutes, 15 = 15 minutes, 30 = 30 minutes
	// 60 = 1 hour, 120 = 2 hours, 240 = 4 hours, 360 = 6 hours, 720 = 12 hours
	// D = 1 day, W = 1 week, M = 1 month
	var interval string
	switch candlestickInterval {
	case 1 * time.Minute:
		interval = "1"
	case 3 * time.Minute:
		interval = "3"
	case 5 * time.Minute:
		interval = "5"
	case 15 * time.Minute:
		interval = "15"
	case 30 * time.Minute:
		interval = "30"
	case 1 * 60 * time.Minute:
		interval = "60"
	case 2 * 60 * time.Minute:
		interval = "120"
	case 4 * 60 * time.Minute:
		interval = "240"
	case 6 * 60 * time.Minute:
		interval = "360"
	case 12 * 60 * time.Minute:
		interval = "720"
	case 1 * 60 * 24 * time.Minute:
		interval = "D"
	case 7 * 60 * 24 * time.Minute:
		interval = "W"
	case 30 * 60 * 24 * time.Minute:
		interval = "M"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}
	q.Add("interval", interval)
	q.Add("limit", fmt.Sprintf("%d", MaxLimit))
	q.Add("start", fmt.Sprintf("%v", startTime.Unix()*1000)) // startTime is in milliseconds https://bybit-exchange.github.io/docs/v5/market/kline

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
	if err == nil && maybeErrorResponse.RetCode != 0 {
		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Code:           maybeErrorResponse.RetCode,
				Err:            common.ErrRateLimit,
			}
		}

		// Check for invalid symbol/market pair errors
		if maybeErrorResponse.RetCode == retCodeNotSupportedSymbols || maybeErrorResponse.RetCode == retCodeInvalidCategory {
			return nil, common.CandleReqError{IsNotRetryable: true, Code: maybeErrorResponse.RetCode, Err: common.ErrInvalidMarketPair}
		}

		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Code:           maybeErrorResponse.RetCode,
			Err:            errors.New(maybeErrorResponse.RetMsg),
		}
	}

	maybeResponse := successfulResponse{}
	err = json.Unmarshal(byts, &maybeResponse)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	if maybeResponse.RetCode != 0 {
		// Check for invalid symbol/market pair errors
		if maybeResponse.RetCode == retCodeNotSupportedSymbols || maybeResponse.RetCode == retCodeInvalidCategory {
			return nil, common.CandleReqError{IsNotRetryable: true, Code: maybeResponse.RetCode, Err: common.ErrInvalidMarketPair}
		}
		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Code:           maybeResponse.RetCode,
			Err:            errors.New(maybeResponse.RetMsg),
		}
	}

	candlesticks, err := maybeResponse.toCandlesticks()
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: err}
	}

	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	if e.debug {
		log.Info().Str("exchange", "Bybit").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	// Reverse slice, because Bybit returns candlesticks in descending order (newest first)
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}
