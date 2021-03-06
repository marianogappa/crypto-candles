package binanceusdmfutures

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

type errorResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

// [
//   [
//     1499040000000,      // Open time
//     "0.01634790",       // Open
//     "0.80000000",       // High
//     "0.01575800",       // Low
//     "0.01577100",       // Close
//     "148976.11427815",  // Volume
//     1499644799999,      // Close time
//     "2434.19055334",    // Quote asset volume
//     308,                // Number of trades
//     "1756.87402397",    // Taker buy base asset volume
//     "28.46694368",      // Taker buy quote asset volume
//     "17928899.62484339" // Ignore.
//   ]
// ]
type successfulResponse struct {
	ResponseCandlesticks [][]interface{}
}

func interfaceToFloatRoundInt(i interface{}) (int, bool) {
	f, ok := i.(float64)
	if !ok {
		return 0, false
	}
	return int(f), true
}

func (r successfulResponse) toCandlesticks() ([]common.Candlestick, error) {
	candlesticks := make([]common.Candlestick, len(r.ResponseCandlesticks))
	for i := 0; i < len(r.ResponseCandlesticks); i++ {
		raw := r.ResponseCandlesticks[i]
		candlestick := binanceCandlestick{}
		if len(raw) != 12 {
			return candlesticks, fmt.Errorf("candlestick %v has len != 12! Invalid syntax from Binance", i)
		}
		rawOpenTime, ok := interfaceToFloatRoundInt(raw[0])
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-int open time! Invalid syntax from Binance", i)
		}
		candlestick.openAt = time.Unix(0, int64(rawOpenTime)*int64(time.Millisecond))

		rawOpen, ok := raw[1].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string open! Invalid syntax from Binance", i)
		}
		openPrice, err := strconv.ParseFloat(rawOpen, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v! Invalid syntax from Binance", i, openPrice)
		}
		candlestick.openPrice = openPrice

		rawHigh, ok := raw[2].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string high! Invalid syntax from Binance", i)
		}
		highPrice, err := strconv.ParseFloat(rawHigh, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had high = %v! Invalid syntax from Binance", i, highPrice)
		}
		candlestick.highPrice = highPrice

		rawLow, ok := raw[3].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string low! Invalid syntax from Binance", i)
		}
		lowPrice, err := strconv.ParseFloat(rawLow, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v! Invalid syntax from Binance", i, lowPrice)
		}
		candlestick.lowPrice = lowPrice

		rawClose, ok := raw[4].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string close! Invalid syntax from Binance", i)
		}
		closePrice, err := strconv.ParseFloat(rawClose, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v! Invalid syntax from Binance", i, closePrice)
		}
		candlestick.closePrice = closePrice

		rawVolume, ok := raw[5].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string volume! Invalid syntax from Binance", i)
		}
		volume, err := strconv.ParseFloat(rawVolume, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had volume = %v! Invalid syntax from Binance", i, volume)
		}
		candlestick.volume = volume

		rawCloseTime, ok := interfaceToFloatRoundInt(raw[6])
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-int close time! Invalid syntax from Binance", i)
		}
		candlestick.closeAt = time.Unix(0, int64(rawCloseTime)*int64(time.Millisecond))

		rawQuoteAssetVolume, ok := raw[7].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string quote asset volume! Invalid syntax from Binance", i)
		}
		quoteAssetVolume, err := strconv.ParseFloat(rawQuoteAssetVolume, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had quote asset volume = %v! Invalid syntax from Binance", i, quoteAssetVolume)
		}
		candlestick.quoteAssetVolume = quoteAssetVolume

		rawNumberOfTrades, ok := interfaceToFloatRoundInt(raw[8])
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-int number of trades! Invalid syntax from Binance", i)
		}
		candlestick.tradeCount = rawNumberOfTrades

		rawTakerBaseAssetVolume, ok := raw[9].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string taker base asset volume! Invalid syntax from Binance", i)
		}
		takerBaseAssetVolume, err := strconv.ParseFloat(rawTakerBaseAssetVolume, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had taker base asset volume = %v! Invalid syntax from Binance", i, takerBaseAssetVolume)
		}
		candlestick.takerBuyBaseAssetVolume = takerBaseAssetVolume

		rawTakerQuoteAssetVolume, ok := raw[10].(string)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-string taker quote asset volume! Invalid syntax from Binance", i)
		}
		takerBuyQuoteAssetVolume, err := strconv.ParseFloat(rawTakerQuoteAssetVolume, 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v had taker quote asset volume = %v! Invalid syntax from Binance", i, takerBuyQuoteAssetVolume)
		}
		candlestick.takerBuyQuoteAssetVolume = takerBuyQuoteAssetVolume

		candlesticks[i] = candlestick.toCandlestick()
	}

	return candlesticks, nil
}

type binanceCandlestick struct {
	openAt                   time.Time
	closeAt                  time.Time
	openPrice                float64
	closePrice               float64
	lowPrice                 float64
	highPrice                float64
	volume                   float64
	quoteAssetVolume         float64
	tradeCount               int
	takerBuyBaseAssetVolume  float64
	takerBuyQuoteAssetVolume float64
}

func (c binanceCandlestick) toCandlestick() common.Candlestick {
	return common.Candlestick{
		Timestamp:    int(c.openAt.Unix()),
		OpenPrice:    common.JSONFloat64(c.openPrice),
		ClosePrice:   common.JSONFloat64(c.closePrice),
		LowestPrice:  common.JSONFloat64(c.lowPrice),
		HighestPrice: common.JSONFloat64(c.highPrice),
	}
}

func (e *BinanceUSDMFutures) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vklines", e.apiURL), nil)
	symbol := fmt.Sprintf("%v%v", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset))

	q := req.URL.Query()
	q.Add("symbol", symbol)

	switch candlestickInterval {
	case 1 * time.Minute:
		q.Add("interval", "1m")
	case 3 * time.Minute:
		q.Add("interval", "3m")
	case 5 * time.Minute:
		q.Add("interval", "5m")
	case 15 * time.Minute:
		q.Add("interval", "15m")
	case 30 * time.Minute:
		q.Add("interval", "30m")
	case 1 * 60 * time.Minute:
		q.Add("interval", "1h")
	case 2 * 60 * time.Minute:
		q.Add("interval", "2h")
	case 4 * 60 * time.Minute:
		q.Add("interval", "4h")
	case 6 * 60 * time.Minute:
		q.Add("interval", "6h")
	case 8 * 60 * time.Minute:
		q.Add("interval", "8h")
	case 12 * 60 * time.Minute:
		q.Add("interval", "12h")
	case 1 * 60 * 24 * time.Minute:
		q.Add("interval", "1d")
	case 3 * 60 * 24 * time.Minute:
		q.Add("interval", "3d")
	case 7 * 60 * 24 * time.Minute:
		q.Add("interval", "1w")
	// TODO This one is problematic because cannot patch holes or do other calculations (because months can have 28, 29, 30 & 31 days)
	case 30 * 60 * 24 * time.Minute:
		q.Add("interval", "1M")
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}

	q.Add("limit", "1000")
	q.Add("startTime", fmt.Sprintf("%v", startTime.Unix()*1000))

	req.URL.RawQuery = q.Encode()

	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: fmt.Errorf("%w: %v", common.ErrExecutingRequest, err)}
	}
	defer resp.Body.Close()

	byts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrBrokenBodyResponse}
	}

	maybeErrorResponse := errorResponse{}
	err = json.Unmarshal(byts, &maybeErrorResponse)
	if err == nil && maybeErrorResponse.Code != 0 {
		if resp.StatusCode == http.StatusTooManyRequests && len(resp.Header["Retry-After"]) == 1 {
			seconds, _ := strconv.Atoi(resp.Header["Retry-After"][0])
			retryAfter := time.Duration(seconds) * time.Second
			return nil, common.CandleReqError{
				IsNotRetryable: false,
				Code:           maybeErrorResponse.Code,
				Err:            common.ErrRateLimit,
				RetryAfter:     retryAfter,
			}
		}

		if maybeErrorResponse.Code == eRRINVALIDSYMBOL {
			return nil, common.CandleReqError{IsNotRetryable: true, Code: maybeErrorResponse.Code, Err: common.ErrInvalidMarketPair}
		}

		return nil, common.CandleReqError{
			IsNotRetryable: false,
			Code:           maybeErrorResponse.Code,
			Err:            errors.New(maybeErrorResponse.Msg),
		}
	}

	maybeResponse := successfulResponse{}
	err = json.Unmarshal(byts, &maybeResponse.ResponseCandlesticks)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	candlesticks, err := maybeResponse.toCandlesticks()
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: err}
	}

	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	if e.debug {
		log.Info().Str("exchange", "BinanceUDSMFutures").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	return candlesticks, nil
}
