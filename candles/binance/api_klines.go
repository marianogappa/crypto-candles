package binance

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

func (r errorResponse) toError() error {
	if r.Code == 0 && r.Msg == "" {
		return nil
	}
	if r.Code == eRRINVALIDSYMBOL {
		return common.ErrInvalidMarketPair
	}
	return fmt.Errorf("binance returned error code! Code: %v, Message: %v", r.Code, r.Msg)
}

// [
// 	[
// 	  1499040000000,      // Open time
// 	  "0.01634790",       // Open
// 	  "0.80000000",       // High
// 	  "0.01575800",       // Low
// 	  "0.01577100",       // Close
// 	  "148976.11427815",  // Volume
// 	  1499644799999,      // Close time
// 	  "2434.19055334",    // Quote asset volume
// 	  308,                // Number of trades
// 	  "1756.87402397",    // Taker buy base asset volume
// 	  "28.46694368",      // Taker buy quote asset volume
// 	  "17928899.62484339" // Ignore.
// 	]
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

func (e *Binance) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
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
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrExecutingRequest}
	}
	defer resp.Body.Close()

	byts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrBrokenBodyResponse}
	}

	maybeErrorResponse := errorResponse{}
	err = json.Unmarshal(byts, &maybeErrorResponse)
	errResp := maybeErrorResponse.toError()
	if err == nil && errResp != nil {
		var retryAfter time.Duration
		if resp.StatusCode == http.StatusTooManyRequests && len(resp.Header["Retry-After"]) == 1 {
			seconds, _ := strconv.Atoi(resp.Header["Retry-After"][0])
			retryAfter = time.Duration(seconds) * time.Second
		}

		return nil, common.CandleReqError{
			IsNotRetryable: false,
			IsExchangeSide: true,
			Code:           maybeErrorResponse.Code,
			Err:            errors.New(maybeErrorResponse.Msg),
			RetryAfter:     retryAfter,
		}
	}

	maybeResponse := successfulResponse{}
	err = json.Unmarshal(byts, &maybeResponse.ResponseCandlesticks)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrInvalidJSONResponse}
	}

	candlesticks, err := maybeResponse.toCandlesticks()
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: err}
	}

	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrOutOfCandlesticks}
	}

	if e.debug {
		log.Info().Str("exchange", "Binance").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	return candlesticks, nil
}

// Example request for klines on Binance:
// https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=3&startTime=1642329924000
// For 1m interval and date Sunday, January 16, 2022 10:45:24 AM (UTC)
//
// Returns
//
// [
//   [
//     1642329960000,     //  Sunday, January 16, 2022 10:46:00 AM
//     "43086.22000000",
//     "43086.22000000",
//     "43069.48000000",
//     "43070.00000000",
//     "8.65209000",
//     1642330019999,
//     "372709.68472200",
//     384,
//     "2.52145000",
//     "108606.91496040",
//     "0"
//   ],
//   [
//     1642330020000,    // Sunday, January 16, 2022 10:47:00 AM
//     "43070.00000000",
//     "43079.63000000",
//     "43069.99000000",
//     "43072.60000000",
//     "5.54560000",
//     1642330079999,
//     "238872.65921540",
//     348,
//     "2.52414000",
//     "108722.43274820",
//     "0"
//   ],
//   [
//     1642330080000,    // Sunday, January 16, 2022 10:48:00 AM
//     "43072.59000000",
//     "43072.60000000",
//     "43068.13000000",
//     "43071.18000000",
//     "4.13011000",
//     1642330139999,
//     "177888.74219360",
//     344,
//     "1.53302000",
//     "66029.17746930",
//     "0"
//   ]
// ]
//
// Binance uses the strategy of having candlesticks on multiples of an hour or a day, and truncating the requested
// millisecond timestamps to the closest mutiple in the future. To test this, use the following snippet:
//
// curl "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=3m&limit=60&startTime=1642330104000" | jq '.[] | .[0] | . / 1000 | todate'
//
// And test with these millisecond timestamps:
//
// 1642329924000 = Sunday, January 16, 2022 10:45:24 AM
// 1642329984000 = Sunday, January 16, 2022 10:46:24 AM
// 1642330044000 = Sunday, January 16, 2022 10:47:24 AM
// 1642330104000 = Sunday, January 16, 2022 10:48:24 AM
//
// On the 1m interval, candlesticks exist at every minute
// On the 3m interval, candlesticks exist at: 00, 03, 06 ...
// On the 5m interval, candlesticks exist at: 00, 05, 10 ...
// On the 15m interval, candlesticks exist at: 00, 15, 30 & 45
// On the 30m interval, candlesticks exist at: 00 & 30
// On the 1h interval, candlesticks exist at: 00
// On the 2h interval, candlesticks exist at: 00:00, 02:00, 04:00 ...
// On the 4h interval, candlesticks exist at: 00:00, 04:00, 08:00 ...
// On the 8h interval, candlesticks exist at: 00:00, 08:00 & 16:00 ...
// On the 12h interval, candlesticks exist at: 00:00 & 12:00
// On the 1d interval, candlesticks exist at every day
// On the 3d interval, things become interesting because months can have 28, 29, 30 & 31 days, but it follows the time.Truncate(3 day) logic
// On the 1w interval, it also follows the time.Truncate(7 day) logic
// On the 1M interval, candlesticks exist at the beginning of each month
