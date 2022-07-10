package kucoin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

type response struct {
	Code string     `json:"code"`
	Msg  string     `json:"msg"`
	Data [][]string `json:"data"`
}

type kucoinCandlestick struct {
	Time     int     // Start time of the candle cycle
	Open     float64 // Opening price
	Close    float64 // Closing price
	High     float64 // Highest price
	Low      float64 // Lowest price
	Volume   float64 // Transaction volume
	Turnover float64 // Transaction amount
}

func responseToCandlesticks(data [][]string) ([]common.Candlestick, error) {
	candlesticks := make([]common.Candlestick, len(data))
	for i := 0; i < len(data); i++ {
		raw := data[i]
		candlestick := kucoinCandlestick{}
		if len(raw) != 7 {
			return candlesticks, fmt.Errorf("candlestick %v has len != 7! Invalid syntax from Kucoin", i)
		}
		rawOpenTime, err := strconv.Atoi(raw[0])
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-int open time! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.Time = rawOpenTime

		rawOpen, err := strconv.ParseFloat(raw[1], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-float open! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.Open = rawOpen

		rawClose, err := strconv.ParseFloat(raw[2], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-float close! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.Close = rawClose

		rawHigh, err := strconv.ParseFloat(raw[3], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-float high! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.High = rawHigh

		rawLow, err := strconv.ParseFloat(raw[4], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-float low! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.Low = rawLow

		rawVolume, err := strconv.ParseFloat(raw[5], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-float volume! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.Volume = rawVolume

		rawTurnover, err := strconv.ParseFloat(raw[6], 64)
		if err != nil {
			return candlesticks, fmt.Errorf("candlestick %v has non-float turnover! Err was %v. Invalid syntax from Kucoin", i, err)
		}
		candlestick.Turnover = rawTurnover

		candlesticks[i] = common.Candlestick{
			Timestamp:    candlestick.Time,
			OpenPrice:    common.JSONFloat64(candlestick.Open),
			ClosePrice:   common.JSONFloat64(candlestick.Close),
			LowestPrice:  common.JSONFloat64(candlestick.Low),
			HighestPrice: common.JSONFloat64(candlestick.High),
		}
	}

	return candlesticks, nil
}

func (e *Kucoin) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vmarket/candles", e.apiURL), nil)
	symbol := fmt.Sprintf("%v-%v", strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset))

	q := req.URL.Query()
	q.Add("symbol", symbol)

	switch candlestickInterval {
	case 1 * time.Minute:
		q.Add("type", "1min")
	case 3 * time.Minute:
		q.Add("type", "3min")
	case 5 * time.Minute:
		q.Add("type", "5min")
	case 15 * time.Minute:
		q.Add("type", "15min")
	case 30 * time.Minute:
		q.Add("type", "30min")
	case 1 * 60 * time.Minute:
		q.Add("type", "1hour")
	case 2 * 60 * time.Minute:
		q.Add("type", "2hour")
	case 4 * 60 * time.Minute:
		q.Add("type", "4hour")
	case 6 * 60 * time.Minute:
		q.Add("type", "6hour")
	case 8 * 60 * time.Minute:
		q.Add("type", "8hour")
	case 12 * 60 * time.Minute:
		q.Add("type", "12hour")
	case 1 * 60 * 24 * time.Minute:
		q.Add("type", "1day")
	case 7 * 60 * 24 * time.Minute:
		q.Add("type", "1week")
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}

	q.Add("startAt", fmt.Sprintf("%v", int(startTime.Unix())))
	q.Add("endAt", fmt.Sprintf("%v", int(startTime.Unix())+1500*int(candlestickInterval/time.Second)))

	req.URL.RawQuery = q.Encode()

	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrExecutingRequest}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		// In this case we should sleep for 11 seconds due to what it says in the docs.
		// https://github.com/marianogappa/crypto-predictions/issues/37#issuecomment-1167566211
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrRateLimit, RetryAfter: 11 * time.Second}
	}

	byts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrBrokenBodyResponse}
	}

	maybeResponse := response{}
	err = json.Unmarshal(byts, &maybeResponse)
	if err == nil && (maybeResponse.Code != "200000" || maybeResponse.Msg != "") {
		if maybeResponse.Code == "400100" && maybeResponse.Msg == "This pair is not provided at present" {
			return nil, common.CandleReqError{IsNotRetryable: true, IsExchangeSide: true, Err: common.ErrInvalidMarketPair}
		}

		err := fmt.Errorf("kucoin returned error code! Code: %v, Message: %v", maybeResponse.Code, maybeResponse.Msg)
		// https://docs.kucoin.com/#request Codes are numeric
		code, _ := strconv.Atoi(maybeResponse.Code)
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: err, Code: code}
	}
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrInvalidJSONResponse}
	}

	candlesticks, err := responseToCandlesticks(maybeResponse.Data)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: err}
	}

	if e.debug {
		log.Info().Str("exchange", "KuCoin").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrOutOfCandlesticks}
	}

	// Reverse slice, because Kucoin returns candlesticks in descending order
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}

// Kucoin uses the strategy of having candlesticks on multiples of an hour or a day, and truncating the requested
// millisecond timestamps to the closest mutiple in the future. To test this, use the following snippet:
//
// curl -s "https://api.kucoin.com/api/v1/market/candles?symbol=BTC-USDT&type=1min&startAt=1642329924&endAt=1642419924" | jq '.data | .[] | .[0] | tonumber | todate'
//
// And test with these millisecond timestamps:
//
// 1642329924 = Sunday, January 16, 2022 10:45:24 AM
// 1642329984 = Sunday, January 16, 2022 10:46:24 AM
// 1642330044 = Sunday, January 16, 2022 10:47:24 AM
// 1642330104 = Sunday, January 16, 2022 10:48:24 AM
//
// Remember that Kucoin & Coinbase are the exchanges that return results in descending order.
//
// Note that if `end` - `start` / `granularity` > 300, rather than failing silently, the following error will be
// returned (which is great):
//
// {"message":"granularity too small for the requested time range. Count of aggregations requested exceeds 300"}
//
//
// On the 1min type, candlesticks exist at every minute
// On the 3min type, candlesticks exist at 00, 03, 06 ...
// On the 5min type, candlesticks exist at 00, 05, 10 ...
// On the 15min type, candlesticks exist at 00, 15, 30 ...
// On the 30min type, candlesticks exist at 00 & 30
// On the 1hour type, candlesticks exist at every hour
// On the 2hour type, candlesticks exist at 00:00, 02:00, 04:00 ...
// On the 4hour type, candlesticks exist at 00:00, 04:00, 08:00 ...
// On the 6hour type, candlesticks exist at 00:00, 06:00, 12:00 & 18:00
// On the 8hour type, candlesticks exist at 00:00, 08:00 & 16:00
// On the 12hour type, candlesticks exist at 00:00 & 12:00
// On the 1day type, candlesticks exist at every day at 00:00:00
//
// The weekly (1week type) is interesting. Check with:
//
// curl -s "https://api.kucoin.com/api/v1/market/candles?symbol=BTC-USDT&type=1week&startAt=1632329924&endAt=1699669924" | jq '.data | .[] | .[0] | tonumber | todate'
//
// It's not clear how it's truncating weeks, cause it's not following the time.Truncate(7 day) logic, and it's not doing
// a "first, second, third, forth week of the month" strategy either. Not sure yet what to do in this case.
