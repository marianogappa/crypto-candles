package bitfinex

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

type response struct {
	resp [][]interface{}
}

func interfaceToFloatRoundInt(i interface{}) (int, bool) {
	f, ok := i.(float64)
	if !ok {
		return 0, false
	}
	return int(f), true
}

func (r response) toCandlesticks() ([]common.Candlestick, error) {
	candlesticks := make([]common.Candlestick, len(r.resp))
	for i := 0; i < len(r.resp); i++ {
		raw := r.resp[i]
		candlestick := common.Candlestick{}
		if len(raw) != 6 {
			return candlesticks, fmt.Errorf("candlestick %v has len != 6! Invalid syntax from Bitfinex", i)
		}
		rawTimestamp, ok := interfaceToFloatRoundInt(raw[0])
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-int open time! Invalid syntax from Bitfinex", i)
		}
		candlestick.Timestamp = int(time.Unix(0, int64(rawTimestamp)*int64(time.Millisecond)).Unix())

		rawOpen, ok := raw[1].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-float open! Invalid syntax from Bitfinex", i)
		}
		candlestick.OpenPrice = common.JSONFloat64(rawOpen)

		rawClose, ok := raw[2].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-float close! Invalid syntax from Bitfinex", i)
		}
		candlestick.ClosePrice = common.JSONFloat64(rawClose)

		rawHigh, ok := raw[3].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-float high! Invalid syntax from Bitfinex", i)
		}
		candlestick.HighestPrice = common.JSONFloat64(rawHigh)

		rawLow, ok := raw[4].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v has non-float low! Invalid syntax from Bitfinex", i)
		}
		candlestick.LowestPrice = common.JSONFloat64(rawLow)

		if candlestick.LowestPrice > candlestick.HighestPrice {
			return candlesticks, fmt.Errorf("candlestick %v had low = %v > high %v! Invalid syntax from Bitfinex", i, rawLow, rawHigh)
		}
		if candlestick.OpenPrice > candlestick.HighestPrice || candlestick.OpenPrice < candlestick.LowestPrice {
			return candlesticks, fmt.Errorf("candlestick %v had open = %v not between low = %v & high = %v! Invalid syntax from Bitfinex", i, rawOpen, rawLow, rawHigh)
		}
		if candlestick.ClosePrice > candlestick.HighestPrice || candlestick.ClosePrice < candlestick.LowestPrice {
			return candlesticks, fmt.Errorf("candlestick %v had close = %v not between low = %v & high = %v! Invalid syntax from Bitfinex", i, rawClose, rawLow, rawHigh)
		}

		candlesticks[i] = candlestick
	}

	return candlesticks, nil
}

type responseError struct {
	resp []interface{}
}

func (e responseError) toCandleReqError() (common.CandleReqError, bool) {
	if len(e.resp) != 3 {
		return common.CandleReqError{}, false
	}
	err := common.CandleReqError{}
	literalError, ok := e.resp[0].(string)
	if !ok || literalError != "error" {
		return common.CandleReqError{}, false
	}
	rawCode, ok := e.resp[1].(float64)
	if !ok {
		return common.CandleReqError{}, false
	}
	err.Code = int(rawCode)
	msg, ok := e.resp[2].(string)
	if !ok {
		return common.CandleReqError{}, false
	}
	err.Err = fmt.Errorf(fmt.Sprintf("%v: %v", err.Code, msg))
	err.IsExchangeSide = true
	err.IsNotRetryable = true

	return err, true
}

func (e *Bitfinex) requestCandlesticks(baseAsset string, quoteAsset string, startTimeSecs int, intervalMinutes int) ([]common.Candlestick, error) {

	timeframe := ""
	switch intervalMinutes {
	case 1:
		timeframe = "1m"
	case 5:
		timeframe = "5m"
	case 15:
		timeframe = "15m"
	case 30:
		timeframe = "30m"
	case 1 * 60:
		timeframe = "1h"
	case 3 * 60:
		timeframe = "3h"
	case 6 * 60:
		timeframe = "6h"
	case 12 * 60:
		timeframe = "12h"
	case 1 * 60 * 24:
		timeframe = "1D"
	case 7 * 60 * 24:
		timeframe = "1W"
	case 14 * 60 * 24:
		timeframe = "14D"
	case 30 * 60 * 24:
		timeframe = "1M"
	default:
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}

	req, _ := http.NewRequest("GET", fmt.Sprintf("%vcandles/trade:%v:t%v%v/hist", e.apiURL, timeframe, strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset)), nil)

	// Some exchanges have the unusual strategy of returning the snapped timestamp to the past rather than the future,
	// so it's important to do the snap to the future before making the request, to not depend on the echange doing so.
	startTimeSecs = common.NormalizeTimestamp(time.Unix(int64(startTimeSecs), 0), time.Duration(intervalMinutes)*time.Minute, "BITFINEX", false)

	q := req.URL.Query()
	q.Add("start", fmt.Sprintf("%v", startTimeSecs*1000))
	q.Add("limit", "10000")
	q.Add("sort", "1")

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

	errorResp := responseError{}
	if err := json.Unmarshal(byts, &errorResp.resp); err == nil {
		fmt.Println("here", string(byts))
		if err, isError := errorResp.toCandleReqError(); isError {
			return nil, err
		}
	}

	okResp := response{}
	if err := json.Unmarshal(byts, &okResp.resp); err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrInvalidJSONResponse}
	}

	candlesticks, err := okResp.toCandlesticks()
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: err}
	}

	// Bitfinex has a weird behaviour where invalid market pairs are returned as HTTP 200 with an empty array
	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: true, IsExchangeSide: true, Err: common.ErrInvalidMarketPair}
	}

	if e.debug {
		log.Info().Str("exchange", "Bitfinex").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	return candlesticks, nil
}

// Bitfinex uses the strategy of having candlesticks on multiples of an h or a day, and truncating the requested
// millisecond timestamps to the closest mutiple in the future. To test this, use the following snippet:
//

// curl -s 'https://api-pub.bitfinex.com/v2/candles/trade:1m:tBTCUSD/hist?limit=3&sort=1&start='$(date -j -f "%Y-%m-%d %H:%M:%S" "2020-04-07 00:00:00" "+%s000") | jq '.[] | .[0] | tonumber | . / 1000 | todate'
//
// On the 1m timeframe, candlesticks exist at every minute
// On the 5m timeframe, candlesticks exist at 00, 05, 10 ...
// On the 15m timeframe, candlesticks exist at  00, 15, 30, 45 ...
// On the 30m timeframe, candlesticks exist at 00 & 30
// On the 1h timeframe, candlesticks exist at every hour
// On the 3h timeframe, candlesticks exist at 00:00, 03:00, 06:00 ...
// On the 6h timeframe, candlesticks exist at 00:00, 06:00, 12:00 ...
// On the 12h timeframe, candlesticks exist at 00:00 & 12:00
// On the 1D timeframe, candlesticks exist every day at 00:00:00
// On the 1W timeframe, INVESTIGATE FURTHER!!
// On the 14D timeframe, INVESTIGATE FURTHER!!
// On the 1M timeframe, INVESTIGATE FURTHER!!
