package coinbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

type successResponse = [][]interface{}

type errorResponse struct {
	Message string `json:"message"`
}

func coinbaseToCandlesticks(response successResponse) ([]common.Candlestick, error) {
	candlesticks := make([]common.Candlestick, len(response))
	for i := 0; i < len(response); i++ {
		raw := response[i]
		timestampFloat, ok := raw[0].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v had timestampMillis = %v! Invalid syntax from Coinbase", i, timestampFloat)
		}
		timestamp := int(timestampFloat)
		lowestPrice, ok := raw[1].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v had lowestPrice = %v! Invalid syntax from Coinbase", i, lowestPrice)
		}
		highestPrice, ok := raw[2].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v had highestPrice = %v! Invalid syntax from Coinbase", i, highestPrice)
		}
		openPrice, ok := raw[3].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v had openPrice = %v! Invalid syntax from Coinbase", i, openPrice)
		}
		closePrice, ok := raw[4].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v had closePrice = %v! Invalid syntax from Coinbase", i, closePrice)
		}
		volume, ok := raw[5].(float64)
		if !ok {
			return candlesticks, fmt.Errorf("candlestick %v had volume = %v! Invalid syntax from Coinbase", i, volume)
		}

		candlestick := common.Candlestick{
			Timestamp:    timestamp,
			LowestPrice:  common.JSONFloat64(lowestPrice),
			HighestPrice: common.JSONFloat64(highestPrice),
			OpenPrice:    common.JSONFloat64(openPrice),
			ClosePrice:   common.JSONFloat64(closePrice),
			Volume:       common.JSONFloat64(volume),
		}
		candlesticks[i] = candlestick
	}

	return candlesticks, nil
}

func (e *Coinbase) requestCandlesticks(baseAsset string, quoteAsset string, startTimeTs int, intervalMinutes int) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vproducts/%v-%v/candles", e.apiURL, strings.ToUpper(baseAsset), strings.ToUpper(quoteAsset)), nil)

	q := req.URL.Query()

	granularity := intervalMinutes * 60

	validGranularities := map[int]bool{
		60:    true,
		300:   true,
		900:   true,
		3600:  true,
		21600: true,
		86400: true,
	}
	if isValid := validGranularities[granularity]; !isValid {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrUnsupportedCandlestickInterval}
	}

	q.Add("granularity", fmt.Sprintf("%v", granularity))

	startTimeTm := time.Unix(int64(startTimeTs), 0)
	startTimeISO8601 := startTimeTm.Format(time.RFC3339)
	endTimeISO8601 := startTimeTm.Add(299 * 60 * time.Second).Format(time.RFC3339)

	q.Add("start", fmt.Sprintf("%v", startTimeISO8601))
	q.Add("end", fmt.Sprintf("%v", endTimeISO8601))

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
	if err == nil && (maybeErrorResponse.Message != "") {
		if maybeErrorResponse.Message == "NotFound" {
			return nil, common.CandleReqError{
				IsNotRetryable: true,
				IsExchangeSide: true,
				Err:            common.ErrInvalidMarketPair,
			}
		}
		return nil, common.CandleReqError{
			IsNotRetryable: false,
			IsExchangeSide: true,
			Err:            errors.New(maybeErrorResponse.Message),
		}
	}

	maybeResponse := successResponse{}
	err = json.Unmarshal(byts, &maybeResponse)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrInvalidJSONResponse}
	}

	candlesticks, err := coinbaseToCandlesticks(maybeResponse)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: err}
	}

	if e.debug {
		log.Info().Str("exchange", "Coinbase").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, IsExchangeSide: true, Err: common.ErrOutOfCandlesticks}
	}

	// Reverse slice, because Coinbase returns candlesticks in descending order
	for i, j := 0, len(candlesticks)-1; i < j; i, j = i+1, j-1 {
		candlesticks[i], candlesticks[j] = candlesticks[j], candlesticks[i]
	}

	return candlesticks, nil
}

// Coinbase uses the strategy of having candlesticks on multiples of an hour or a day, and truncating the requested
// millisecond timestamps to the closest mutiple in the future. To test this, use the following snippet:
//
// curl -s "https://api.pro.coinbase.com/products/BTC-USD/candles?granularity=60&start=2022-01-16T10:45:24Z&end=2022-01-16T10:59:24Z" | jq '.[] | .[0] | todate'
//
// Note that if `end` - `start` / `granularity` > 300, rather than failing silently, the following error will be
// returned (which is great):
//
// {"message":"granularity too small for the requested time range. Count of aggregations requested exceeds 300"}
//
//
// On the 60 resolution, candlesticks exist at every minute
// On the 300 resolution, candlesticks exist at: 00, 05, 10 ...
// On the 900 resolution, candlesticks exist at: 00, 15, 30 & 45
// On the 3600 resolution, candlesticks exist at every hour
// On the 21600 resolution, candlesticks exist at: 00:00, 06:00, 12:00 & 18:00
// On the 86400 resolution, candlesticks exist at every day at 00:00:00
