package bitstamp

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

type responseDataOHLC struct {
	High      string `json:"high"`
	Timestamp string `json:"timestamp"`
	Volume    string `json:"volume"`
	Low       string `json:"low"`
	Close     string `json:"close"`
	Open      string `json:"open"`
}

func (o responseDataOHLC) toCandlestick() (common.Candlestick, error) {
	c := common.Candlestick{}

	timestamp, err := strconv.Atoi(o.Timestamp)
	if err != nil {
		return common.Candlestick{}, err
	}
	c.Timestamp = timestamp

	rawFloat, err := strconv.ParseFloat(o.Close, 64)
	if err != nil {
		return common.Candlestick{}, err
	}
	c.ClosePrice = common.JSONFloat64(rawFloat)

	rawFloat, err = strconv.ParseFloat(o.Open, 64)
	if err != nil {
		return common.Candlestick{}, err
	}
	c.OpenPrice = common.JSONFloat64(rawFloat)

	rawFloat, err = strconv.ParseFloat(o.High, 64)
	if err != nil {
		return common.Candlestick{}, err
	}
	c.HighestPrice = common.JSONFloat64(rawFloat)

	rawFloat, err = strconv.ParseFloat(o.Low, 64)
	if err != nil {
		return common.Candlestick{}, err
	}
	c.LowestPrice = common.JSONFloat64(rawFloat)

	return c, nil
}

type responseData struct {
	Pair string             `json:"pair"`
	OHLC []responseDataOHLC `json:"ohlc"`
}

type responseError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

func (e responseError) String() string {
	return fmt.Sprintf("%v: %v: %v", e.Code, e.Field, e.Message)
}

type response struct {
	Code   string          `json:"code"`
	Errors []responseError `json:"errors"`
	Data   responseData    `json:"data"`
}

func (r response) toCandlesticks() ([]common.Candlestick, error) {
	cs := []common.Candlestick{}

	for _, item := range r.Data.OHLC {
		c, err := item.toCandlestick()
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

func (r response) toError() error {
	ss := []string{}
	for _, subError := range r.Errors {
		ss = append(ss, subError.String())
	}
	return errors.New(strings.Join(ss, ", "))
}

func (e *Bitstamp) requestCandlesticks(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("%vohlc/%v%v/", e.apiURL, strings.ToLower(baseAsset), strings.ToLower(quoteAsset)), nil)

	// Bitstamp has the unusual strategy of returning the snapped timestamp to the past rather than the future, so
	// for this particular case it's important to do the snap to the future before making the request.
	startTimeSecs := common.NormalizeTimestamp(startTime, candlestickInterval, "BITSTAMP", false)

	q := req.URL.Query()
	q.Add("start", fmt.Sprintf("%v", startTimeSecs))
	q.Add("step", fmt.Sprintf("%v", int(candlestickInterval/time.Second)))
	q.Add("limit", "1000")

	req.URL.RawQuery = q.Encode()

	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: fmt.Errorf("%w: %v", common.ErrExecutingRequest, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		// https://www.bitstamp.net/api/#what-is-api
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrRateLimit}
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: common.ErrInvalidMarketPair}
	}

	// Catch-all for non-200 errors
	if resp.StatusCode != http.StatusOK {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: fmt.Errorf("exchange returned status code %v", resp.StatusCode)}
	}

	byts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrBrokenBodyResponse}
	}

	maybeResponse := response{}
	if err := json.Unmarshal(byts, &maybeResponse); err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrInvalidJSONResponse}
	}

	// All listed errors are unretryable.
	// https://www.bitstamp.net/api/#ohlc_data
	if len(maybeResponse.Errors) > 0 {
		return nil, common.CandleReqError{IsNotRetryable: true, Err: maybeResponse.toError()}
	}

	candlesticks, err := maybeResponse.toCandlesticks()
	if err != nil {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: err}
	}

	if e.debug {
		log.Info().Str("exchange", "Bitstamp").Str("market", fmt.Sprintf("%v/%v", baseAsset, quoteAsset)).Int("candlestick_count", len(candlesticks)).Msg("Candlestick request successful!")
	}

	if len(candlesticks) == 0 {
		return nil, common.CandleReqError{IsNotRetryable: false, Err: common.ErrOutOfCandlesticks}
	}

	return candlesticks, nil
}

// Bitstamp uses the strategy of having candlesticks on multiples of an hour or a day, and truncating the requested
// millisecond timestamps to the closest mutiple in the PAST!!, not the future. To test this, use the following snippet:
//
// curl -s "https://www.bitstamp.net/api/v2/ohlc/btcusd/?limit=20&step=60&start="$(date -j -f "%Y-%m-%d %H:%M:%S" "2020-04-07 00:00:00" "+%s") | jq '.data.ohlc | .[] | .timestamp | tonumber | todate'
//
// On the 60 type, candlesticks exist at every minute
// On the 180 type, candlesticks exist at 00, 03, 16 ...
// On the 300 type, candlesticks exist at 00, 05, 10 ...
// On the 900 type, candlesticks exist at 00, 15, 30 ...
// On the 1800 type, candlesticks exist at 00 & 30
// On the 3600 type, candlesticks exist at every hour
// On the 7200 type, candlesticks exist at 00:00, 02:00, 04:00 ...
// On the 14400 type, candlesticks exist at 00:00, 04:00, 08:00 ...
// On the 21600 type, candlesticks exist at 00:00, 06:00, 12:00 & 18:00
// On the 43200 type, candlesticks exist at 00:00 & 12:00
// On the 86400 type, candlesticks exist at every day at 00:00:00
// On the 259200 type, INVESTIGATE FURTHER!!
