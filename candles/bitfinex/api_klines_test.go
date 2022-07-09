package bitfinex

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/stretchr/testify/require"
)

func TestHappyToCandlesticks(t *testing.T) {
	testResponse := `
	[
		[
		  1564774860000,
		  10450,
		  10450,
		  10450,
		  10450,
		  0.02551957
		],
		[
		  1564774920000,
		  10449.59487965,
		  10449.48380001,
		  10449.59487965,
		  10449,
		  0.33075187
		],
		[
		  1564774980000,
		  10449.15056109,
		  10445,
		  10449.15056109,
		  10442,
		  0.78276958
		]
	  ]
	`

	expected := []common.Candlestick{
		{
			Timestamp:    1564774860,
			OpenPrice:    10450,
			ClosePrice:   10450,
			HighestPrice: 10450,
			LowestPrice:  10450,
		},
		{
			Timestamp:    1564774920,
			OpenPrice:    10449.59487965,
			ClosePrice:   10449.48380001,
			HighestPrice: 10449.59487965,
			LowestPrice:  10449,
		},
		{
			Timestamp:    1564774980,
			OpenPrice:    10449.15056109,
			ClosePrice:   10445,
			HighestPrice: 10449.15056109,
			LowestPrice:  10442,
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testResponse))
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	actual, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	require.Nil(t, err)
	require.Len(t, actual, 3)
	require.Equal(t, expected, actual)
}

func TestUnhappyToCandlesticksWithRequest(t *testing.T) {
	testResponse := `
	[
		[
		  "INVALID",
		  10450,
		  10450,
		  10450,
		  10450,
		  0.02551957
		]
	  ]
	`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testResponse))
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	require.NotNil(t, err)
}

func TestPatience(t *testing.T) {
	require.Equal(t, NewBitfinex().GetPatience(), 1*time.Minute)
}

func TestInvalidMarketPair(t *testing.T) {
	testResponse := `[]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testResponse))
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	require.Error(t, err, common.ErrInvalidMarketPair)
}

func TestInvalidIntervalMinutes(t *testing.T) {
	b := NewBitfinex()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "just so that it does not actually call bitfinex, but it shouldn't"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), -1)
	require.Error(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestTimeframe1m(t *testing.T) {
	timeframes := map[int]string{
		1:            "1m",
		5:            "5m",
		15:           "15m",
		30:           "30m",
		1 * 60:       "1h",
		3 * 60:       "3h",
		6 * 60:       "6h",
		12 * 60:      "12h",
		1 * 60 * 24:  "1D",
		7 * 60 * 24:  "1W",
		14 * 60 * 24: "14D",
		30 * 60 * 24: "1M",
	}

	for intervalMinutes, timeframe := range timeframes {
		t.Run(timeframe, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, timeframe, strings.Split(r.URL.Path, ":")[1])
			}))
			defer ts.Close()

			b := NewBitfinex()
			b.requester.Strategy = common.RetryStrategy{Attempts: 1}
			b.apiURL = ts.URL + "/"

			b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), intervalMinutes)
		})
	}
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		`[[1564774860000, 10450, 10450, 10450, 10450, 0.02551957, "EXTRA ITEM"]]`,
		`[["INVALID", 10450, 10450, 10450, 10450, 0.02551957 ]]`,
		`[[1564774860000, "INVALID", 10450, 10450, 10450, 0.02551957 ]]`,
		`[[1564774860000, 10450, "INVALID", 10450, 10450, 0.02551957 ]]`,
		`[[1564774860000, 10450, 10450, "INVALID", 10450, 0.02551957 ]]`,
		`[[1564774860000, 10450, 10450, 10450, "INVALID", 0.02551957 ]]`,
		// OCHL: L > H
		`[[1564774860000, 10450, 10450, 999999, 9999999, 0.02551957 ]]`,
		// OCHL: O < L
		`[[1564774860000, 1, 10450, 999999, 2, 0.02551957 ]]`,
		// OCHL: C < L
		`[[1564774860000, 10450, 1, 999999, 2, 0.02551957 ]]`,
		// OCHL: O > H
		`[[1564774860000, 9999999, 10450, 999999, 2, 0.02551957 ]]`,
		// OCHL: C > H
		`[[1564774860000, 10450, 9999999, 999999, 2, 0.02551957 ]]`,
	}

	for i, ts := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			r := response{}
			require.Nil(t, json.Unmarshal([]byte(ts), &r.resp))
			_, err := r.toCandlesticks()
			require.NotNil(t, err, "for %v was %v", string(ts), err)
		})
	}
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`[1564774860000, 10450, 10450, 10450, 10450, 0.02551957]`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "invalid url"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid url")
	}
}

func TestKlinesErrReadingResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[
			"error",
			10020,
			"limit: invalid"
		  ]`)
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	if _, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1); err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func TestKlinesInvalidErrorResponses(t *testing.T) {
	// First element not a string
	err, ok := responseError{resp: unmarshalArrInterface(t, `[123, 10020, "limit: invalid"]`)}.toCandleReqError()
	require.Equal(t, common.CandleReqError{}, err)
	require.False(t, ok)

	// Second element not an int
	err, ok = responseError{resp: unmarshalArrInterface(t, `["error", "not an int", "limit: invalid"]`)}.toCandleReqError()
	require.Equal(t, common.CandleReqError{}, err)
	require.False(t, ok)

	// Third element not a string
	err, ok = responseError{resp: unmarshalArrInterface(t, `["error", 10020, 123]`)}.toCandleReqError()
	require.Equal(t, common.CandleReqError{}, err)
	require.False(t, ok)

	// More than 3 elements
	err, ok = responseError{resp: unmarshalArrInterface(t, `["error", 10020, "limit: invalid", "extra element"]`)}.toCandleReqError()
	require.Equal(t, common.CandleReqError{}, err)
	require.False(t, ok)
}

func unmarshalArrInterface(t *testing.T, s string) []interface{} {
	var res []interface{}
	err := json.Unmarshal([]byte(s), &res)
	require.Nil(t, err)
	return res
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[1564774860000, "INVALID", 10450, 10450, 10450, 0.02551957 ]`)
	}))
	defer ts.Close()

	b := NewBitfinex()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2019-08-02T19:41:00+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid floats in json")
	}
}

func tp(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func tInt(s string) int {
	return int(tp(s).Unix())
}

var (
	msBTCUSD = common.MarketSource{
		Type:       common.COIN,
		Provider:   "BITFINEX",
		BaseAsset:  "BTC",
		QuoteAsset: "USD",
	}
)
