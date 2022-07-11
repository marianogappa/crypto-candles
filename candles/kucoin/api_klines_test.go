package kucoin

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
	testCandlestick := `
	{
		"code": "200000",
		"data": [
		  [
			"1642419900",
			"42675.2",
			"42717.9",
			"42728.8",
			"42664.5",
			"2.99849062",
			"128046.022671917"
		  ],
		  [
			"1642419840",
			"42713.1",
			"42675.2",
			"42713.2",
			"42671.5",
			"2.98171616",
			"127310.210308322"
		  ],
		  [
			"1642419780",
			"42700",
			"42711",
			"42712.9",
			"42699.9",
			"1.63931627",
			"70011.578948013"
		  ]
		]
	}
	`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	actual, err := b.RequestCandlesticks(msBTCUSDT, tp("2022-01-17T11:43:00+00:00"), time.Minute)
	require.Nil(t, err)

	expected := []common.Candlestick{
		{
			Timestamp:    1642419780,
			OpenPrice:    42700,
			ClosePrice:   42711,
			HighestPrice: 42712.9,
			LowestPrice:  42699.9,
		},
		{
			Timestamp:    1642419840,
			OpenPrice:    42713.1,
			ClosePrice:   42675.2,
			HighestPrice: 42713.2,
			LowestPrice:  42671.5,
		},
		{
			Timestamp:    1642419900,
			OpenPrice:    42675.2,
			ClosePrice:   42717.9,
			HighestPrice: 42728.8,
			LowestPrice:  42664.5,
		},
	}
	require.Equal(t, expected, actual)
}

func TestOutOfCandlesticks(t *testing.T) {
	testCandlestick := `
	{
		"code": "200000",
		"data": []
	}
	`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2022-01-17T11:43:00+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestInvalidMarketPair(t *testing.T) {
	testCandlestick := `
	{
		"code": "400100",
		"msg": "This pair is not provided at present."
	  }
	`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2022-01-17T11:43:00+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestErrRateLimit(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2022-01-17T11:43:00+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		// candlestick %v has len != 7! Invalid syntax from Kucoin", i)
		`[["1566789720"]]`,
		// candlestick %v has non-int open time! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["INVALID","10411.5","10401.9","10411.5","10396.3","29.11357276","302889.301529914"]]`,
		// candlestick %v has non-float open! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["1566789720","INVALID","10401.9","10411.5","10396.3","29.11357276","302889.301529914"]]`,
		// candlestick %v has non-float close! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["1566789720","10411.5","INVALID","10411.5","10396.3","29.11357276","302889.301529914"]]`,
		// candlestick %v has non-float high! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["1566789720","10411.5","10401.9","INVALID","10396.3","29.11357276","302889.301529914"]]`,
		// candlestick %v has non-float low! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["1566789720","10411.5","10401.9","10411.5","INVALID","29.11357276","302889.301529914"]]`,
		// candlestick %v has non-float volume! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["1566789720","10411.5","10401.9","10411.5","10396.3","INVALID","302889.301529914"]]`,
		// candlestick %v has non-float turnover! Err was %v. Invalid syntax from Kucoin", i, err)
		`[["1566789720","10411.5","10401.9","10411.5","10396.3","29.11357276","INVALID"]]`,
	}

	for i, ts := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			sr := [][]string{}
			err := json.Unmarshal([]byte(ts), &sr)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			cs, err := responseToCandlesticks(sr)
			if err == nil {
				t.Fatalf("Candlestick should have failed to convert but converted successfully to: %v", cs)
			}
		})
	}
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`{"code": "200000", "data":[["1566789720","10411.5","10401.9","10411.5","10396.3","29.11357276","302889.301529914"]]}`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "invalid url"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid url")
	}
}

func TestKlinesErrReadingResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"message":"error!"}`)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func TestKlinesNon200Response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to 500 response")
	}
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "200000", "data":[["1566789720","10411.5","INVALID","10411.5","10396.3","29.11357276","302889.301529914"]]}`)
	}))
	defer ts.Close()

	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid floats in json")
	}
}

func TestTimeframe1m(t *testing.T) {
	timeframes := map[time.Duration]string{
		1 * time.Minute:           "1min",
		3 * time.Minute:           "3min",
		5 * time.Minute:           "5min",
		15 * time.Minute:          "15min",
		30 * time.Minute:          "30min",
		1 * 60 * time.Minute:      "1hour",
		2 * 60 * time.Minute:      "2hour",
		4 * 60 * time.Minute:      "4hour",
		6 * 60 * time.Minute:      "6hour",
		8 * 60 * time.Minute:      "8hour",
		12 * 60 * time.Minute:     "12hour",
		1 * 60 * 24 * time.Minute: "1day",
		7 * 60 * 24 * time.Minute: "1week",
	}

	for candlestickInterval, timeframe := range timeframes {
		t.Run(timeframe, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, timeframe, strings.Split(r.URL.Path, ":")[1])
			}))
			defer ts.Close()

			b := NewKucoin()
			b.requester.Strategy = common.RetryStrategy{Attempts: 1}
			b.apiURL = ts.URL + "/"

			b.RequestCandlesticks(msBTCUSDT, tp("2019-08-02T19:41:00+00:00"), candlestickInterval)
		})
	}
}

func TestUnsupportedCandlestickInterval(t *testing.T) {
	b := NewKucoin()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "just so we don't actually call Kucoin"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2019-08-02T19:41:00+00:00"), 160*time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestPatience(t *testing.T) {
	require.Equal(t, 0*time.Minute, NewKucoin().Patience())
}

func TestName(t *testing.T) {
	require.Equal(t, "KUCOIN", NewKucoin().Name())
}

func tp(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

var (
	msBTCUSDT = common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
)
