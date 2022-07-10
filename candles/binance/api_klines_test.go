package binance

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
	testCandlestick := `[
		[
		1499040000000,
		"0.01634790",
		"0.80000000",
		"0.01575800",
		"0.01577100",
		"148976.11427815",
		1499644799999,
		"2434.19055334",
		308,
		"1756.87402397",
		"28.46694368",
		"17928899.62484339"
		]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))
	defer ts.Close()

	b := NewBinance()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	expected := common.Candlestick{
		Timestamp:    1499040000,
		OpenPrice:    f(0.01634790),
		ClosePrice:   f(0.01577100),
		LowestPrice:  f(0.01575800),
		HighestPrice: f(0.80000000),
	}

	actual, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), time.Minute)
	require.Nil(t, err)
	require.Len(t, actual, 1)
	require.Equal(t, actual[0], expected)
}

func TestOutOfCandlesticks(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestErrUnsupportedCandlestickInterval(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), 160*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestErrTooManyRequests(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Retry-After", "5")
		w.WriteHeader(429)
		fmt.Fprintln(w, `{"code":-1234,"msg":"Too many requests"}`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), 1*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		// candlestick %v has len != 12! Invalid syntax from Binance
		`[
			[
				1499040000000
			]
		]`,
		// candlestick %v has non-int open time! Invalid syntax from Binance
		`[
			[
				"1499040000000",
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string open! Invalid syntax from Binance
		`[
			[
				1499040000000,
				0.01634790,
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had open = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"INVALID",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string high! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				0.80000000,
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had high = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"INVALID",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string low! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				0.01575800,
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had low = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"INVALID",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string close! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				0.01577100,
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had close = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"INVALID",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string volume! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				148976.11427815,
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had volume = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"INVALID",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-int close time! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				"1499644799999",
				"2434.19055334",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string quote asset volume! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				2434.19055334,
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had quote asset volume = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"INVALID",
				308,
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-int number of trades! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				"308",
				"1756.87402397",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string taker base asset volume! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				1756.87402397,
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v had taker base asset volume = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"INVALID",
				"28.46694368",
				"17928899.62484339"
			]
		]`,
		// candlestick %v has non-string taker quote asset volume! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				28.46694368,
				"17928899.62484339"
			]
		]`,
		// candlestick %v had taker quote asset volume = %v! Invalid syntax from Binance
		`[
			[
				1499040000000,
				"0.01634790",
				"0.80000000",
				"0.01575800",
				"0.01577100",
				"148976.11427815",
				1499644799999,
				"2434.19055334",
				308,
				"1756.87402397",
				"INVALID",
				"17928899.62484339"
			]
		]`,
	}

	for i, ts := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			sr := successfulResponse{}
			err := json.Unmarshal([]byte(ts), &sr.ResponseCandlesticks)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			cs, err := sr.toCandlesticks()
			if err == nil {
				t.Fatalf("Candlestick should have failed to convert but converted successfully to: %v", cs)
			}
		})
	}
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`[
			[
			1499040000000,
			"0.01634790",
			"0.80000000",
			"0.01575800",
			"0.01577100",
			"148976.11427815",
			1499644799999,
			"2434.19055334",
			308,
			"1756.87402397",
			"28.46694368",
			"17928899.62484339"
			]
		]`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewBinance()
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

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code":-1100,"msg":"Illegal characters found in parameter 'symbol'; legal range is '^[A-Z0-9-_.]{1,20}$'."}`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func TestKlinesErrorInvalidMarketPair(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code":-1121,"msg":"Invalid symbol."}`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[
			[
			1499040000000,
			"invalid",
			"0.80000000",
			"0.01575800",
			"0.01577100",
			"148976.11427815",
			1499644799999,
			"2434.19055334",
			308,
			"1756.87402397",
			"28.46694368",
			"17928899.62484339"
			]
		]`)
	}))
	defer ts.Close()

	b := NewBinance()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"
	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid floats in json")
	}
}

func TestTimeframe1m(t *testing.T) {
	timeframes := map[time.Duration]string{
		1 * time.Minute:            "1m",
		3 * time.Minute:            "3m",
		5 * time.Minute:            "5m",
		15 * time.Minute:           "15m",
		30 * time.Minute:           "30m",
		1 * 60 * time.Minute:       "1h",
		2 * 60 * time.Minute:       "2h",
		4 * 60 * time.Minute:       "4h",
		6 * 60 * time.Minute:       "6h",
		8 * 60 * time.Minute:       "8h",
		12 * 60 * time.Minute:      "12h",
		1 * 60 * 24 * time.Minute:  "1d",
		3 * 60 * 24 * time.Minute:  "3d",
		7 * 60 * 24 * time.Minute:  "1w",
		30 * 60 * 24 * time.Minute: "1M",
	}

	for candlestickInterval, timeframe := range timeframes {
		t.Run(timeframe, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, timeframe, strings.Split(r.URL.Path, ":")[1])
			}))
			defer ts.Close()

			b := NewBinance()
			b.requester.Strategy = common.RetryStrategy{Attempts: 1}
			b.apiURL = ts.URL + "/"

			b.RequestCandlesticks(msBTCUSDT, tp("2019-08-02T19:41:00+00:00"), candlestickInterval)
		})
	}
}

func TestPatience(t *testing.T) {
	require.Equal(t, 0*time.Minute, NewBinance().Patience())
}

func TestName(t *testing.T) {
	require.Equal(t, "BINANCE", NewBinance().Name())
}

func f(fl float64) common.JSONFloat64 {
	return common.JSONFloat64(fl)
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
