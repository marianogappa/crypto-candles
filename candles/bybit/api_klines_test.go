package bybit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/stretchr/testify/require"
)

func TestHappyToCandlesticks(t *testing.T) {
	testResponse := `{
		"retCode": 0,
		"retMsg": "OK",
		"result": {
			"category": "spot",
			"list": [
				["1499040000000", "0.01634790", "0.80000000", "0.01575800", "0.01577100", "148976.11427815", "2434.19055334"]
			],
			"symbol": "BTCUSDT"
		}
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	b := NewBybit()
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
		fmt.Fprintln(w, `{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [], "symbol": "BTCUSDT"}}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestErrUnsupportedCandlestickInterval(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [], "symbol": "BTCUSDT"}}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), 160*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestErrTooManyRequests(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		fmt.Fprintln(w, `{"retCode": 10018, "retMsg": "Too many requests"}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2017-07-03T00:00:00+00:00"), 1*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		// candlestick %v has len != 7! Invalid syntax from Bybit
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "0.01634790"]], "symbol": "BTCUSDT"}}`,
		// candlestick %v has invalid timestamp! Invalid syntax from Bybit
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["INVALID", "0.01634790", "0.80000000", "0.01575800", "0.01577100", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`,
		// candlestick %v had open = %v! Invalid syntax from Bybit
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "INVALID", "0.80000000", "0.01575800", "0.01577100", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`,
		// candlestick %v had high = %v! Invalid syntax from Bybit
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "0.01634790", "INVALID", "0.01575800", "0.01577100", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`,
		// candlestick %v had low = %v! Invalid syntax from Bybit
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "0.01634790", "0.80000000", "INVALID", "0.01577100", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`,
		// candlestick %v had close = %v! Invalid syntax from Bybit
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "0.01634790", "0.80000000", "0.01575800", "INVALID", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`,
	}

	for i, ts := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			sr := successfulResponse{}
			err := json.Unmarshal([]byte(ts), &sr)
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
		`{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "0.01634790", "0.80000000", "0.01575800", "0.01577100", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewBybit()
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

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"retCode": 10001, "retMsg": "Invalid symbol"}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func TestKlinesErrorInvalidMarketPair(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"retCode": 10001, "retMsg": "Invalid symbol"}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestKlinesErrorInvalidMarketPair10002(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"retCode": 10002, "retMsg": "Invalid category"}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestKlinesErrorNotSupportedSymbols(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"retCode": 10001, "retMsg": "Not supported symbols"}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
	require.Equal(t, err.(common.CandleReqError).Code, retCodeNotSupportedSymbols)
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [["1499040000000", "INVALID", "0.80000000", "0.01575800", "0.01577100", "148976.11427815", "2434.19055334"]], "symbol": "BTCUSDT"}}`)
	}))
	defer ts.Close()

	b := NewBybit()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"
	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid floats in json")
	}
}

func TestTimeframeIntervals(t *testing.T) {
	timeframes := map[time.Duration]string{
		1 * time.Minute:            "1",
		3 * time.Minute:            "3",
		5 * time.Minute:            "5",
		15 * time.Minute:           "15",
		30 * time.Minute:           "30",
		1 * 60 * time.Minute:       "60",
		2 * 60 * time.Minute:       "120",
		4 * 60 * time.Minute:       "240",
		6 * 60 * time.Minute:       "360",
		12 * 60 * time.Minute:      "720",
		1 * 60 * 24 * time.Minute:  "D",
		7 * 60 * 24 * time.Minute:  "W",
		30 * 60 * 24 * time.Minute: "M",
	}

	for candlestickInterval, interval := range timeframes {
		t.Run(interval, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, interval, r.URL.Query().Get("interval"))
				fmt.Fprintln(w, `{"retCode": 0, "retMsg": "OK", "result": {"category": "spot", "list": [], "symbol": "BTCUSDT"}}`)
			}))
			defer ts.Close()

			b := NewBybit()
			b.requester.Strategy = common.RetryStrategy{Attempts: 1}
			b.apiURL = ts.URL + "/"

			b.RequestCandlesticks(msBTCUSDT, tp("2019-08-02T19:41:00+00:00"), candlestickInterval)
		})
	}
}

func TestPatience(t *testing.T) {
	require.Equal(t, 1*time.Minute, NewBybit().Patience())
}

func TestName(t *testing.T) {
	require.Equal(t, "BYBIT", NewBybit().Name())
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
		Provider:   common.BYBIT,
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
)
