package bitstamp

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
	testResponse := `
	{
		"data": {
		  "pair": "BTC/USD",
		  "ohlc": [
			{
			  "high": "19122.76",
			  "timestamp": "1656868680",
			  "volume": "0.02005000",
			  "low": "19111.99",
			  "close": "19111.99",
			  "open": "19122.76"
			},
			{
			  "high": "19122.79",
			  "timestamp": "1656868740",
			  "volume": "0.91282000",
			  "low": "19113.03",
			  "close": "19113.03",
			  "open": "19122.79"
			},
			{
			  "high": "19122.30",
			  "timestamp": "1656868800",
			  "volume": "0.04470000",
			  "low": "19120.33",
			  "close": "19121.32",
			  "open": "19122.30"
			}
		  ]
		}
	  }
	`

	expected := []common.Candlestick{
		{
			HighestPrice: 19122.76,
			Timestamp:    1656868680,
			LowestPrice:  19111.99,
			ClosePrice:   19111.99,
			OpenPrice:    19122.76,
		},
		{
			HighestPrice: 19122.79,
			Timestamp:    1656868740,
			LowestPrice:  19113.03,
			ClosePrice:   19113.03,
			OpenPrice:    19122.79,
		},
		{
			HighestPrice: 19122.30,
			Timestamp:    1656868800,
			LowestPrice:  19120.33,
			ClosePrice:   19121.32,
			OpenPrice:    19122.30,
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testResponse))
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	actual, err := b.RequestCandlesticks(msBTCUSD, tInt("2022-07-03T17:18:00+00:00"), 1)
	require.Nil(t, err)
	require.Len(t, actual, 3)
	require.Equal(t, expected, actual)
}

func TestNoCandlesticks(t *testing.T) {
	testResponse := `
	{
		"data": {
		  "pair": "BTC/USD",
		  "ohlc": []
		}
	  }
	`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(testResponse))
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2022-07-03T17:18:00+00:00"), 1)
	require.Error(t, err, common.ErrOutOfCandlesticks)
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		// Invalid string high
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "INVALID", "timestamp": "1656868680", "volume": "0.02005000", "low": "19111.99", "close": "19111.99", "open": "19122.76"} ] } }`,
		// Invalid string low
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "19122.76", "timestamp": "1656868680", "volume": "0.02005000", "low": "INVALID", "close": "19111.99", "open": "19122.76"} ] } }`,
		// Invalid string close
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "19122.76", "timestamp": "1656868680", "volume": "0.02005000", "low": "19111.99", "close": "INVALID", "open": "19122.76"} ] } }`,
		// Invalid string open
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "19122.76", "timestamp": "1656868680", "volume": "0.02005000", "low": "19111.99", "close": "19111.99", "open": "INVALID"} ] } }`,
		// Invalid string timestamp
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "19122.76", "timestamp": "INVALID", "volume": "0.02005000", "low": "19111.99", "close": "19111.99", "open": "19122.76"} ] } }`,
		// non-integer timestamp
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "19122.76", "timestamp": "1656868680.12345", "volume": "0.02005000", "low": "19111.99", "close": "19111.99", "open": "19122.76"} ] } }`,
	}

	for i, ts := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			r := response{}
			require.Nil(t, json.Unmarshal([]byte(ts), &r))
			_, err := r.toCandlesticks()
			require.NotNil(t, err, "for %v was %v", string(ts), err)
		})
	}
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`{"data": {"pair": "BTC/USD", "ohlc": [{"high": "19122.76", "timestamp": "1656868680", "volume": "0.02005000", "low": "19111.99", "close": "19111.99", "open": "19122.76"} ] } }`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "invalid url"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid url")
	}
}

func TestKlinesErrReadingResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{
			"code": "validation-error",
			"errors": [
			  {
				"field": "limit",
				"message": "Must be between 1 and 1000.",
				"code": "validation-error"
			  }
			]
		  }`)
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func Test404(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func Test428(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func TestKlinesNon200Response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to 500 response")
	}
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"data": {"pair": "BTC/USD", "ohlc": [{"high": "INVALID", "timestamp": "1656868680", "volume": "0.02005000", "low": "19111.99", "close": "19111.99", "open": "19122.76"} ] } }`)
	}))
	defer ts.Close()

	b := NewBitstamp()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSD, tInt("2021-07-04T14:14:18+00:00"), 1)
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
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
)
