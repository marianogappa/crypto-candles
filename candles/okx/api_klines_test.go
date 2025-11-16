package okx

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/stretchr/testify/require"
)

var (
	msBTCUSD = common.MarketSource{Type: common.COIN, Provider: common.OKX, BaseAsset: "BTC", QuoteAsset: "USD"}
)

func f(v float64) common.JSONFloat64 {
	return common.JSONFloat64(v)
}

func TestHappyToCandlesticks(t *testing.T) {
	// Real OKX API response format: [ts, o, h, l, c, confirm]
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "96535.7", "96536.6", "96321", "96374.2", "1"],
			["1763283600000", "96047.9", "96538.9", "95875.2", "96535.2", "1"],
			["1763280000000", "96021.2", "96135", "95797.7", "96049", "1"]
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v5/market/history-index-candles", r.URL.Path)
		require.Equal(t, "BTC-USD", r.URL.Query().Get("instId"))
		require.Equal(t, "1H", r.URL.Query().Get("bar"))
		require.Equal(t, "100", r.URL.Query().Get("limit"))
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.SetDebug(true)
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	expected := []common.Candlestick{
		{
			Timestamp:    1763280000, // Oldest first (reversed from API response)
			OpenPrice:    f(96021.2),
			ClosePrice:   f(96049),
			LowestPrice:  f(95797.7),
			HighestPrice: f(96135),
		},
		{
			Timestamp:    1763283600,
			OpenPrice:    f(96047.9),
			ClosePrice:   f(96535.2),
			LowestPrice:  f(95875.2),
			HighestPrice: f(96538.9),
		},
		{
			Timestamp:    1763287200, // Newest last (reversed from API response)
			OpenPrice:    f(96535.7),
			ClosePrice:   f(96374.2),
			LowestPrice:  f(96321),
			HighestPrice: f(96536.6),
		},
	}

	// Use startTime matching the first candlestick timestamp to avoid gap filling issues
	actual, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), len(expected), "Should have at least as many candlesticks as expected")
	// Find the expected candlesticks in the actual results (PatchCandlestickHoles may add extra ones)
	for _, exp := range expected {
		found := false
		for _, act := range actual {
			if act.Timestamp == exp.Timestamp {
				require.Equal(t, exp, act, "Candlestick with timestamp %d should match", exp.Timestamp)
				found = true
				break
			}
		}
		require.True(t, found, "Should find candlestick with timestamp %d", exp.Timestamp)
	}
}

func TestOutOfCandlesticks(t *testing.T) {
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": []
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestErrUnsupportedCandlestickInterval(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "0", "msg": "", "data": []}`)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), 160*time.Minute)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestErrTooManyRequests(t *testing.T) {
	testResponse := `{
		"code": "50903",
		"msg": "Too many requests"
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
	require.False(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrInvalidMarketPair(t *testing.T) {
	testResponse := `{
		"code": "51000",
		"msg": "Invalid instrument"
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
	require.True(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrInvalidMarketPairWithInvalidMsg(t *testing.T) {
	testResponse := `{
		"code": "51000",
		"msg": "Invalid instId"
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestErrInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `not json`)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidJSONResponse)
	require.False(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrBrokenBodyResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusOK)
		// Write less than Content-Length to simulate broken body
		w.Write([]byte("short"))
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrBrokenBodyResponse)
	require.False(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrInvalidCandlestickFormat(t *testing.T) {
	// Response with invalid candlestick format (less than 5 elements)
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "96535.7", "96536.6"]
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has len < 5")
}

func TestErrInvalidTimestampFormat(t *testing.T) {
	// Response with invalid timestamp (not a valid number string)
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["invalid", "96535.7", "96536.6", "96321", "96374.2", "1"]
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid timestamp")
}

func TestErrInvalidPriceFormat(t *testing.T) {
	// Response with invalid price format (not a valid float string)
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "invalid", "96536.6", "96321", "96374.2", "1"]
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "had open =")
}

func TestAllSupportedIntervals(t *testing.T) {
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "96535.7", "96536.6", "96321", "96374.2", "1"]
		]
	}`

	supportedIntervals := []struct {
		name     string
		interval time.Duration
		expected string
	}{
		{"1m", 1 * time.Minute, "1m"},
		{"3m", 3 * time.Minute, "3m"},
		{"5m", 5 * time.Minute, "5m"},
		{"15m", 15 * time.Minute, "15m"},
		{"30m", 30 * time.Minute, "30m"},
		{"1H", 1 * time.Hour, "1H"},
		{"2H", 2 * time.Hour, "2H"},
		{"4H", 4 * time.Hour, "4H"},
		{"6H", 6 * time.Hour, "6H"},
		{"12H", 12 * time.Hour, "12H"},
		{"1D", 24 * time.Hour, "1D"},
		{"1W", 7 * 24 * time.Hour, "1W"},
		{"1M", 30 * 24 * time.Hour, "1M"},
	}

	for _, tc := range supportedIntervals {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, tc.expected, r.URL.Query().Get("bar"))
				fmt.Fprintln(w, testResponse)
			}))
			defer ts.Close()

			o := NewOKX()
			o.requester.Strategy = common.RetryStrategy{Attempts: 1}
			o.apiURL = ts.URL + "/api/v5/"

			startTime := time.Now().Add(-48 * time.Hour)
			_, err := o.RequestCandlesticks(msBTCUSD, startTime, tc.interval)
			require.Nil(t, err, "Interval %s should be supported", tc.name)
		})
	}
}

func TestSymbolFormat(t *testing.T) {
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "96535.7", "96536.6", "96321", "96374.2", "1"]
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// OKX uses BTC-USD format (uppercase with hyphen) for index candlesticks
		require.Equal(t, "BTC-USD", r.URL.Query().Get("instId"))
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Nil(t, err)
}

func TestRequestParameters(t *testing.T) {
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "96535.7", "96536.6", "96321", "96374.2", "1"]
		]
	}`

	startTime := time.Unix(1763280000, 0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "BTC-USD", r.URL.Query().Get("instId"))
		require.Equal(t, "1H", r.URL.Query().Get("bar"))
		require.Equal(t, "100", r.URL.Query().Get("limit"))
		// OKX uses 'before' parameter (timestamp in milliseconds)
		beforeParam := r.URL.Query().Get("before")
		require.NotEmpty(t, beforeParam, "before parameter should be set")
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, startTime, time.Hour)
	require.Nil(t, err)
}

func TestErrorResponseWithNonZeroCode(t *testing.T) {
	// Test error response in successfulResponse structure
	testResponse := `{
		"code": "51000",
		"msg": "Invalid instrument",
		"data": []
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	_, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
	require.True(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestCandlesticksReversed(t *testing.T) {
	// OKX returns candlesticks in descending order (newest first)
	// This test verifies they are reversed to ascending order (oldest first)
	testResponse := `{
		"code": "0",
		"msg": "",
		"data": [
			["1763287200000", "96535.7", "96536.6", "96321", "96374.2", "1"],
			["1763283600000", "96047.9", "96538.9", "95875.2", "96535.2", "1"],
			["1763280000000", "96021.2", "96135", "95797.7", "96049", "1"]
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	o := NewOKX()
	o.requester.Strategy = common.RetryStrategy{Attempts: 1}
	o.apiURL = ts.URL + "/api/v5/"

	actual, err := o.RequestCandlesticks(msBTCUSD, time.Unix(1763280000, 0), time.Hour)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), 3)

	// Verify chronological order (oldest to newest)
	for i := 1; i < len(actual); i++ {
		require.Greater(t, actual[i].Timestamp, actual[i-1].Timestamp,
			"Candlesticks should be in chronological order (oldest first)")
	}

	// Verify first candlestick is the oldest
	require.Equal(t, 1763280000, actual[0].Timestamp, "First candlestick should be the oldest")
	require.Equal(t, f(96021.2), actual[0].OpenPrice, "First candlestick should match oldest data")
}
