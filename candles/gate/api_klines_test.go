package gate

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
	msBTCUSDT = common.MarketSource{Type: common.COIN, Provider: common.GATE, BaseAsset: "BTC", QuoteAsset: "USDT"}
)

func f(v float64) common.JSONFloat64 {
	return common.JSONFloat64(v)
}

func TestHappyToCandlesticks(t *testing.T) {
	// Real Gate API response format: [timestamp, volume_quote, close, high, low, open, volume_base, is_closed]
	testResponse := `[
		["1763118000", "71260782.42422470", "96165.1", "96942.2", "95707.1", "96758.9", "739.87607500", "true"],
		["1763121600", "92234436.30238250", "95347.4", "96165.2", "94548.7", "96165.1", "966.04426800", "true"],
		["1763125200", "96825476.44251080", "95240.9", "95700", "94575", "95347.4", "1018.82525800", "true"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v4/spot/candlesticks", r.URL.Path)
		require.Equal(t, "BTC_USDT", r.URL.Query().Get("currency_pair"))
		require.Equal(t, "1h", r.URL.Query().Get("interval"))
		require.Equal(t, "1000", r.URL.Query().Get("limit"))
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.SetDebug(true)
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	expected := []common.Candlestick{
		{
			Timestamp:    1763118000,
			OpenPrice:    f(96758.9),
			ClosePrice:   f(96165.1),
			LowestPrice:  f(95707.1),
			HighestPrice: f(96942.2),
		},
		{
			Timestamp:    1763121600,
			OpenPrice:    f(96165.1),
			ClosePrice:   f(95347.4),
			LowestPrice:  f(94548.7),
			HighestPrice: f(96165.2),
		},
		{
			Timestamp:    1763125200,
			OpenPrice:    f(95347.4),
			ClosePrice:   f(95240.9),
			LowestPrice:  f(94575),
			HighestPrice: f(95700),
		},
	}

	// Use startTime matching the first candlestick timestamp to avoid gap filling issues
	actual, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
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

func TestHappyToCandlesticksWithNumericTimestamp(t *testing.T) {
	// Gate API can also return numeric timestamps instead of strings
	testResponse := `[
		[1763118000, "71260782.42422470", "96165.1", "96942.2", "95707.1", "96758.9", "739.87607500", "true"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	actual, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), 1)
	// Find the candlestick with the expected timestamp
	found := false
	for _, cs := range actual {
		if cs.Timestamp == 1763118000 {
			found = true
			break
		}
	}
	require.True(t, found, "Should find candlestick with timestamp 1763118000")
}

func TestOutOfCandlesticks(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestErrUnsupportedCandlestickInterval(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), 160*time.Minute)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestErrTooManyRequests(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintln(w, `{"label": "TOO_MANY_REQUESTS", "message": "Too many requests"}`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
	require.False(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrDataTooFarBack(t *testing.T) {
	// Real Gate API error response when requesting data too far back
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, `{"label": "INVALID_PARAM_VALUE", "message": "Candlestick too long ago. Maximum 10000 points ago are allowed"}`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	// Use a time that would pass our guard but fail at the API (edge case)
	// This tests the API error handling, not the guard
	_, err := g.RequestCandlesticks(msBTCUSDT, time.Now().Add(-10000*time.Hour), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrDataTooFarBack)
	require.True(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrInvalidMarketPair(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, `{"label": "INVALID_PARAM_VALUE", "message": "Invalid currency_pair"}`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
	require.True(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrInvalidMarketPairWithInvalidLabel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, `{"label": "INVALID", "message": "Some error"}`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestErrInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintln(w, `not json`)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
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

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrBrokenBodyResponse)
	require.False(t, err.(common.CandleReqError).IsNotRetryable)
}

func TestErrInvalidCandlestickFormat(t *testing.T) {
	// Response with invalid candlestick format (less than 6 elements)
	testResponse := `[
		["1763118000", "71260782.42422470", "96165.1"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has len < 6")
}

func TestErrInvalidTimestampFormat(t *testing.T) {
	// Response with invalid timestamp (not string or number)
	testResponse := `[
		[null, "71260782.42422470", "96165.1", "96942.2", "95707.1", "96758.9", "739.87607500", "true"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid timestamp type")
}

func TestErrInvalidPriceFormat(t *testing.T) {
	// Response with invalid price format (not a string)
	testResponse := `[
		["1763118000", "71260782.42422470", null, "96942.2", "95707.1", "96758.9", "739.87607500", "true"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-string close")
}

func TestGuardMaxPointsBack(t *testing.T) {
	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}

	// Test that guard catches requests more than MaxPointsBack intervals back
	tooFarBack := time.Now().Add(-10001 * time.Hour)
	_, err := g.RequestCandlesticks(msBTCUSDT, tooFarBack, time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrDataTooFarBack)
	require.True(t, err.(common.CandleReqError).IsNotRetryable)
	require.Contains(t, err.Error(), "requested 10001 intervals back")
}

func TestGuardMaxPointsBackWithDifferentIntervals(t *testing.T) {
	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}

	testCases := []struct {
		name     string
		interval time.Duration
		back     time.Duration
	}{
		{"1 minute", time.Minute, -10001 * time.Minute},
		{"1 hour", time.Hour, -10001 * time.Hour},
		{"1 day", 24 * time.Hour, -10001 * 24 * time.Hour},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tooFarBack := time.Now().Add(tc.back)
			_, err := g.RequestCandlesticks(msBTCUSDT, tooFarBack, tc.interval)
			require.Error(t, err)
			require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrDataTooFarBack)
		})
	}
}

func TestAllSupportedIntervals(t *testing.T) {
	testResponse := `[
		["1763118000", "71260782.42422470", "96165.1", "96942.2", "95707.1", "96758.9", "739.87607500", "true"]
	]`

	supportedIntervals := []struct {
		name     string
		interval time.Duration
		expected string
	}{
		{"1s", 1 * time.Second, "1s"},
		{"10s", 10 * time.Second, "10s"},
		{"1m", 1 * time.Minute, "1m"},
		{"5m", 5 * time.Minute, "5m"},
		{"15m", 15 * time.Minute, "15m"},
		{"30m", 30 * time.Minute, "30m"},
		{"1h", 1 * time.Hour, "1h"},
		{"4h", 4 * time.Hour, "4h"},
		{"8h", 8 * time.Hour, "8h"},
		{"1d", 24 * time.Hour, "1d"},
		{"7d", 7 * 24 * time.Hour, "7d"},
		{"30d", 30 * 24 * time.Hour, "30d"},
	}

	for _, tc := range supportedIntervals {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, tc.expected, r.URL.Query().Get("interval"))
				fmt.Fprintln(w, testResponse)
			}))
			defer ts.Close()

			g := NewGate()
			g.requester.Strategy = common.RetryStrategy{Attempts: 1}
			g.apiURL = ts.URL + "/api/v4/"

			// Use a recent startTime that's within MaxPointsBack for all intervals
			// For 1s interval, we need startTime within 10000 seconds (~2.7 hours) from now
			startTime := time.Now().Add(-5000 * tc.interval) // Use 5000 intervals back to be safe
			_, err := g.RequestCandlesticks(msBTCUSDT, startTime, tc.interval)
			require.Nil(t, err, "Interval %s should be supported", tc.name)
		})
	}
}

func TestSymbolFormat(t *testing.T) {
	testResponse := `[
		["1763118000", "71260782.42422470", "96165.1", "96942.2", "95707.1", "96758.9", "739.87607500", "true"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Gate uses BTC_USDT format (uppercase with underscore)
		require.Equal(t, "BTC_USDT", r.URL.Query().Get("currency_pair"))
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, time.Unix(1763118000, 0), time.Hour)
	require.Nil(t, err)
}

func TestRequestParameters(t *testing.T) {
	testResponse := `[
		["1763118000", "71260782.42422470", "96165.1", "96942.2", "95707.1", "96758.9", "739.87607500", "true"]
	]`

	startTime := time.Unix(1763118000, 0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "BTC_USDT", r.URL.Query().Get("currency_pair"))
		require.Equal(t, "1h", r.URL.Query().Get("interval"))
		require.Equal(t, "1000", r.URL.Query().Get("limit"))
		require.Equal(t, fmt.Sprintf("%d", startTime.Unix()), r.URL.Query().Get("from"))
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	g := NewGate()
	g.requester.Strategy = common.RetryStrategy{Attempts: 1}
	g.apiURL = ts.URL + "/api/v4/"

	_, err := g.RequestCandlesticks(msBTCUSDT, startTime, time.Hour)
	require.Nil(t, err)
}
