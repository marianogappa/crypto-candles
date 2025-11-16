package mexc

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
	msBTCUSDT = common.MarketSource{Type: common.COIN, Provider: common.MEXC, BaseAsset: "BTC", QuoteAsset: "USDT"}
)

func f(v float64) common.JSONFloat64 {
	return common.JSONFloat64(v)
}

func TestHappyToCandlesticks(t *testing.T) {
	// Real MEXC API response format: [openTime, open, high, low, close, volume, closeTime, quoteAssetVolume]
	testResponse := `[
		[1763078400000, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"],
		[1763164800000, "94559.3", "96822.84", "94559.3", "95593.29", "7751.40897033", 1763251200000, "743296950.96"],
		[1763251200000, "95593.29", "96634.34", "94849.88", "95729.1", "2779.12549036", 1763337600000, "266399579.93"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v3/klines", r.URL.Path)
		require.Equal(t, "BTCUSDT", r.URL.Query().Get("symbol"))
		require.Equal(t, "1d", r.URL.Query().Get("interval"))
		require.Equal(t, "1000", r.URL.Query().Get("limit"))
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.SetDebug(true)
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	expected := []common.Candlestick{
		{
			Timestamp:    1763078400, // 2025-11-12 00:00:00 UTC
			OpenPrice:    f(99709.8),
			ClosePrice:   f(94559.3),
			LowestPrice:  f(94006.34),
			HighestPrice: f(99848.78),
		},
		{
			Timestamp:    1763164800, // 2025-11-13 00:00:00 UTC
			OpenPrice:    f(94559.3),
			ClosePrice:   f(95593.29),
			LowestPrice:  f(94559.3),
			HighestPrice: f(96822.84),
		},
		{
			Timestamp:    1763251200, // 2025-11-14 00:00:00 UTC
			OpenPrice:    f(95593.29),
			ClosePrice:   f(95729.1),
			LowestPrice:  f(94849.88),
			HighestPrice: f(96634.34),
		},
	}

	// Use startTime matching the first candlestick timestamp to avoid gap filling issues
	actual, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
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
	// MEXC API can return numeric timestamps (though typically strings)
	testResponse := `[
		[1763078400000, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	expected := common.Candlestick{
		Timestamp:    1763078400,
		OpenPrice:    f(99709.8),
		ClosePrice:   f(94559.3),
		LowestPrice:  f(94006.34),
		HighestPrice: f(99848.78),
	}

	actual, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), 1)
	// Find the candlestick with the expected timestamp
	found := false
	for _, cs := range actual {
		if cs.Timestamp == 1763078400 {
			require.Equal(t, expected, cs, "Candlestick should match")
			found = true
			break
		}
	}
	require.True(t, found, "Should find candlestick with timestamp 1763078400")
}

func TestOutOfCandlesticks(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestErrUnsupportedCandlestickInterval(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 160*time.Minute)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestErrTooManyRequests(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintln(w, `{"code": 429, "msg": "Too many requests"}`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
}

func TestErrInvalidMarketPair(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": 400, "msg": "Invalid symbol"}`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestErrInvalidMarketPairWithInvalidLabel(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": 400, "msg": "Invalid trading pair"}`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestErrInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": 0, "invalid": json}`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidJSONResponse)
}

func TestErrBrokenBodyResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.Write([]byte("incomplete"))
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrBrokenBodyResponse)
}

func TestErrInvalidCandlestickFormat(t *testing.T) {
	// Response with missing required fields
	testResponse := `[
		[1763078400000, "99709.8"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "len < 5")
}

func TestErrInvalidTimestampFormat(t *testing.T) {
	testResponse := `[
		[null, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid timestamp")
}

func TestErrInvalidPriceFormat(t *testing.T) {
	testResponse := `[
		[1763078400000, "invalid", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid")
}

func TestErrNonStringPrice(t *testing.T) {
	// MEXC requires prices to be strings, not numbers
	testResponse := `[
		[1763078400000, 99709.8, "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-string")
}

func TestAllSupportedIntervals(t *testing.T) {
	testResponse := `[
		[1763078400000, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	supportedIntervals := []struct {
		name     string
		interval time.Duration
		expected string
	}{
		{"1m", 1 * time.Minute, "1m"},
		{"5m", 5 * time.Minute, "5m"},
		{"15m", 15 * time.Minute, "15m"},
		{"30m", 30 * time.Minute, "30m"},
		{"1h", 1 * time.Hour, "1h"},
		{"4h", 4 * time.Hour, "4h"},
		{"1d", 24 * time.Hour, "1d"},
		{"1w", 7 * 24 * time.Hour, "1w"},
		{"1M", 30 * 24 * time.Hour, "1M"},
	}

	for _, tc := range supportedIntervals {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, tc.expected, r.URL.Query().Get("interval"))
				fmt.Fprintln(w, testResponse)
			}))
			defer ts.Close()

			m := NewMEXC()
			m.requester.Strategy = common.RetryStrategy{Attempts: 1}
			m.apiURL = ts.URL + "/api/v3/"

			// Use a recent startTime
			startTime := time.Now().Add(-5000 * tc.interval)
			_, err := m.RequestCandlesticks(msBTCUSDT, startTime, tc.interval)
			require.Nil(t, err, "Interval %s should be supported", tc.name)
		})
	}
}

func TestSymbolFormat(t *testing.T) {
	testResponse := `[
		[1763078400000, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "BTCUSDT", r.URL.Query().Get("symbol"), "Symbol should be uppercase and concatenated")
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Nil(t, err)
}

func TestRequestParameters(t *testing.T) {
	testResponse := `[
		[1763078400000, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	startTime := time.Unix(1763078400, 0)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "BTCUSDT", r.URL.Query().Get("symbol"))
		require.Equal(t, "1d", r.URL.Query().Get("interval"))
		require.Equal(t, "1000", r.URL.Query().Get("limit"))
		require.Equal(t, fmt.Sprintf("%d", startTime.Unix()*1000), r.URL.Query().Get("startTime"), "startTime should be in milliseconds")
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, startTime, 24*time.Hour)
	require.Nil(t, err)
}

func TestTimestampConversion(t *testing.T) {
	// Test that milliseconds are correctly converted to seconds
	testResponse := `[
		[1763078400000, "99709.8", "99848.78", "94006.34", "94559.3", "18456.55186359", 1763164800000, "1782019927.26"]
	]`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	actual, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), 1)
	// Verify timestamp is in seconds (not milliseconds)
	require.Equal(t, 1763078400, actual[0].Timestamp, "Timestamp should be converted from milliseconds to seconds")
}

func TestErrorResponseWithCode(t *testing.T) {
	// Test error response with non-zero code
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": 500, "msg": "Internal server error"}`)
	}))
	defer ts.Close()

	m := NewMEXC()
	m.requester.Strategy = common.RetryStrategy{Attempts: 1}
	m.apiURL = ts.URL + "/api/v3/"

	_, err := m.RequestCandlesticks(msBTCUSDT, time.Unix(1763078400, 0), 24*time.Hour)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Internal server error")
}
