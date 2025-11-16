package bitget

import (
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
		"code": "00000",
		"msg": "success",
		"requestTime": 1659074220000,
		"data": [
			{
				"ts": "1659074220000",
				"open": "24001.74",
				"high": "24004.04",
				"low": "23987.61",
				"close": "23987.61",
				"baseVol": "4.2725",
				"quoteVol": "102530.137364",
				"usdtVol": "102530.137364"
			}
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	b := NewBitget()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	expected := common.Candlestick{
		Timestamp:    1659074220,
		OpenPrice:    f(24001.74),
		ClosePrice:   f(23987.61),
		LowestPrice:  f(23987.61),
		HighestPrice: f(24004.04),
	}

	// Use a startTime close to the candlestick timestamp to avoid excessive gap filling
	actual, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:37:00+00:00"), time.Minute)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), 1, "Should have at least 1 candlestick")
	// Find the expected candlestick
	found := false
	for _, cs := range actual {
		if cs.Timestamp == expected.Timestamp {
			require.Equal(t, expected, cs)
			found = true
			break
		}
	}
	require.True(t, found, "Should find expected candlestick")
}

func TestOutOfCandlesticks(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": []}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestErrUnsupportedCandlestickInterval(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": []}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), 160*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
}

func TestErrTooManyRequests(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		fmt.Fprintln(w, `{"code": "42901", "msg": "Too many requests"}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), 1*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrRateLimit)
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		// candlestick %v has invalid timestamp! Invalid syntax from Bitget
		`{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "INVALID", "open": "24001.74", "high": "24004.04", "low": "23987.61", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`,
		// candlestick %v had open = %v! Invalid syntax from Bitget
		`{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "1659074220000", "open": "INVALID", "high": "24004.04", "low": "23987.61", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`,
		// candlestick %v had high = %v! Invalid syntax from Bitget
		`{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "1659074220000", "open": "24001.74", "high": "INVALID", "low": "23987.61", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`,
		// candlestick %v had low = %v! Invalid syntax from Bitget
		`{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "1659074220000", "open": "24001.74", "high": "24004.04", "low": "INVALID", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`,
		// candlestick %v had close = %v! Invalid syntax from Bitget
		`{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "1659074220000", "open": "24001.74", "high": "24004.04", "low": "23987.61", "close": "INVALID", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`,
	}

	for i, testResponse := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintln(w, testResponse)
			}))
			defer ts.Close()

			b := NewBitget()
			b.requester.Strategy = common.RetryStrategy{Attempts: 1}
			b.apiURL = ts.URL + "/"

			_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
			if err == nil {
				t.Fatalf("Candlestick should have failed to convert but converted successfully")
			}
		})
	}
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "1659074220000", "open": "24001.74", "high": "24004.04", "low": "23987.61", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "invalid url"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid url")
	}
}

func TestKlinesErrReadingResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "40001", "msg": "Invalid request"}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to error response")
	}
}

func TestKlinesErrorInvalidMarketPair(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "40001", "msg": "Invalid symbol"}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": [{"ts": "1659074220000", "open": "invalid", "high": "24004.04", "low": "23987.61", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}]}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"
	_, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid floats in json")
	}
}

func TestTimeframeIntervals(t *testing.T) {
	timeframes := map[time.Duration]string{
		1 * time.Minute:            "1min",
		5 * time.Minute:            "5min",
		15 * time.Minute:           "15min",
		30 * time.Minute:           "30min",
		1 * 60 * time.Minute:       "1h",
		4 * 60 * time.Minute:       "4h",
		6 * 60 * time.Minute:       "6h",
		12 * 60 * time.Minute:      "12h",
		1 * 60 * 24 * time.Minute:  "1day",
		3 * 60 * 24 * time.Minute:  "3day",
		7 * 60 * 24 * time.Minute:  "1week",
		30 * 60 * 24 * time.Minute: "1M",
	}

	for candlestickInterval, period := range timeframes {
		t.Run(period, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, period, r.URL.Query().Get("period"))
				fmt.Fprintln(w, `{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": []}`)
			}))
			defer ts.Close()

			b := NewBitget()
			b.requester.Strategy = common.RetryStrategy{Attempts: 1}
			b.apiURL = ts.URL + "/"

			b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), candlestickInterval)
		})
	}
}

func TestSymbolFormat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "BTCUSDT_SPBL", r.URL.Query().Get("symbol"))
		fmt.Fprintln(w, `{"code": "00000", "msg": "success", "requestTime": 1659074220000, "data": []}`)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:00:00+00:00"), time.Minute)
}

func TestCandlesticksReversed(t *testing.T) {
	// Bitget returns candlesticks in descending order (newest first), so we need to reverse them
	testResponse := `{
		"code": "00000",
		"msg": "success",
		"requestTime": 1659074220000,
		"data": [
			{"ts": "1659074280000", "open": "24002.74", "high": "24005.04", "low": "23988.61", "close": "23988.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"},
			{"ts": "1659074220000", "open": "24001.74", "high": "24004.04", "low": "23987.61", "close": "23987.61", "baseVol": "4.2725", "quoteVol": "102530.137364", "usdtVol": "102530.137364"}
		]
	}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testResponse)
	}))
	defer ts.Close()

	b := NewBitget()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	// Use a startTime close to the candlestick timestamps to avoid excessive gap filling
	actual, err := b.RequestCandlesticks(msBTCUSDT, tpBitget("2022-07-29T00:37:00+00:00"), time.Minute)
	require.Nil(t, err)
	require.GreaterOrEqual(t, len(actual), 2, "Should have at least 2 candlesticks")
	// Verify they are in chronological order (oldest first) after reversal
	// Find the two candlesticks we expect
	foundFirst := false
	foundSecond := false
	for _, cs := range actual {
		if cs.Timestamp == 1659074220 {
			foundFirst = true
			require.Equal(t, f(24001.74), cs.OpenPrice)
			require.Equal(t, f(23987.61), cs.ClosePrice)
		}
		if cs.Timestamp == 1659074280 {
			foundSecond = true
			require.Equal(t, f(24002.74), cs.OpenPrice)
			require.Equal(t, f(23988.61), cs.ClosePrice)
		}
	}
	require.True(t, foundFirst, "Should find first candlestick")
	require.True(t, foundSecond, "Should find second candlestick")
	// Verify chronological order
	for i := 1; i < len(actual); i++ {
		require.GreaterOrEqual(t, actual[i].Timestamp, actual[i-1].Timestamp, "Candlesticks should be in chronological order")
	}
}

func TestPatience(t *testing.T) {
	require.Equal(t, 1*time.Minute, NewBitget().Patience())
}

func TestName(t *testing.T) {
	require.Equal(t, "BITGET", NewBitget().Name())
}

func TestSuccessCodeConstant(t *testing.T) {
	// Test that the SuccessCode constant matches what we expect
	require.Equal(t, "00000", SuccessCode)
}

func f(fl float64) common.JSONFloat64 {
	return common.JSONFloat64(fl)
}

func tpBitget(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

var (
	msBTCUSDT = common.MarketSource{
		Type:       common.COIN,
		Provider:   common.BITGET,
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
)
