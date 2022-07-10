package ftx

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
	testCandlestick := `
	{
		"success": true,
		"result": [
		  {
			"startTime": "2020-04-06T23:00:00+00:00",
			"time": 1586214000000,
			"open": 7274,
			"high": 7281.5,
			"low": 7272,
			"close": 7281.5,
			"volume": 0
		  },
		  {
			"startTime": "2020-04-06T23:01:00+00:00",
			"time": 1586214060000,
			"open": 7281.5,
			"high": 7281.5,
			"low": 7277,
			"close": 7280,
			"volume": 0
		  },
		  {
			"startTime": "2020-04-06T23:02:00+00:00",
			"time": 1586214120000,
			"open": 7280,
			"high": 7280,
			"low": 7271.5,
			"close": 7274,
			"volume": 0
		  }
		]
	  }
	`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))
	defer ts.Close()

	b := NewFTX()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	actual, err := b.RequestCandlesticks(msBTCUSDT, tp("2020-04-06T23:00:00+00:00"), time.Minute)
	require.Nil(t, err)
	require.Len(t, actual, 3)
	expected := []common.Candlestick{
		{
			Timestamp:    1586214000,
			OpenPrice:    f(7274),
			HighestPrice: f(7281.5),
			LowestPrice:  f(7272),
			ClosePrice:   f(7281.5),
		},
		{
			Timestamp:    1586214060,
			OpenPrice:    f(7281.5),
			HighestPrice: f(7281.5),
			LowestPrice:  f(7277),
			ClosePrice:   f(7280),
		},
		{
			Timestamp:    1586214120,
			OpenPrice:    f(7280),
			HighestPrice: f(7280),
			LowestPrice:  f(7271.5),
			ClosePrice:   f(7274),
		},
	}
	require.Equal(t, expected, actual)
}

func TestOutOfCandlesticks(t *testing.T) {
	testCandlestick := `
	{
		"success": true,
		"result": []
	  }
	`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))
	defer ts.Close()

	b := NewFTX()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2020-04-06T23:00:00+00:00"), time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`{"success": true, "result": [{"startTime": "2020-04-06T23:00:00+00:00", "time": 1586214000000, "open": 7274, "high": 7281.5, "low": 7272, "close": 7281.5, "volume": 0 } ] }`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewFTX()
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

	b := NewFTX()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid response body")
	}
}

func TestKlinesErrorResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"success": false, "error": "Invalid parameter end_time"}`)
	}))
	defer ts.Close()

	b := NewFTX()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.NotNil(t, err)
}

func TestKlines404Response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	}))
	defer ts.Close()

	b := NewFTX()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.NotNil(t, err)
}

func TestKlinesInvalidJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `invalid json`)
	}))
	defer ts.Close()

	b := NewFTX()
	b.SetDebug(false)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.NotNil(t, err)
}

func TestName(t *testing.T) {
	require.Equal(t, "FTX", NewFTX().Name())
}
func TestPatience(t *testing.T) {
	require.Equal(t, 0*time.Second, NewFTX().Patience())
}
func TestInvalidCandlestickInterval(t *testing.T) {
	_, err := NewFTX().RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), 60*time.Hour)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
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
