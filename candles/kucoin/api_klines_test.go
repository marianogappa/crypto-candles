package kucoin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
)

func TestHappyToCandlesticks(t *testing.T) {
	testCandlestick := `[["1566789720","10411.5","10401.9","10411.5","10396.3","29.11357276","302889.301529914"]]`

	sr := [][]string{}
	err := json.Unmarshal([]byte(testCandlestick), &sr)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	cs, err := responseToCandlesticks(sr)
	if err != nil {
		t.Fatalf("Candlestick should have converted successfully but returned: %v", err)
	}
	if len(cs) != 1 {
		t.Fatalf("Should have converted 1 candlesticks but converted: %v", len(cs))
	}
	expected := common.Candlestick{
		Timestamp:    1566789720,
		OpenPrice:    f(10411.5),
		ClosePrice:   f(10401.9),
		LowestPrice:  f(10396.3),
		HighestPrice: f(10411.5),
	}
	if cs[0] != expected {
		t.Fatalf("Candlestick should have been %v but was %v", expected, cs[0])
	}
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

func f(fl float64) common.JSONFloat64 {
	return common.JSONFloat64(fl)
}

func tp(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func tInt(s string) int {
	return int(tp(s).Unix())
}

var (
	msBTCUSDT = common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
)
