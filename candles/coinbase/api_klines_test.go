package coinbase

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
	testCandlestick := `[[1626868560,31540.72,31584.3,31540.72,31576.13,0.08432516]]`

	sr := successResponse{}
	err := json.Unmarshal([]byte(testCandlestick), &sr)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	cs, err := coinbaseToCandlesticks(sr)
	if err != nil {
		t.Fatalf("Candlestick should have converted successfully but returned: %v", err)
	}
	if len(cs) != 1 {
		t.Fatalf("Should have converted 1 candlesticks but converted: %v", len(cs))
	}
	expected := common.Candlestick{
		Timestamp:    1626868560,
		OpenPrice:    f(31540.72),
		ClosePrice:   f(31576.13),
		LowestPrice:  f(31540.72),
		HighestPrice: f(31584.3),
	}
	if cs[0] != expected {
		t.Fatalf("Candlestick should have been %v but was %v", expected, cs[0])
	}
}

func TestUnhappyToCandlesticks(t *testing.T) {
	tests := []string{
		`[["1626868560",31540.72,31584.3,31540.72,31576.13,0.08432516]]`,
		`[[1626868560,"31540.72",31584.3,31540.72,31576.13,0.08432516]]`,
		`[[1626868560,31540.72,"31584.3",31540.72,31576.13,0.08432516]]`,
		`[[1626868560,31540.72,31584.3,"31540.72",31576.13,0.08432516]]`,
		`[[1626868560,31540.72,31584.3,31540.72,"31576.13",0.08432516]]`,
	}

	for i, ts := range tests {
		t.Run(fmt.Sprintf("Unhappy toCandlesticks %v", i), func(t *testing.T) {
			sr := successResponse{}
			err := json.Unmarshal([]byte(ts), &sr)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			cs, err := coinbaseToCandlesticks(sr)
			if err == nil {
				t.Fatalf("Candlestick should have failed to convert but converted successfully to: %v", cs)
			}
		})
	}
}

func TestKlinesInvalidUrl(t *testing.T) {
	i := 0
	replies := []string{
		`[[1626868560,31540.72,31584.3,31540.72,31576.13,0.08432516]]`,
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, replies[i%len(replies)])
		i++
	}))
	defer ts.Close()

	b := NewCoinbase()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "invalid url"
	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid url, but instead had %v", err)
	}
}

func TestKlinesErrReadingResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1")
	}))
	defer ts.Close()

	b := NewCoinbase()
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

	b := NewCoinbase()
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

	b := NewCoinbase()
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

	b := NewCoinbase()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	if err == nil {
		t.Fatalf("should have failed due to invalid json")
	}
}

func TestKlinesInvalidFloatsInJSONResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[["1626868560",31540.72,31584.3,31540.72,31576.13,0.08432516]]`)
	}))
	defer ts.Close()

	b := NewCoinbase()
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

var (
	msBTCUSDT = common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
)
