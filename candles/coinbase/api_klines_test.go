package coinbase

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
	testCandlestick := `
	[
		[
			1642330740,
			42915.09,
			42993.82,
			42986.05,
			42940.33,
			14.98295725
		],
		[
			1642330680,
			42974.87,
			43011.69,
			43007.47,
			42983.91,
			9.55765529
		],
		[
			1642330620,
			43007.46,
			43037.04,
			43033.15,
			43007.73,
			1.0528287
		]
	]
	`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, testCandlestick)
	}))

	b := NewCoinbase()
	b.SetDebug(true)
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	actual, err := b.RequestCandlesticks(msBTCUSDT, tp("2022-01-16T10:57:00+00:00"), time.Minute)
	require.Nil(t, err)

	expected := []common.Candlestick{
		{
			Timestamp:    1642330620,
			LowestPrice:  f(43007.46),
			HighestPrice: f(43037.04),
			OpenPrice:    f(43033.15),
			ClosePrice:   f(43007.73),
		},
		{
			Timestamp:    1642330680,
			LowestPrice:  f(42974.87),
			HighestPrice: f(43011.69),
			OpenPrice:    f(43007.47),
			ClosePrice:   f(42983.91),
		},
		{
			Timestamp:    1642330740,
			LowestPrice:  f(42915.09),
			HighestPrice: f(42993.82),
			OpenPrice:    f(42986.05),
			ClosePrice:   f(42940.33),
		},
	}
	fmt.Printf("%+v\n", actual)

	require.Equal(t, expected, actual)
}

func TestOutOfCandlesticks(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `[]`)
	}))

	b := NewCoinbase()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"

	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2022-01-16T10:57:00+00:00"), time.Minute)
	require.Equal(t, err.(common.CandleReqError).Err, common.ErrOutOfCandlesticks)
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

func TestKlinesInvalidGranularity(t *testing.T) {
	b := NewCoinbase()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = "just so it doesn't actually call Coinbase"
	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), 160*time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrUnsupportedCandlestickInterval)
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
func TestKlinesErrorNotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"message": "NotFound"}`)
	}))
	defer ts.Close()

	b := NewCoinbase()
	b.requester.Strategy = common.RetryStrategy{Attempts: 1}
	b.apiURL = ts.URL + "/"
	_, err := b.RequestCandlesticks(msBTCUSDT, tp("2021-07-04T14:14:18+00:00"), time.Minute)
	require.ErrorIs(t, err.(common.CandleReqError).Err, common.ErrInvalidMarketPair)
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

func TestPatience(t *testing.T) {
	require.Equal(t, 2*time.Minute, NewCoinbase().Patience())
}

func TestName(t *testing.T) {
	require.Equal(t, "COINBASE", NewCoinbase().Name())
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
