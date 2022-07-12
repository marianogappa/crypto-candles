package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRequestRetrierWorksFirstTime(t *testing.T) {
	var (
		candlestick1       = Candlestick{Timestamp: 1, OpenPrice: 2, ClosePrice: 3, LowestPrice: 4, HighestPrice: 5}
		candlestick2       = Candlestick{Timestamp: 6, OpenPrice: 7, ClosePrice: 8, LowestPrice: 9, HighestPrice: 10}
		sampleCandlesticks = []Candlestick{candlestick1, candlestick2}
		fn, callCount      = testFn([]response{{candlesticks: sampleCandlesticks, err: nil}})
		strategy           = RetryStrategy{Attempts: 3, FirstSleepTime: 1 * time.Millisecond}
		requester          = NewRequesterWithRetry(fn, strategy, pBool(true))
	)

	candlesticks, err := requester.Request("BTC", "USDT", time.Now(), time.Minute)

	require.Equal(t, sampleCandlesticks, candlesticks)
	require.Equal(t, nil, err)
	require.Equal(t, 1, *callCount)
}

func TestRequestRetrierWorksSecondTime(t *testing.T) {
	var (
		candlestick1       = Candlestick{Timestamp: 1, OpenPrice: 2, ClosePrice: 3, LowestPrice: 4, HighestPrice: 5}
		candlestick2       = Candlestick{Timestamp: 6, OpenPrice: 7, ClosePrice: 8, LowestPrice: 9, HighestPrice: 10}
		sampleCandlesticks = []Candlestick{candlestick1, candlestick2}
		call1              = response{candlesticks: nil, err: CandleReqError{IsNotRetryable: false, Err: ErrRateLimit}}
		call2              = response{candlesticks: sampleCandlesticks, err: nil}
		fn, callCount      = testFn([]response{call1, call2})
		strategy           = RetryStrategy{FirstSleepTime: 1 * time.Millisecond, SleepTimeMultiplier: 1}
		requester          = NewRequesterWithRetry(fn, strategy, pBool(true))
	)

	candlesticks, err := requester.Request("BTC", "USDT", time.Now(), time.Minute)

	require.Equal(t, sampleCandlesticks, candlesticks)
	require.Equal(t, nil, err)
	require.Equal(t, 2, *callCount)
}

func TestRequestRetrierDoesNotRetryBecauseUnretryable(t *testing.T) {
	var (
		errInvalidMarketPair = CandleReqError{IsNotRetryable: true, Err: ErrInvalidMarketPair}
		call1                = response{candlesticks: nil, err: errInvalidMarketPair}
		fn, callCount        = testFn([]response{call1})
		strategy             = RetryStrategy{FirstSleepTime: 1 * time.Millisecond, SleepTimeMultiplier: 1}
		requester            = NewRequesterWithRetry(fn, strategy, pBool(true))
	)

	candlesticks, err := requester.Request("BTC", "USDT", time.Now(), time.Minute)

	require.Nil(t, candlesticks)
	require.Equal(t, errInvalidMarketPair, err)
	require.Equal(t, 1, *callCount)
}

func TestRequestRetrierWorksThirdTime(t *testing.T) {
	var (
		candlestick1       = Candlestick{Timestamp: 1, OpenPrice: 2, ClosePrice: 3, LowestPrice: 4, HighestPrice: 5}
		candlestick2       = Candlestick{Timestamp: 6, OpenPrice: 7, ClosePrice: 8, LowestPrice: 9, HighestPrice: 10}
		sampleCandlesticks = []Candlestick{candlestick1, candlestick2}
		call1              = response{candlesticks: nil, err: CandleReqError{IsNotRetryable: false, Err: ErrRateLimit, RetryAfter: 1 * time.Millisecond}}
		call2              = response{candlesticks: nil, err: CandleReqError{IsNotRetryable: false, Err: ErrRateLimit}}
		call3              = response{candlesticks: sampleCandlesticks, err: nil}
		fn, callCount      = testFn([]response{call1, call2, call3})
		strategy           = RetryStrategy{Attempts: 3, FirstSleepTime: 1 * time.Millisecond, SleepTimeMultiplier: 1}
		requester          = NewRequesterWithRetry(fn, strategy, pBool(true))
	)

	candlesticks, err := requester.Request("BTC", "USDT", time.Now(), time.Minute)

	require.Equal(t, sampleCandlesticks, candlesticks)
	require.Equal(t, nil, err)
	require.Equal(t, 3, *callCount)
}

func TestRequestRetrierGivesUpAtThirdAttempt(t *testing.T) {
	var (
		errRateLimit  = CandleReqError{IsNotRetryable: false, Err: ErrRateLimit}
		call1         = response{candlesticks: nil, err: errRateLimit}
		call2         = response{candlesticks: nil, err: errRateLimit}
		call3         = response{candlesticks: nil, err: errRateLimit}
		fn, callCount = testFn([]response{call1, call2, call3})
		strategy      = RetryStrategy{Attempts: 3, FirstSleepTime: 1 * time.Millisecond, SleepTimeMultiplier: 1}
		requester     = NewRequesterWithRetry(fn, strategy, pBool(true))
	)

	candlesticks, err := requester.Request("BTC", "USDT", time.Now(), time.Minute)

	require.Nil(t, candlesticks)
	require.Equal(t, errRateLimit, err)
	require.Equal(t, 3, *callCount)
}

func pBool(b bool) *bool { return &b }

type response struct {
	candlesticks []Candlestick
	err          error
}

func testFn(responses []response) (func(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]Candlestick, error), *int) {
	callCount := 0
	fn := func(baseAsset string, quoteAsset string, startTime time.Time, candlestickInterval time.Duration) ([]Candlestick, error) {
		res := responses[callCount%len(responses)]
		callCount++
		return res.candlesticks, res.err
	}
	return fn, &callCount
}
