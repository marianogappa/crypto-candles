package common

import (
	"math"
	"time"

	"github.com/rs/zerolog/log"
)

// RetryStrategy is a strategy for retrying Exchange requests, e.g. how many attempts to do, how much to sleep between
// retries, how much to increase sleep time across retries.
type RetryStrategy struct {
	Attempts            int
	FirstSleepTime      time.Duration
	SleepTimeMultiplier float64
}

// RequesterWithRetry runs an exchange's candlestick request, with a supplied retry strategy.
type RequesterWithRetry struct {
	fn       func(string, string, int, int) ([]Candlestick, error)
	Strategy RetryStrategy
	debug    *bool
}

// NewRequesterWithRetry constructs a RequesterWithRetry
func NewRequesterWithRetry(fn func(string, string, int, int) ([]Candlestick, error), strategy RetryStrategy, debug *bool) RequesterWithRetry {
	if strategy.Attempts == 0 {
		strategy.Attempts = 3
	}
	if strategy.FirstSleepTime == 0 {
		strategy.FirstSleepTime = 1 * time.Second
	}
	if strategy.SleepTimeMultiplier == 0.0 {
		strategy.SleepTimeMultiplier = 2.0
	}
	return RequesterWithRetry{fn, strategy, debug}
}

// Request runs an exchange's candlestick request, with a supplied retry strategy.
func (r RequesterWithRetry) Request(baseAsset string, quoteAsset string, startTimeTs int, intervalMinutes int) ([]Candlestick, error) {
	var (
		err          error
		candlesticks []Candlestick
		sleepTime    = r.Strategy.FirstSleepTime
		attempts     = r.Strategy.Attempts
	)
	for attempts > 0 {
		if candlesticks, err = r.fn(baseAsset, quoteAsset, startTimeTs, intervalMinutes); err == nil {
			return candlesticks, nil
		}
		candleReqErr := err.(CandleReqError)
		if candleReqErr.IsNotRetryable {
			break
		}
		if candleReqErr.RetryAfter > 0 {
			sleepTime = candleReqErr.RetryAfter
		}
		attempts--
		if attempts == 0 {
			break
		}
		if *r.debug {
			log.Info().Msgf("Request failed with error: %v, retrying (%v attempts left) candlestick request after sleeping for %v", candleReqErr.Err, attempts, sleepTime)
		}
		time.Sleep(sleepTime)
		sleepTime = time.Duration(int64(math.Round(float64(sleepTime) * r.Strategy.SleepTimeMultiplier)))
	}
	return nil, err
}
