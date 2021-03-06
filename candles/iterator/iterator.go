// Package iterator provides a candlestick Iterator.
//
// You can use it like this:
//
// for {
//   candlestick, err := iter.Next()
//   if err != nil {
//     return err
//   }
//   ... use candlestick ...
// }
//
// It also implements the Scanner interface, so you can also use it like this:
//
// var candlestick common.Candlestick
// for iter.Scan(&candlestick) {
//   ... use candlestick ...
// }
// if iter.Error != nil {
//   return err
// }
package iterator

import (
	"fmt"
	"time"

	"github.com/marianogappa/crypto-candles/candles/cache"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

// Iterator is the interface for iterating over candlesticks. It implements the Iterator and Scanner interfaces.
type Iterator interface {
	Next() (common.Candlestick, error)

	Scan(*common.Candlestick) bool
	Error() error

	SetStartFromNext(bool)
	SetTimeNowFunc(func() time.Time)
}

// Impl is the struct for the market Iterator.
type Impl struct {
	marketSource        common.MarketSource
	candlestickCache    *cache.MemoryCache
	candlestickProvider common.CandlestickProvider
	candlestickInterval time.Duration
	candlesticks        []common.Candlestick
	metric              cache.Metric
	timeNowFunc         func() time.Time
	startFromNext       bool
	startTime           time.Time
	lastTs              int
	lastErr             error

	hasStarted bool // used to panic if SetStartFromNext() is called after Next() is called.
}

// NewIterator constructs a market Iterator.
func NewIterator(marketSource common.MarketSource, startTime time.Time, candlestickInterval time.Duration, candlestickCache *cache.MemoryCache, candlestickProvider common.CandlestickProvider) (*Impl, error) {
	iter := Impl{
		marketSource:        marketSource,
		candlestickCache:    candlestickCache,
		candlestickProvider: candlestickProvider,
		candlesticks:        []common.Candlestick{},
		candlestickInterval: candlestickInterval,
		metric:              cache.Metric{Name: marketSource.String(), CandlestickInterval: candlestickInterval},
		startTime:           startTime,
		timeNowFunc:         time.Now,
	}
	iter.lastTs = iter.calculateLastTs()

	return &iter, nil
}

func (it *Impl) calculateLastTs() int {
	startTs := common.NormalizeTimestamp(it.startTime, it.candlestickInterval, it.candlestickProvider.Name(), it.startFromNext)
	return startTs - int(it.candlestickInterval/time.Second)
}

// SetTimeNowFunc overrides time.Now() for testing purposes. Current time is used to decide if there are no new
// candlesticks available, because the requested time would be in the future or the recent present.
func (it *Impl) SetTimeNowFunc(f func() time.Time) {
	it.timeNowFunc = f
}

// SetStartFromNext moves the startTime to one candlestickInterval in the future. This is useful when the caller
// has already consumed the "startTime" candlestick and has saved this time in their state, so they want to start
// consuming from the next time.
func (it *Impl) SetStartFromNext(b bool) {
	if it.hasStarted {
		panic("SetStartFromNext() cannot be called after Next() is called")
	}
	it.startFromNext = b
	it.lastTs = it.calculateLastTs()
}

// Next is the "Next" iterator function, providing the next available Candlestick.
//
// It can fail for many reasons because it depends on requesting to an exchange, which means it could fail if the
// Internet cable got mauled by a cat.
//
// Some common failure reasons:
//
// - ErrNoNewTicksYet: timestamp is already in the present.
// - ErrExchangeReturnedNoTicks: exchange got the request and returned no results.
func (it *Impl) Next() (common.Candlestick, error) {
	it.hasStarted = true

	// If the candlesticks buffer is empty, try to get candlesticks from the cache.
	if len(it.candlesticks) == 0 && it.candlestickCache != nil {
		ticks, err := it.candlestickCache.Get(it.metric, it.nextISO8601())
		if err == nil {
			it.candlesticks = ticks
		}
	}

	// If the ticks buffer isn't empty (cache hit), use it.
	if len(it.candlesticks) > 0 {
		candlestick := it.candlesticks[0]
		it.candlesticks = it.candlesticks[1:]
		it.lastTs = candlestick.Timestamp
		return candlestick, nil
	}

	// If we reach here, before asking the exchange, let's see if it's too early to have new values.
	if it.nextTime().After(it.timeNowFunc().Add(-it.candlestickProvider.Patience() - it.candlestickInterval)) {
		return common.Candlestick{}, common.ErrNoNewTicksYet
	}

	// If we reach here, the buffer was empty and the cache was empty too. Last chance: try the exchange.
	candlesticks, err := it.candlestickProvider.RequestCandlesticks(it.marketSource, it.nextTime(), it.candlestickInterval)
	if err != nil {
		return common.Candlestick{}, err
	}

	// If the exchange returned early candlesticks, prune them.
	candlesticks = it.pruneOlderCandlesticks(candlesticks)
	if len(candlesticks) == 0 {
		return common.Candlestick{}, common.ErrExchangeReturnedNoTicks
	}

	// The first retrieved candlestick from the exchange must be exactly the required one.
	nextTs := it.nextTs()
	if candlesticks[0].Timestamp != nextTs {
		expected := time.Unix(int64(nextTs), 0).Format(time.RFC3339)
		actual := time.Unix(int64(candlesticks[0].Timestamp), 0).Format(time.RFC3339)
		return common.Candlestick{}, fmt.Errorf("%w: expected %v but got %v", common.ErrExchangeReturnedOutOfSyncTick, expected, actual)
	}

	// Put in the cache for future uses.
	if it.candlestickCache != nil {
		if err := it.candlestickCache.Put(it.metric, candlesticks); err != nil && err != cache.ErrCacheNotConfiguredForCandlestickInterval {
			log.Info().Msgf("IteratorImpl.Next: ignoring error putting into cache: %v\n", err)
		}
	}

	// Also put in the buffer, except for the first candlestick.
	candlestick := candlesticks[0]
	it.candlesticks = candlesticks[1:]
	it.lastTs = candlestick.Timestamp

	// Return the first candlestick from exchange request.
	return candlestick, nil
}

// Scan is the Scanner interface implementation. Returns true if the scanning happened without errors. If it returns
// false, the error is available on iter.Error().
func (it *Impl) Scan(candlestick *common.Candlestick) bool {
	cs, err := it.Next()
	it.lastErr = err
	*candlestick = cs
	return err == nil
}

// Error returns the error of the last Scan operation, or nil if it was successful.
func (it *Impl) Error() error {
	return it.lastErr
}

func (it *Impl) nextISO8601() common.ISO8601 {
	return common.ISO8601(it.nextTime().Format(time.RFC3339))
}

func (it *Impl) nextTime() time.Time {
	return time.Unix(int64(it.nextTs()), 0)
}

func (it *Impl) nextTs() int {
	return it.lastTs + int(it.candlestickInterval/time.Second)
}

func (it *Impl) pruneOlderCandlesticks(candlesticks []common.Candlestick) []common.Candlestick {
	nextTs := it.nextTs()
	for _, tick := range candlesticks {
		if tick.Timestamp < nextTs {
			candlesticks = candlesticks[1:]
		}
	}
	return candlesticks
}
