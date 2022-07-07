// Package cache implements an in-memory LRU cache layer between crypto exchanges and the CandlestickIterators.
//
// It solves this problem: if there are 1000 predictions about BTC/USDT that need the current value of the market
// pair right now, (1) it would take 1000*(network request against exchange) to get the same value 1000 times, and
// (2) the exchange would rate-limit the IP making the request.
//
// The package exposes a MemoryCache struct instantiated via NewMemoryCache.
//
// Usage:
//
// ```
//
// cache := cache.NewMemoryCache(
// 	map[time.Duration]int{
// 		time.Minute:    10000,
// 		1 * time.Hour:  1000,
// 		24 * time.Hour: 1000,
// 	},
// )
//
// metric := cache.Metric{Name: "COIN:BINANCE:BTC-USDT", CandlestickInterval: 1 * time.Minute}
//
// startISO8601 := common.ISO8601("2022-03-20T12:22:00Z")
// startTs, err := startISO8601.Seconds()
//
// err := cache.Put(metric, []common.Candlestick{{Timestamp: startTs, ClosePrice: 1234, ...}, {Timestamp: startTs+60, ClosePrice: 1234, ...}, ...})
//
// candlesticks, err := cache.Get(metric, startISO8601)
//
// ```
//
// Internally, it is composed of a cache per candlestick interval. Each cache has its defined interval's granularity.
package cache

import (
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/marianogappa/crypto-candles/candles/common"
)

// MemoryCache implements the in-memory LRU cache layer that this package exposes.
type MemoryCache struct {
	caches map[time.Duration]*lru.Cache

	CacheMisses   int
	CacheRequests int
}

var (
	// ErrCacheNotConfiguredForCandlestickInterval is returned when a Put operation tries to store candlesticks for
	// a candlestick interval not configured in the cache constructor.
	ErrCacheNotConfiguredForCandlestickInterval = errors.New("cache not configured for candlestick interval")

	// ErrTimestampMustBeMultipleOfCandlestickInterval is returned when a Put operation supplies candlesticks with
	// timestamps that are not multiples of the interval, or have gaps within the supplied slice.
	ErrTimestampMustBeMultipleOfCandlestickInterval = errors.New("timestamp must be multiple of candlestick interval")

	// ErrReceivedCandlestickWithZeroValue is returned when a Put operation supplies candlesticks with any of its 4
	// price values being the number 0. This is considered an error; sorry LUNA :shrugs:.
	ErrReceivedCandlestickWithZeroValue = errors.New("received candlestick with zero value on either of OHLC components")

	// ErrReceivedNonSubsequentCandlestick is returned when a Put operation supplies candlesticks with gaps within the
	// supplied slice.
	ErrReceivedNonSubsequentCandlestick = errors.New("received non-subsequent candlestick")

	// ErrInvalidISO8601 is returned when a Get operation supplies an invalid string for the start datetime.
	ErrInvalidISO8601 = errors.New("invalid ISO8601")

	// ErrCacheMiss is returned by a Get operation to signify that there are no available cache entries for the
	// requested metric and datetime.
	ErrCacheMiss = errors.New("cache miss")
)

// NewMemoryCache instantiates the in-memory LRU cache layer that this package exposes.
//
// The cacheSize parameter configure which candlestick intervals are supported, and how many cache entries are
// available per cache. Each cache entry spans the magic number of 500 subsequent candlesticks.
func NewMemoryCache(cacheSizes map[time.Duration]int) *MemoryCache {
	caches := map[time.Duration]*lru.Cache{}
	for candlestickInterval, size := range cacheSizes {
		if size <= 0 {
			size = 1
		}
		cache, _ := lru.New(size)
		caches[candlestickInterval] = cache
	}
	return &MemoryCache{caches: caches}
}

// Put pushes a slice of candlesticks from the given (metric, candlestick interval) into the cache. May evict older
// entries.
//
// * Fails with ErrReceivedCandlestickWithZeroValue if a candlestick with zero values is supplied.
//
// * Fails with ErrReceivedNonSubsequentCandlestick if supplied candlesticks are not sorted ascendingly.
//
// * Fails with ErrReceivedNonSubsequentCandlestick if supplied candlesticks are not exactly candlestickInterval apart.
//
// * Fails with ErrTimestampMustBeMultipleOfCandlestickInterval if candlesticks' timestamps are not multiples of the
//   candlestick interval.
//
// * Fails with ErrCacheNotConfiguredForCandlestickInterval if the cache was not configured to have candlesticks of the
//   candlestick interval of the supplied metric.
func (c *MemoryCache) Put(metric Metric, candlesticks []common.Candlestick) error {
	if _, ok := c.caches[metric.CandlestickInterval]; !ok {
		return ErrCacheNotConfiguredForCandlestickInterval
	}
	if len(candlesticks) == 0 {
		return nil
	}
	return c.put(metric, candlesticks)
}

// Get retrieves candlesticks for the given (metric, candlestick interval) starting at the supplied datetime. The
// supplied datetime will be normalized to the immediately next multiple datetime for the candlestick interval.
//
// It will retrieve all subsequent candlesticks starting _exactly_ at the normalized datetime, and up to the end of the
// cache entry. This means that it's possible that the cache still has subsequent candlesticks in a subsequent entry.
// If there's no entry for exactly that datetime, it will fail with ErrCacheMiss. It will stop at the first gap, rather
// than return gaps.
//
// * Fails with ErrInvalidISO8601 if the supplied datetime is invalid (note that the type wraps string, so it does
//   not prevent invalid strings to be supplied).
//
// * Fails with ErrCacheMiss if there are no values available in the cache. Client must handle this error, as it's
//   completely normal to have cache misses.
func (c *MemoryCache) Get(metric Metric, initialISO8601 common.ISO8601) ([]common.Candlestick, error) {
	if _, ok := c.caches[metric.CandlestickInterval]; !ok {
		return nil, ErrCacheNotConfiguredForCandlestickInterval
	}
	tm, err := initialISO8601.Time()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidISO8601, initialISO8601)
	}
	c.CacheRequests++

	startingTimestamp := common.NormalizeTimestamp(tm, metric.CandlestickInterval, "TODO_PROVIDER", false)

	return c.get(metric, startingTimestamp)
}

// Metric is the one namespace for candlestick sequences. It contains an arbitrary name (but used as the provider and
// market being cached) and the candlestick interval for the candlesticks.
type Metric struct {
	Name                string
	CandlestickInterval time.Duration
}
