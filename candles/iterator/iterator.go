package iterator

import (
	"fmt"
	"time"

	"github.com/marianogappa/crypto-candles/candles/cache"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/rs/zerolog/log"
)

// Impl is the struct for the market Iterator.
type Impl struct {
	marketSource        common.MarketSource
	candlesticks        []common.Candlestick
	lastTs              int
	candlestickCache    *cache.MemoryCache
	candlestickProvider common.CandlestickProvider
	timeNowFunc         func() time.Time
	intervalMinutes     int
	metric              cache.Metric
}

// NewIterator constructs a market Iterator.
func NewIterator(marketSource common.MarketSource, startTime time.Time, candlestickCache *cache.MemoryCache, candlestickProvider common.CandlestickProvider, timeNowFunc func() time.Time, startFromNext bool, intervalMinutes int) (*Impl, error) {
	startTs := common.NormalizeTimestamp(startTime, time.Duration(intervalMinutes)*time.Minute, "TODO_PROVIDER", startFromNext)
	metric := cache.Metric{Name: marketSource.String(), CandlestickInterval: time.Duration(intervalMinutes) * time.Minute}

	return &Impl{
		marketSource:        marketSource,
		candlestickCache:    candlestickCache,
		candlestickProvider: candlestickProvider,
		candlesticks:        []common.Candlestick{},
		timeNowFunc:         timeNowFunc,
		lastTs:              startTs - intervalMinutes*60,
		intervalMinutes:     intervalMinutes,
		metric:              metric,
	}, nil
}

// NextTick is the "Next" iterator function, providing the next available Tick (as opposed to Candlestick).
//
// It can fail for many reasons because it depends on requesting to an exchange, which means it could fail if the
// Internet cable got mauled by a cat.
//
// Some common failure reasons:
//
// - ErrNoNewTicksYet: timestamp is already in the present.
// - ErrExchangeReturnedNoTicks: exchange got the request and returned no results.
func (t *Impl) NextTick() (common.Tick, error) {
	cs, err := t.NextCandlestick()
	if err != nil {
		return common.Tick{}, err
	}
	return common.Tick{Timestamp: cs.Timestamp, Value: cs.ClosePrice}, nil
}

// NextCandlestick is the "Next" iterator function, providing the next available Candlestick (as opposed to Tick).
//
// It can fail for many reasons because it depends on requesting to an exchange, which means it could fail if the
// Internet cable got mauled by a cat.
//
// Some common failure reasons:
//
// - ErrNoNewTicksYet: timestamp is already in the present.
// - ErrExchangeReturnedNoTicks: exchange got the request and returned no results.
func (t *Impl) NextCandlestick() (common.Candlestick, error) {
	// If the candlesticks buffer is empty, try to get candlesticks from the cache.
	if len(t.candlesticks) == 0 && t.candlestickCache != nil {
		ticks, err := t.candlestickCache.Get(t.metric, t.nextISO8601())
		if err == nil {
			t.candlesticks = ticks
		}
	}

	// If the ticks buffer isn't empty (cache hit), use it.
	if len(t.candlesticks) > 0 {
		candlestick := t.candlesticks[0]
		t.candlesticks = t.candlesticks[1:]
		t.lastTs = candlestick.Timestamp
		return candlestick, nil
	}

	// If we reach here, before asking the exchange, let's see if it's too early to have new values.
	if t.nextTime().After(t.timeNowFunc().Add(-t.candlestickProvider.GetPatience() - time.Duration(t.candlestickDurationSecs())*time.Second)) {
		return common.Candlestick{}, common.ErrNoNewTicksYet
	}

	// If we reach here, the buffer was empty and the cache was empty too. Last chance: try the exchange.
	candlesticks, err := t.candlestickProvider.RequestCandlesticks(t.marketSource, t.nextTs(), t.intervalMinutes)
	if err != nil {
		return common.Candlestick{}, err
	}

	// If the exchange returned early candlesticks, prune them.
	candlesticks = t.pruneOlderCandlesticks(candlesticks)
	if len(candlesticks) == 0 {
		return common.Candlestick{}, common.ErrExchangeReturnedNoTicks
	}

	// The first retrieved candlestick from the exchange must be exactly the required one.
	nextTs := t.nextTs()
	if candlesticks[0].Timestamp != nextTs {
		expected := time.Unix(int64(nextTs), 0).Format(time.RFC3339)
		actual := time.Unix(int64(candlesticks[0].Timestamp), 0).Format(time.RFC3339)
		return common.Candlestick{}, fmt.Errorf("%w: expected %v but got %v", common.ErrExchangeReturnedOutOfSyncTick, expected, actual)
	}

	// Put in the cache for future uses.
	if t.candlestickCache != nil {
		if err := t.candlestickCache.Put(t.metric, candlesticks); err != nil {
			log.Info().Msgf("IteratorImpl.Next: ignoring error putting into cache: %v\n", err)
		}
	}

	// Also put in the buffer, except for the first candlestick.
	candlestick := candlesticks[0]
	t.candlesticks = candlesticks[1:]
	t.lastTs = candlestick.Timestamp

	// Return the first candlestick from exchange request.
	return candlestick, nil
}

func (t *Impl) nextISO8601() common.ISO8601 {
	return common.ISO8601(t.nextTime().Format(time.RFC3339))
}

func (t *Impl) nextTime() time.Time {
	return time.Unix(int64(t.nextTs()), 0)
}

func (t *Impl) nextTs() int {
	return t.lastTs + t.candlestickDurationSecs()
}

func (t *Impl) candlestickDurationSecs() int {
	return t.intervalMinutes * 60
}

func (t *Impl) pruneOlderCandlesticks(candlesticks []common.Candlestick) []common.Candlestick {
	nextTs := t.nextTs()
	for _, tick := range candlesticks {
		if tick.Timestamp < nextTs {
			candlesticks = candlesticks[1:]
		}
	}
	return candlesticks
}
