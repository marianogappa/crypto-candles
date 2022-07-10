package candles

import (
	"fmt"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/binance"
	"github.com/marianogappa/crypto-candles/candles/binanceusdmfutures"
	"github.com/marianogappa/crypto-candles/candles/bitfinex"
	"github.com/marianogappa/crypto-candles/candles/bitstamp"
	"github.com/marianogappa/crypto-candles/candles/cache"
	"github.com/marianogappa/crypto-candles/candles/coinbase"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/marianogappa/crypto-candles/candles/ftx"
	"github.com/marianogappa/crypto-candles/candles/iterator"
	"github.com/marianogappa/crypto-candles/candles/kucoin"
)

// Market struct implements the crypto market.
type Market struct {
	cache     *cache.MemoryCache
	exchanges map[string]common.Exchange
	debug     bool
}

// NewMarket constructs a market.
func NewMarket(options ...func(*Market)) Market {
	m := Market{exchanges: buildExchanges()}

	for _, option := range options {
		option(&m)
	}
	if m.cache == nil {
		m.cache = buildDefaultCache()
	}

	return m
}

// WithCacheSizes configures the cache sizes for the market instance at construction time.
func WithCacheSizes(cacheSizes map[time.Duration]int) func(*Market) {
	return func(m *Market) {
		m.cache = cache.NewMemoryCache(cacheSizes)
	}
}

// Iterator returns a market iterator for a given operand at a given time and for a given candlestick interval.
func (m Market) Iterator(marketSource common.MarketSource, startTime time.Time, candlestickInterval time.Duration, options ...func(*iterator.Impl)) (iterator.Iterator, error) {
	if marketSource.Type != common.COIN {
		return nil, common.ErrInvalidMarketType
	}
	exchange := m.exchanges[strings.ToUpper(marketSource.Provider)]
	if exchange == nil {
		return nil, fmt.Errorf("%w: the '%v' provider is not supported", common.ErrUnsuportedCandlestickProvider, marketSource.Provider)
	}
	return iterator.NewIterator(marketSource, startTime, candlestickInterval, m.cache, exchange, options...)
}

// SetDebug sets debug logging across all exchanges and the Market struct itself. Useful to know how many times an
// exchange is being requested.
func (m *Market) SetDebug(debug bool) {
	m.debug = debug
	for _, exchange := range m.exchanges {
		exchange.SetDebug(debug)
	}
}

// CalculateCacheHitRatio returns the hit ratio of the cache of the market. Used to see if the cache is useful.
func (m Market) CalculateCacheHitRatio() float64 {
	if m.cache.CacheRequests == 0 {
		return 0
	}
	return float64(m.cache.CacheMisses) / float64(m.cache.CacheRequests) * 100
}

func buildExchanges() map[string]common.Exchange {
	return map[string]common.Exchange{
		common.BINANCE:            binance.NewBinance(),
		common.FTX:                ftx.NewFTX(),
		common.COINBASE:           coinbase.NewCoinbase(),
		common.KUCOIN:             kucoin.NewKucoin(),
		common.BINANCEUSDMFUTURES: binanceusdmfutures.NewBinanceUSDMFutures(),
		common.BITSTAMP:           bitstamp.NewBitstamp(),
		common.BITFINEX:           bitfinex.NewBitfinex(),
	}
}

func buildDefaultCache() *cache.MemoryCache {
	return cache.NewMemoryCache(
		map[time.Duration]int{
			time.Minute:    10000,
			1 * time.Hour:  1000,
			24 * time.Hour: 1000,
		},
	)
}
