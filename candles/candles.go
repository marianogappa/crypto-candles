// Package candles implements a Market, from which Iterators can be created.
//
// The Market guarantees that no two requests to the same exchange happen concurrently, and owns the cache, so you
// should only construct a Market once.
//
// An Iterator iterates over candlesticks of a given candlestick interval (e.g. 1 hour) starting at a specified time
// (e.g. 2022-01-02T03:04:05Z), for a given crypto market pair (e.g. BTC/USDT) provided by an exchange (e.g. BINANCE).
//
// Here's an example usage:
//
// ```
// package main
//
// import (
//
//	"fmt"
//	"log"
//	"time"
//	"encoding/json"
//
//	"github.com/marianogappa/crypto-candles/candles"
//	"github.com/marianogappa/crypto-candles/candles/common"
//
// )
//
//	func main() {
//		m := candles.NewMarket()
//		iter, err := m.Iterator(
//			common.MarketSource{Type: common.COIN, Provider: common.BINANCE, BaseAsset: "BTC", QuoteAsset: "USDT"},
//			time.Now().Add(-12*time.Hour), // Start time
//			1*time.Hour,                   // Candlestick interval
//		)
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		for i := 0; i < 10; i++ {
//			candlestick, err := iter.Next()
//			if err != nil {
//				log.Fatal(err)
//			}
//			bs, _ := json.Marshal(candlestick)
//			fmt.Printf("%+v\n", string(bs))
//		}
//	}
//
// ```
package candles

import (
	"fmt"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/binance"
	"github.com/marianogappa/crypto-candles/candles/binanceusdmfutures"
	"github.com/marianogappa/crypto-candles/candles/bitfinex"
	"github.com/marianogappa/crypto-candles/candles/bitstamp"
	"github.com/marianogappa/crypto-candles/candles/bybit"
	"github.com/marianogappa/crypto-candles/candles/cache"
	"github.com/marianogappa/crypto-candles/candles/coinbase"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/marianogappa/crypto-candles/candles/iterator"
	"github.com/marianogappa/crypto-candles/candles/kucoin"
)

// Market is the main struct of the candles package. From a Market, Iterators are created.
//
// The Market guarantees that no two requests to the same exchange happen concurrently, and owns the cache, so you
// should only construct a Market once.
type Market struct {
	cache     *cache.MemoryCache
	exchanges map[string]common.Exchange
	debug     bool
}

// NewMarket constructs a Market.
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
func (m Market) Iterator(marketSource common.MarketSource, startTime time.Time, candlestickInterval time.Duration) (iterator.Iterator, error) {
	if marketSource.Type != common.COIN {
		return nil, common.ErrInvalidMarketType
	}
	exchange := m.exchanges[strings.ToUpper(marketSource.Provider)]
	if exchange == nil {
		return nil, fmt.Errorf("%w: the '%v' provider is not supported", common.ErrUnsuportedCandlestickProvider, marketSource.Provider)
	}
	return iterator.NewIterator(marketSource, startTime, candlestickInterval, m.cache, exchange)
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
		common.COINBASE:           coinbase.NewCoinbase(),
		common.KUCOIN:             kucoin.NewKucoin(),
		common.BINANCEUSDMFUTURES: binanceusdmfutures.NewBinanceUSDMFutures(),
		common.BITSTAMP:           bitstamp.NewBitstamp(),
		common.BITFINEX:           bitfinex.NewBitfinex(),
		common.BYBIT:              bybit.NewBybit(),
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
