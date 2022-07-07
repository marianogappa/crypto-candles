package candles

import (
	"fmt"
	"strings"
	"time"

	"github.com/marianogappa/crypto-candles/candles/binance"
	"github.com/marianogappa/crypto-candles/candles/binanceusdmfutures"
	"github.com/marianogappa/crypto-candles/candles/bitstamp"
	"github.com/marianogappa/crypto-candles/candles/cache"
	"github.com/marianogappa/crypto-candles/candles/coinbase"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/marianogappa/crypto-candles/candles/ftx"
	"github.com/marianogappa/crypto-candles/candles/iterator"
	"github.com/marianogappa/crypto-candles/candles/kucoin"
)

// IMarket only exists so that tests can use a test iterator.
type IMarket interface {
	GetIterator(marketSource common.MarketSource, startTime time.Time, startFromNext bool, intervalMinutes int) (common.Iterator, error)
}

// Market struct implements the crypto market.
type Market struct {
	cache                      *cache.MemoryCache
	timeNowFunc                func() time.Time
	debug                      bool
	exchanges                  map[string]common.Exchange
	supportedVariableProviders map[string]struct{}
}

// NewMarket constructs a market.
func NewMarket(cacheSizes map[time.Duration]int) Market {
	exchanges := map[string]common.Exchange{
		common.BINANCE:            binance.NewBinance(),
		common.FTX:                ftx.NewFTX(),
		common.COINBASE:           coinbase.NewCoinbase(),
		common.KUCOIN:             kucoin.NewKucoin(),
		common.BINANCEUSDMFUTURES: binanceusdmfutures.NewBinanceUSDMFutures(),
		common.BITSTAMP:           bitstamp.NewBitstamp(),
	}
	supportedVariableProviders := map[string]struct{}{}
	for exchangeName := range exchanges {
		supportedVariableProviders[strings.ToUpper(exchangeName)] = struct{}{}
	}
	cache := cache.NewMemoryCache(
		map[time.Duration]int{
			time.Minute:    10000,
			1 * time.Hour:  1000,
			24 * time.Hour: 1000,
		},
	)

	return Market{cache: cache, timeNowFunc: time.Now, exchanges: exchanges, supportedVariableProviders: supportedVariableProviders}
}

// SetDebug sets debug logging across all exchanges and the Market struct itself. Useful to know how many times an
// exchange is being requested.
func (m *Market) SetDebug(debug bool) {
	m.debug = debug
	for _, exchange := range m.exchanges {
		exchange.SetDebug(debug)
	}
}

// GetIterator returns a market iterator for a given operand at a given time and for a given candlestick interval.
func (m Market) GetIterator(marketSource common.MarketSource, startTime time.Time, startFromNext bool, intervalMinutes int) (common.Iterator, error) {
	switch marketSource.Type {
	case common.COIN:
		if _, ok := m.supportedVariableProviders[marketSource.Provider]; !ok {
			return nil, fmt.Errorf("the '%v' provider is not supported for %v", marketSource.Provider, marketSource.String())
		}
		exchange := m.exchanges[strings.ToLower(marketSource.Provider)]
		return iterator.NewIterator(marketSource, startTime, m.cache, exchange, m.timeNowFunc, startFromNext, intervalMinutes)
	default:
		return nil, fmt.Errorf("invalid marketSource type %v", marketSource.Type.String())
	}
}

// CalculateCacheHitRatio returns the hit ratio of the cache of the market. Used to see if the cache is useful.
func (m Market) CalculateCacheHitRatio() float64 {
	if m.cache.CacheRequests == 0 {
		return 0
	}
	return float64(m.cache.CacheMisses) / float64(m.cache.CacheRequests) * 100
}
