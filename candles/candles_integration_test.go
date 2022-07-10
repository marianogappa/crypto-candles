package candles

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/marianogappa/crypto-candles/candles/iterator"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	testCases := []struct {
		name                 string
		marketSource         common.MarketSource
		startTime            time.Time
		startFromNext        bool
		candlestickInterval  time.Duration
		expectedCandlesticks []common.Candlestick
		expectedErrs         []error
	}{

		{
			name:                "Binance",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.BINANCE, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1657378800,
					OpenPrice:    21596.03,
					ClosePrice:   21546.9,
					LowestPrice:  21536.98,
					HighestPrice: 21650,
				},
				{
					Timestamp:    1657382400,
					OpenPrice:    21545.89,
					ClosePrice:   21693.15,
					LowestPrice:  21530.32,
					HighestPrice: 21718.34,
				},
				{
					Timestamp:    1657386000,
					OpenPrice:    21693.15,
					ClosePrice:   21880.69,
					LowestPrice:  21666.15,
					HighestPrice: 21980,
				},
			},
			expectedErrs: []error{nil, nil, nil},
		},
	}
	mkt := NewMarket(WithCacheSizes(map[time.Duration]int{}))
	mkt.SetDebug(false)
	mkt.CalculateCacheHitRatio()
	for _, ts := range testCases {
		t.Run(ts.name, func(t *testing.T) {
			it, err := mkt.Iterator(ts.marketSource, ts.startTime, ts.candlestickInterval, iterator.WithStartFromNext(ts.startFromNext))
			require.Nil(t, err)
			for i, expectedCandlestick := range ts.expectedCandlesticks {
				candlestick, err := it.Next()
				require.ErrorIs(t, err, ts.expectedErrs[i])
				require.Equal(t, expectedCandlestick, candlestick)
			}
		})
	}
	mkt.CalculateCacheHitRatio()
}

func TestInvalidMarketType(t *testing.T) {
	mkt := NewMarket(WithCacheSizes(map[time.Duration]int{}))
	_, err := mkt.Iterator(common.MarketSource{Type: common.UNSUPPORTED}, time.Now(), time.Minute)
	require.ErrorIs(t, err, common.ErrInvalidMarketType)
}

func TestUnsupportedCandlestickProvider(t *testing.T) {
	mkt := NewMarket(WithCacheSizes(map[time.Duration]int{}))
	_, err := mkt.Iterator(common.MarketSource{Type: common.COIN, Provider: "UNSUPPORTED"}, time.Now(), time.Minute)
	require.ErrorIs(t, err, common.ErrUnsuportedCandlestickProvider)
}

func tp(s string) time.Time {
	tm, _ := time.Parse(time.RFC3339, s)
	return tm.UTC()
}
