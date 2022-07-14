package bitfinex_test

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/common"
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
			name:                "Bitfinex",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.BITFINEX, BaseAsset: "BTC", QuoteAsset: "USD"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1657378800,
					OpenPrice:    21600.35038478,
					ClosePrice:   21556.26203864,
					LowestPrice:  21535.0140056,
					HighestPrice: 21642,
				},
				{
					Timestamp:    1657382400,
					OpenPrice:    21555,
					ClosePrice:   21694,
					LowestPrice:  21532,
					HighestPrice: 21717,
				},
				{
					Timestamp:    1657386000,
					OpenPrice:    21698,
					ClosePrice:   21870,
					LowestPrice:  21672,
					HighestPrice: 21976,
				},
			},
			expectedErrs: []error{nil, nil, nil},
		},
	}
	mkt := candles.NewMarket(candles.WithCacheSizes(map[time.Duration]int{}))
	for _, ts := range testCases {
		t.Run(ts.name, func(t *testing.T) {
			it, err := mkt.Iterator(ts.marketSource, ts.startTime, ts.candlestickInterval)
			it.SetStartFromNext(ts.startFromNext)
			require.Nil(t, err)
			for i, expectedCandlestick := range ts.expectedCandlesticks {
				candlestick, err := it.Next()
				require.ErrorIs(t, err, ts.expectedErrs[i])
				require.Equal(t, expectedCandlestick, candlestick)
			}
		})
	}
}

func tp(s string) time.Time {
	tm, _ := time.Parse(time.RFC3339, s)
	return tm.UTC()
}
