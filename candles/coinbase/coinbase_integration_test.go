package coinbase_test

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
			name:                "Coinbase",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.COINBASE, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1657378800,
					OpenPrice:    21581.54,
					ClosePrice:   21553.63,
					LowestPrice:  21553.63,
					HighestPrice: 21648.43,
				},
				{
					Timestamp:    1657382400,
					OpenPrice:    21549.41,
					ClosePrice:   21706.26,
					LowestPrice:  21537.17,
					HighestPrice: 21714.67,
				},
				{
					Timestamp:    1657386000,
					OpenPrice:    21678.13,
					ClosePrice:   21881.61,
					LowestPrice:  21672.87,
					HighestPrice: 21975.64,
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
