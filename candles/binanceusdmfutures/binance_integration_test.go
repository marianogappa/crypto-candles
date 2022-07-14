package binanceusdmfutures_test

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
			name:                "Binance USDM Futures",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.BINANCEUSDMFUTURES, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1657378800,
					OpenPrice:    21585.1,
					ClosePrice:   21538.7,
					LowestPrice:  21524.5,
					HighestPrice: 21639.8,
				},
				{
					Timestamp:    1657382400,
					OpenPrice:    21538.6,
					ClosePrice:   21683.5,
					LowestPrice:  21523.2,
					HighestPrice: 21717,
				},
				{
					Timestamp:    1657386000,
					OpenPrice:    21683.5,
					ClosePrice:   21872.4,
					LowestPrice:  21660,
					HighestPrice: 22000,
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
