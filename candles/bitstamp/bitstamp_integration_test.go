package bitstamp_test

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
			name:                "Bitstamp",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.BITSTAMP, BaseAsset: "BTC", QuoteAsset: "USD"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1657378800,
					OpenPrice:    21591.07,
					ClosePrice:   21535.85,
					LowestPrice:  21530,
					HighestPrice: 21643.8,
				},
				{
					Timestamp:    1657382400,
					OpenPrice:    21539.82,
					ClosePrice:   21691.03,
					LowestPrice:  21530.39,
					HighestPrice: 21703.55,
				},
				{
					Timestamp:    1657386000,
					OpenPrice:    21690.43,
					ClosePrice:   21875.13,
					LowestPrice:  21660.39,
					HighestPrice: 21955.18,
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
