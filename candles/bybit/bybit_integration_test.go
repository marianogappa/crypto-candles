package bybit_test

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/bybit"
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
			name:                "Bybit",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.BYBIT, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1657378800,
					OpenPrice:    21593.12,
					ClosePrice:   21552.62,
					LowestPrice:  21540,
					HighestPrice: 21645.06,
				},
				{
					Timestamp:    1657382400,
					OpenPrice:    21552.62,
					ClosePrice:   21692.48,
					LowestPrice:  21527.5,
					HighestPrice: 21713.82,
				},
				{
					Timestamp:    1657386000,
					OpenPrice:    21692.48,
					ClosePrice:   21875.2,
					LowestPrice:  21669.17,
					HighestPrice: 21994.72,
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

func TestIntegrationMaxLimit(t *testing.T) {
	// Test that a single API call returns 1000 candlesticks (the max limit)
	// Using 1-minute intervals, we need at least 1000 minutes back (about 16.7 hours)
	// We'll go back 2 days to ensure we have enough data
	startTime := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	marketSource := common.MarketSource{Type: common.COIN, Provider: common.BYBIT, BaseAsset: "BTC", QuoteAsset: "USDT"}

	// Test directly with Bybit to verify a single API call returns 1000 results
	b := bybit.NewBybit()
	candlesticks, err := b.RequestCandlesticks(marketSource, startTime, time.Minute)
	require.Nil(t, err)
	require.Equal(t, 1000, len(candlesticks), "Expected exactly 1000 candlesticks from a single API call")

	// Verify they are in chronological order
	for i := 1; i < len(candlesticks); i++ {
		require.Greater(t, candlesticks[i].Timestamp, candlesticks[i-1].Timestamp,
			"Candlesticks should be in chronological order")
	}
}

func tp(s string) time.Time {
	tm, _ := time.Parse(time.RFC3339, s)
	return tm.UTC()
}
