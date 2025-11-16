package bitget_test

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/bitget"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	// t.Skip() // Skip by default, but run manually to verify implementation

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
			name:                 "Bitget BTC-USDT",
			marketSource:         common.MarketSource{Type: common.COIN, Provider: common.BITGET, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:            tp("2022-07-09T15:00:00Z"),
			startFromNext:        false,
			candlestickInterval:  time.Hour,
			expectedCandlesticks: []common.Candlestick{
				// These will be filled after testing
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
	t.Skip() // Skip by default, but run manually to verify implementation

	// Test that a single API call returns candlesticks
	startTime := time.Now().Add(-48 * time.Hour).Truncate(time.Minute)
	marketSource := common.MarketSource{Type: common.COIN, Provider: common.BITGET, BaseAsset: "BTC", QuoteAsset: "USDT"}

	// Test directly with Bitget to verify a single API call returns results
	b := bitget.NewBitget()
	candlesticks, err := b.RequestCandlesticks(marketSource, startTime, time.Minute)
	require.Nil(t, err)
	require.Greater(t, len(candlesticks), 0, "Expected at least some candlesticks from a single API call")

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
