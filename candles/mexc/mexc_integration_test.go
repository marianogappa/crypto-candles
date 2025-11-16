package mexc_test

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/marianogappa/crypto-candles/candles/mexc"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	t.Skip() // Skip by default, but run manually to verify implementation

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
			name:                "MEXC BTC-USDT",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.MEXC, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:           tp("2022-07-09T15:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
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

	// Test that a single API call returns candlesticks and matches golden results
	// This test uses golden results (hardcoded expected values) instead of testing A = A
	// Use a fixed startTime based on the golden results to ensure consistent test results
	startTime := time.Unix(1763078400, 0) // 2025-11-12 00:00:00 UTC
	marketSource := common.MarketSource{Type: common.COIN, Provider: common.MEXC, BaseAsset: "BTC", QuoteAsset: "USDT"}

	// Golden results: These values were obtained by making a real API request once and hardcoding the results
	// Fixed startTime ensures these values remain stable across test runs
	expectedCandlesticks := []common.Candlestick{
		{
			Timestamp:    1763078400, // 2025-11-12 00:00:00 UTC
			OpenPrice:    99709.8,
			ClosePrice:   94559.3,
			LowestPrice:  94006.34,
			HighestPrice: 99848.78,
		},
		{
			Timestamp:    1763164800, // 2025-11-13 00:00:00 UTC
			OpenPrice:    94559.3,
			ClosePrice:   95593.29,
			LowestPrice:  94559.3,
			HighestPrice: 96822.84,
		},
		{
			Timestamp:    1763251200, // 2025-11-14 00:00:00 UTC
			OpenPrice:    95593.29,
			ClosePrice:   95729.1,
			LowestPrice:  94849.88,
			HighestPrice: 96634.34,
		},
	}

	// Test directly with MEXC to verify a single API call returns results
	m := mexc.NewMEXC()
	candlesticks, err := m.RequestCandlesticks(marketSource, startTime, 24*time.Hour)
	if err != nil {
		t.Logf("Error details: %+v", err)
	}
	require.Nil(t, err)
	require.Greater(t, len(candlesticks), 0, "Expected at least some candlesticks from a single API call")

	// Verify they are in chronological order
	for i := 1; i < len(candlesticks); i++ {
		require.Greater(t, candlesticks[i].Timestamp, candlesticks[i-1].Timestamp,
			"Candlesticks should be in chronological order")
	}

	// Compare against golden results (not testing A = A, but against hardcoded values)
	// Find the expected candlesticks in the actual results (they may be at different indices)
	for _, expected := range expectedCandlesticks {
		found := false
		for _, actual := range candlesticks {
			if actual.Timestamp == expected.Timestamp {
				require.Equal(t, expected, actual, "Candlestick with timestamp %d should match golden result", expected.Timestamp)
				found = true
				break
			}
		}
		require.True(t, found, "Should find candlestick with timestamp %d in results", expected.Timestamp)
	}
}

func tp(s string) time.Time {
	tm, _ := time.Parse(time.RFC3339, s)
	return tm.UTC()
}


