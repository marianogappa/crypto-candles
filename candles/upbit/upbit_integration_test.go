package upbit_test

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/marianogappa/crypto-candles/candles/upbit"
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
			name:                "Upbit BTC-USDT",
			marketSource:        common.MarketSource{Type: common.COIN, Provider: common.UPBIT, BaseAsset: "BTC", QuoteAsset: "USDT"},
			startTime:           tp("2025-11-16T10:00:00Z"),
			startFromNext:       false,
			candlestickInterval: time.Hour,
			// Golden results: These values were obtained by making a real API request once and hardcoding the results
			// To regenerate: run TestGenerateGoldenResults and copy the output
			expectedCandlesticks: []common.Candlestick{
				{
					Timestamp:    1763287200, // 2025-11-16 10:00:00 UTC (rounded from API timestamp)
					OpenPrice:    96499.99,
					ClosePrice:   96317.16,
					LowestPrice:  96301.48,
					HighestPrice: 96615.91,
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
	t.Skip() // Skip by default, but run manually to verify implementation

	// Test that a single API call returns candlesticks and matches golden results
	// This test uses golden results (hardcoded expected values) instead of testing A = A
	// Use a fixed startTime based on the golden results to ensure consistent test results
	// The startTime corresponds to the first golden result timestamp (1763287200 = 2025-11-16 10:00:00 UTC)
	// Note: This is rounded from the API timestamp 1763287198 due to gap filling
	startTime := time.Unix(1763287200, 0)
	marketSource := common.MarketSource{Type: common.COIN, Provider: common.UPBIT, BaseAsset: "BTC", QuoteAsset: "USDT"}

	// Golden results: These values were obtained by making a real API request once and hardcoding the results
	// Fixed startTime ensures these values remain stable across test runs
	// Note: Timestamps may be rounded by gap filling to match the interval
	expectedCandlesticks := []common.Candlestick{
		{
			Timestamp:    1763287200, // 2025-11-16 10:00:00 UTC (rounded from API timestamp)
			OpenPrice:    96499.99,
			ClosePrice:   96317.16,
			LowestPrice:  96301.48,
			HighestPrice: 96615.91,
		},
	}

	// Test directly with Upbit to verify a single API call returns results
	u := upbit.NewUpbit()
	candlesticks, err := u.RequestCandlesticks(marketSource, startTime, time.Hour)
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
