package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/stretchr/testify/require"
)

type operation struct {
	opType              string
	marketSource        common.MarketSource
	candlestickInterval time.Duration
	candlesticks        []common.Candlestick
	initialISO8601      common.ISO8601
	expectedErr         error
	expectedTicks       []common.Candlestick
}

func TestCache(t *testing.T) {
	opBTCUSDT := common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
	opETHUSDT := common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "ETH",
		QuoteAsset: "USDT",
	}

	tss := []struct {
		name string
		ops  []operation
	}{
		// Minutely tests
		{
			name: "MINUTELY: Get empty returns ErrCacheMiss",
			ops: []operation{
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         ErrCacheMiss,
					expectedTicks:       []common.Candlestick{},
				},
			},
		},
		{
			name: "MINUTELY: Get with an invalid date returns ErrInvalidISO8601",
			ops: []operation{
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      common.ISO8601("invalid"),
					expectedErr:         ErrInvalidISO8601,
					expectedTicks:       []common.Candlestick{},
				},
			},
		},
		{
			name: "MINUTELY: Put empty returns empty",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks:        []common.Candlestick{},
					expectedErr:         nil,
				},
			},
		},
		{
			name: "MINUTELY: Put with non-zero second fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:01"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
					expectedErr: ErrTimestampMustBeMultipleOfCandlestickInterval,
				},
			},
		},
		{
			name: "MINUTELY: Put with zero value fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 0, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
					expectedErr: ErrReceivedCandlestickWithZeroValue,
				},
			},
		},
		{
			name: "MINUTELY: Put with non-subsequent timestamps fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:06:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
					expectedErr: ErrReceivedNonSubsequentCandlestick,
				},
			},
		},
		{
			name: "MINUTELY: Put with non-zero seconds fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:01"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
					expectedErr: ErrTimestampMustBeMultipleOfCandlestickInterval,
				},
			},
		},
		{
			name: "MINUTELY: Valid Put succeeds",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
			},
		},
		{
			name: "MINUTELY: Valid Put succeeds, and a get of a different key does not return anything, but a get of same key works",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opETHUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         ErrCacheMiss,
					expectedTicks:       []common.Candlestick{},
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
			},
		},
		{
			name: "MINUTELY: A secondary PUT overrides the first one's values, with full overlap",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
				},
			},
		},
		{
			name: "MINUTELY: A secondary PUT with overlap makes the sequence larger on GET",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-02 03:06:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-02 03:06:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
				},
			},
		},
		{
			name: "MINUTELY: A secondary PUT without overlap does not make the sequence larger on GET, and a second get gets the other one",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:07:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-02 03:08:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:07:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:07:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-02 03:08:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
				},
			},
		},
		{
			name: "MINUTELY: Two separate series work at the same time",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opETHUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
				{
					opType:              "GET",
					marketSource:        opETHUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
				},
			},
		},
		{
			name: "MINUTELY: Get of a day on an empty time is a cache miss",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:06:00"),
					expectedErr:         ErrCacheMiss,
					expectedTicks:       []common.Candlestick{},
				},
			},
		},
		{
			name: "MINUTELY: Get of a minute before, but with non-zero seconds, returns the tick of the next minute",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 03:03:01"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:04:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 03:05:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
			},
		},
		{
			name: "MINUTELY: Putting ticks that span two truncated intervals works, but requires two gets to get both ticks",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 16:39:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-02 16:40:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 16:39:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 16:39:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 1 * time.Minute,
					initialISO8601:      tpToISO("2020-01-02 16:40:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 16:40:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
			},
		},
		// Daily tests
		{
			name: "DAILY: Get empty returns ErrCacheMiss",
			ops: []operation{
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-02 03:04:00"),
					expectedErr:         ErrCacheMiss,
					expectedTicks:       []common.Candlestick{},
				},
			},
		},
		{
			name: "DAILY: Get with an invalid date returns ErrInvalidISO8601",
			ops: []operation{
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      common.ISO8601("invalid"),
					expectedErr:         ErrInvalidISO8601,
					expectedTicks:       []common.Candlestick{},
				},
			},
		},
		{
			name: "DAILY: Put empty returns empty",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks:        []common.Candlestick{},
					expectedErr:         nil,
				},
			},
		},
		{
			name: "DAILY: Put with non-zero second fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 03:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
					expectedErr: ErrTimestampMustBeMultipleOfCandlestickInterval,
				},
			},
		},
		{
			name: "DAILY: Put with zero value fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 0, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
					expectedErr: ErrReceivedCandlestickWithZeroValue,
				},
			},
		},
		{
			name: "DAILY: Put with non-subsequent timestamps fails",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-04 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: ErrReceivedNonSubsequentCandlestick,
				},
			},
		},
		{
			name: "DAILY: Valid Put succeeds",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
			},
		},
		{
			name: "DAILY: Valid Put succeeds, and a get of a different key does not return anything, but a get of same key works",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2021-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2021-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opETHUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2021-01-02 00:00:00"),
					expectedErr:         ErrCacheMiss,
					expectedTicks:       []common.Candlestick{},
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2021-01-02 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2021-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2021-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
			},
		},
		{
			name: "DAILY: A secondary PUT overrides the first one's values, with full overlap",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-02 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
				},
			},
		},
		{
			name: "DAILY: A secondary PUT with overlap makes the sequence larger on GET",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-04 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-02 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
						{Timestamp: tInt("2020-01-04 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
					},
				},
			},
		},
		{
			name: "DAILY: A secondary PUT without overlap does not make the sequence larger on GET, and a second get gets the other one",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-05 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-06 00:00:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-02 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-05 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-05 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-06 00:00:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
				},
			},
		},
		{
			name: "DAILY: Two separate series work at the same time",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "PUT",
					marketSource:        opETHUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-02 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
				{
					opType:              "GET",
					marketSource:        opETHUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-02 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 3456, HighestPrice: 3456, ClosePrice: 3456, LowestPrice: 3456},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 4567, HighestPrice: 4567, ClosePrice: 4567, LowestPrice: 4567},
					},
				},
			},
		},
		{
			name: "DAILY: Get of a day on an empty time is a cache miss",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-04 00:00:00"),
					expectedErr:         ErrCacheMiss,
					expectedTicks:       []common.Candlestick{},
				},
			},
		},
		{
			name: "DAILY: Get of a minute before, but with non-zero seconds, returns the tick of the next minute",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-01-01 03:03:01"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
			},
		},
		{
			name: "DAILY: Putting ticks that span two intervals works, but requires two gets to get both ticks",
			ops: []operation{
				{
					opType:              "PUT",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					candlesticks: []common.Candlestick{
						{Timestamp: tInt("2020-03-16 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
						{Timestamp: tInt("2020-03-17 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
					expectedErr: nil,
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-03-16 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-03-16 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, ClosePrice: 1234, LowestPrice: 1234},
					},
				},
				{
					opType:              "GET",
					marketSource:        opBTCUSDT,
					candlestickInterval: 24 * time.Hour,
					initialISO8601:      tpToISO("2020-03-17 00:00:00"),
					expectedErr:         nil,
					expectedTicks: []common.Candlestick{
						{Timestamp: tInt("2020-03-17 00:00:00"), OpenPrice: 2345, HighestPrice: 2345, ClosePrice: 2345, LowestPrice: 2345},
					},
				},
			},
		},
	}
	for _, ts := range tss {
		t.Run(ts.name, func(t *testing.T) {
			cache := NewMemoryCache(map[time.Duration]int{time.Minute: 128, 24 * time.Hour: 128})
			var (
				actualCandlesticks []common.Candlestick
				actualErr          error
			)

			for _, op := range ts.ops {
				metric := Metric{Name: op.marketSource.String(), CandlestickInterval: op.candlestickInterval}
				if op.opType == "GET" {
					actualCandlesticks, actualErr = cache.Get(metric, op.initialISO8601)
				} else if op.opType == "PUT" {
					actualErr = cache.Put(metric, op.candlesticks)
				}
				if actualErr != nil && op.expectedErr == nil {
					t.Logf("expected no error but had '%v'", actualErr)
					t.FailNow()
				}
				if actualErr == nil && op.expectedErr != nil {
					t.Logf("expected error '%v' but had no error", op.expectedErr)
					t.FailNow()
				}
				if op.expectedErr != nil && actualErr != nil && !errors.Is(actualErr, op.expectedErr) {
					t.Logf("expected error '%v' but had error '%v'", op.expectedErr, actualErr)
					t.FailNow()
				}
				if op.expectedErr == nil && op.opType == "GET" {
					require.Equal(t, op.expectedTicks, actualCandlesticks)
				}
			}
		})
	}
}

func tpToISO(s string) common.ISO8601 {
	t, _ := time.Parse("2006-01-02 15:04:05", s)
	return common.ISO8601(t.Format(time.RFC3339))
}

func tp(s string) time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05", s)
	return t
}

func tInt(s string) int {
	return int(tp(s).Unix())
}

func TestDoesNotFailWhenCreatedWithZeroSize(t *testing.T) {
	NewMemoryCache(map[time.Duration]int{time.Minute: 0, 24 * time.Hour: 0})
}

func TestNotConfiguredForCandlestickInterval(t *testing.T) {
	c := NewMemoryCache(map[time.Duration]int{})
	err := c.Put(Metric{Name: "test", CandlestickInterval: 160 * time.Minute}, []common.Candlestick{{}})
	require.ErrorIs(t, err, ErrCacheNotConfiguredForCandlestickInterval)
	_, err = c.Get(Metric{Name: "test", CandlestickInterval: 160 * time.Minute}, common.ISO8601("2020-01-02T03:04:05Z"))
	require.ErrorIs(t, err, ErrCacheNotConfiguredForCandlestickInterval)
}
