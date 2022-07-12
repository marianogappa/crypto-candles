package common

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJsonFloat64(t *testing.T) {
	tss := []struct {
		f        float64
		expected string
	}{
		{f: 1.2, expected: "1.2"},
		{f: 0.0000001234, expected: "0.0000001234"},
		{f: 1.000000, expected: "1"},
		{f: 0.000000, expected: "0"},
		{f: 0.001000, expected: "0.001"},
		{f: 10.0, expected: "10"},
	}
	for _, ts := range tss {
		t.Run(ts.expected, func(t *testing.T) {
			bs, err := json.Marshal(JSONFloat64(ts.f))
			if err != nil {
				t.Fatalf("Marshalling failed with %v", err)
			}
			if string(bs) != ts.expected {
				t.Fatalf("Expected marshalling of %f to be exactly '%v' but was '%v'", ts.f, ts.expected, string(bs))
			}
		})
	}
}

func TestJsonFloat64Fails(t *testing.T) {
	tss := []struct {
		f float64
	}{
		{f: math.Inf(1)},
		{f: math.NaN()},
	}
	for _, ts := range tss {
		t.Run(fmt.Sprintf("%f", ts.f), func(t *testing.T) {
			_, err := json.Marshal(JSONFloat64(ts.f))
			if err == nil {
				t.Fatal("Expected marshalling to fail")
			}
		})
	}
}

func TestToMillis(t *testing.T) {
	ms, err := ISO8601("2021-07-04T14:14:18Z").Millis()
	if err != nil {
		t.Fatalf("should not have errored, but errored with %v", err)
	}
	if ms != 162540805800 {
		t.Fatalf("expected ms to be %v but were %v", 162540805800, ms)
	}

	_, err = ISO8601("invalid").Millis()
	if err == nil {
		t.Fatal("should have errored, but didn't")
	}
}

func TestCandlestickToTicks(t *testing.T) {
	ticks := Candlestick{
		Timestamp:    1499040000,
		OpenPrice:    f(0.01634790),
		ClosePrice:   f(0.01577100),
		LowestPrice:  f(0.01575800),
		HighestPrice: f(0.80000000),
	}.ToTicks()

	if len(ticks) != 2 {
		t.Fatalf("expected len(ticks) == 2 but was %v", len(ticks))
	}

	expectedTicks := []Tick{
		{
			Timestamp: 1499040000,
			Value:     f(0.01575800),
		},
		{
			Timestamp: 1499040000,
			Value:     f(0.80000000),
		},
	}

	if !reflect.DeepEqual(expectedTicks, ticks) {
		t.Fatalf("expected ticks to be %v but were %v", expectedTicks, ticks)
	}
}

func TestCandlesticksToTicks(t *testing.T) {
	candlesticks := []Candlestick{
		{
			Timestamp:    20,
			OpenPrice:    f(1),
			ClosePrice:   f(2),
			LowestPrice:  f(3),
			HighestPrice: f(4),
		},
		{
			Timestamp:    21,
			OpenPrice:    f(5),
			ClosePrice:   f(6),
			LowestPrice:  f(7),
			HighestPrice: f(8),
		},
		{
			Timestamp:    22,
			OpenPrice:    f(9),
			ClosePrice:   f(10),
			LowestPrice:  f(11),
			HighestPrice: f(12),
		},
	}

	expected := []Tick{
		{Timestamp: 20, Value: 2},
		{Timestamp: 21, Value: 6},
		{Timestamp: 22, Value: 10},
	}

	require.Equal(t, expected, CandlesticksToTicks(candlesticks))
}

func f(fl float64) JSONFloat64 {
	return JSONFloat64(fl)
}

func TestPatchCandlestickHoles(t *testing.T) {
	tss := []struct {
		name         string
		candlesticks []Candlestick
		startTs      int
		durSecs      int
		expected     []Candlestick
	}{
		{
			name:         "Base case",
			candlesticks: []Candlestick{},
			startTs:      120,
			durSecs:      60,
			expected:     []Candlestick{},
		},
		{
			name: "Does not need to do anything",
			candlesticks: []Candlestick{
				{Timestamp: 120, OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
			startTs: 120,
			durSecs: 60,
			expected: []Candlestick{
				{Timestamp: 120, OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
		},
		{
			name: "Removes older entries returned",
			candlesticks: []Candlestick{
				{Timestamp: 60, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 120, OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
			startTs: 120,
			durSecs: 60,
			expected: []Candlestick{
				{Timestamp: 120, OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
		},
		{
			name: "Removes older entries returned, leaving nothing",
			candlesticks: []Candlestick{
				{Timestamp: 60, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
			},
			startTs:  120,
			durSecs:  60,
			expected: []Candlestick{},
		},
		{
			name: "Needs to add an initial tick",
			candlesticks: []Candlestick{
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
			startTs: 120,
			durSecs: 60,
			expected: []Candlestick{
				{Timestamp: 120, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
		},
		{
			name: "Needs to add an initial tick, as well as in the middle",
			candlesticks: []Candlestick{
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 360, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
			startTs: 120,
			durSecs: 60,
			expected: []Candlestick{
				{Timestamp: 120, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 180, OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: 240, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
				{Timestamp: 300, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
				{Timestamp: 360, OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
		},
		{
			name: "Adjusts start time to zero seconds",
			candlesticks: []Candlestick{
				{Timestamp: tInt("2020-01-02 00:03:00"), OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: tInt("2020-01-02 00:04:00"), OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: tInt("2020-01-02 00:05:00"), OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
			startTs: tInt("2020-01-02 00:02:58"),
			durSecs: 60,
			expected: []Candlestick{
				{Timestamp: tInt("2020-01-02 00:03:00"), OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: tInt("2020-01-02 00:04:00"), OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: tInt("2020-01-02 00:05:00"), OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
		},
		{
			name: "Adjusts start time to zero seconds rounding up",
			candlesticks: []Candlestick{
				{Timestamp: tInt("2020-01-02 00:03:00"), OpenPrice: 1, HighestPrice: 1, ClosePrice: 1, LowestPrice: 1},
				{Timestamp: tInt("2020-01-02 00:04:00"), OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: tInt("2020-01-02 00:05:00"), OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
			startTs: tInt("2020-01-02 00:03:02"),
			durSecs: 60,
			expected: []Candlestick{
				{Timestamp: tInt("2020-01-02 00:04:00"), OpenPrice: 2, HighestPrice: 2, ClosePrice: 2, LowestPrice: 2},
				{Timestamp: tInt("2020-01-02 00:05:00"), OpenPrice: 3, HighestPrice: 3, ClosePrice: 3, LowestPrice: 3},
			},
		},
	}
	for _, ts := range tss {
		t.Run(ts.name, func(t *testing.T) {
			actual := PatchCandlestickHoles(ts.candlesticks, ts.startTs, ts.durSecs)
			require.Equal(t, ts.expected, actual)
		})
	}
}

func tp(s string) time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05", s)
	return t
}

func tInt(s string) int {
	return int(tp(s).Unix())
}

func TestNormalizeTimestamp(t *testing.T) {
	tss := []struct {
		name                string
		tm                  ISO8601
		candlestickInterval time.Duration
		provider            string
		startFromNext       bool
		expected            ISO8601
	}{
		{
			name:                "1m, BINANCE, startFromNext = false",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 1 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T01:43:00Z"),
		},
		{
			name:                "1m, BINANCE, startFromNext = true",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 1 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T01:44:00Z"),
		},
		{
			name:                "1m, BINANCE, startFromNext = false, already normalized",
			tm:                  ISO8601("2021-01-02T01:42:00Z"),
			candlestickInterval: 1 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T01:42:00Z"),
		},
		{
			name:                "1m, BINANCE, startFromNext = true, already normalized",
			tm:                  ISO8601("2021-01-02T01:42:00Z"),
			candlestickInterval: 1 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T01:43:00Z"),
		},
		{
			name:                "5m, BINANCE, startFromNext = false",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 5 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T01:45:00Z"),
		},
		{
			name:                "5m, BINANCE, startFromNext = true",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 5 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T01:50:00Z"),
		},
		{
			name:                "5m, BINANCE, startFromNext = false, already normalized",
			tm:                  ISO8601("2021-01-02T01:45:00Z"),
			candlestickInterval: 5 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T01:45:00Z"),
		},
		{
			name:                "5m, BINANCE, startFromNext = true, already normalized",
			tm:                  ISO8601("2021-01-02T01:45:00Z"),
			candlestickInterval: 5 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T01:50:00Z"),
		},
		{
			name:                "15m, BINANCE, startFromNext = false",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 15 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T01:45:00Z"),
		},
		{
			name:                "15m, BINANCE, startFromNext = true",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 15 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T02:00:00Z"),
		},
		{
			name:                "15m, BINANCE, startFromNext = false, already normalized",
			tm:                  ISO8601("2021-01-02T01:45:00Z"),
			candlestickInterval: 15 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T01:45:00Z"),
		},
		{
			name:                "15m, BINANCE, startFromNext = true, already normalized",
			tm:                  ISO8601("2021-01-02T01:45:00Z"),
			candlestickInterval: 15 * time.Minute,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T02:00:00Z"),
		},
		{
			name:                "1h, BINANCE, startFromNext = false",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 1 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T02:00:00Z"),
		},
		{
			name:                "1h, BINANCE, startFromNext = true",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 1 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T03:00:00Z"),
		},
		{
			name:                "1h, BINANCE, startFromNext = false, already normalized",
			tm:                  ISO8601("2021-01-02T02:00:00Z"),
			candlestickInterval: 1 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T02:00:00Z"),
		},
		{
			name:                "1h, BINANCE, startFromNext = true, already normalized",
			tm:                  ISO8601("2021-01-02T02:00:00Z"),
			candlestickInterval: 1 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-02T03:00:00Z"),
		},
		{
			name:                "1d, BINANCE, startFromNext = false",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 24 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-03T00:00:00Z"),
		},
		{
			name:                "1d, BINANCE, startFromNext = true",
			tm:                  ISO8601("2021-01-02T01:42:24Z"),
			candlestickInterval: 24 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-04T00:00:00Z"),
		},
		{
			name:                "1d, BINANCE, startFromNext = false, already normalized",
			tm:                  ISO8601("2021-01-02T00:00:00Z"),
			candlestickInterval: 24 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       false,
			expected:            ISO8601("2021-01-02T00:00:00Z"),
		},
		{
			name:                "1d, BINANCE, startFromNext = true, already normalized",
			tm:                  ISO8601("2021-01-02T00:00:00Z"),
			candlestickInterval: 24 * time.Hour,
			provider:            "BINANCE",
			startFromNext:       true,
			expected:            ISO8601("2021-01-03T00:00:00Z"),
		},
	}
	for _, ts := range tss {
		t.Run(ts.name, func(t *testing.T) {
			tm, err := ts.tm.Time()
			require.Nil(t, err)
			actual := NormalizeTimestamp(tm, ts.candlestickInterval, ts.provider, ts.startFromNext)
			expected, err := ts.expected.Seconds()
			require.Nil(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
