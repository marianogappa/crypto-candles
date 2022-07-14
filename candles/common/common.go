package common

import (
	"time"
)

// PatchCandlestickHoles takes a slice of candlesticks and it patches any holes in it, either at the beginning or within
// any pair of candlesticks whose difference in seconds doesn't match the supplied "durSecs", by cloning the latest
// available candlestick "on the left", or the first candlestick (i.e. "on the right") if it's at the beginning.
func PatchCandlestickHoles(cs []Candlestick, startTimeTs, durSecs int) []Candlestick {
	startTimeTs = NormalizeTimestamp(time.Unix(int64(startTimeTs), 0), time.Duration(durSecs)*time.Second, "TODO_PROVIDER", false)
	lastTs := startTimeTs - durSecs
	for len(cs) > 0 && cs[0].Timestamp < lastTs+durSecs {
		cs = cs[1:]
	}
	if len(cs) == 0 {
		return cs
	}

	fixedCSS := []Candlestick{}
	for _, candlestick := range cs {
		if candlestick.Timestamp == lastTs+durSecs {
			fixedCSS = append(fixedCSS, candlestick)
			lastTs = candlestick.Timestamp
			continue
		}
		for candlestick.Timestamp >= lastTs+durSecs {
			clonedCandlestick := candlestick
			clonedCandlestick.Timestamp = lastTs + durSecs
			fixedCSS = append(fixedCSS, clonedCandlestick)
			lastTs += durSecs
		}
	}
	return fixedCSS
}

// NormalizeTimestamp takes a time and a candlestick interval, and normalizes the timestamp by returning the immediately
// next multiple of that time as defined by .Truncate(candlestickInterval), unless the time already satisfies it.
//
// It also optionally returns the next time (i.e. it appends a candlestick interval to it).
//
// TODO: this function only currently supports 1m, 5m, 15m, 1h & 1d intervals. Using other intervals will
// result in silently incorrect behaviour due to exchanges behaving differently. Please review api_klines files for
// documented differences in behaviour.
func NormalizeTimestamp(rawTm time.Time, candlestickInterval time.Duration, provider string, startFromNext bool) int {
	rawTm = rawTm.UTC()
	tm := rawTm.Truncate(candlestickInterval).UTC()
	if tm != rawTm {
		tm = tm.Add(candlestickInterval)
	}
	return int(tm.Add(candlestickInterval * time.Duration(b2i(startFromNext))).Unix())
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
