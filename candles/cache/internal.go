package cache

import (
	"fmt"
	"time"

	"github.com/marianogappa/crypto-candles/common"
)

func (c *MemoryCache) put(metric Metric, candlesticks []common.Candlestick) error {
	var lastTimestamp int
	for i, candlestick := range candlesticks {
		if lastTimestamp != 0 && candlestick.Timestamp-lastTimestamp != int(metric.CandlestickInterval/time.Second) {
			lastDateTime := time.Unix(int64(lastTimestamp), 0).UTC().Format(time.Kitchen)
			thisDateTime := time.Unix(int64(candlestick.Timestamp), 0).UTC().Format(time.Kitchen)
			return fmt.Errorf("%w: last date was %v and this was %v", ErrReceivedNonSubsequentCandlestick, lastDateTime, thisDateTime)
		}
		if candlestick.OpenPrice == 0 || candlestick.HighestPrice == 0 || candlestick.LowestPrice == 0 || candlestick.ClosePrice == 0 {
			return ErrReceivedCandlestickWithZeroValue
		}

		var (
			candlestickTime = time.Unix(int64(candlestick.Timestamp), 0)
			truncatedTime   = candlestickTime.Truncate(metric.CandlestickInterval * 500)
			key             = fmt.Sprintf("%v-%v-%v", metric.Name, metric.CandlestickInterval.String(), truncatedTime.Format(time.RFC3339))
			index           = int(candlestickTime.Sub(truncatedTime) / metric.CandlestickInterval)
		)
		if i == 0 && candlestickTime != truncatedTime.Add(time.Duration(index)*metric.CandlestickInterval) {
			return ErrTimestampMustBeMultipleOfCandlestickInterval
		}

		elem, ok := c.caches[metric.CandlestickInterval].Get(key)
		if !ok {
			elem = [500]common.Candlestick{}
		}
		typedElem := elem.([500]common.Candlestick)
		typedElem[index] = candlestick
		c.caches[metric.CandlestickInterval].Add(key, typedElem)

		lastTimestamp = candlestick.Timestamp
	}

	return nil
}

func (c *MemoryCache) get(metric Metric, startingTimestamp int) ([]common.Candlestick, error) {
	var (
		candlestickTime = time.Unix(int64(startingTimestamp), 0)
		truncatedTime   = candlestickTime.Truncate(metric.CandlestickInterval * 500)
		key             = fmt.Sprintf("%v-%v-%v", metric.Name, metric.CandlestickInterval.String(), truncatedTime.Format(time.RFC3339))
		index           = int(candlestickTime.Sub(truncatedTime) / metric.CandlestickInterval)
		candlesticks    = []common.Candlestick{}
	)

	elem, ok := c.caches[metric.CandlestickInterval].Get(key)
	if !ok {
		c.CacheMisses++
		return []common.Candlestick{}, ErrCacheMiss
	}
	typedElem := elem.([500]common.Candlestick)
	for i := index; i <= 499; i++ {
		if typedElem[i] == (common.Candlestick{}) {
			break
		}
		candlesticks = append(candlesticks, typedElem[i])
	}

	if len(candlesticks) == 0 {
		c.CacheMisses++
		return candlesticks, ErrCacheMiss
	}
	return candlesticks, nil
}
