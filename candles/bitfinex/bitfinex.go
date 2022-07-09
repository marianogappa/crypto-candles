package bitfinex

import (
	"sync"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
)

// Bitfinex struct enables requesting candlesticks from Bitfinex
type Bitfinex struct {
	apiURL    string
	debug     bool
	lock      sync.Mutex
	requester common.RequesterWithRetry
}

// NewBitfinex is the constructor for Bitfinex
func NewBitfinex() *Bitfinex {
	e := &Bitfinex{
		apiURL: "https://api-pub.bitfinex.com/v2/",
	}

	e.requester = common.NewRequesterWithRetry(
		e.requestCandlesticks,
		common.RetryStrategy{Attempts: 3, FirstSleepTime: 1 * time.Second, SleepTimeMultiplier: 2.0},
		&e.debug,
	)

	return e
}

// RequestCandlesticks requests candlesticks for the given market pair, of candlestick interval "intervalMinutes",
// starting at "startTimeTs".
//
// The supplied "intervalMinutes" may not be supported by this exchange.
//
// Candlesticks will start at the next multiple of "startTimeTs" as defined by
// time.Truncate(intervalMinutes * time.Minute)), except in some documented exceptions.
// This is enforced by the exchange.
//
// Some exchanges return candlesticks with gaps, but this method will patch the gaps by cloning the candlestick
// received right before the gap as many times as gaps, or the first candlestick if the gaps is at the start.
//
// Most of the usage of this method is with 1 minute intervals, the interval used to follow predictions.
func (e *Bitfinex) RequestCandlesticks(marketSource common.MarketSource, startTimeTs int, intervalMinutes int) ([]common.Candlestick, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	candlesticks, err := e.requestCandlesticks(marketSource.BaseAsset, marketSource.QuoteAsset, startTimeTs, intervalMinutes)
	if err != nil {
		return nil, err
	}

	return common.PatchCandlestickHoles(candlesticks, startTimeTs, 60*intervalMinutes), nil
}

// GetPatience returns the delay that this exchange usually takes in order for it to return candlesticks.
//
// Some exchanges may return results for unfinished candles (e.g. the current minute) and some may not, so callers
// should not request unfinished candles. This patience should be taken into account in addition to unfinished candles.
func (e *Bitfinex) GetPatience() time.Duration { return 1 * time.Minute }

// SetDebug sets exchange-wide debug logging. It's useful to know how many times requests are being sent to exchanges.
func (e *Bitfinex) SetDebug(debug bool) {
	e.debug = debug
}
