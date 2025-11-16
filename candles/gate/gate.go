package gate

import (
	"sync"
	"time"

	"github.com/marianogappa/crypto-candles/candles/common"
)

// Gate struct enables requesting candlesticks from Gate.io
type Gate struct {
	apiURL    string
	debug     bool
	lock      sync.Mutex
	requester common.RequesterWithRetry
}

// NewGate is the constructor for Gate
func NewGate() *Gate {
	e := &Gate{
		apiURL: "https://api.gateio.ws/api/v4/",
	}

	e.requester = common.NewRequesterWithRetry(
		e.requestCandlesticks,
		common.RetryStrategy{Attempts: 3, FirstSleepTime: 1 * time.Second, SleepTimeMultiplier: 2.0},
		&e.debug,
	)

	return e
}

// RequestCandlesticks requests candlesticks for the given market source, of a given candlestick interval,
// starting at a given time.Time.
//
// The supplied candlestick interval may not be supported by this exchange.
//
// Candlesticks will start at the next multiple of startTime as defined by
// time.Truncate(candlestickInterval), except in some documented exceptions.
//
// Some exchanges return candlesticks with gaps, but this method will patch the gaps by cloning the candlestick
// received right before the gap as many times as gaps, or the first candlestick if the gaps is at the start.
//
// Most of the usage of this method is with 1 minute intervals, the interval used to follow predictions.
func (e *Gate) RequestCandlesticks(marketSource common.MarketSource, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	candlesticks, err := e.requestCandlesticks(marketSource.BaseAsset, marketSource.QuoteAsset, startTime, candlestickInterval)
	if err != nil {
		return nil, err
	}

	return common.PatchCandlestickHoles(candlesticks, int(startTime.Unix()), int(candlestickInterval/time.Second)), nil
}

// Patience returns the delay that this exchange usually takes in order for it to return candlesticks.
//
// Some exchanges may return results for unfinished candles (e.g. the current minute) and some may not, so callers
// should not request unfinished candles. This patience should be taken into account in addition to unfinished candles.
func (e *Gate) Patience() time.Duration { return 1 * time.Minute }

// Name is the name of this candlestick provider.
func (e *Gate) Name() string { return common.GATE }

// SetDebug sets exchange-wide debug logging. It's useful to know how many times requests are being sent to exchanges.
func (e *Gate) SetDebug(debug bool) {
	e.debug = debug
}


