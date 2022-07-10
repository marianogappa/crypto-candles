// Package common contains shared interfaces and code across the market super-package.
package common

import (
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	// BINANCE is an enumesque string value representing the BINANCE exchange
	BINANCE = "BINANCE"
	// FTX is an enumesque string value representing the FTX exchange
	FTX = "FTX"
	// COINBASE is an enumesque string value representing the COINBASE exchange
	COINBASE = "COINBASE"
	// KUCOIN is an enumesque string value representing the KUCOIN exchange
	KUCOIN = "KUCOIN"
	// BINANCEUSDMFUTURES is an enumesque string value representing the BINANCEUSDMFUTURES exchange
	BINANCEUSDMFUTURES = "BINANCEUSDMFUTURES"
	// BITSTAMP is an enumesque string value representing the BITSTAMP exchange
	BITSTAMP = "BITSTAMP"
	// BITFINEX is an enumesque string value representing the BITFINEX exchange
	BITFINEX = "BITFINEX"
)

var (
	// ErrUnsupportedCandlestickInterval means: unsupported candlestick interval
	ErrUnsupportedCandlestickInterval = errors.New("unsupported candlestick interval")

	// ErrExecutingRequest means: error executing client.Do() http request method
	ErrExecutingRequest = errors.New("error executing client.Do() http request method")

	// ErrBrokenBodyResponse means: exchange returned broken body response
	ErrBrokenBodyResponse = errors.New("exchange returned broken body response")

	// ErrInvalidJSONResponse means: exchange returned invalid JSON response
	ErrInvalidJSONResponse = errors.New("exchange returned invalid JSON response")
)

// Exchange is the interface for a crypto exchange.
type Exchange interface {
	CandlestickProvider
	SetDebug(debug bool)
}

// CandlestickProvider wraps a crypto exchanges' API method to retrieve historical candlesticks behind a common
// interface.
type CandlestickProvider interface {
	// RequestCandlesticks requests candlesticks for a given marketPair/asset at a given starting time.
	//
	// Since this is an HTTP request against one of many different exchanges, there's a myriad of things that can go
	// wrong (e.g. Internet out, AWS outage affects exchange, exchange does not honor its API), so implementations do
	// a best-effort of grouping known errors into wrapped errors, but clients must be prepared to handle unknown
	// errors.
	//
	// Resulting candlesticks will start from the given startTimeTs rounded to the next minute or day (respectively for
	// marketPair/asset).
	//
	// Some exchanges return results with gaps. In this case, implementations will fill gaps with the next known value.
	//
	// * Fails with ErrInvalidMarketPair if the marketSource's marketPair / asset does not exist at the exchange. In some
	//   cases, an exchange may not have data for a marketPair / asset and still not explicitly return an error.
	RequestCandlesticks(marketSource MarketSource, startTime time.Time, candlestickInterval time.Duration) ([]Candlestick, error)

	// Patience documents the recommended latency a client should observe for requesting the latest candlesticks
	// for a given market pair. Clients may ignore it, but are more likely to have to deal with empty results, errors
	// and rate limiting.
	Patience() time.Duration

	// Name is the uppercase name of the candlestick provider e.g. BINANCE
	Name() string
}

// CandleReqError is an error arising from a call to requestCandlesticks
type CandleReqError struct {
	Code           int
	Err            error
	IsNotRetryable bool
	IsExchangeSide bool
	RetryAfter     time.Duration
}

func (e CandleReqError) Error() string { return e.Err.Error() }

// Candlestick is the generic struct for candlestick data for all supported exchanges.
type Candlestick struct {
	// Timestamp is the UNIX timestamp (i.e. seconds since UTC Epoch) at which the candlestick started.
	Timestamp int `json:"t"`

	// OpenPrice is the price at which the candlestick opened.
	OpenPrice JSONFloat64 `json:"o"`

	// ClosePrice is the price at which the candlestick closed.
	ClosePrice JSONFloat64 `json:"c"`

	// LowestPrice is the lowest price reached during the candlestick duration.
	LowestPrice JSONFloat64 `json:"l"`

	// HighestPrice is the highest price reached during the candlestick duration.
	HighestPrice JSONFloat64 `json:"h"`
}

// ToTicks converts a Candlestick to two Ticks. Lowest value is put first, because since there's no way to tell
// which one happened first, this library chooses to be pessimistic.
func (c Candlestick) ToTicks() []Tick {
	return []Tick{
		{Timestamp: c.Timestamp, Value: c.LowestPrice},
		{Timestamp: c.Timestamp, Value: c.HighestPrice},
	}
}

// ToTick converts a Candlestick to a Tick.
func (c Candlestick) ToTick() Tick {
	return Tick{Timestamp: c.Timestamp, Value: c.ClosePrice}
}

// Tick is the closePrice & timestamp of a Candlestick.
type Tick struct {
	Timestamp int         `json:"t"`
	Value     JSONFloat64 `json:"v"`
}

// JSONFloat64 exists only for the purpose of marshalling floats in a nicer way.
type JSONFloat64 float64

// MarshalJSON overrides the marshalling of floats in a nicer way.
func (jf JSONFloat64) MarshalJSON() ([]byte, error) {
	f := float64(jf)
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return nil, errors.New("unsupported value")
	}
	bs := []byte(fmt.Sprintf("%.12f", f))
	var i int
	for i = len(bs) - 1; i >= 0; i-- {
		if bs[i] == '0' {
			continue
		}
		if bs[i] == '.' {
			return bs[:i], nil
		}
		break
	}
	return bs[:i+1], nil
}

// MarketSource uniquely identifies what market an Iterator is built for, e.g. the prices of BTC/USDT in BINANCE
type MarketSource struct {
	Type       MarketType
	Provider   string // e.g. "BINANCE", "KUCOIN"
	BaseAsset  string // e.g. "BTC" in BTC/USDT
	QuoteAsset string // e.g. "USDT" in BTC/USDT
}

func (m MarketSource) String() string {
	return fmt.Sprintf("%v:%v:%v-%v", m.Type.String(), m.Provider, m.BaseAsset, m.QuoteAsset)
}

// MarketType is the type of market that an Iterator is built for. The only supported MarketType is COIN e.g. BTC/USDT.
// At the moment it's not a very useful concept, but if MarketCaps are added, then this namespacing will be warranted.
type MarketType int

const (
	// UNSUPPORTED represents a yet-unsupported MarketType
	UNSUPPORTED MarketType = iota
	// COIN is the basic market pair MarketType e.g. BTC/USDT
	COIN
)

func (m MarketType) String() string {
	switch m {
	case COIN:
		return "COIN"
	default:
		return "UNSUPPORTED"
	}
}

// ISO8601 adds convenience methods for converting ISO8601-formatted date strings.
type ISO8601 string

// Time converts an ISO8601-formatted date string into a time.Time.
func (t ISO8601) Time() (time.Time, error) {
	return time.Parse(time.RFC3339, string(t))
}

// Seconds converts an ISO8601-formatted date string into a Unix timestamp.
func (t ISO8601) Seconds() (int, error) {
	tm, err := t.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to convert %v to seconds because %v", string(t), err.Error())
	}
	return int(tm.Unix()), nil
}

// Millis converts an ISO8601-formatted date string into a Javascript millisecond timestamp.
func (t ISO8601) Millis() (int, error) {
	tm, err := t.Seconds()
	if err != nil {
		return 0, err
	}
	return tm * 100, nil
}

var (
	// ErrInvalidMarketType means: invalid market type
	ErrInvalidMarketType = errors.New("invalid market type")

	// ErrUnsuportedCandlestickProvider means: unsupported candlestick provider
	ErrUnsuportedCandlestickProvider = errors.New("unsupported candlestick provider")

	// ErrOutOfTicks means: out of ticks
	ErrOutOfTicks = errors.New("out of ticks")

	// ErrOutOfCandlesticks means: exchange ran out of candlesticks
	ErrOutOfCandlesticks = errors.New("exchange ran out of candlesticks")

	// ErrOutOfTrades means: exchange ran out of trades
	ErrOutOfTrades = errors.New("exchange ran out of trades")

	// ErrInvalidMarketPair means: market pair or asset does not exist on exchange
	ErrInvalidMarketPair = errors.New("market pair or asset does not exist on exchange")

	// ErrRateLimit means: exchange asked us to enhance our calm
	ErrRateLimit = errors.New("exchange asked us to enhance our calm")

	// From TickIterator

	// ErrNoNewTicksYet means: no new ticks yet
	ErrNoNewTicksYet = errors.New("no new ticks yet")

	// ErrExchangeReturnedNoTicks means: exchange returned no ticks
	ErrExchangeReturnedNoTicks = errors.New("exchange returned no ticks")

	// ErrExchangeReturnedOutOfSyncTick means: exchange returned out of sync tick
	ErrExchangeReturnedOutOfSyncTick = errors.New("exchange returned out of sync tick")

	// From PatchTickHoles

	// ErrOutOfSyncTimestampPatchingHoles means: out of sync timestamp found patching holes
	ErrOutOfSyncTimestampPatchingHoles = errors.New("out of sync timestamp found patching holes")
)
