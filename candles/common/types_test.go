package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToTick(t *testing.T) {
	actual := Candlestick{
		Timestamp:    1,
		OpenPrice:    2,
		ClosePrice:   3,
		LowestPrice:  4,
		HighestPrice: 5,
	}.ToTick()

	expected := Tick{Timestamp: 1, Value: 3}

	require.Equal(t, expected, actual)
}

func TestCandleReqError(t *testing.T) {
	err := CandleReqError{Err: errors.New("for test")}
	require.Equal(t, "for test", err.Error())
}

func TestMarketSourceString(t *testing.T) {
	ms := MarketSource{
		Type:       COIN,
		Provider:   BINANCE,
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
	expected := "COIN:BINANCE:BTC-USDT"
	require.Equal(t, expected, ms.String())
}

func TestMarketTypeFromString(t *testing.T) {
	require.Equal(t, COIN, MarketTypeFromString("COIN"))
	require.Equal(t, UNSUPPORTED, MarketTypeFromString("ANYTHING ELSE"))
}

func TestMarketTypeString(t *testing.T) {
	require.Equal(t, "COIN", COIN.String())
	require.Equal(t, "UNSUPPORTED", UNSUPPORTED.String())
}
