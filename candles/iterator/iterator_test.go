package iterator

import (
	"testing"
	"time"

	"github.com/marianogappa/crypto-candles/candles/cache"
	"github.com/marianogappa/crypto-candles/candles/common"
	"github.com/stretchr/testify/require"
)

type testSpec struct {
	name                             string
	marketSource                     common.MarketSource
	startTime                        time.Time
	candlestickProvider              *testCandlestickProvider
	timeNowFunc                      func() time.Time
	candlestickInterval              time.Duration
	startFromNext                    bool
	errCreatingIterator              error
	expectedCandlestickProviderCalls []call
	expectedCallResponses            []response
}

func TestIterator(t *testing.T) {
	msBTCUSDT := common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
	tss := []testSpec{
		// Minutely tests
		{
			name:                             "Minutely: ErrNoNewTicksYet because timestamp to request is too early (startFromNext == false)",
			candlestickInterval:              1 * time.Minute,
			marketSource:                     msBTCUSDT,
			startTime:                        tp("2020-01-02 00:01:10"), // 49 secs to nw
			candlestickProvider:              newTestCandlestickProvider(nil),
			timeNowFunc:                      func() time.Time { return tp("2020-01-02 00:01:59") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: nil,
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrNoNewTicksYet}},
		},
		{
			name:                             "Minutely: ErrNoNewTicksYet because timestamp to request is too early (startFromNext == true)",
			candlestickInterval:              1 * time.Minute,
			marketSource:                     msBTCUSDT,
			startTime:                        tp("2020-01-02 00:01:10"),
			candlestickProvider:              newTestCandlestickProvider(nil),
			timeNowFunc:                      func() time.Time { return tp("2020-01-02 00:02:59") },
			startFromNext:                    true,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: nil,
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrNoNewTicksYet}},
		},
		{
			name:                "Minutely: ErrOutOfCandlestics because candlestickProvider returned that",
			candlestickInterval: 1 * time.Minute,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{}, err: common.ErrOutOfCandlesticks},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-02 00:02:00")}},
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrOutOfCandlesticks}},
		},
		{
			name:                "Minutely: ErrExchangeReturnedNoTicks because exchange returned old ticks",
			candlestickInterval: 1 * time.Minute,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234},
					{Timestamp: tInt("2020-01-02 00:01:00"), OpenPrice: 1235, HighestPrice: 1235, LowestPrice: 1235, ClosePrice: 1235}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-02 00:02:00")}},
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrExchangeReturnedNoTicks}},
		},
		{
			name:                "Minutely: ErrExchangeReturnedOutOfSyncTick because exchange returned ticks after requested one",
			candlestickInterval: 1 * time.Minute,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-02 00:03:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234},
					{Timestamp: tInt("2020-01-02 00:04:00"), OpenPrice: 1235, HighestPrice: 1235, LowestPrice: 1235, ClosePrice: 1235}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-02 00:02:00")}},
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrExchangeReturnedOutOfSyncTick}},
		},
		{
			name:                "Minutely: Succeeds to request one tick",
			candlestickInterval: 1 * time.Minute,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-02 00:02:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-02 00:02:00")}},
			expectedCallResponses:            []response{{tick: common.Tick{Timestamp: tInt("2020-01-02 00:02:00"), Value: 1234}, err: nil}},
		},
		{
			name:                "Minutely: Succeeds to request two ticks, making only one request",
			candlestickInterval: 1 * time.Minute,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:02:00"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-02 00:02:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234},
					{Timestamp: tInt("2020-01-02 00:03:00"), OpenPrice: 1235, HighestPrice: 1235, LowestPrice: 1235, ClosePrice: 1235}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-02 00:02:00")}},
			expectedCallResponses: []response{
				{tick: common.Tick{Timestamp: tInt("2020-01-02 00:02:00"), Value: 1234}, err: nil},
				{tick: common.Tick{Timestamp: tInt("2020-01-02 00:03:00"), Value: 1235}, err: nil}},
		},
		{
			name:                "Minutely: Ignores cache Put error",
			candlestickInterval: 1 * time.Minute,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:02:00"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-02 00:02:00"), OpenPrice: 0, HighestPrice: 0, LowestPrice: 0, ClosePrice: 0}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-02 00:02:00")}},
			expectedCallResponses: []response{
				{tick: common.Tick{Timestamp: tInt("2020-01-02 00:02:00"), Value: 0}, err: nil},
			},
		},
		// Daily tests
		{
			name:                             "Daily: ErrNoNewTicksYet because timestamp to request is too early (startFromNext == false)",
			candlestickInterval:              24 * time.Hour,
			marketSource:                     msBTCUSDT,
			startTime:                        tp("2020-01-02 00:01:10"),
			candlestickProvider:              newTestCandlestickProvider(nil),
			timeNowFunc:                      func() time.Time { return tp("2020-01-02 23:59:59") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: nil,
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrNoNewTicksYet}},
		},
		{
			name:                             "Daily: ErrNoNewTicksYet because timestamp to request is too early (startFromNext == true)",
			candlestickInterval:              24 * time.Hour,
			marketSource:                     msBTCUSDT,
			startTime:                        tp("2020-01-02 00:01:10"),
			candlestickProvider:              newTestCandlestickProvider(nil),
			timeNowFunc:                      func() time.Time { return tp("2020-01-03 00:02:59") },
			startFromNext:                    true,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: nil,
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrNoNewTicksYet}},
		},
		{
			name:                "Daily: ErrExchangeReturnedNoTicks because exchange returned old ticks",
			candlestickInterval: 24 * time.Hour,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-01 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234},
					{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1235, HighestPrice: 1235, LowestPrice: 1235, ClosePrice: 1235}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-04 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-03 00:00:00")}},
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrExchangeReturnedNoTicks}},
		},
		{
			name:                "Daily: ErrExchangeReturnedOutOfSyncTick because exchange returned ticks after requested one",
			candlestickInterval: 24 * time.Hour,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-04 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234},
					{Timestamp: tInt("2020-01-05 00:00:00"), OpenPrice: 1235, HighestPrice: 1235, LowestPrice: 1235, ClosePrice: 1235}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-05 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-03 00:00:00")}},
			expectedCallResponses:            []response{{tick: common.Tick{}, err: common.ErrExchangeReturnedOutOfSyncTick}},
		},
		{
			name:                "Daily: Succeeds to request one tick",
			candlestickInterval: 24 * time.Hour,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-03 00:00:00"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-04 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-03 00:00:00")}},
			expectedCallResponses: []response{
				{tick: common.Tick{Timestamp: tInt("2020-01-03 00:00:00"), Value: 1234}, err: nil}},
		},
		{
			name:                "Daily: Succeeds to request two ticks, making only one request",
			candlestickInterval: 24 * time.Hour,
			marketSource:        msBTCUSDT,
			startTime:           tp("2020-01-02 00:01:10"),
			candlestickProvider: newTestCandlestickProvider([]testCandlestickProviderResponse{
				{candlesticks: []common.Candlestick{
					{Timestamp: tInt("2020-01-03 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234},
					{Timestamp: tInt("2020-01-04 00:00:00"), OpenPrice: 1235, HighestPrice: 1235, LowestPrice: 1235, ClosePrice: 1235}}, err: nil},
			}),
			timeNowFunc:                      func() time.Time { return tp("2020-01-05 00:00:00") },
			startFromNext:                    false,
			errCreatingIterator:              nil,
			expectedCandlestickProviderCalls: []call{{marketSource: msBTCUSDT, startTime: tp("2020-01-03 00:00:00")}},
			expectedCallResponses: []response{
				{tick: common.Tick{Timestamp: tInt("2020-01-03 00:00:00"), Value: 1234}, err: nil},
				{tick: common.Tick{Timestamp: tInt("2020-01-04 00:00:00"), Value: 1235}, err: nil}},
		},
	}
	for _, ts := range tss {
		t.Run(ts.name, func(t *testing.T) {
			cache := cache.NewMemoryCache(map[time.Duration]int{time.Minute: 128, 24 * time.Hour: 128})
			iterator, err := NewIterator(ts.marketSource, ts.startTime, ts.candlestickInterval, cache, ts.candlestickProvider, WithTimeNowFunc(ts.timeNowFunc), WithStartFromNext(ts.startFromNext))
			require.ErrorIs(t, err, ts.errCreatingIterator)

			for _, expectedResp := range ts.expectedCallResponses {
				actualCandlestick, actualErr := iterator.Next()
				actualTick := actualCandlestick.ToTick()
				require.ErrorIs(t, actualErr, expectedResp.err)
				if expectedResp.err == nil {
					require.Equal(t, expectedResp.tick, actualTick)
				}
			}

			require.Equal(t, ts.expectedCandlestickProviderCalls, ts.candlestickProvider.calls)
		})
	}
}

func TestTickIteratorUsesCache(t *testing.T) {
	msBTCUSDT := common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
	cache := cache.NewMemoryCache(map[time.Duration]int{time.Minute: 128, 24 * time.Hour: 128})
	cstick1 := common.Candlestick{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}
	tick1 := common.Tick{Timestamp: tInt("2020-01-02 00:00:00"), Value: 1234}
	cstick2 := common.Candlestick{Timestamp: tInt("2020-01-02 00:01:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}
	tick2 := common.Tick{Timestamp: tInt("2020-01-02 00:01:00"), Value: 1234}
	cstick3 := common.Candlestick{Timestamp: tInt("2020-01-02 00:02:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}
	tick3 := common.Tick{Timestamp: tInt("2020-01-02 00:02:00"), Value: 1234}

	testCandlestickProvider1 := newTestCandlestickProvider([]testCandlestickProviderResponse{
		{candlesticks: []common.Candlestick{cstick1, cstick2, cstick3}, err: nil},
		{candlesticks: nil, err: common.ErrOutOfCandlesticks},
	})
	it1, _ := NewIterator(
		msBTCUSDT,
		tp("2020-01-02 00:00:00"),
		time.Minute,
		cache,
		testCandlestickProvider1,
		WithTimeNowFunc(func() time.Time { return tp("2022-01-03 00:00:00") }),
	)
	cs, err := it1.Next()
	tick := cs.ToTick()
	require.Nil(t, err)
	require.Equal(t, tick, tick1)
	cs, err = it1.Next()
	tick = cs.ToTick()
	require.Nil(t, err)
	require.Equal(t, tick, tick2)
	cs, err = it1.Next()
	tick = cs.ToTick()
	require.Nil(t, err)
	require.Equal(t, tick, tick3)
	_, err = it1.Next()
	require.Equal(t, common.ErrOutOfCandlesticks, err)

	require.Len(t, testCandlestickProvider1.calls, 2)

	testCandlestickProvider2 := newTestCandlestickProvider([]testCandlestickProviderResponse{{candlesticks: nil, err: common.ErrOutOfCandlesticks}})
	it2, _ := NewIterator(
		msBTCUSDT,
		tp("2020-01-02 00:00:00"),
		time.Minute,
		cache, // Reusing cache, so cache should kick in and prevent testCandlestickProvider2 from being called
		testCandlestickProvider2,
		WithTimeNowFunc(func() time.Time { return tp("2022-01-03 00:00:00") }),
	)
	cs, err = it2.Next()
	tick = cs.ToTick()
	require.Nil(t, err)
	require.Equal(t, tick, tick1)
	cs, err = it2.Next()
	tick = cs.ToTick()
	require.Nil(t, err)
	require.Equal(t, tick, tick2)
	cs, err = it2.Next()
	tick = cs.ToTick()
	require.Nil(t, err)
	require.Equal(t, tick, tick3)
	_, err = it2.Next()
	require.Equal(t, common.ErrOutOfCandlesticks, err)

	require.Len(t, testCandlestickProvider2.calls, 1) // Cache was used!! Only last call after cache consumed.
}

func TestScannerInterface(t *testing.T) {
	msBTCUSDT := common.MarketSource{
		Type:       common.COIN,
		Provider:   "BINANCE",
		BaseAsset:  "BTC",
		QuoteAsset: "USDT",
	}
	cstick1 := common.Candlestick{Timestamp: tInt("2020-01-02 00:00:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}
	cstick2 := common.Candlestick{Timestamp: tInt("2020-01-02 00:01:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}
	cstick3 := common.Candlestick{Timestamp: tInt("2020-01-02 00:02:00"), OpenPrice: 1234, HighestPrice: 1234, LowestPrice: 1234, ClosePrice: 1234}

	testCandlestickProvider1 := newTestCandlestickProvider([]testCandlestickProviderResponse{
		{candlesticks: []common.Candlestick{cstick1, cstick2, cstick3}, err: nil},
		{candlesticks: nil, err: common.ErrOutOfCandlesticks},
	})
	it, _ := NewIterator(
		msBTCUSDT,
		tp("2020-01-02 00:00:00"),
		time.Minute,
		nil,
		testCandlestickProvider1,
	)
	var cs common.Candlestick
	require.True(t, it.Scan(&cs))
	require.Nil(t, it.Error())
	require.Equal(t, cstick1, cs)
	require.True(t, it.Scan(&cs))
	require.Nil(t, it.Error())
	require.Equal(t, cstick2, cs)
	require.True(t, it.Scan(&cs))
	require.Nil(t, it.Error())
	require.Equal(t, cstick3, cs)
	require.False(t, it.Scan(&cs))
	require.ErrorIs(t, it.Error(), common.ErrOutOfCandlesticks)
}

type response struct {
	tick common.Tick
	err  error
}

type testCandlestickProviderResponse struct {
	candlesticks []common.Candlestick
	err          error
}

type call struct {
	marketSource common.MarketSource
	startTime    time.Time
}

type testCandlestickProvider struct {
	calls     []call
	responses []testCandlestickProviderResponse
}

func newTestCandlestickProvider(responses []testCandlestickProviderResponse) *testCandlestickProvider {
	return &testCandlestickProvider{responses: responses}
}

func (p *testCandlestickProvider) RequestCandlesticks(marketSource common.MarketSource, startTime time.Time, candlestickInterval time.Duration) ([]common.Candlestick, error) {
	resp := p.responses[len(p.calls)]
	p.calls = append(p.calls, call{marketSource: marketSource, startTime: startTime.UTC()})
	return resp.candlesticks, resp.err
}

func (p *testCandlestickProvider) Patience() time.Duration { return 0 * time.Second }
func (p *testCandlestickProvider) Name() string            { return "TEST" }

func tp(s string) time.Time {
	t, _ := time.Parse("2006-01-02 15:04:05", s)
	return t.UTC()
}

func tInt(s string) int {
	return int(tp(s).Unix())
}
