# crypto-candles

![MIT](https://img.shields.io/github/license/marianogappa/crypto-candles)
![Code coverage](https://img.shields.io/codecov/c/github/marianogappa/crypto-candles)
![Go Report Card](https://goreportcard.com/badge/github.com/marianogappa/crypto-candles)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/marianogappa/crypto-candles)](https://pkg.go.dev/github.com/marianogappa/crypto-candles)

Universal crypto candlestick iterator library &amp; CLI

- [x] Binance
- [x] Binance USDM Futures
- [x] FTX
- [x] Coinbase
- [x] Kucoin
- [x] Bitstamp
- [x] Bitfinex

## Library usage

```go
package main

import (
	"fmt"
	"log"
	"time"
	"encoding/json"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/common"
)

func main() {
	m := candles.NewMarket()
	iter, err := m.Iterator(
		common.MarketSource{Type: common.COIN, Provider: common.BINANCE, BaseAsset: "BTC", QuoteAsset: "USDT"},
		time.Now().Add(-12*time.Hour), // Start time
		1*time.Hour,                   // Candlestick interval
	)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		candlestick, err := iter.Next()
		if err != nil {
			log.Fatal(err)
		}
		bs, _ := json.Marshal(candlestick)
		fmt.Printf("%+v\n", string(bs))
	}
}
```

Outputs

```
{"t":1657436400,"o":21297.279999999999,"c":21338.119999999999,"l":21245.150000000001,"h":21360.580000000002}
{"t":1657440000,"o":21339.459999999999,"c":21324.16,"l":21255.25,"h":21424.669999999998}
{"t":1657443600,"o":21325.360000000001,"c":21325,"l":21286,"h":21361.52}
{"t":1657447200,"o":21326.130000000001,"c":21277.09,"l":21220.529999999999,"h":21355.849999999999}
{"t":1657450800,"o":21277.09,"c":21301.84,"l":21161.669999999998,"h":21304.959999999999}
{"t":1657454400,"o":21301.84,"c":21307.220000000001,"l":21270.459999999999,"h":21345.139999999999}
{"t":1657458000,"o":21307.23,"c":21080,"l":20951.009999999998,"h":21313.990000000002}
{"t":1657461600,"o":21078.970000000001,"c":20865.630000000001,"l":20835.73,"h":21080}
{"t":1657465200,"o":20863.919999999998,"c":20899.759999999998,"l":20839.23,"h":20979.759999999998}
{"t":1657468800,"o":20898.439999999999,"c":20932.349999999999,"l":20655,"h":21052.470000000001}
```

## CLI usage

Get binary from [latest release](https://github.com/marianogappa/crypto-candles/releases/latest) or `go install github.com/marianogappa/crypto-candles@latest`

```shell
$ crypto-candles -baseAsset BTC -quoteAsset USDT -provider BINANCE -startTime '2022-01-02T03:04:05Z' -candlestickInterval 1h
```

## Features

**Built-in in-memory LRU Caching**

Historical candlesticks shouldn't change, so this kind of data benefits from aggressive caching. This library has a configurable concurrency-safe in-memory cache (enabled by default) so that repeated requests for the same data will be served by the cache rather than going to the exchanges, thus mitigating rate-limiting issues. Caches are configurable per-candlestick interval.

**Built-in retries with back-off**

Requests to exchanges can fail for various reasons, some of which are retryable. The library will retry retryable requests with a back-off by default, and will deal with exchange-specific rate-limiting actions.

**Built-in patching of data holes**

Exchanges' historical candlestick data has holes (i.e. there are instants for which there's no candlestick information for certain market pairs on certain candlestick intervals). This is problematic for consumers, because it's tricky to differentiate the case where the exchange has no data from the case where the consumer hasn't consumed the data point yet, which can lead to requesting the same data point forever. Also, algorithms often prefer to assume the price is a continuous function without gaps. This library patches in holes by cloning immediately preceding candlesticks.

**Concurrency-safe**

The main problem with making concurrent requests to exchanges is not that libraries are not concurrency-safe, but that making concurrent requests will cause the exchange to rate-limit the caller. This library mutexes on a per-exchange basis, so concurrent requests to the same exchange become sequential, but concurrent requests to different exchanges remain concurrent.

**Unified error types**

Requests to exchanges can fail for various reasons, but some errors are common to all exchanges. In these cases, the library provides some common error types, e.g.:

- `common.ErrUnsupportedCandlestickInterval`
- `common.ErrRateLimit`
- `common.ErrInvalidMarketPair`

## Contribute

crypto-candles is open source software. Use it for whatever you want, and help me improve it if you can. Please open issues and send me PRs.
