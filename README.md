# crypto-candles

![MIT](https://img.shields.io/github/license/marianogappa/crypto-candles)
![Code coverage](https://img.shields.io/codecov/c/github/marianogappa/crypto-candles)
![Go Report Card](https://goreportcard.com/badge/github.com/marianogappa/crypto-candles)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/marianogappa/crypto-candles)](https://pkg.go.dev/github.com/marianogappa/crypto-candles)

Universal crypto candlestick iterator library &amp; CLI

[Original blogpost: 10 Gotchas for building a universal crypto candlestick iterator in Go](https://marianogappa.github.io/software/2022/07/27/10-gotchas-for-building-a-universal-crypto-candlestick-iterator-in-go/)

- [x] Binance
- [x] Binance USDM Futures
- [x] Coinbase
- [x] Kucoin
- [x] Bitstamp
- [x] Bitfinex
- [x] Bybit
- [x] Upbit
- [x] OKX
- [x] HTX (Huobi)
- [x] Bitget
- [x] MEXC
- [x] Gate

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
{"t":1641096000,"o":47329.47,"c":46910.3,"l":46770.23,"h":47415.73}
{"t":1641099600,"o":46910.3,"c":47081.28,"l":46832.08,"h":47089.63}
{"t":1641103200,"o":47081.28,"c":47122.61,"l":47011.24,"h":47244}
{"t":1641106800,"o":47122.6,"c":47173.45,"l":47087.62,"h":47300}
{"t":1641110400,"o":47173.44,"c":47222.4,"l":46970.05,"h":47247.12}
{"t":1641114000,"o":47222.4,"c":47273.24,"l":47144.12,"h":47499.45}
{"t":1641117600,"o":47273.23,"c":47188.89,"l":47140.3,"h":47348.34}
{"t":1641121200,"o":47188.88,"c":47235.5,"l":47159.57,"h":47400}
{"t":1641124800,"o":47238.89,"c":47292.95,"l":47013.5,"h":47447}
{"t":1641128400,"o":47289.67,"c":47137.35,"l":47090,"h":47300.86}
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
