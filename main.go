package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/marianogappa/crypto-candles/candles"
	"github.com/marianogappa/crypto-candles/candles/common"
)

func main() {
	var (
		flagMarketType          = flag.String("marketType", "COIN", "for now only 'COIN' is supported, representing market pairs e.g. BTC/USDT")
		flagProvider            = flag.String("provider", "BINANCE", "one of BINANCE|FTX|COINBASE|KUCOIN|BINANCEUSDMFUTURES|BITSTAMP|BITFINEX")
		flagBaseAsset           = flag.String("baseAsset", "", "e.g. BTC in BTC/USDT")
		flagQuoteAsset          = flag.String("quoteAsset", "", "e.g. USDT in BTC/USDT")
		flagStartTime           = flag.String("startTime", "", "ISO8601/RFC3339 date to start retrieving candlesticks e.g. 2022-07-10T14:01:00Z")
		flagCandlestickInterval = flag.String("candlestickInterval", "", "the candlestick interval in time.ParseDuration format e.g. 1h, 1m, 24h")
		flagLimit               = flag.Int("limit", 10, "how many candlesticks to return")
	)

	flag.Parse()

	if *flagProvider == "" {
		die("Empty provider.")
	}
	if *flagBaseAsset == "" {
		die("Empty base asset.")
	}
	if *flagQuoteAsset == "" {
		die("Empty quote asset.")
	}
	if *flagStartTime == "" {
		die("Empty start time.")
	}
	if *flagCandlestickInterval == "" {
		die("Empty candlestick interval.")
	}
	if *flagLimit <= 0 {
		die("Limit is negative or zero.")
	}
	if *flagMarketType != "COIN" {
		die("marketType must be 'COIN'.")
	}

	startTime, err := time.Parse(time.RFC3339, *flagStartTime)
	if err != nil {
		die(fmt.Sprintf("invalid startTime '%v': %v.", *flagStartTime, err))
	}
	candlestickInterval, err := time.ParseDuration(*flagCandlestickInterval)
	if err != nil {
		die(fmt.Sprintf("invalid candlestickInterval '%v': %v.", *flagCandlestickInterval, err))
	}

	m := candles.NewMarket(candles.WithCacheSizes(map[time.Duration]int{}))
	iter, err := m.Iterator(
		common.MarketSource{Type: common.MarketTypeFromString(*flagMarketType), Provider: *flagProvider, BaseAsset: *flagBaseAsset, QuoteAsset: *flagQuoteAsset},
		startTime,
		candlestickInterval,
	)
	if err != nil {
		die(fmt.Sprintf("error building iterator: %v", err))
	}

	for i := 0; i < *flagLimit; i++ {
		candlestick, err := iter.Next()
		if err != nil {
			die(err.Error())
		}
		bs, _ := json.Marshal(candlestick)
		fmt.Println(string(bs))
	}
}

func die(s string) {
	log.Println(s)
	flag.Usage()
	os.Exit(0)
}
