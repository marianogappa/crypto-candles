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
		flagProvider            = flag.String("provider", "BINANCE", "one of BINANCE|COINBASE|KUCOIN|BINANCEUSDMFUTURES|BITSTAMP|BITFINEX")
		flagBaseAsset           = flag.String("baseAsset", "", "e.g. BTC in BTC/USDT")
		flagQuoteAsset          = flag.String("quoteAsset", "", "e.g. USDT in BTC/USDT")
		flagStartTime           = flag.String("startTime", "", "ISO8601/RFC3339 date to start retrieving candlesticks e.g. 2022-07-10T14:01:00Z")
		flagCandlestickInterval = flag.String("candlestickInterval", "", "the candlestick interval in time.ParseDuration format e.g. 1h, 1m, 24h")
		flagLimit               = flag.Int("limit", 10, "how many candlesticks to return")
	)

	flag.Parse()

	if *flagProvider == "" {
		exit("Empty provider.", true)
	}
	if *flagBaseAsset == "" {
		exit("Empty base asset.", true)
	}
	if *flagQuoteAsset == "" {
		exit("Empty quote asset.", true)
	}
	if *flagStartTime == "" {
		exit("Empty start time.", true)
	}
	if *flagCandlestickInterval == "" {
		exit("Empty candlestick interval.", true)
	}
	if *flagLimit <= 0 {
		exit("Limit is negative or zero.", true)
	}
	if *flagMarketType != "COIN" {
		exit("marketType must be 'COIN'.", true)
	}

	startTime, err := time.Parse(time.RFC3339, *flagStartTime)
	if err != nil {
		exit(fmt.Sprintf("invalid startTime '%v': %v.", *flagStartTime, err), true)
	}
	candlestickInterval, err := time.ParseDuration(*flagCandlestickInterval)
	if err != nil {
		exit(fmt.Sprintf("invalid candlestickInterval '%v': %v.", *flagCandlestickInterval, err), true)
	}

	m := candles.NewMarket(candles.WithCacheSizes(map[time.Duration]int{}))
	iter, err := m.Iterator(
		common.MarketSource{Type: common.MarketTypeFromString(*flagMarketType), Provider: *flagProvider, BaseAsset: *flagBaseAsset, QuoteAsset: *flagQuoteAsset},
		startTime,
		candlestickInterval,
	)
	if err != nil {
		exit(fmt.Sprintf("error building iterator: %v", err), true)
	}

	for i := 0; i < *flagLimit; i++ {
		candlestick, err := iter.Next()
		if err != nil {
			exit(err.Error(), false)
		}
		bs, _ := json.Marshal(candlestick)
		fmt.Println(string(bs))
	}
}

func exit(s string, showUsage bool) {
	log.Println(s)
	if showUsage {
		flag.Usage()
		os.Exit(1)
	}
	os.Exit(0)
}
