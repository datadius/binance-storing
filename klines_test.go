package main

import (
	"log"
	"net/http"
	"testing"
	"time"
)

func TestKlineLoading(t *testing.T) {
	var client = &http.Client{
		Timeout: 15 * time.Second,
	}

	symbols := GetSymbols(client)
	symbolsKlines := GetSymbolKlines(client, symbols, "1h", "2", 3)

	for _, symbol := range symbolsKlines {
		for _, kline := range symbol.Klines {
			log.Println(
				kline.KlinesDatetime.Time(),
				symbol.Symbol,
				"F",
				"1h",
				kline.Open,
				kline.High,
				kline.Low,
				kline.Close,
				kline.Volume,
				kline.CloseTime.Time(),
				kline.QuoteAssetVolume,
				kline.NrOfTrades,
				kline.TakerBuyBaseAssetVolume,
				kline.TakerBuyQuoteAssetVolume,
				0,
			)
		}
	}
}
