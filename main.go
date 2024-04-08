package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-co-op/gocron/v2"
	_ "github.com/lib/pq"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

/////////////////////////////////////////////////
// IF MEMORY BECOMES A PROBLEM RUN DEALLOCATE ALL;
/////////////////////////////////////////////////

type Timestamp time.Time

func (t Timestamp) String() string {
	return time.Time(t).String()
}

func (t Timestamp) Time() time.Time {
	return time.Time(t)
}

func main() {
	// connection string
	psqlconn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("PGHOST"),
		38290,
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		"railway",
	)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// close database
	defer db.Close()

	// check db
	err = db.Ping()
	CheckError(err)

	fmt.Println("Connected!")

	var klineClient = &http.Client{
		Timeout: 15 * time.Second,
	}

	CreateKlinesTable(db)

	symbols := GetSymbols(klineClient)

	symbolsKlinesTable := GetSymbolsSliceFromKlinesTable(db)

	diffSymbols := Difference(symbols, symbolsKlinesTable)

	if len(diffSymbols) > 0 {

		BulkInsertKlinesRequests(klineClient, "F", "1h", diffSymbols, "1000", db)

		BulkInsertKlinesRequests(klineClient, "F", "4h", diffSymbols, "1000", db)
	}

	scheduler, err := gocron.NewScheduler()
	if err != nil {
		log.Println("Error creating scheduler: ", err)
	}

	// */5 * * * *
	// 0 * * * *
	job, err := scheduler.NewJob(gocron.CronJob("0 * * * *", false), gocron.NewTask(func() {
		BulkInsertKlinesRequests(klineClient, "F", "1h", symbols, "2", db)
		DeleteKlinesOldKlines("1h", "999", db)
		symbols = GetSymbols(klineClient)
	}))

	if err != nil {
		log.Println("Error creating job: ", err)
	}

	log.Println("Starting cron job ", job.ID())

	job_4h, err := scheduler.NewJob(gocron.CronJob("0 */4 * * *", false), gocron.NewTask(func() {
		BulkInsertKlinesRequests(klineClient, "F", "4h", symbols, "2", db)
		DeleteKlinesOldKlines("4h", "3999", db)
	}))

	if err != nil {
		log.Println("Error creating job: ", err)
	}

	log.Println("Starting cron job ", job_4h.ID())

	scheduler.Start()

	// block until you are ready to shut down
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	log.Println("Blocking, press ctrl+c to exit...")
	select {
	case <-done:
	}

	// when you're done, shut it down
	err = scheduler.Shutdown()
	if err != nil {
		log.Println("Error shutting down scheduler: ", err)
	}
}

func DeleteKlinesOldKlines(interval string, hours string, db *sql.DB) {
	// delete old klines
	deleteOldKlinesStmt := fmt.Sprintf(
		`DELETE FROM Klines WHERE interval='%s' and datetime < now() - interval '%s hours'`,
		interval,
		hours,
	)

	res, err := db.Exec(deleteOldKlinesStmt)

	if err != nil {
		log.Println("Error for deleting rows ", interval, err)
	}

	log.Println("Result from deletion ", interval, hours, res)

}

func GetSymbols(client *http.Client) []string {
	urlSpot := url.URL{
		Scheme: "https",
		Host:   "fapi.binance.com",
		Path:   "/fapi/v1/ticker/24hr",
	}

	var req *http.Response
	req, err := client.Get(urlSpot.String())

	if err != nil {
		log.Println("Error symbols get request: ", err)
	}

	responseBody, err := io.ReadAll(req.Body)
	_ = req.Body.Close()

	if err != nil {
		log.Println("Read response body: ", err)
	}
	if req.StatusCode >= 400 && req.StatusCode <= 500 {
		log.Println("Error response. Status Code: ", req.StatusCode)
		log.Println("Response Body: ", string(responseBody))
	}

	var symbols []Symbol
	err = json.Unmarshal(responseBody, &symbols)
	if err != nil {
		log.Println("Error unmarshal response body: ", err)
	}

	var symbolsFiltered []string
	for _, symbol := range symbols {
		if strings.HasSuffix(symbol.Symbol, "USDT") {
			symbolsFiltered = append(symbolsFiltered, symbol.Symbol)
		}
	}

	return symbolsFiltered

}

type Symbol struct {
	Symbol             string  `json:"symbol"`
	PriceChange        string  `json:"priceChange"`
	PriceChangePercent string  `json:"priceChangePercent"`
	WeightedAvgPrice   string  `json:"weightedAvgPrice"`
	LastPrice          string  `json:"lastPrice"`
	LastQty            string  `json:"lastQty"`
	OpenPrice          string  `json:"openPrice"`
	HighPrice          string  `json:"highPrice"`
	LowPrice           string  `json:"lowPrice"`
	Volume             string  `json:"volume"`
	QuoteVolume        string  `json:"quoteVolume"`
	OpenTime           float64 `json:"openTime"`
	CloseTime          float64 `json:"closeTime"`
	FirstId            int     `json:"firstId"`
	LastId             int     `json:"lastId"`
	Count              int     `json:"count"`
}

type Kline struct {
	KlinesDatetime           Timestamp
	Open                     float64
	High                     float64
	Low                      float64
	Close                    float64
	Volume                   float64
	CloseTime                Timestamp
	QuoteAssetVolume         float64
	NrOfTrades               int
	TakerBuyBaseAssetVolume  float64
	TakerBuyQuoteAssetVolume float64
}

func (k *Kline) UnmarshalJSON(p []byte) error {
	var tmp []interface{}
	err := json.Unmarshal(p, &tmp)
	if err != nil {
		return err
	}

	k.KlinesDatetime = Timestamp(time.UnixMilli(int64(tmp[0].(float64))))
	k.Open, err = strconv.ParseFloat(tmp[1].(string), 64)
	if err != nil {
		log.Println("Error parsing open float: ", err, tmp)
	}
	k.High, err = strconv.ParseFloat(tmp[2].(string), 64)
	if err != nil {
		log.Println("Error parsing high float: ", err, tmp)
	}
	k.Low, err = strconv.ParseFloat(tmp[3].(string), 64)
	if err != nil {
		log.Println("Error parsing low  ", err, tmp)
	}
	k.Close, err = strconv.ParseFloat(tmp[4].(string), 64)
	if err != nil {
		log.Println("Error parsing close  ", err, tmp)
	}
	k.Volume, err = strconv.ParseFloat(tmp[5].(string), 32)
	if err != nil {
		log.Println("Error parsing volume  ", err, tmp)
	}
	k.CloseTime = Timestamp(time.UnixMilli(int64(tmp[6].(float64))))
	k.QuoteAssetVolume, err = strconv.ParseFloat(tmp[7].(string), 64)
	if err != nil {
		log.Println("Error parsing quote asset volume  ", err, tmp)
	}
	k.NrOfTrades = int(tmp[8].(float64))
	k.TakerBuyBaseAssetVolume, err = strconv.ParseFloat(tmp[9].(string), 64)
	if err != nil {
		log.Println("Error parsing taker buy base asset volume  ", err, tmp)
	}
	k.TakerBuyQuoteAssetVolume, err = strconv.ParseFloat(tmp[10].(string), 64)
	if err != nil {
		log.Println("Error parsing taker buy quote asset volume  ", err, tmp)
	}

	return nil
}

func GetBinanceKlineData(client *http.Client, symbol, interval, limit string) []Kline {
	urlSpot := url.URL{
		Scheme: "https",
		Host:   "fapi.binance.com",
		Path:   "/fapi/v1/klines"}
	q := urlSpot.Query()
	q.Set("symbol", symbol)
	q.Set("interval", interval)
	q.Set("limit", limit)
	urlSpot.RawQuery = q.Encode()

	var req *http.Response
	req, err := client.Get(urlSpot.String())

	if err != nil {
		log.Println("Error get request: ", err)
	}

	responseBody, err := io.ReadAll(req.Body)
	_ = req.Body.Close()

	if err != nil {
		log.Println("Read response body: ", err)
	}
	if req.StatusCode >= 400 && req.StatusCode <= 500 {
		log.Println("Error response. Status Code: ", req.StatusCode)
		log.Println("Response Body: ", string(responseBody))
	}

	var klines []Kline
	err = json.Unmarshal(responseBody, &klines)
	if err != nil {
		log.Println("Error unmarshal response body: ", err)
	}

	return klines
}

func CreateKlinesTable(db *sql.DB) {
	// create table
	createTb := `
        CREATE TABLE IF NOT EXISTS Klines (
        datetime TIMESTAMP NOT NULL,
        symbol varchar(30) NOT NULL,
        type varchar(1) NOT NULL,
        interval varchar(3) NOT NULL,
        open numeric(32,8) NOT NULL,
        high numeric(32,8) NOT NULL,
        low numeric(32,8) NOT NULL,
        close numeric(32,8) NOT NULL,
        volume numeric(32,8),
        close_time TIMESTAMP,
        quote_asset_volume numeric(32,8),
        nr_of_trades int,
        taker_buy_base_asset_volume numeric(32,8),
        taker_buy_quote_asset_volume numeric(32,8),
        ignore int,
        PRIMARY KEY (datetime,symbol,type, interval)
        );`

	_, err := db.Exec(createTb)
	CheckError(err)
}

func InsertKlinesTable(
	symbol string,
	contract string,
	interval string,
	klines []Kline,
	db *sql.DB,
) {
	//https://pkg.go.dev/github.com/lib/pq#hdr-Bulk_imports
	//Look into having bulk imports
	insertStmt := `insert into 
        "klines"("datetime", 
                 "symbol", 
                 "type", 
                 "interval",
                 "open", 
                 "high", 
                 "low", 
                 "close", 
                 "volume", 
                 "close_time",  
                 "quote_asset_volume", 
                 "nr_of_trades", 
                 "taker_buy_base_asset_volume",
                 "taker_buy_quote_asset_volume",
                 "ignore") 
        values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (datetime, symbol, type, interval)
        DO UPDATE SET open = $5, high = $6, low = $7, close = $8, volume = $9, close_time = $10, 
        quote_asset_volume = $11, nr_of_trades = $12, 
        taker_buy_base_asset_volume = $13, taker_buy_quote_asset_volume = $14, ignore = $15;`

	for _, kline := range klines {
		result, err := db.Exec(insertStmt,
			kline.KlinesDatetime.Time(),
			symbol,
			contract,
			interval,
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

		if err != nil {
			log.Println("Error while inserting the kline: ", kline)
			log.Println("Error Result: ", result)
			log.Panicln("Error inserting kline: ", err)
		}
	}
}

func BulkInsertKlinesRequests(
	client *http.Client,
	contract string,
	interval string,
	symbols []string,
	size string,
	db *sql.DB) {
	txn, err := db.Begin()

	if err != nil {
		log.Println("Failed to prepare transaction: ", err)
	}

	stmt, _ := txn.Prepare(`insert into 
        "klines"("datetime", 
                 "symbol", 
                 "type", 
                 "interval",
                 "open", 
                 "high", 
                 "low", 
                 "close", 
                 "volume", 
                 "close_time",  
                 "quote_asset_volume", 
                 "nr_of_trades", 
                 "taker_buy_base_asset_volume",
                 "taker_buy_quote_asset_volume",
                 "ignore") 
        values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (datetime, symbol, type, interval)
        DO UPDATE SET open = $5, high = $6, low = $7, close = $8, volume = $9, close_time = $10, 
        quote_asset_volume = $11, nr_of_trades = $12, 
        taker_buy_base_asset_volume = $13, taker_buy_quote_asset_volume = $14, ignore = $15;`)

	for _, symbol := range symbols {
		klines := GetBinanceKlineData(client, symbol, interval, size)
		for _, kline := range klines {
			_, err = stmt.Exec(
				kline.KlinesDatetime.Time(),
				symbol,
				contract,
				interval,
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

			if err != nil {
				log.Println("Error while inserting the kline: ", kline)
				log.Panicln("Error inserting kline: ", err)
			}
		}
	}

	err = stmt.Close()
	if err != nil {
		log.Panicln("Error closing statement: ", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Panicln("Error committing transaction: ", err)
	}

}

func GetSizeOfKlinesTable(db *sql.DB) int {
	var size int
	err := db.QueryRow("SELECT COUNT(*) FROM Klines").Scan(&size)
	if err != nil {
		log.Println("Error getting size of table: ", err)
	}
	return size
}

func GetSymbolsSliceFromKlinesTable(db *sql.DB) []string {
	var symbols []string
	rows, err := db.Query("SELECT DISTINCT symbol FROM Klines WHERE interval = '1h'")
	if err != nil {
		log.Println("Error getting symbols: ", err)
	}
	defer rows.Close()
	for rows.Next() {
		var symbol string
		err := rows.Scan(&symbol)
		if err != nil {
			log.Println("Error getting individual symbol: ", err)
		}
		symbols = append(symbols, symbol)
	}
	return symbols
}

func Difference(a, b []string) (diff []string) {
	m := make(map[string]bool)

	for _, item := range b {
		m[item] = true
	}

	for _, item := range a {
		if _, ok := m[item]; !ok {
			diff = append(diff, item)
		}
	}
	return
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
