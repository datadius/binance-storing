package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	host   = "localhost"
	port   = 5432
	user   = "postgres"
	dbname = "binanceklines"
)

type Timestamp time.Time

func (t Timestamp) String() string {
	return time.Time(t).String()
}

func main() {
	// connection string
	psqlconn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		os.Getenv("POSTGRES_PASSWORD"),
		dbname,
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

	GetBinanceKlineData("ETHUSDT", "1h", "10")
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
	k.Volume, err = strconv.ParseFloat(tmp[5].(string), 64)
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

func GetBinanceKlineData(symbol, interval, limit string) {
	// get data
	// https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=80
	urlSpot := url.URL{
		Scheme: "https",
		Host:   "api.binance.com",
		Path:   "/api/v3/klines"}
	q := urlSpot.Query()
	q.Set("symbol", symbol)
	q.Set("interval", interval)
	q.Set("limit", limit)
	urlSpot.RawQuery = q.Encode()

	var req *http.Response
	req, err := http.Get(urlSpot.String())

	if err != nil {
		log.Println("Error get request: ", err)
	}

	responseBody, err := io.ReadAll(req.Body)

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

	for _, kline := range klines {
		log.Printf(
			"Datetime: %s, Close: %f",
			kline.KlinesDatetime,
			kline.Close,
		)
	}

}
func CreateKlinesTable(db *sql.DB) {
	// create table
	createTb := `CREATE TABLE IF NOT EXISTS Klines (
            datetime TIMESTAMP NOT NULL,
            symbol varchar(30) NOT NULL,
            type varchar(1) NOT NULL,
            open numeric(17,8) NOT NULL,
            high numeric(17,8) NOT NULL,
            low numeric(17,8) NOT NULL,
            close numeric(17,8) NOT NULL,
            volume numeric(17,8),
            close_time TIMESTAMP,
            quote_asset_volume numeric(17,8),
            nr_of_trades int,
            taker_buy_base_asset_volume numeric(17,8),
            taker_buy_quote_asset_volume numeric(17,8),
            ignore int,
            PRIMARY KEY (datetime,symbol,type)
        );`

	_, err := db.Exec(createTb)
	CheckError(err)
}

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
