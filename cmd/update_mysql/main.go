package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron"
	"time"
)

func main() {
	var redisHost string
	var sqlAddr string
	flag.StringVar(&redisHost, "redis", "127.0.0.1:6379", "redis addr")
	flag.StringVar(&sqlAddr, "sql", "root@tcp(127.0.0.1:3306)/StockData?parseTime=true&loc=Local", "mysql addr")
	flag.Parse()
	db, err := sql.Open("mysql", sqlAddr)
	if err != nil {
		logrus.Fatal("Failed to connect mysql:", err.Error())
	}
	defer db.Close()
	pool := redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", redisHost)
	}, 3)
	conn := pool.Get()
	_, err = conn.Do("PING")
	if err != nil {
		logrus.Fatal("Failed to connect redis:", err.Error())
	}
	crontab := cron.New()
	update := func() {
		logrus.Info("Start update")
		defer logrus.Info("End update")
		year, month, day := time.Now().Date()
		row := db.QueryRow(fmt.Sprintf(`select count(*) from t_day_stock_original where DATE>="%d-%2d-%2d";`, year, month, day))
		var recordNum int
		err := row.Scan(&recordNum)
		if err != nil {
			logrus.Fatal("Failed to scan the result from mysql when get the count(*):", err.Error())
		}
		if recordNum > 0 {
			logrus.Info("Already have the records for today, do nothing")
			return
		}
		conn := pool.Get()
		_, err = conn.Do("PING")
		if err != nil {
			conn = pool.Get()
		}
		tx, err := db.Begin()
		if err != nil {
			logrus.Fatal("Failed to start tx")
		}
		keys, err := redis.Strings(conn.Do("KEYS", "stock/*"))
		if err != nil {
			logrus.Fatal("Failed to get keys")
		}
		validRecords := []*JsonRecord{}
		todayBegining := time.Date(year, month, day, 0, 0, 0, 0, time.Local)
		indexes := map[string]bool{
			"000001.SH": true,
			"000300.SH": true,
			"399001.SZ": true,
			"399905.SZ": true,
		}
		isIndex := func(str string) bool {
			return indexes[str]
		}
		var stockUpdated bool
		acquire := func(keys []string) {
			if len(keys) == 0 {
				return
			}
			keysIf := make([]interface{}, len(keys))
			for i := range keys {
				keysIf[i] = keys[i]
			}
			strs, err := redis.Strings(conn.Do("MGET", keysIf...))
			if err != nil {
				logrus.Fatal("Failed to MGET redis:", err.Error())
			}
			for _, data := range strs {
				record := &JsonRecord{}
				err := json.Unmarshal([]byte(data), record)
				if err != nil {
					logrus.Error("Cannot unmarshal record")
				}
				if ((record.Status == "C" && record.Volume > 0) || isIndex(record.Target)) && record.Timestamp.After(todayBegining) {
					validRecords = append(validRecords, record)
					if !isIndex(record.Target) {
						stockUpdated = true
					}
				}
			}
		}
		for start := 0; start < len(keys); start += 100 {
			ending := start + 100
			if ending > len(keys) {
				ending = len(keys)
			}
			acquire(keys[start:ending])
		}
		if len(validRecords) == 0 || !stockUpdated {
			logrus.WithField("stockUpdated", stockUpdated).Info("No valid record, do nothing")
			return
		}
		query := bytes.NewBufferString("INSERT INTO t_day_stock_original(STOCK,DATE,OPEN,CLOSE,HIGH,LOW,VOLUME,VALUE,NUMBER_OF_TRADES,PRECLOSE) VALUES")
		for i, record := range validRecords {
			if i != 0 {
				query.WriteRune(',')
			}
			query.WriteString(fmt.Sprintf(`('%s','%s',%f,%f,%f,%f,%f,0,0,%f)`,
				record.Target,
				record.Timestamp.Format("2006-01-02"),
				record.Open,
				record.Close,
				record.High,
				record.Low,
				record.Volume,
				record.Last,
			))
		}
		query.WriteRune(';')
		_, err = tx.Exec(query.String())
		if err != nil {
			logrus.Fatal("Failed to exec sql statement")
		}
		err = tx.Commit()
		if err != nil {
			logrus.Fatal("Failed commit")
		}
		logrus.Info("Updated ", len(validRecords), " records")
	}
	update()
	crontab.AddFunc("0 0 * * * *", update)
	crontab.Start()
	wait := make(chan bool)
	<-wait
}

type JsonRecord struct {
	Target                       string
	Status                       string
	ProductType                  int
	Timestamp                    time.Time
	Open, High, Low, Close, Last float64
	Volume                       float64
	Suspension                   bool
}
