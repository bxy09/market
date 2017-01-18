package main

import (
	"bytes"
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	flagSHDbf   = flag.String("dbfSH", "/mnt/dbf/UT5/SHHQ/SHOW2003.DBF", "SH dbf to watch")
	flagSZDbf   = flag.String("dbfSZ", "/mnt/dbf/UT5/SZHQ/SJSHQ.DBF", "SZ dbf to watch")
	flagSZxx    = flag.String("dbfSZXX", "/mnt/dbf/UT5/SZHQ/SJSXX.DBF", "SZ xx dbf to watch")
	flagRedis   = flag.String("redis", "localhost:6379", "redis address")
	flagCSVPath = flag.String("path", "/home/gpx/data", "csv store path")
	flagDebug   = flag.Bool("debug", false, "debug mode")

	lastUpdateTimeStrSH string
	lastUpdateTimeStrSZ string

	updateIdSH int
	updateIdSZ int

	recordData    = map[string]Tick{}
	lastRecordMap = map[string]Record{}
	recordsHashSH = map[string][]byte{}
	recordsHashSZ = map[string][]byte{}

	suspensionSZ = map[string]bool{}

	sjsxxDuration = time.Minute
)

type FinancialType int

//各种类型的FinancialType
const (
	UNDEF FinancialType = iota
	FUTURES
	STOCK
)

type Tick struct {
	Id                           int64
	Target                       string
	Timestamp                    time.Time
	ProductType                  FinancialType
	Last, Open, High, Low, Close float64
	Volume                       float64
	Suspension                   bool
}

func initRedisConn() (redis.Conn, error) {
	r, err := redis.DialTimeout("tcp", *flagRedis, time.Second, time.Second, time.Second)
	if err != nil {
		log.Fatal(err)
	}
	return r, err
}

func (t *Tick) ToStringArray(target string) (s []string) {
	val := reflect.ValueOf(t).Elem()
	s = make([]string, val.NumField()+1)
	s[0] = target
	for i := 0; i < val.NumField(); i++ {
		value := fmt.Sprintf("%v", val.Field(i).Interface())
		s[i+1] = value
	}
	return s
}

func store(redis redis.Conn, csvwriter *csv.Writer, target string, tick Tick) {
	bytes, _ := json.Marshal(tick)
	err := redis.Send("PUBLISH", "stock/"+target, bytes)
	if err != nil {
		log.Info(err)
	}
	err = redis.Send("HSET", "stock_online", "stock/"+target, bytes)
	if err != nil {
		log.Info(err)
	}
	err = redis.Send("SET", "stock/"+target, bytes)
	if err != nil {
		log.Info(err)
	}
	if csvwriter != nil {
		csvwriter.Write(tick.ToStringArray(target))
	}
}

func sendOnce_sz(redis redis.Conn, csvwriter *csv.Writer, file Interface) {
	records := GetRecords(file)
	timestamp := time.Now()
	var err error

	//特殊记录
	sr := records[1]
	if sr.Data["HQCJSL"] != "0" {
		return
	}
	timeStr := strings.Trim(sr.Data["HQCJBS"], " ")
	if len(timeStr) < 6 {
		timeStr = "0" + timeStr
	}
	dateTimeStr := strings.Trim(sr.Data["HQZQJC"], " ") + timeStr
	loc, _ := time.LoadLocation("Asia/Shanghai")
	timestamp, err = time.ParseInLocation("20060102150405", dateTimeStr, loc)
	if err != nil {
		log.Warn("Parse SZ time err. Error message:", err)
		return
	}
	if dateTimeStr == lastUpdateTimeStrSZ {
		//TODO 目前不做处理
		//timestamp.Add(1000)
	} else {
		lastUpdateTimeStrSZ = dateTimeStr
	}
	//取完信息后删除特殊记录
	delete(records, 1)

	for _, r := range records {
		target := r.Data["HQZQDM"] + ".SZ"
		hash := r.HashCode
		lastHash, exist := recordsHashSZ[target]
		if !exist || bytes.Compare(hash, lastHash) != 0 {
			recordsHashSZ[target] = hash
		} else {
			continue
		}

		last, err := strconv.ParseFloat(r.Data["HQZRSP"], 64)
		if err != nil {
			log.Warn("Parse SZ last err. Error message:", err)
			continue
		}
		open, err := strconv.ParseFloat(r.Data["HQJRKP"], 64)
		if err != nil {
			log.Warn("Parse SZ open err. Error message:", err)
			continue
		}
		clos, err := strconv.ParseFloat(r.Data["HQZJCJ"], 64)
		if err != nil {
			log.Warn("Parse SZ close err. Error message:", err)
			continue
		}
		high, err := strconv.ParseFloat(r.Data["HQZGCJ"], 64)
		if err != nil {
			log.Warn("Parse SZ high err. Error message:", err)
			continue
		}
		low, err := strconv.ParseFloat(r.Data["HQZDCJ"], 64)
		if err != nil {
			log.Warn("Parse SZ low err. Error message:", err)
			continue
		}
		vol, err := strconv.ParseFloat(r.Data["HQCJSL"], 64)
		if err != nil {
			log.Warn("Parse SZ volume err. Error message:", err)
			continue
		}
		tick := recordData[target]
		tick.Suspension = suspensionSZ[target]
		tick.Target = target
		tick.Timestamp = timestamp
		tick.Id = int64(updateIdSZ)
		tick.ProductType = STOCK
		if suspensionSZ[target] || tick.Low < 0.00001 || tick.Low > 99990.0 {
			//如果当前股票是停牌
			tick.Last = last
			tick.Open = last
			tick.Close = last
			tick.High = last
			tick.Low = last
			tick.Volume = 0
		} else {
			tick.Last = last
			tick.Open = open
			tick.Close = clos
			tick.High = high
			tick.Low = low
			tick.Volume = vol
		}
		lastRecordMap[target] = r
		recordData[target] = tick
		store(redis, csvwriter, target, tick)
	}
	updateIdSZ++
}

func sendOnce_sh(redis redis.Conn, csvwriter *csv.Writer, file Interface) {
	records := GetRecords(file)
	timestamp := time.Now()
	var err error

	//特殊记录
	sr := records[1]
	if sr.Data["S11"] != "0" {
		// 闭市
		return
	}

	dateTimeStr := strings.Trim(sr.Data["S6"], " ") + strings.Trim(sr.Data["S2"], " ")
	loc, _ := time.LoadLocation("Asia/Shanghai")
	timestamp, err = time.ParseInLocation("20060102150405", dateTimeStr, loc)
	if err != nil {
		log.Warn("Parse SH time err. Error message:", err)
		return
	}
	if dateTimeStr == lastUpdateTimeStrSH {
		//TODO 目前不做处理
		//timestamp.Add(1000)
	} else {
		lastUpdateTimeStrSH = dateTimeStr
	}

	//取完信息后删除特殊记录
	delete(records, 1)

	for _, r := range records {
		target := r.Data["S1"] + ".SH"
		hash := r.HashCode
		lastHash, exist := recordsHashSH[target]
		if !exist || bytes.Compare(hash, lastHash) != 0 {
			recordsHashSH[target] = hash
		} else {
			continue
		}

		last, err := strconv.ParseFloat(r.Data["S3"], 64)
		if err != nil {
			log.Warn("Parse SH last err. Error message:", err)
			continue
		}
		open, err := strconv.ParseFloat(r.Data["S4"], 64)
		if err != nil {
			log.Warn("Parse SH open err. Error message:", err)
			continue
		}
		clos, err := strconv.ParseFloat(r.Data["S8"], 64)
		if err != nil {
			log.Warn("Parse SH close err. Error message:", err)
			continue
		}
		high, err := strconv.ParseFloat(r.Data["S6"], 64)
		if err != nil {
			log.Warn("Parse SH high err. Error message:", err)
			continue
		}
		low, err := strconv.ParseFloat(r.Data["S7"], 64)
		if err != nil {
			log.Warn("Parse SH low err. Error message:", err)
			continue
		}
		vol, err := strconv.ParseFloat(r.Data["S11"], 64)
		if err != nil {
			log.Warn("Parse SH volume err. Error message:", err)
			continue
		}
		tick := recordData[target]


		tick.Suspension = r.Deleted || low > 99900
		tick.Target = target
		tick.Timestamp = timestamp
		tick.Id = int64(updateIdSZ)
		tick.ProductType = STOCK
		if r.Deleted || tick.Low < 0.00001 || tick.Low > 99990.0 {
			//如果当前股票是停牌
			tick.Last = last
			tick.Open = last
			tick.Close = last
			tick.High = last
			tick.Low = last
			tick.Volume = 0
		} else {
			tick.Last = last
			tick.Open = open
			tick.Close = clos
			tick.High = high
			tick.Low = low
			tick.Volume = vol
		}
		lastRecordMap[target] = r
		recordData[target] = tick
		store(redis, csvwriter, target, tick)
	}
	updateIdSH++
}

func SJSXX() {
	lastHashSJSXX := []byte{}

	for {
		contentSJSXX, err := ioutil.ReadFile(*flagSZxx)
		if err != nil {
			log.Warn(err)
		} else {
			hasher := md5.New()
			hash := hasher.Sum(contentSJSXX)
			if bytes.Compare(hash, lastHashSJSXX) != 0 {
				log.Debug("SJSXX changed")
				reader := bytes.NewReader(contentSJSXX)
				records := GetRecords(reader)
				delete(records, 1)
				for _, r := range records {
					target := r.Data["XXZQDM"] + ".SZ"
					if r.Data["XXTPBZ"] == "T" {
						suspensionSZ[target] = true
					} else {
						suspensionSZ[target] = false
					}
				}
				lastHashSJSXX = hash
			} else {
				log.Debug("SJSXX unchanged")
			}
		}
		time.Sleep(sjsxxDuration)
	}
}

func main() {
	flag.Parse()
	if *flagDebug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	var r redis.Conn
	var err error
	r, err = initRedisConn()
	if err != nil {
		log.Fatal(err)
	}

	hasher := md5.New()
	lastHashSH := []byte{}
	lastHashSZ := []byte{}
	var lastDay string
	var csvWriter *csv.Writer

	go SJSXX()

	router := mux.NewRouter()
	router.HandleFunc("/stock/{target}", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Serve ", r.URL)
		vars := mux.Vars(r)
		target := vars["target"]
		t, ok := lastRecordMap[target]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		bytes, err := json.Marshal(t)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write(bytes)
	})
	go func() {
		err := http.ListenAndServe(":80", router)
		if err != nil {
			log.Fatal(err)
		}
	}()

	for {
		time.Sleep(10 * time.Millisecond)

		// check if it's trade day
		if time.Now().Weekday() == time.Saturday || time.Now().Weekday() == time.Sunday {
			time.Sleep(1 * time.Hour)
			continue
		}

		// check if it's trade time (after 9:25 before 15:05)
		if time.Now().Hour() < 9 || (time.Now().Hour() == 9 && time.Now().Minute() < 25) || (time.Now().Hour() == 15 && time.Now().Minute() > 5) || time.Now().Hour() > 15 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// change CSV filename everyday automatically
		today := time.Now().Format("20060102")
		if lastDay != today && *flagCSVPath != "null" {
			lastDay = today
			csvFileName := path.Join(*flagCSVPath, fmt.Sprintf("stock_%s.csv", today))
			fmt.Println(csvFileName)
			csvFile, err := os.OpenFile(csvFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
			defer csvFile.Close()
			csvWriter = csv.NewWriter(csvFile)
		}

		err = r.Send("MULTI")
		if err != nil {
			log.Warn("Send to redis error:", err)
		}

		contentSH, err := ioutil.ReadFile(*flagSHDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(contentSH)
			if bytes.Compare(hash, lastHashSH) != 0 {
				log.Debug("SH changed")
				reader := bytes.NewReader(contentSH)
				sendOnce_sh(r, csvWriter, reader)
				lastHashSH = hash
			} else {
				log.Debug("SH unchanged")
			}
		}

		contentSZ, err := ioutil.ReadFile(*flagSZDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(contentSZ)
			if bytes.Compare(hash, lastHashSZ) != 0 {
				log.Debug("SZ changed")
				reader := bytes.NewReader(contentSZ)
				sendOnce_sz(r, csvWriter, reader)
				lastHashSZ = hash
			} else {
				log.Debug("SZ unchanged")
			}
		}
		_, err = r.Do("EXEC")
		if err != nil {
			log.Error("Send to redis error:", err)
			rc, _ := initRedisConn()
			if rc != nil {
				r = rc
			}
		}
		if csvWriter != nil {
			csvWriter.Flush()
		}
	}
}
