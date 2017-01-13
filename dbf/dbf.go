package dbf

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"reflect"
	"strings"
	"time"
	"net/url"
	"github.com/bxy09/market"
	"github.com/bxy09/market/key"
	"context"
	"sync"
	"strconv"
)

var (
	flagSZDbf   = flag.String("dbfSZ", "SJSHQ.DBF", "SZ dbf to watch")
	flagSZxxDbf   = flag.String("dbfSZXX", "SJSXXN.DBF", "SZxx dbf to watch")
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

	sjsxxDuration = 10 * time.Second //time.Minute
	szDuration = 10 * time.Second //time.Minute
)

type FinancialType int

//各种类型的FinancialType
const (
	UNDEF FinancialType = iota
	FUTURES
	STOCK
)

type dbfKey struct {
	name string
	uid  int
}

func (k dbfKey) UID() int {
	return k.uid
}

func (k dbfKey) String() string {
	return k.name
}

type Tick struct {
	Id                           int64
	Target                       string
	Timestamp                    time.Time
	ProductType                  FinancialType
	Last, Open, High, Low, Close float64
	Volume                       float64
	Suspension                   bool

	key                          market.QKey
	Status                       string
}

func (t Tick) ToStringArray(target string) (s []string) {
	val := reflect.ValueOf(t).Elem()
	s = make([]string, val.NumField()+1)
	s[0] = target
	for i := 0; i < val.NumField(); i++ {
		value := fmt.Sprintf("%v", val.Field(i).Interface())
		s[i+1] = value
	}
	return s
}

func (t Tick) Key() market.QKey {
	return t.key
}

func (t Tick) Time() time.Time {
	return t.Timestamp
}

func (t Tick) MarshalJSON() ([]byte, error) {
	return json.Marshal(outputRecord{
		Target:      strings.TrimPrefix(t.key.String(), "stock/"),
		ProductType: 2, //for stock market
		Timestamp:   t.Timestamp,
		Open:        t.Open,
		Close:       t.Close,
		High:        t.High,
		Low:         t.Low,
		Last:        t.Last,
		Volume:      t.Volume,
		Suspension:  t.Status != "O",
		Status:      t.Status,
	})
}

func sendOnce_sz(file Interface) {
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

	result := make(map[int]*Tick)
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

		tick.key, err = key.ParseFromStr(target)
		if err != nil {
			log.Warn("ParserFromStr error:", err.Error())
		} else {
			record := &Tick{
				Id: tick.Id,
				Target: tick.Target,
				Timestamp: tick.Timestamp,
				ProductType: tick.ProductType,
				Last: tick.Last,
				Open: tick.Open,
				High: tick.High,
				Low: tick.Low,
				Close: tick.Close,
				Volume: tick.Volume,
				Suspension: tick.Suspension,
				key: tick.key,
				Status: tick.Status,
			}
			result[tick.Key().UID()] = record
		}
	}
	workingDBF.latestRecords = result
	updateIdSZ++
}

func SJSXX() {
	lastHashSJSXX := []byte{}

	for {
		contentSJSXX, err := ioutil.ReadFile(*flagSZxxDbf)
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
				workingDBF.onUpdateHandler(&dbfRecord{})
			} else {
				log.Debug("SJSXX unchanged")
			}
		}
		time.Sleep(sjsxxDuration)
	}
}

var DBFScheme = "dbf"

type dbfRecord struct {
	time                         time.Time
	key                          market.QKey
	open, high, low, close, last float64
	volume                       float64
	status                       string
}

type outputRecord struct {
	Target                       string
	Status                       string
	ProductType                  int
	Timestamp                    time.Time
	Open, High, Low, Close, Last float64
	Volume                       float64
	Suspension                   bool
}

func (record *dbfRecord) Key() market.QKey {
	return record.key
}

func (record *dbfRecord) Time() time.Time {
	return record.time
}

func (record *dbfRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(outputRecord{
		Target:      strings.TrimPrefix(record.key.String(), "stock/"),
		ProductType: 2, //for stock market
		Timestamp:   record.time,
		Open:        record.open,
		Close:       record.close,
		High:        record.high,
		Low:         record.low,
		Last:        record.last,
		Volume:      record.volume,
		Suspension:  record.status != "O",
		Status:      record.status,
	})
}

type dbf struct {
	lock          	sync.RWMutex
	//latestRecords map[int]*dbfRecord
	latestRecords 	map[int]*Tick
	onUpdateHandler	func(market.Record)
}

func (m *dbf) Run(ctx context.Context) error {
	flag.Parse()
	if *flagDebug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	hasher := md5.New()
	lastHashSZ := []byte{}
	m.latestRecords = map[int]*Tick{}
	workingDBF = m

	go SJSXX()

	for {
		time.Sleep(szDuration)

		contentSZ, err := ioutil.ReadFile(*flagSZDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(contentSZ)
			if bytes.Compare(hash, lastHashSZ) != 0 {
				log.Debug("SZ changed")
				lastHashSZ = hash
				reader := bytes.NewReader(contentSZ)
				sendOnce_sz(reader)

				for _, r := range m.latestRecords {
					m.lock.RLock()
					if m.onUpdateHandler != nil {
						m.onUpdateHandler(r)
					}
					m.lock.RUnlock()
				}
			} else {
				log.Debug("SZ unchanged")
			}
		}
	}
	return nil
}

func (m *dbf) OnUpdate(onUpdate func(market.Record)) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.onUpdateHandler = onUpdate
}

func (m *dbf) Latest(key market.QKey) market.Record {
	m.lock.RLock()
	defer m.lock.RUnlock()
	record, exist := m.latestRecords[key.UID()]
	if !exist {
		return nil
	}
	return record
}

func (m *dbf) LatestAll() []market.Record {
	m.lock.RLock()
	defer m.lock.RUnlock()
	records := make([]market.Record, len(m.latestRecords))
	for i, r := range m.latestRecords {
		records[i] = r
		fmt.Println(i, ":", r)
	}
	return records
}

var workingDBF *dbf

func initDBF(url *url.URL) (market.Market, error) {
	return &dbf{
	}, nil
}

func init() {
	market.Markets[DBFScheme] = initDBF
}
