package dbf

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"flag"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
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

	lastUpdateTimeStrSZ string

	updateIdSZ int

	recordData    = map[string]dbfRecord{}
	lastRecordMap = map[string]Record{}
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

func sendOnce_sz(file Interface) {
	records := GetRecords(file)
	timestamp := time.Now()
	var err error

	//特殊记录
	sr := records[1]
	if sr.Data["HQCJSL"] != "0" {
		//return
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
		tick.suspension = suspensionSZ[target]
		tick.time = timestamp
		tick.id = int64(updateIdSZ)
		tick.productType = STOCK
		if suspensionSZ[target] || tick.low < 0.00001 || tick.low > 99990.0 {
			//如果当前股票是停牌
			tick.last = last
			tick.open = last
			tick.close = last
			tick.high = last
			tick.low = last
			tick.volume = 0
		} else {
			tick.last = last
			tick.open = open
			tick.close = clos
			tick.high = high
			tick.low = low
			tick.volume = vol
		}
		lastRecordMap[target] = r
		recordData[target] = tick

		tick.key, err = key.ParseFromStr("stock/" + target)
		if err != nil {
			log.Warn("ParserFromStr error:", err.Error())
		} else {
			record := &dbfRecord{
				id: tick.id,
				time: tick.time,
				last: tick.last,
				open: tick.open,
				high: tick.high,
				low: tick.low,
				close: tick.close,
				volume: tick.volume,
				key: tick.key,
				status: tick.status,
				suspension: tick.suspension,
				productType: tick.productType,
			}
			workingDBF.latestRecords[tick.Key().UID()] = record
		}
	}
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
			} else {
				log.Debug("SJSXX unchanged")
			}
		}
		time.Sleep(sjsxxDuration)
	}
}

var DBFScheme = "dbf"

type dbfRecord struct {
	id							 int64
	time                         time.Time
	productType                  FinancialType
	key                          market.QKey
	open, high, low, close, last float64
	volume                       float64
	status                       string
	suspension                   bool
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

func (record *dbfRecord) Price() float64 {
	return record.close
}

func (record *dbfRecord) LPrice() float64 {
	return record.last
}

func (record *dbfRecord) MarshalJSON() ([]byte, error) {
	return json.Marshal(outputRecord{
		Target:      strings.TrimPrefix(record.key.String(), "stock/"),
		ProductType: int(record.productType), //for stock market
		Timestamp:   record.time,
		Open:        record.open,
		Close:       record.close,
		High:        record.high,
		Low:         record.low,
		Last:        record.last,
		Volume:      record.volume,
		Suspension:  record.suspension,
		Status:      record.status,
	})
}

type dbf struct {
	lock          	sync.RWMutex
	latestRecords 	map[int]*dbfRecord
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
	log.Debug(len(m.latestRecords))
	idx := 0
	for _, r := range m.latestRecords {
		records[idx] = r
		idx++
	}
	return records
}

var workingDBF *dbf

func initDBF(url *url.URL) (market.Market, error) {
	return &dbf{
		latestRecords: map[int]*dbfRecord{},
	}, nil
}

func init() {
	market.Markets[DBFScheme] = initDBF
}
