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
	"errors"
	"fmt"
	"os"
)

var (
	flagDebug   = flag.Bool("debug", false, "debug mode")

	queryDuration = time.Minute
)

type FinancialType int

//各种类型的FinancialType
const (
	UNDEF FinancialType = iota
	FUTURES
	STOCK
)

const (
	LAST = iota
	OPEN
	CLOSE
	HIGH
	LOW
	VOLUME
)

func (m *dbf)saveLatestAll(file Interface) {
	m.lock.Lock()
	m.lock.Unlock()
	records := GetRecords(file)
	timestamp := time.Now()
	var err error
	stockFormat := m.stockFormat

	//特殊记录
	var closeField, dateFieldA, dateFieldB string
	if stockFormat == "SZ" {
		closeField = "HQCJSL"
		dateFieldA = "HQCJBS"
		dateFieldB = "HQZQJC"
	} else if stockFormat == "SH" {
		closeField = "S11"
		dateFieldA = "S2"
		dateFieldB = "S6"
	} else {
		log.Warn("Unknown stock format")
		return
	}
	sr := records[1]
	if sr.Data[closeField] != "0" {
		//return
	}
	timeStr := strings.Trim(sr.Data[dateFieldA], " ")
	if len(timeStr) < 6 {
		timeStr = "0" + timeStr
	}
	dateTimeStr := strings.Trim(sr.Data[dateFieldB], " ") + timeStr
	loc, _ := time.LoadLocation("Asia/Shanghai")
	timestamp, err = time.ParseInLocation("20060102150405", dateTimeStr, loc)
	if err != nil {
		log.Warn(fmt.Sprintf("Parse %s time err. Error message:", stockFormat), err)
		return
	}
	if dateTimeStr == m.lastUpdateTimeStr {
		//TODO 目前不做处理
		//timestamp.Add(1000)
	} else {
		m.lastUpdateTimeStr = dateTimeStr
	}
	//取完信息后删除特殊记录
	delete(records, 1)

	targetFieldsSZ := []string{"HQZRSP", "HQJRKP", "HQZJCJ", "HQZGCJ", "HQZDCJ", "HQCJSL"}
	targetFieldsSH := []string{"S3", "S4", "S8", "S6", "S7", "S11"}
	fieldNames := []int{LAST, OPEN, CLOSE, HIGH, LOW, VOLUME}
	var targetId string
	var targetFields []string
	fieldValues := map[int]float64{}
	if stockFormat == "SZ" {
		targetFields = targetFieldsSZ
		targetId = "HQZQDM"
	} else if stockFormat == "SH" {
		targetFields = targetFieldsSH
		targetId = "S1"
	} else {
		log.Warn("Unknown stock format")
		return
	}
	for _, r := range records {
		target := r.Data[targetId] + "." + stockFormat
		targetKey, err := key.ParseFromStr("stock/" + target)
		if err != nil {
			log.Warn("ParseFromStr err. Error message:", err.Error())
			continue
		}
		targetUid := targetKey.UID()
		hash := r.HashCode
		lastHash, exist := m.recordsHash[targetUid]
		if !exist || bytes.Compare(hash, lastHash) != 0 {
			m.recordsHash[targetUid] = hash
		} else {
			continue
		}
		for i, field := range targetFields {
			fieldValues[fieldNames[i]], err = strconv.ParseFloat(r.Data[field], 64)
			if err != nil {
				log.Warn(fmt.Sprintf("Parse %s last err. Error message:", stockFormat), err.Error())
				continue
			}
		}
		tick := m.recordData[targetUid]
		tick.key = targetKey
		tick.suspension = m.suspension[targetUid]
		tick.time = timestamp
		tick.id = int64(m.updateId)
		tick.productType = STOCK
		if m.suspension[targetUid] || tick.low < 0.00001 || tick.low > 99990.0 {
			//如果当前股票是停牌
			tick.last = fieldValues[LAST]
			tick.open = fieldValues[LAST]
			tick.close = fieldValues[LAST]
			tick.high = fieldValues[LAST]
			tick.low = fieldValues[LAST]
			tick.volume = 0
		} else {
			tick.last = fieldValues[LAST]
			tick.open = fieldValues[OPEN]
			tick.close = fieldValues[CLOSE]
			tick.high = fieldValues[HIGH]
			tick.low = fieldValues[LOW]
			tick.volume = fieldValues[VOLUME]
		}
		m.lastRecordMap[targetUid] = r
		m.recordData[targetUid] = tick
		m.latestRecords[targetUid] = &dbfRecord{
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
	}
	m.updateId++
}

func (m *dbf)queryStock(ctx context.Context) error {
	stockDbf := m.stockDbf
	stockFormat := m.stockFormat
	hasher := md5.New()
	lastHash := []byte{}

	for {
		content, err := ioutil.ReadFile(stockDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(content)
			if bytes.Compare(hash, lastHash) != 0 {
				log.Debug(fmt.Sprintf("%s changed", stockFormat))
				lastHash = hash
				reader := bytes.NewReader(content)
				m.saveLatestAll(reader)
				for _, r := range m.latestRecords {
					m.lock.RLock()
					if m.onUpdateHandler != nil {
						m.onUpdateHandler(r)
					}
					m.lock.RUnlock()
				}
			} else {
				log.Debug(fmt.Sprintf("%s unchanged", stockFormat))
			}
		}

		timer := time.NewTimer(queryDuration)
		select {
		case <-timer.C:
			timer.Stop()
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *dbf)checkStock(ctx context.Context) error {
	isStopDbf := m.isStopDbf
	hasher := md5.New()
	lastHash := []byte{}
	dbfName := strings.SplitN(isStopDbf, ".", 2)[0]

	for {
		content, err := ioutil.ReadFile(isStopDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(content)
			if bytes.Compare(hash, lastHash) != 0 {
				log.Debug(fmt.Sprintf("%s changed", dbfName))
				reader := bytes.NewReader(content)
				records := GetRecords(reader)
				delete(records, 1)
				m.lock.Lock()
				for _, r := range records {
					target := r.Data["XXZQDM"] + ".SZ"
					targetKey, err := key.ParseFromStr("stock/" + target)
					if err != nil {
						log.Warn("ParseFromStr err. Error message:", err.Error())
					}
					targetUid := targetKey.UID()
					if r.Data["XXTPBZ"] == "T" {
						m.suspension[targetUid] = true
					} else {
						m.suspension[targetUid] = false
					}
				}
				m.lock.Unlock()
				lastHash = hash
			} else {
				log.Debug(fmt.Sprintf("%s unchanged", dbfName))
			}
		}

		timer := time.NewTimer(queryDuration)
		select {
		case <-timer.C:
			timer.Stop()
		case <-ctx.Done():
			return nil
		}
	}
}

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
	lock          		sync.RWMutex
	recordData   		map[int]dbfRecord
	lastRecordMap		map[int]Record
	recordsHash 		map[int][]byte
	latestRecords		map[int]*dbfRecord
	onUpdateHandler		func(market.Record)
	lastUpdateTimeStr 	string
	updateId 			int
	suspension			map[int]bool
	stockFormat			string
	isStopDbf			string
	stockDbf			string
}

func (m *dbf) Run(ctx context.Context) error {
	log.Debug(time.Now().String(), ": Try to connect with dbf server")

	if m.isStopDbf != "" {
		go m.checkStock(ctx)
	}
	if m.stockFormat != "" {
		go m.queryStock(ctx)
	}
	select {
	case <-ctx.Done():
		return nil
	}
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
	idx := 0
	for _, r := range m.latestRecords {
		records[idx] = r
		idx++
	}
	return records
}

//DBFScheme Dbf 格式的市场数据, 完整的路径定义为:
//dbf:///mnt/dbf/SJSHQ.DBF?format=sz&&isStop=SJSXXN.DBF
var DBFScheme = "dbf"

//ErrIsDir 指定的路径是一个目录
var ErrIsDir = errors.New("Is dir, want file")

func initDBF(url *url.URL) (market.Market, error) {
	flag.Parse()
	if *flagDebug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	fInfo, err := os.Stat(url.Path)
	if err != nil {
		return nil, err
	}
	if fInfo.IsDir() {
		return nil, ErrIsDir
	}
	tmp := strings.Split(url.Path, "/")
	stockDbfFile := tmp[len(tmp)-1]
	return &dbf{
		stockFormat	:	strings.ToUpper(url.Query().Get("format")),
		isStopDbf:		url.Query().Get("isStop"),
		stockDbf:		stockDbfFile,
		recordData:		map[int]dbfRecord{},
		lastRecordMap:	map[int]Record{},
		recordsHash:	map[int][]byte{},
		latestRecords: 	map[int]*dbfRecord{},
		suspension: 	map[int]bool{},
		updateId:		0,
	}, nil
}

func init() {
	market.Markets[DBFScheme] = initDBF
}
