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
	flagSHDbf   = flag.String("dbfSH", "SHOW2003.DBF", "SH dbf to watch")
	flagSZDbf   = flag.String("dbfSZ", "SJSHQ.DBF", "SZ dbf to watch")
	flagSZxxDbf = flag.String("dbfSZXX", "SJSXXN.DBF", "SZxx dbf to watch")
	flagDebug   = flag.Bool("debug", false, "debug mode")

	szxxDuration = 10 * time.Second //time.Minute
	szDuration = 10 * time.Second //time.Minute
	shDuration = 10 * time.Second //time.Minute
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

func (m *dbf)saveLatest(stockId string, file Interface) {
	records := GetRecords(file)
	timestamp := time.Now()
	var err error

	//特殊记录
	var closeField, dateFieldA, dateFieldB string
	if stockId == "SZ" {
		closeField = "HQCJSL"
		dateFieldA = "HQCJBS"
		dateFieldB = "HQZQJC"
	} else if stockId == "SH" {
		closeField = "S11"
		dateFieldA = "S2"
		dateFieldB = "S6"
	} else {
		log.Warn("Unknown stock")
		return
	}
	sr := records[1]
	if sr.Data[closeField] != "0" {
		return
	}
	timeStr := strings.Trim(sr.Data[dateFieldA], " ")
	if len(timeStr) < 6 {
		timeStr = "0" + timeStr
	}
	dateTimeStr := strings.Trim(sr.Data[dateFieldB], " ") + timeStr
	loc, _ := time.LoadLocation("Asia/Shanghai")
	timestamp, err = time.ParseInLocation("20060102150405", dateTimeStr, loc)
	if err != nil {
		log.Warn(fmt.Sprintf("Parse %s time err. Error message:", stockId), err)
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

	var targetId string
	var updateId *int
	targetFieldsSZ := []string{"HQZRSP", "HQJRKP", "HQZJCJ", "HQZGCJ", "HQZDCJ", "HQCJSL"}
	targetFieldsSH := []string{"S3", "S4", "S8", "S6", "S7", "S11"}
	var targetFields []string
	fieldNames := []int{LAST, OPEN, CLOSE, HIGH, LOW, VOLUME}
	fieldValues := map[int]float64{}
	if stockId == "SZ" {
		targetFields = targetFieldsSZ
		targetId = "HQZQDM"
		updateId = &m.updateIdSZ
	} else if stockId == "SH" {
		targetFields = targetFieldsSH
		targetId = "S1"
		updateId = &m.updateIdSH
	} else {
		log.Warn("Unknown stock")
		return
	}
	for _, r := range records {
		target := r.Data[targetId] + "." + stockId
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
				log.Warn(fmt.Sprintf("Parse %s last err. Error message:", stockId), err.Error())
				continue
			}
		}
		tick := m.recordData[targetUid]
		tick.key = targetKey
		tick.suspension = m.suspension[targetUid]
		tick.time = timestamp
		tick.id = int64(*updateId)
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
	(*updateId)++
}

func (m *dbf)queryStock(stockId string, ctx context.Context) error {
	var flagDbf *string
	var duration time.Duration
	stockId = strings.ToUpper(stockId)
	if stockId == "SZ" {
		flagDbf = flagSZDbf
		duration = szDuration
	} else if stockId == "SH" {
		flagDbf = flagSHDbf
		duration = shDuration
	} else {
		log.Warn("Unknown stock")
		return errors.New("Unknown stock")
	}
	hasher := md5.New()
	lastHash := []byte{}

	for {
		content, err := ioutil.ReadFile(*flagDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(content)
			if bytes.Compare(hash, lastHash) != 0 {
				log.Debug(fmt.Sprintf("%s changed", stockId))
				lastHash = hash
				reader := bytes.NewReader(content)
				m.saveLatest(stockId, reader)
				for _, r := range m.latestRecords {
					m.lock.RLock()
					if m.onUpdateHandler != nil {
						m.onUpdateHandler(r)
					}
					m.lock.RUnlock()
				}
			} else {
				log.Debug(fmt.Sprintf("%s unchanged", stockId))
			}
		}

		timer := time.NewTimer(duration)
		select {
		case <-timer.C:
			timer.Stop()
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *dbf)checkStock(ctx context.Context) error {
	hasher := md5.New()
	lastHash := []byte{}

	for {
		content, err := ioutil.ReadFile(*flagSZxxDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(content)
			if bytes.Compare(hash, lastHash) != 0 {
				log.Debug("SJSXX changed")
				reader := bytes.NewReader(content)

				records := GetRecords(reader)
				delete(records, 1)
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
				lastHash = hash
			} else {
				log.Debug("SJSXX unchanged")
			}
		}

		timer := time.NewTimer(szxxDuration)
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
	updateIdSZ 			int
	updateIdSH 			int
	suspension			map[int]bool
	stockId				string
	isStopDbf			string
}

func (m *dbf) Run(ctx context.Context) error {
	log.Debug(time.Now().String(), ": Try to connect with dbf server")

	if m.isStopDbf != "" {
		go m.checkStock(ctx)
	}
	if m.stockId != "" {
		go m.queryStock(m.stockId, ctx)
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
	return &dbf{
		stockId	:		url.Query().Get("format"),
		isStopDbf:		url.Query().Get("isStop"),
		recordData:		map[int]dbfRecord{},
		lastRecordMap:	map[int]Record{},
		recordsHash:	map[int][]byte{},
		latestRecords: 	map[int]*dbfRecord{},
		suspension: 	map[int]bool{},
		updateIdSZ:		0,
		updateIdSH:		0,
	}, nil
}

func init() {
	market.Markets[DBFScheme] = initDBF
}
