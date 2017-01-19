package dbf

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/bxy09/market"
	"github.com/bxy09/market/key"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type FinancialType int

//各种类型的FinancialType
const (
	UNDEF FinancialType = iota
	FUTURES
	STOCK
)

const (
	LastIdx = iota
	OpenIdx
	CloseIdx
	HighIdx
	LowIdx
	VolumeIdx
)

func (m *dbf) saveLatestAll(file Interface) {
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
		m.logger.Warn("Unknown stock format")
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
		m.logger.Warn(fmt.Sprintf("Parse %s time err. Error message:", stockFormat), err)
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
	fieldNames := []int{LastIdx, OpenIdx, CloseIdx, HighIdx, LowIdx, VolumeIdx}
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
		m.logger.Warn("Unknown stock format")
		return
	}
	for _, r := range records {
		target := r.Data[targetId] + "." + stockFormat
		targetKey, err := key.ParseFromStr("stock/" + target)
		if err != nil {
			m.logger.Warn("ParseFromStr err. Error message:", err.Error())
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
				m.logger.Warn(fmt.Sprintf("Parse %s last err. Error message:", stockFormat), err.Error())
				continue
			}
		}
		tick := m.recordData[targetUid]
		tick.key = targetKey
		tick.time = timestamp
		if m.suspension[targetUid] {
			tick.status = "Y"
		} else {
			tick.status = "O"
		}
		if m.suspension[targetUid] || tick.low < 0.00001 || tick.low > 99990.0 {
			//如果当前股票是停牌
			tick.last = fieldValues[LastIdx]
			tick.open = fieldValues[LastIdx]
			tick.close = fieldValues[LastIdx]
			tick.high = fieldValues[LastIdx]
			tick.low = fieldValues[LastIdx]
			tick.volume = 0
		} else {
			tick.last = fieldValues[LastIdx]
			tick.open = fieldValues[OpenIdx]
			tick.close = fieldValues[CloseIdx]
			tick.high = fieldValues[HighIdx]
			tick.low = fieldValues[LowIdx]
			tick.volume = fieldValues[VolumeIdx]
		}
		m.lastRecordMap[targetUid] = r
		m.recordData[targetUid] = tick
		m.latestRecords[targetUid] = &dbfRecord{
			time:   tick.time,
			last:   tick.last,
			open:   tick.open,
			high:   tick.high,
			low:    tick.low,
			close:  tick.close,
			volume: tick.volume,
			key:    tick.key,
			status: tick.status,
		}
	}
	m.updateId++
}

func (m *dbf) queryStock(ctx context.Context) error {
	hasher := md5.New()
	lastHashSJS := []byte{}
	lastHashSZ := []byte{}
	var stopDbfReader *os.File
	stopDbfReader, err := os.OpenFile(m.stockDbf, os.O_RDONLY, 0666)
	if err != nil {
		m.logger.Warn(err)
		return err
	}
	var isStopDbfName string
	var isStopDbfReader *os.File
	if m.isStopDbf != "" {
		tmp := strings.Split(m.isStopDbf, "/")
		isStopDbfName = strings.SplitN(tmp[len(tmp)-1], ".", 2)[0]
		isStopDbfReader, err = os.OpenFile(m.isStopDbf, os.O_RDONLY, 0666)
		if err != nil {
			m.logger.Warn(err)
			return err
		}
	}

	for {
		hasher.Reset()
		isStopDbfReader.Seek(0, os.SEEK_SET)
		_, err = io.Copy(hasher, isStopDbfReader)
		if err != nil {
			m.logger.Warn(err)
			continue
		}
		hash := hasher.Sum(nil)
		if bytes.Compare(hash, lastHashSJS) != 0 {
			m.logger.Debug(fmt.Sprintf("%s changed", isStopDbfName))
			isStopDbfReader.Seek(0, os.SEEK_SET)
			records := GetRecords(isStopDbfReader)
			delete(records, 1)
			m.lock.Lock()
			for _, r := range records {
				target := r.Data["XXZQDM"] + ".SZ"
				targetKey, err := key.ParseFromStr("stock/" + target)
				if err != nil {
					m.logger.Warn("ParseFromStr err. Error message:", err.Error())
				}
				targetUid := targetKey.UID()
				if r.Data["XXTPBZ"] == "Y" {
					m.suspension[targetUid] = true
				} else {
					m.suspension[targetUid] = false
				}
			}
			m.lock.Unlock()
			lastHashSJS = hash
		} else {
			m.logger.Debug(fmt.Sprintf("%s unchanged", isStopDbfName))
		}

		hasher.Reset()
		stopDbfReader.Seek(0, os.SEEK_SET)
		_, err = io.Copy(hasher, stopDbfReader)
		if err != nil {
			m.logger.Warn(err)
			continue
		}
		hash = hasher.Sum(nil)
		if bytes.Compare(hash, lastHashSZ) != 0 {
			m.logger.Debug(fmt.Sprintf("%s changed", m.stockFormat))
			stopDbfReader.Seek(0, os.SEEK_SET)
			m.saveLatestAll(stopDbfReader)
			for _, r := range m.latestRecords {
				m.lock.RLock()
				if m.onUpdateHandler != nil {
					m.onUpdateHandler(r)
				}
				m.lock.RUnlock()
			}
			lastHashSZ = hash
		} else {
			m.logger.Debug(fmt.Sprintf("%s unchanged", m.stockFormat))
		}

		timer := time.NewTimer(m.minTimeLeap)
		select {
		case <-timer.C:
			timer.Stop()
		case <-ctx.Done():
			return nil
		}
	}
}

type dbfRecord struct {
	time                         time.Time
	key                          market.QKey
	open, high, low, close, last float64
	volume                       float64
	status                       string
}

type outputRecord struct {
	Target                       string
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
		ProductType: int(STOCK), //for stock market
		Timestamp:   record.time,
		Open:        record.open,
		Close:       record.close,
		High:        record.high,
		Low:         record.low,
		Last:        record.last,
		Volume:      record.volume,
		Suspension:  record.status != "O",
	})
}

type dbf struct {
	logger            *logrus.Logger
	minTimeLeap       time.Duration
	lock              sync.RWMutex
	recordData        map[int]dbfRecord
	lastRecordMap     map[int]Record
	recordsHash       map[int][]byte
	latestRecords     map[int]*dbfRecord
	onUpdateHandler   func(market.Record)
	lastUpdateTimeStr string
	updateId          int
	suspension        map[int]bool
	stockFormat       string
	isStopDbf         string
	stockDbf          string
}

func (m *dbf) Run(ctx context.Context) error {
	m.logger.Debug(time.Now().String(), ": Try to connect with dbf server")
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
//dbf:///mnt/dbf/SJSHQ.DBF?format=sz&&isStop=SJSXXN.DBF&&debug=true&&minLeap=5s
var DBFScheme = "dbf"

//ErrIsDir 指定的路径是一个目录
var ErrIsDir = errors.New("Is dir, want file")

func initDBF(url *url.URL) (market.Market, error) {
	logger := logrus.New()
	if url.Query().Get("debug") == "true" {
		logger.Level = logrus.DebugLevel
	} else {
		logger.Level = logrus.InfoLevel
	}
	fInfo, err := os.Stat(url.Path)
	if err != nil {
		return nil, err
	}
	if fInfo.IsDir() {
		return nil, ErrIsDir
	}
	stockDbfPath := url.Path
	tmp := strings.SplitN(url.Path, "/", len(url.Path))
	tmp[len(tmp)-1] = url.Query().Get("isStop")
	isStopDbfPath := strings.Join(tmp, "/")
	ret := &dbf{
		logger:        logger,
		stockFormat:   strings.ToUpper(url.Query().Get("format")),
		isStopDbf:     isStopDbfPath,
		stockDbf:      stockDbfPath,
		recordData:    map[int]dbfRecord{},
		lastRecordMap: map[int]Record{},
		recordsHash:   map[int][]byte{},
		latestRecords: map[int]*dbfRecord{},
		suspension:    map[int]bool{},
		updateId:      0,
	}
	if du, err := time.ParseDuration(url.Query().Get("minLeap")); err == nil {
		ret.minTimeLeap = du
	} else {
		ret.minTimeLeap = time.Second
	}
	return ret, nil
}

func init() {
	market.Markets[DBFScheme] = initDBF
}
