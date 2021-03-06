package fast

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Sirupsen/logrus"
	"github.com/bxy09/market"
	"github.com/bxy09/market/key"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type fast struct {
	minTimeLeap     time.Duration
	lock            sync.RWMutex
	path            string
	parameter       map[string]interface{}
	ss              snapShot
	mute            bool
	onUpdateHandler func(market.Record)
}

func (m *fast) Run(ctx context.Context) error {
	var failedForLastTime bool
	var updatedCount int
	do := func() error {
		bytes, err := ioutil.ReadFile(m.path)
		if err != nil {
			return err
		}
		ss, err := unmarshalSnapShot(bytes, m.parameter)
		if err != nil {
			return err
		}
		m.lock.Lock()
		for k, r := range ss {
			if m.ss[k] != nil && m.ss[k].Time().Equal(r.Time()) {
				delete(ss, k)
			} else {
				m.ss[k] = r
			}
		}
		m.lock.Unlock()
		updatedCount = len(ss)
		for _, r := range ss {
			m.lock.RLock()
			if m.onUpdateHandler != nil {
				m.onUpdateHandler(r)
			}
			m.lock.RUnlock()
		}
		return nil
	}
	var done bool
	for !done{
		startTime := time.Now()
		err := do()
		if err != nil {
			if failedForLastTime {
				return err
			} else {
				failedForLastTime = true
			}
		} else {
			failedForLastTime = false
		}
		costTime := time.Now().Sub(startTime)
		extraSleepTime := m.minTimeLeap - costTime
		if !m.mute {
			logrus.WithFields(logrus.Fields{
				"costTime": costTime.String(),
				"updated":  updatedCount,
			}).Info("Trace done")
		}
		if extraSleepTime > 0 {
			select {
			case <-ctx.Done():
				logrus.Info("Done done")
				done = true
			case <-time.After(extraSleepTime):
			}
		}
	}
	return nil
}

func (m *fast) OnUpdate(handler func(market.Record)) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.onUpdateHandler = handler
}

func (m *fast) Latest(key market.QKey) market.Record {
	m.lock.RLock()
	defer m.lock.RUnlock()
	record, exist := m.ss[key.UID()]
	if !exist {
		return nil
	}
	return record
}

func (m *fast) LatestAll() []market.Record {
	m.lock.RLock()
	defer m.lock.RUnlock()
	records := make([]market.Record, len(m.ss))
	idx := 0
	for _, r := range m.ss {
		records[idx] = r
		idx++
	}
	return records
}

type fastRecord struct {
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

func (record *fastRecord) Key() market.QKey {
	return record.key
}

func (record *fastRecord) Time() time.Time {
	return record.time
}

func (record *fastRecord) Price() float64 {
	return record.close
}

func (record *fastRecord) LPrice() float64 {
	return record.last
}

func (record *fastRecord) MarshalJSON() ([]byte, error) {
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
		Suspension:  record.status != "T111",
	})
}

const (
	VolumeIdx = 3
	OpenIdx   = 5
	CloseIdx  = 6
	HighIdx   = 7
	LowIdx    = 8
	LastIdx   = 9
)

//ErrShortOfWords 数据缺少字段
var ErrShortOfWords = errors.New("Short of words")

func unmarshalSnapShot(data []byte, parameters map[string]interface{}) (snapShot, error) {
	mSuffix := ".SH"
	if mktName, exist := parameters["market"]; exist {
		if mktNameStr, ok := mktName.(string); ok && mktNameStr == "SZ" {
			mSuffix = ".SZ"
		}
	}

	utfReader := transform.NewReader(bytes.NewReader(data), simplifiedchinese.GBK.NewDecoder())
	tempBuf := make([]byte, len(data)*2)
	utf8Size, err := utfReader.Read(tempBuf)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(tempBuf[:utf8Size])
	buffer := bytes.NewBufferString("")

	result := make(map[int]*fastRecord)
	var date = time.Now()
	lineIdx := 0
	for true { // lines
		ru, _, err := reader.ReadRune()
		if (err == io.EOF && buffer.Len() > 0) || (err == nil && ru == '\n') {
			words := strings.Split(buffer.String(), "|")
			if lineIdx == 0 {
				if len(words) < 7 {
					return nil, ErrShortOfWords
				}
				date, err = time.ParseInLocation("20060102-15:04:05.999", words[6], time.Local)
				if err != nil {
					return nil, err
				}
			} else {
				if len(words) < 13 {
					return nil, ErrShortOfWords
				}
				//get time out
				clock, err := time.ParseInLocation("15:04:05.999", words[len(words)-1], time.Local)
				if err != nil {
					return nil, err
				}
				y, m, d := date.Date()
				h, minute, s := clock.Clock()
				ct := time.Date(y, m, d, h, minute, s, clock.Nanosecond(), time.Local)
				//get target out
				qkey, err := key.ParseFromStr("stock/" + words[1] + mSuffix)
				if err != nil {
					return nil, err
				}
				//get price out
				var close, open, high, low, volume, last float64
				fps := []*float64{&close, &open, &high, &low, &volume, &last}
				idxs := []int{CloseIdx, OpenIdx, HighIdx, LowIdx, VolumeIdx, LastIdx}
				for i, fp := range fps {
					*fp, err = strconv.ParseFloat(strings.TrimSpace(words[idxs[i]]), 64)
					if err != nil {
						return nil, err
					}
				}
				if last > 0.0001 {
					record := &fastRecord{
						time:   ct,
						key:    qkey,
						close:  close,
						open:   open,
						high:   high,
						low:    low,
						last:   last,
						volume: volume,
						status: strings.TrimSpace(words[len(words)-2]),
					}
					result[record.Key().UID()] = record
				} else {
					logrus.WithField("target", qkey.String()).Warn("扫描数据时发现有对象的 Last 字段为零, 提过该对象")
				}
			}
			buffer.Reset()
			lineIdx++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if ru == '\r' || ru == '\n' {
			continue
		}
		buffer.WriteRune(ru)
	}
	return snapShot(result), nil
}

type snapShot map[int]*fastRecord

//FastScheme Fast 格式的市场数据, 完整的路径定义为:
// fast:///mnt/data/mkdt001.txt
var FastScheme = "fast"

//ErrIsDir 指定的路径是一个目录
var ErrIsDir = errors.New("Is dir, want file")

func initFast(url *url.URL) (market.Market, error) {
	fInfo, err := os.Stat(url.Path)
	if err != nil {
		return nil, err
	}
	if fInfo.IsDir() {
		return nil, ErrIsDir
	}
	bytes, err := ioutil.ReadFile(url.Path)
	if err != nil {
		return nil, err
	}
	var parameter map[string]interface{} = nil
	if url.Query().Get("market") == "SZ" {
		parameter = map[string]interface{}{"market": "SZ"}
	}
	ss, err := unmarshalSnapShot(bytes, parameter)
	if err != nil {
		return nil, err
	}
	ret := &fast{path: url.Path, ss: ss, parameter: parameter}
	if du, err := time.ParseDuration(url.Query().Get("minLeap")); err == nil {
		ret.minTimeLeap = du
	} else {
		ret.minTimeLeap = time.Second
	}
	if url.Query().Get("mute") != "" {
		ret.mute = true
	}
	return ret, nil
}

func init() {
	market.Markets[FastScheme] = initFast
}
