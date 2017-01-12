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
	_ "github.com/bxy09/market/key"
	"context"
	"sync"
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

	sjsxxDuration = time.Second //time.Minute
	szDuration = 3 * time.Second //time.Minute
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
				workingDBF.onUpdate(&dbfRecord{})
			} else {
				log.Debug("SJSXX unchanged")
			}
		}
		time.Sleep(sjsxxDuration)
	}
}

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
	lock          sync.RWMutex
	latestRecords map[int]*dbfRecord
	onUpdate      func(market.Record)
}

func (t *dbf) Run(ctx context.Context) error {
	flag.Parse()
	if *flagDebug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	hasher := md5.New()
	lastHashSH := []byte{}

	workingDBF = t
	go SJSXX()

	for {
		time.Sleep(szDuration)

		contentSH, err := ioutil.ReadFile(*flagSZDbf)
		if err != nil {
			log.Warn(err)
		} else {
			hash := hasher.Sum(contentSH)
			if bytes.Compare(hash, lastHashSH) != 0 {
				log.Debug("SZ changed")
				lastHashSH = hash
				t.onUpdate(&dbfRecord{})
			} else {
				log.Debug("SZ unchanged")
			}
		}
	}
	return nil
}

func (t *dbf) OnUpdate(onUpdate func(market.Record)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.onUpdate = onUpdate
}
func (t *dbf) Latest(market.QKey) market.Record {
	return nil
}
func (t *dbf) LatestAll() []market.Record {
	return nil
}

var workingDBF *dbf

func initDBF(url *url.URL) (market.Market, error) {
	return &dbf{
	}, nil
}

func init() {
	market.Markets[DBFScheme] = initDBF
}
