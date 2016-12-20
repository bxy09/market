package tdf

// #cgo CFLAGS: -I${SRCDIR}/include
// #cgo LDFLAGS: -s -L${SRCDIR}/lib -lreader -lrt -lTDFAPI_v2.7
// #include "reader.h"
import "C"
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/bxy09/market"
	"github.com/bxy09/market/key"
	"net/url"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var windIndexTransTable = map[string]string{
	"999999.SH": "000001.SH",
	"999998.SH": "000002.SH",
	"999997.SH": "000003.SH",
	"999996.SH": "000004.SH",
	"999995.SH": "000005.SH",
	"999994.SH": "000006.SH",
	"999993.SH": "000007.SH",
	"999992.SH": "000008.SH",
	"999991.SH": "000010.SH",
	"999990.SH": "000011.SH",
	"999989.SH": "000012.SH",
	"999988.SH": "000013.SH",
	"999987.SH": "000016.SH",
	"999986.SH": "000015.SH",
	"000300.SH": "000300.SH",
}

func handle(gName string, status C.char, cdate C.int, clock C.int, close C.int, preClose C.int, open C.int, high C.int, low C.int, volume C.int) {
	gClock := int(clock)
	gStatus := byte(status)
	gDate := int(cdate)
	gClose := float64(int(close)) / 10000
	gPreClose := float64(int(preClose)) / 10000
	gOpen := float64(int(open)) / 10000
	gHigh := float64(int(high)) / 10000
	gLow := float64(int(low)) / 10000
	gVolume := float64(volume)
	key, err := key.ParseFromStr("stock/" + gName)
	if err != nil {
		logrus.WithField("keyInput", gName).Error("Failed to parser the target:", err.Error())
		return
	}
	gY := gDate / 10000
	gM := time.Month(gDate % 10000 / 100)
	gD := gDate % 100
	gH := gClock / 10000000
	gMinute := gClock % 10000000 / 100000
	gS := gClock % 100000 / 1000
	date := time.Date(gY, gM, gD, gH, gMinute, gS, 0, time.Local)
	if gPreClose < 0.001 {
		return
	}
	if gClose < 0.001 {
		gClose = gPreClose
		gHigh = gPreClose
		gLow = gPreClose
		gOpen = gPreClose
	}
	if workingTDF != nil && workingTDF.onUpdate != nil {
		workingTDF.onUpdate(&tdfRecord{
			status: string(gStatus),
			close:  gClose,
			open:   gOpen,
			high:   gHigh,
			low:    gLow,
			volume: gVolume,
			last:   gPreClose,
			key:    key,
			time:   date,
		})
	}
}

//export GoRecvIdx
func GoRecvIdx(name *C.char, status C.char, cdate C.int, clock C.int, close C.int, preClose C.int, open C.int, high C.int, low C.int, volume C.int) {
	gName := C.GoString(name)
	if strings.HasSuffix(gName, ".SH") {
		if tName, ok := windIndexTransTable[gName]; ok {
			gName = tName
		} else {
			gName = strings.TrimPrefix(gName, "99")
			if len(gName) == 7 {
				gName = "00" + gName
			}
		}
	}
	handle(gName, status, cdate, clock, close, preClose, open, high, low, volume)
}

//export GoRecvMkt
func GoRecvMkt(name *C.char, status C.char, cdate C.int, clock C.int, close C.int, preClose C.int, open C.int, high C.int, low C.int, volume C.int) {
	gName := C.GoString(name)
	handle(gName, status, cdate, clock, close, preClose, open, high, low, volume)
}

var disconnected = make(chan bool)

//export GoRecvDisconnect
func GoRecvDisconnect() {
	fmt.Println("Receive disconnectd signal")
	select {
	case disconnected <- true:
	default:
		fmt.Println("disconnected signal blocked")
	}
}

//FastScheme Fast 格式的市场数据, 完整的路径定义为:
// tdf://username:password@host:port
var TDFScheme = "tdf"

// ErrNoUser 没有用户信息
var ErrNoUser = errors.New("No user info")

// ErrBusy 该服务忙, TDF一次只能承担一个连接
var ErrBusy = errors.New("tdf is busy")

type tdfRecord struct {
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

func (record *tdfRecord) Key() market.QKey {
	return record.key
}

func (record *tdfRecord) Time() time.Time {
	return record.time
}

func (record *tdfRecord) Price() float64 {
	return record.close
}

func (record *tdfRecord) LPrice() float64 {
	return record.last
}

func (record *tdfRecord) MarshalJSON() ([]byte, error) {
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

type tdf struct {
	host          string
	port          string
	uname         string
	password      string
	handler       unsafe.Pointer
	lock          sync.RWMutex
	latestRecords map[int]*tdfRecord
	onUpdate      func(market.Record)
}

//ErrFailedToStart 无法建立连接
var ErrFailedToStart = errors.New("Failed to start session")
var ErrDisconnected = errors.New("Disconnected")

func (t *tdf) Run(ctx context.Context) error {
	select {
	case <-tdfAvailable:
	default:
		return ErrBusy
	}
	workingTDF = t
	defer func() {
		tdfAvailable <- true
	}()
	fmt.Println(time.Now().String(), ": Try to connect with tdf server:", t.host, ":", t.port)
	handler := C.trace(C.CString(t.host), C.CString(t.port), C.CString(t.uname), C.CString(t.password))
	if handler == nil {
		return ErrFailedToStart
	}
	fmt.Println("Waiting for signales")
	var err error
	select {
	case <-ctx.Done():
	case <-disconnected:
		err = ErrDisconnected
	}
	C.close_handler(handler)
	return err
}

func (t *tdf) OnUpdate(onUpdate func(market.Record)) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.onUpdate = onUpdate
}
func (t *tdf) Latest(market.QKey) market.Record {
	return nil
}
func (t *tdf) LatestAll() []market.Record {
	return nil
}

var tdfAvailable = make(chan bool, 1)
var workingTDF *tdf

func initTDF(url *url.URL) (market.Market, error) {
	if url.User == nil {
		return nil, ErrNoUser
	}
	uname := url.User.Username()
	password, _ := url.User.Password()
	words := strings.SplitN(url.Host, ":", 2)
	host := words[0]
	port := "80"
	if len(words) > 1 {
		port = words[1]
	}
	return &tdf{
		uname:    uname,
		password: password,
		host:     host,
		port:     port,
	}, nil
}

func init() {
	market.Markets[TDFScheme] = initTDF
	tdfAvailable <- true
}
