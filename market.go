package market

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"time"
)

//Record 某个股票某一时刻的记录
type Record interface {
	json.Marshaler
	Key() QKey
	Time() time.Time
}

//QKey 快速键, 具有可读的名称, 同时也有快速查询所用的唯一ID
type QKey interface {
	UID() int
	String() string
}

//Market 市场数据
type Market interface {
	Run(ctx context.Context) error
	OnUpdate(func(Record))
	Latest(QKey) Record
	LatestAll() []Record
}

//Markets 所有支持的数据类型
var Markets = map[string]func(url *url.URL) (Market, error){}

//ErrBadScheme 错误的数据协议
var ErrBadScheme = errors.New("Bad Scheme for market")

//TraceMarket 通过指定配置方式,跟踪市场
//可能的报错包括 ErrBadScheme
func TraceMarket(url *url.URL) (Market, error) {
	if initiater, ok := Markets[url.Scheme]; ok {
		return initiater(url)
	}
	return nil, ErrBadScheme
}
