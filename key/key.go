package key

import (
	"errors"
	"fmt"
	"github.com/bxy09/market"
	"regexp"
)

//囊括基本的金融对象编码规则
//000 00000000
//头三位代表市场类型, 后面八位代表标的名称,不同的市场类型采用不同的对象编码

//000 中国沪市 pairKey
//001 中国深市 pairKey

var ErrNoSuchMkt = errors.New("no such market")

func ParseFromUID(uid int) (market.QKey, error) {
	mkt := uid / 100000000
	switch mkt {
	case 0:
		return pairKey{uid: uid, name: fmt.Sprintf("stock/%06d.SH", uid%100000000/100)}, nil
	case 1:
		return pairKey{uid: uid, name: fmt.Sprintf("stock/%06d.SZ", uid%100000000/100)}, nil
	default:
		return nil, ErrNoSuchMkt
	}
}

func ParseFromStr(str string) (market.QKey, error) {
	if pairRegexp.MatchString(str) {
		var code int
		var symbol rune
		_, err := fmt.Sscanf(str, "stock/%06d.S%c", &code, &symbol)
		if err != nil {
			return nil, err
		}
		mkt := 0
		if symbol == 'Z' {
			mkt = 1
		}
		return pairKey{name: str, uid: mkt*100000000 + code*100}, nil
	}
	return nil, ErrNoSuchMkt
}

var pairRegexp = regexp.MustCompile("stock/([0-9]{6}).S[ZH]")

//pairKey 后续八位前六位直接采用对应股票的编号,后两位补零
type pairKey struct {
	name string
	uid  int
}

func (k pairKey) UID() int {
	return k.uid
}

func (k pairKey) String() string {
	return k.name
}
