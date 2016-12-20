package key_test

import (
	"github.com/bxy09/market/key"
	"testing"
)

func TestKeyParse(t *testing.T) {
	testPair := func(name string, uid int) {
		k, err := key.ParseFromStr(name)
		if err != nil {
			t.Fatal(err)
		}
		if k.UID() != uid {
			t.Fatal(k.UID(), name)
		}
		if k.String() != name {
			t.Fatal(k.String(), name)
		}
		k2, err := key.ParseFromUID(uid)
		if err != nil {
			t.Fatal(err, uid, name)
		}
		if k2.String() != name {
			t.Fatal(k2.String(), name)
		}
		if k2.String() != name {
			t.Fatal(k2.String(), name)
		}
	}
	testPair("stock/000001.SZ", 100000100)
	testPair("stock/000001.SH", 100)
	testPair("stock/600000.SH", 60000000)
	wrongSts := []string{
		"a/000.SZ",
		"000001.SZ",
		"stock/000001.SN",
	}
	for _, str := range wrongSts {
		_, err := key.ParseFromStr(str)
		if err == nil {
			t.Error(str)
		}
	}
	wrongUIDs := []int{
		700000300,
	}
	for _, uid := range wrongUIDs {
		_, err := key.ParseFromUID(uid)
		if err == nil {
			t.Fatal(uid)
		}
	}
}
