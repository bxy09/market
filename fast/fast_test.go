package fast

import (
	"io/ioutil"
	"testing"
	"github.com/bxy09/market"
	"net/url"
	"os/exec"
	"time"
	"context"
	"sync"
)

func TestUnmarshalSnapShot(t *testing.T) {
	bytes, err := ioutil.ReadFile("sample.txt")
	if err != nil {
		t.Fatal(err)
	}
	ss, err := unmarshalSnapShot(bytes, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range ss {
		t.Log(k, v.Key(), v.Time())
		bytes, err := v.MarshalJSON()
		if err != nil {
			t.Error(err)
		} else {
			t.Log(string(bytes))
		}
	}
}

func TestTraceFast(t *testing.T) {
	err := exec.Command("cp", "sample.txt", "/tmp/trace.txt").Run()
	if err != nil {
		t.Fatal(err)
	}

	url, err := url.Parse("fast:///tmp/trace.txt?minLeap=0.1s")
	m, err := market.TraceMarket(url)
	if err != nil {
		t.Fatal(err)
	}
	err = exec.Command("cp", "sample.txt.updated","/tmp/trace.txt").Run()
	if err != nil {
		t.Fatal(err)
	}
	ctx, canceler := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func () {
		err := m.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		wg.Done()
	}()
	got := make(chan market.Record)
	m.OnUpdate(func(r market.Record){
		got<-r
	})
	select {
	case r:=<-got:
		t.Log(r)
	case <-time.After(time.Minute):
		t.Error("Do not got updated info")
	}
	canceler()
	wg.Wait()
}
