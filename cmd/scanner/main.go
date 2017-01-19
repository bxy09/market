package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/bxy09/market"
	_ "github.com/bxy09/market/dbf"
	_ "github.com/bxy09/market/fast"
	//_ "github.com/bxy09/market/tdf"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"os"
	"time"
)

func main() {
	flag.Parse()
	if flag.NArg() != 3 {
		logrus.Error("Usage ./xxx url redis_addr dir")
		os.Exit(1)
	}
	u, err := url.Parse(flag.Arg(0))
	if err != nil {
		logrus.Error("Illegal url format:", err.Error())
		os.Exit(2)
	}
	m, err := market.TraceMarket(u)
	if err != nil {
		logrus.Error("Failed to trace:", err.Error())
		os.Exit(3)
	}
	ctx, _ := context.WithCancel(context.Background())
	pool := redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", flag.Arg(1))
	}, 3)
	conn := pool.Get()
	_, err = conn.Do("PING")
	if err != nil {
		logrus.Error("Failed to connect redis! : ", flag.Arg(1), " : ", err.Error())
		os.Exit(4)
	}
	dirPath := flag.Arg(2)
	dInfo, err := os.Stat(dirPath)
	if err != nil {
		logrus.Error("Failed to get dir path info:", err.Error())
		os.Exit(4)
	}
	if !dInfo.IsDir() {
		logrus.Error("The path is not a dir")
		os.Exit(5)
	}
	var fH *os.File
	var fHTime time.Time = time.Unix(0, 0)
	forceDo := make(chan bool)
	records := make(chan market.Record, 4000)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for true {
			select {
			case <-forceDo:
			case <-ticker.C:
			}
			conn := pool.Get()
			_, err := conn.Do("PING")
			if err != nil {
				conn = pool.Get()
			}
			var sendDone bool = false
			for !sendDone {
				select {
				case record := <-records:
					bytes, err := record.MarshalJSON()
					if err != nil {
						logrus.Error("Failed to marshal record: ", err.Error())
						continue
					}
					ry, rm, rd := record.Time().Date()
					cy, cm, cd := fHTime.Date()
					if ry == cy && rm == cm && rd == cd && fH != nil {
					} else {
						fHTime = record.Time()
						if fH != nil {
							err = fH.Close()
							logrus.Info("Close the last file:", err)
						}
						fH, err = os.OpenFile(dirPath+"/"+fHTime.Format("2006-01-02"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
						if err != nil {
							logrus.Error("Failed to open file:", err.Error())
						}
					}
					if fH != nil {
						fH.Write(bytes)
						fH.WriteString("\n")
					}
					err = conn.Send("publish", record.Key().String(), string(bytes))
					if err != nil {
						panic("Redis connection is invalid:" + err.Error())
					}
					err = conn.Send("set", record.Key().String(), string(bytes))
					if err != nil {
						panic("Redis connection is invalid:" + err.Error())
					}
				default:
					err := conn.Flush()
					if err != nil {
						panic("Redis connection is invalid:" + err.Error())
					}
					sendDone = true
				}
			}
		}
	}()
	m.OnUpdate(func(record market.Record) {
		var sent bool
		for !sent {
			select {
			case records <- record:
				sent = true
			default:
				forceDo <- true
			}
		}
	})
	err = m.Run(ctx)
	if err != nil {
		fmt.Println(time.Now().String(), ": Running failed:", err.Error())
		os.Exit(5)
	}
	fmt.Println("Running quit")
	return
}
