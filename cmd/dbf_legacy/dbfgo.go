package main

import (
	"crypto/md5"
	"fmt"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
	"io"
	"strings"
)

type Interface interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type DbfHead struct {
	Version    []byte
	UpdateDate string
	Records    int64
	HeaderLen  int64
	RecordLen  int64
}

type Field struct {
	Name         string
	Type         string
	DataAddr     []byte
	Length       int64
	DecimalCount []byte
	WorkArea     []byte
}

type Record struct {
	Deleted bool
	//Data   string
	Data     map[string]string
	HashCode []byte
}

func GetDbfHead(reader Interface) (dbfhead DbfHead) {
	//fileinfo, _ := reader.Stat()
	buf := make([]byte, 16)
	reader.Seek(0, 0)
	_, err := reader.Read(buf)
	if err != nil {
		panic(err)
	}
	dbfhead.Version = buf[0:1]
	dbfhead.UpdateDate = fmt.Sprintf("%d", buf[1:4])
	dbfhead.Records = Changebytetoint(buf[4:8])
	dbfhead.HeaderLen = Changebytetoint(buf[8:10])
	dbfhead.RecordLen = Changebytetoint(buf[10:12])
	return dbfhead
}
func RemoveNullfrombyte(b []byte) (s string) {
	for _, val := range b {
		if val == 0 {
			continue
		}
		s = s + string(val)
	}
	return
}
func GetFields(reader Interface) []Field {
	dbfhead := GetDbfHead(reader)
	off := dbfhead.HeaderLen - 32
	fieldlist := make([]Field, off/32)
	buf := make([]byte, off)
	_, err := reader.ReadAt(buf, 32)
	if err != nil {
		panic(err)
	}
	curbuf := make([]byte, 32)
	for i, val := range fieldlist {
		a := i * 32
		curbuf = buf[a:]
		val.Name = RemoveNullfrombyte(curbuf[0:11])
		val.Type = fmt.Sprintf("%s", curbuf[11:12])
		val.DataAddr = curbuf[12:16]
		val.Length = Changebytetoint(curbuf[16:17])
		val.DecimalCount = curbuf[17:18]
		val.WorkArea = curbuf[20:21]
		fieldlist[i] = val
	}
	return fieldlist
}

func Changebytetoint(b []byte) (x int64) {
	for i, val := range b {
		if i == 0 {
			x = x + int64(val)
		} else {
			x = x + int64(2<<7*int64(i)*int64(val))
		}
	}
	return
}

func GetRecords(fp Interface) (records map[int]Record) {
	hasher := md5.New()
	decoder := simplifiedchinese.GBK.NewDecoder()
	dbfhead := GetDbfHead(fp)
	fp.Seek(0, 0)
	fields := GetFields(fp)
	recordlen := dbfhead.RecordLen
	start := dbfhead.HeaderLen
	buf := make([]byte, recordlen)
	i := 1
	temp := map[int]Record{}
	for {
		_, err := fp.ReadAt(buf, start)
		if err != nil {
			return temp
			panic(err)
		}
		record := Record{}
		if string(buf[0:1]) == "*" {
			record.Deleted = true
		} else if string(buf[0:1]) == " " {
			record.Deleted = false
		}
		tempdata := map[string]string{}
		a := int64(1)
		for _, val := range fields {
			fieldlen := val.Length
			fieldval := buf[a : a+fieldlen]
			decoded, _, _ := transform.Bytes(decoder, fieldval)
			s := strings.Trim(fmt.Sprintf("%s", decoded), " ")
			tempdata[val.Name] = s
			a = a + fieldlen
		}
		record.Data = tempdata
		record.HashCode = hasher.Sum(buf)
		temp[i] = record
		start = start + recordlen
		i = i + 1
	}
}

func GetRecordbyField(fieldname string, fieldval string, fp Interface) (record map[int]Record) {
	fields := GetFields(fp)
	records := GetRecords(fp)
	temp := map[int]Record{}
	i := 1
	for _, val := range records {
		for _, val1 := range fields {
			if val1.Name == fieldname && val.Deleted {
				if val.Data[val1.Name] == fieldval || val.Data[val1.Name] == " " {
					temp[i] = val
				}
			}
		}
		i = i + 1
	}
	return temp
}
