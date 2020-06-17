package main

import (
	"fmt"
	"github.com/zhangmingkai4315/mjson"
	"reflect"
	"time"
)

type testStat struct {
	Type       string  `json:"type" merge:"unique"`
	QueryCount int     `json:"queryCount" merge:"int_plus"`
	ResolveAvg float64 `json:"resolveAvg" merge:"float64_avg"`
}

type DNSInfo struct {
	ID       string     `json:"id"`
	TestStat []testStat `json:"stat" merge:"[]struct"`
}

type Person struct {
	Name      string   `json:"name" merge:"string_concat"`
	Age       int      `json:"age" merge:"int_avg"`
	Locations []string `json:"locations" merge:"append_str"`
	Salary    int      `json:"salary" merge:"int_plus"`
}

func main() {
	manager := mjson.NewMergeManager(time.Duration(1) * time.Second)

	manager.RegistType(reflect.TypeOf(DNSInfo{}), "")

	d1 := &DNSInfo{
		ID: "1",
		TestStat: []testStat{
			{
				Type:       "abc",
				QueryCount: 10,
				ResolveAvg: 1,
			}, {
				Type:       "def",
				QueryCount: 10,
				ResolveAvg: 1,
			},
		},
	}
	d2 := &DNSInfo{
		ID: "1",
		TestStat: []testStat{
			{
				Type:       "abc",
				QueryCount: 10,
				ResolveAvg: 1,
			}, {
				Type:       "gkj",
				QueryCount: 10,
				ResolveAvg: 1,
			},
		},
	}
	d3 := &DNSInfo{
		ID: "1",
		TestStat: []testStat{
			{
				Type:       "def",
				QueryCount: 10,
				ResolveAvg: 1,
			}, {
				Type:       "gkj",
				QueryCount: 10,
				ResolveAvg: 1,
			},
		},
	}
	manager.Push(d1)
	manager.Push(d2)
	manager.Push(d3)
	data := <-manager.Output
	fmt.Printf("%v", data)

}
