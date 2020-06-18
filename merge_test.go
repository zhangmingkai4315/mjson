package mjson

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type info struct {
	Total int `json:"total" merge:"int_plus"`
}


type testStat struct {
	Type       string  `json:"type" merge:"unique"`
	QueryCount int     `json:"queryCount" merge:"int_plus"`
	ResolveAvg float64 `json:"resolveAvg" merge:"float64_avg"`
}

type DNSInfo struct {
	ID       string     `json:"id"`
	Info     info       `json:"info" merge:"struct"`
	TestStat []testStat `json:"stat" merge:"[]struct"`
}

func TestNewMergeManager(t *testing.T) {
	newManager, err  := NewMergeManager(time.Duration(1)*time.Second, reflect.TypeOf(DNSInfo{}))
	if err != nil{
		t.Errorf("TestNewMergeManager return a error :%s", err)
	}
	if newManager == nil{
		t.Error("TestNewMergeManager return a nil manager ")
	}

	if len(newManager.mergeFunc) != 6{
		t.Errorf("hash key and merge function should have 6 items but got %d", len(newManager.mergeFunc))
	}
	if newManager.readyForMerge == false{
		t.Error("after regist a new struct success, readyForMerge should return true")
	}

	newManager, err  = NewMergeManager(time.Duration(1)*time.Second, reflect.TypeOf("hello"))
	if err == nil || newManager != nil{
		t.Errorf("create a merge manager with not struct type should return nil and with a error")
	}
}


func TestMergeManager_Push(t *testing.T) {
	newManager, _ := NewMergeManager(time.Duration(1)*time.Millisecond, reflect.TypeOf(DNSInfo{}))

	d1 := &DNSInfo{
		ID: "1",
		Info: info{Total:20},
		TestStat: []testStat{
			{
				Type:       "abc",
				QueryCount: 10,
				ResolveAvg: 1.2,
			}, {
				Type:       "def",
				QueryCount: 10,
				ResolveAvg: 2.0,
			},
		},
	}
	d2 := &DNSInfo{
		ID: "1",
		Info: info{Total:60},
		TestStat: []testStat{
			{
				Type:       "abc",
				QueryCount: 10,
				ResolveAvg: 1.6,
			}, {
				Type:       "gkj",
				QueryCount: 50,
				ResolveAvg: 1.6,
			},
		},
	}
	d3 := &DNSInfo{
		ID: "1",
		Info: info{Total:20},
		TestStat: []testStat{
			{
				Type:       "def",
				QueryCount: 10,
				ResolveAvg: 1.0,
			}, {
				Type:       "gkj",
				QueryCount: 10,
				ResolveAvg: 1.0,
			},
		},
	}
	newManager.Push(d1)
	// it will wait for one millsecond get the value
	item := <-newManager.Output
	assert.Equal(t, *d1, item)

	newManager.Push(d1)
	newManager.Push(d2)
	newManager.Push(d3)
	// it will wait for one millsecond get the value
	item = <-newManager.Output

	result := DNSInfo{
		ID:"1",
		Info:info{
			Total:100,
		},
		TestStat:[]testStat{
			testStat{Type:"abc", QueryCount:20, ResolveAvg:1.4},
			testStat{Type:"def", QueryCount:20, ResolveAvg:1.5},
			testStat{Type:"gkj", QueryCount:60, ResolveAvg:1.3},
		},
	}

	assert.Equal(t,result, item)

}

