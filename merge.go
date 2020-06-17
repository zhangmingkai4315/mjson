package mjson

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)

// MergeManager hold received data and re
type MergeManager struct {
	locker        sync.RWMutex
	container     []interface{}
	cycle         time.Duration
	receiver      chan interface{}
	Output        chan interface{}
	mergeFunc     map[string]func(interface{}, interface{}) (interface{}, error)
	readyForMerge bool
}

func NewMergeManager(interval time.Duration) *MergeManager {
	mergerManager := MergeManager{
		container: make([]interface{}, 0),
		cycle:     interval,
		receiver:  make(chan interface{}),
		Output:    make(chan interface{}),
		mergeFunc: make(map[string]func(interface{}, interface{}) (interface{}, error)),
	}
	go func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				err := mergerManager.mergeContainerData()
				if err != nil {
					log.Printf("Error: %s", err.Error())
				}
			case data := <-mergerManager.receiver:
				mergerManager.container = append(mergerManager.container, data)
			}
		}
	}()

	return &mergerManager
}

func (merger *MergeManager) Push(item interface{}) {
	merger.locker.Lock()
	defer merger.locker.Unlock()
	merger.container = append(merger.container, item)
}

func (merger *MergeManager) mergeContainerData() error {
	merger.locker.Lock()
	defer func() {
		defer merger.locker.Unlock()
		merger.container = make([]interface{}, 0)
	}()
	if merger.readyForMerge == false {
		return errors.New("no registered struct for merge")
	}
	if len(merger.container) == 0 {
		return nil
	}
	if len(merger.container) == 1 {
		merger.Output <- merger.container[0]
	}
	temp, err := merger.mergeItems(merger.container[0], merger.container[1], "")
	if err != nil {
		return err
	}
	var index = 2
	for index < len(merger.container) {
		temp, err = merger.mergeItems(temp, merger.container[index], "")
		if err != nil {
			break
		}
		index++
	}
	merger.Output <- temp

	return nil
}

func (merger *MergeManager) mergeItems(a interface{}, b interface{}, prefix string) (reflect.Value, error) {

	// if a is a reflect.Value
	valA := reflect.ValueOf(a)
	if reflect.TypeOf(a).Name() == "Value" {
		valA = a.(reflect.Value)
	}
	valB := reflect.ValueOf(b)
	if reflect.TypeOf(b).Name() == "Value" {
		valB = a.(reflect.Value)
	}

	// create a result container
	var result reflect.Value
	result = reflect.New(valA.Type())

	// if valA is a ptr, then using Indirect get its true type
	if valA.Kind() == reflect.Ptr {
		valA = reflect.Indirect(valA)
		result = reflect.New(reflect.Indirect(valA).Type())
	}
	// if its a pointer, resolve its value
	if valB.Kind() == reflect.Ptr {
		valB = reflect.Indirect(valB)
	}

	if valA.Kind() != reflect.Struct || valB.Kind() != reflect.Struct {
		return reflect.Value{}, errors.New("input arguments must be struct type")
	}
	inputStructType := valA.Type()

	for i := 0; i < inputStructType.NumField(); i++ {
		var mergeKey string

		fieldName := inputStructType.Field(i).Name
		fieldType := inputStructType.Field(i).Type
		// get the hash key of this field
		if prefix == "" {
			mergeKey = inputStructType.String() + "_" + fieldName
		} else {
			mergeKey = prefix + "_" + fieldName
		}

		if fieldType.Kind() == reflect.Struct {
			data, err := merger.mergeItems(valA.Field(i).Interface(), valB.Field(i).Interface(), mergeKey)
			if err != nil {
				return reflect.Value{}, err
			}
			reflect.Indirect(result).Field(i).Set(reflect.Indirect(data))
			continue
		} else {
			mergeFunc, ok := merger.mergeFunc[mergeKey]
			if ok == false {
				continue
			}
			mergeResult, err := mergeFunc(valA.Field(i).Interface(), valB.Field(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}
			if valA.Field(i).Kind() == reflect.Slice {
				// mergeResult []interface{} to []struct type
				vec := reflect.MakeSlice(valA.Field(i).Type(), 0, 0)

				//vec.Elem().Set(reflect.ValueOf(mergeResult))
				slice, ok := mergeResult.([]interface{}) //
				if ok != true {
					continue
				}
				for _, val := range slice {
					vec = reflect.Append(vec, reflect.ValueOf(val))
				}

				vec = merger.mergeSlice(vec)

				reflect.Indirect(result).Field(i).Set(vec)
			} else {
				reflect.Indirect(result).Field(i).Set(reflect.ValueOf(mergeResult))
			}

		}
	}
	return result, nil
}

func (merger *MergeManager) mergeSlice(vec reflect.Value) reflect.Value {
	// do merge slice job

	return vec
}

func (merger *MergeManager) RegistType(structType reflect.Type, prefix string) error {
	structName := structType.String()
	if structType.Kind() != reflect.Struct {
		return errors.New("RegistType Error: only support struct type")
	}
	for i := 0; i < structType.NumField(); i++ {
		varName := structType.Field(i).Name
		varType := structType.Field(i).Type
		mergeTag, _ := structType.Field(i).Tag.Lookup("merge")
		fmt.Printf("%v %v merge=%s\n", varName, varType, mergeTag)
		if varType.Kind() == reflect.Struct {
			merger.RegistType(varType, structName+"_"+varName)
		} else {
			if prefix == "" {
				merger.mergeFunc[structName+"_"+varName] = getMergeFunc(mergeTag)
			} else {
				merger.mergeFunc[prefix+"_"+varName] = getMergeFunc(mergeTag)
			}

		}

	}
	merger.readyForMerge = true
	return nil
}
