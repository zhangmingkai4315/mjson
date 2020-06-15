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
	locker sync.RWMutex
	container []interface{}
	cycle time.Duration
	receiver chan interface{}
	Output chan interface{}
	mergeFunc map[string]func(interface{}, interface{})(interface{},error)
	readyForMerge bool
}


func NewMergeManager(interval time.Duration) *MergeManager{
	mergerManager := MergeManager{
		container: make([]interface{}, 0),
		cycle:     interval,
		receiver:  make(chan interface{}),
		Output:    make(chan interface{}),
		mergeFunc: make(map[string]func(interface{}, interface{})(interface{}, error)),
	}
	go func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				err := mergerManager.mergeContainerData()
				if err != nil{
					log.Printf("Error: %s", err.Error())
				}
			case data :=<-mergerManager.receiver:
				mergerManager.container = append(mergerManager.container, data)
			}
		}
	}()

	return &mergerManager
}


func (merger *MergeManager)Push(item interface{}){
	merger.locker.Lock()
	defer merger.locker.Unlock()
	merger.container = append(merger.container, item)
}

func (merger *MergeManager)mergeContainerData()error{
	merger.locker.Lock()
	defer merger.locker.Unlock()
	if merger.readyForMerge == false{
		return errors.New("no registered struct for merge")
	}
	if len(merger.container) == 0{
		return nil
	}
	if len(merger.container) == 1{
		merger.Output<- merger.container[0]
	}

	temp, err := merger.mergeItems(merger.container[0], merger.container[1])
	if err != nil{
		return  err
	}
	var index = 2
	for index < len(merger.container){
		if err != nil{
			return err
		}
		temp, err =  merger.mergeItems(temp, merger.container[index])
		index++
	}
	merger.Output<-temp
	merger.container = make([]interface{},0)
	return nil
}

func (merger *MergeManager) mergeItems(a interface{}, b interface{})(interface{}, error){
	valA := reflect.ValueOf(a)
	valB := reflect.ValueOf(b)

	// if its a pointer, resolve its value
	if valA.Kind() == reflect.Ptr {
		valA = reflect.Indirect(valA)

	}
	y := reflect.New(reflect.Indirect(valA).Type())
	// if its a pointer, resolve its value
	if valB.Kind() == reflect.Ptr {
		valB = reflect.Indirect(valB)
	}


	if valA.Kind() != reflect.Struct || valB.Kind() != reflect.Struct {
		return nil, errors.New("input arguments must be struct type")
	}

	structA := valA.Type()


	for i := 0; i < structA.NumField(); i++ {
		varName := structA.Field(i).Name
		mergeFunc, ok := merger.mergeFunc[varName]

		if ok == false{
			continue
		}
		result, err := mergeFunc(valA.Field(i).Interface(),valB.Field(i).Interface())
		if err != nil{
			return nil, err
		}
		reflect.Indirect(y).FieldByName(varName).Set(reflect.ValueOf(result))
		//reflect.ValueOf(&a).Elem().Field(i).Set(reflect.ValueOf(result))
	}
	return y,nil
}


func (merger *MergeManager)RegistType(schema interface{}) error {
	val := reflect.ValueOf(schema)
	if val.Kind() != reflect.Struct{
		return errors.New("RegistType Error: only support struct type")
	}

	structType := val.Type()

	for i := 0; i < structType.NumField(); i++ {
		varName := structType.Field(i).Name
		varType := structType.Field(i).Type
		mergeTag,_ := structType.Field(i).Tag.Lookup("merge")
		fmt.Printf("%v %v merge=%s\n", varName,varType,mergeTag)
		merger.mergeFunc[varName] = getMergeFunc(mergeTag)
	}
	merger.readyForMerge = true
	return nil
}


func floatPlusFunc(a interface{}, b interface{})(interface{} ,error){
	aFloat64, ok := a.(float64)
	if !ok{
		return nil, errors.New("first argument cast float64 fail")
	}
	bFloat64, ok := b.(float64)
	if !ok{
		return nil, errors.New("second argument cast float64 fail")
	}
	return aFloat64 + bFloat64, nil
}


func intPlusFunc(a interface{}, b interface{})(interface{} ,error){
	aInt, ok := a.(int)
	if !ok{
		return nil, errors.New("first argument cast int fail")
	}
	bInt, ok := b.(int)
	if !ok{
		return nil, errors.New("second argument cast int fail")
	}
	return aInt + bInt, nil
}


func float64AvgFunc(a interface{}, b interface{})(interface{} ,error){
	aFloat64, ok := a.(float64)
	if !ok{
		return nil, errors.New("first argument cast float64 fail")
	}
	bFloat64, ok := b.(float64)
	if !ok{
		return nil, errors.New("second argument cast float64 fail")
	}
	return (aFloat64 + bFloat64)/2, nil
}


func intAvgFunc(a interface{}, b interface{})(interface{} ,error){
	aInt, ok := a.(int)
	if !ok{
		return nil, errors.New("first argument cast int fail")
	}
	bInt, ok := b.(int)
	if !ok{
		return nil, errors.New("second argument cast int fail")
	}
	return (aInt + bInt)/2, nil
}

func appendStringFunc(a interface{}, b interface{})(interface{} ,error){
	aInterfaceSlice, ok := a.([]string)
	if !ok{
		return nil, errors.New("first argument cast []interface fail")
	}
	bInterfaceSlice, ok := b.([]string)
	if !ok{
		return nil, errors.New("second argument cast []interface fail")
	}
	return append(aInterfaceSlice,bInterfaceSlice...), nil
}

func stringConcatFunc(a interface{}, b interface{})(interface{} ,error){
	aStr, ok := a.(string)
	if !ok{
		return nil, errors.New("first argument cast string fail")
	}
	bStr, ok := b.(string)
	if !ok{
		return nil, errors.New("second argument cast string fail")
	}
	return aStr + bStr, nil
}

func replaceFunc(a interface{}, b interface{})(interface{} ,error){
	return b, nil
}

func keepFunc(a interface{}, b interface{})(interface{} ,error){
	return a, nil
}

func getMergeFunc(tagName string)func(interface{},interface{})(interface{},error){
	switch tagName {
	case "float64_avg":
		return float64AvgFunc
	case "int_avg":
		return intAvgFunc
	case "float64_plus":
		return floatPlusFunc
	case "int_plus":
		return intPlusFunc
	case "append_str":
		return appendStringFunc
	case "keep":
		return keepFunc
	case "string_concat":
		return stringConcatFunc
	case "replace":
	case "default":
		fallthrough
	default:
		return replaceFunc
	}
	return replaceFunc
}


