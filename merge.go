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

	temp, err := merger.mergeItems(merger.container[0], merger.container[1], "")
	if err != nil{
		return  err
	}
	var index = 2
	for index < len(merger.container){
		if err != nil{
			return err
		}
		temp, err =  merger.mergeItems(temp, merger.container[index],"")
		index++
	}
	merger.Output<-temp
	merger.container = make([]interface{},0)
	return nil
}

func (merger *MergeManager) mergeItems(a interface{}, b interface{}, prefix string)(interface{}, error){
	valA := reflect.ValueOf(a)
	valB := reflect.ValueOf(b)

	// if its a pointer, resolve its value
	if valA.Kind() == reflect.Ptr {
		valA = reflect.Indirect(valA)
	}
	// if its a pointer, resolve its value
	if valB.Kind() == reflect.Ptr {
		valB = reflect.Indirect(valB)
	}


	y := reflect.New(reflect.Indirect(valA).Type())

	if valA.Kind() != reflect.Struct || valB.Kind() != reflect.Struct {
		return nil, errors.New("input arguments must be struct type")
	}

	structA := valA.Type()


	for i := 0; i < structA.NumField(); i++ {
		if structA.Field(i).Type.Kind() == reflect.Struct{
			fmt.Printf("%v is struct not implemet!",structA.Field(i).Name )
			continue
		}
		varName := structA.Field(i).Name
		var mergeKey string
		if prefix == ""{
			mergeKey = structA.String() + "_" + varName
		}else{
			mergeKey = prefix + "_" + varName
		}
		mergeFunc, ok := merger.mergeFunc[mergeKey]
		if ok == false{
			continue
		}
		result, err := mergeFunc(valA.Field(i).Interface(),valB.Field(i).Interface())
		if err != nil{
			return nil, err
		}
		reflect.Indirect(y).FieldByName(varName).Set(reflect.ValueOf(result))
	}
	return y,nil
}


func (merger *MergeManager)RegistType(structType reflect.Type, prefix string) error {
	structName := structType.String()
	if structType.Kind() != reflect.Struct{
		return errors.New("RegistType Error: only support struct type")
	}
	for i := 0; i < structType.NumField(); i++ {
		varName := structType.Field(i).Name
		varType := structType.Field(i).Type
		mergeTag,_ := structType.Field(i).Tag.Lookup("merge")
		fmt.Printf("%v %v merge=%s\n", varName,varType,mergeTag)
		if varType.Kind() == reflect.Struct{
			merger.RegistType(varType, structName+"_"+varName)
		}else{
			if prefix == ""{
				merger.mergeFunc[structName +"_"+varName] = getMergeFunc(mergeTag)
			}else{
				merger.mergeFunc[prefix +"_"+varName] = getMergeFunc(mergeTag)
			}

		}

	}
	merger.readyForMerge = true
	return nil
}
