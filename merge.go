package mjson

import (
	"errors"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"
)

// MergeManager
type MergeManager struct {
	locker        sync.RWMutex
	container     reflect.Value
	cycle         time.Duration
	receiver      chan interface{}
	Output        chan interface{}
	mergeFunc     map[string]func(interface{}, interface{}) (interface{}, error)
	readyForMerge bool
	t reflect.Type
}

func NewMergeManager(interval time.Duration, t reflect.Type) (*MergeManager,error) {
	mergerManager := MergeManager{
		t : t,
		container: reflect.MakeSlice(reflect.SliceOf(t), 0, 0),
		cycle:     interval,
		receiver:  make(chan interface{}),
		Output:    make(chan interface{}),
		mergeFunc: make(map[string]func(interface{}, interface{}) (interface{}, error)),
	}

	err := mergerManager.registType(t, t.Name())
	if err != nil{
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-ticker.C:
				mergerManager.merge()
			case data := <-mergerManager.receiver:
				mergerManager.container = reflect.Append(mergerManager.container, reflect.ValueOf(data))
			}
		}
	}()
	return &mergerManager, nil
}

func (merger *MergeManager) merge(){
	merger.locker.Lock()
	defer func() {
		defer merger.locker.Unlock()
		merger.container = reflect.MakeSlice(reflect.SliceOf(merger.t), 0, 0)
	}()
	if merger.readyForMerge == false {
		log.Printf("Error: no registered struct for merge")
		return
	}
	result, err := merger.mergeContainerData(merger.container, merger.t.Name())
	if err != nil {
		log.Printf("Error: %s", err.Error())
		return
	}
	merger.Output <- result.Interface()
}

func (merger *MergeManager) Push(item interface{}) {
	merger.locker.Lock()
	defer merger.locker.Unlock()
	if merger.container.IsNil(){
		merger.container = reflect.MakeSlice(reflect.TypeOf(item), 0,0)
	}
	merger.container = reflect.Append(merger.container, reflect.Indirect(reflect.ValueOf(item)))
}

func (merger *MergeManager) mergeContainerData(container reflect.Value ,prefix string)(reflect.Value, error) {

	if container.Len() == 0 {
		return container, errors.New("empty container")
	}
	if container.Len() == 1 {
		return container.Index(0), nil
	}
	temp, err := merger.mergeItems(container.Index(0), container.Index(1), prefix)
	if err != nil {
		return reflect.Value{}, err
	}
	var index = 2
	for index < container.Len() {
		temp, err = merger.mergeItems(temp, container.Index(index), prefix)
		if err != nil {
			return reflect.Value{}, err
		}
		index++
	}
	return temp, nil
}

func (merger *MergeManager) mergeItems(valA reflect.Value, valB reflect.Value, prefix string) (reflect.Value, error) {

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
		if fieldType.Kind() == reflect.Slice{
			fieldName = "[]"+fieldName
		}
		if prefix == "" {
			mergeKey = inputStructType.Name() + "_" + fieldName
		} else {
			mergeKey = prefix + "_" + fieldName
		}

		if fieldType.Kind() == reflect.Struct {
			data, err := merger.mergeItems(valA.Field(i), valB.Field(i), mergeKey)
			if err != nil {
				return reflect.Value{}, err
			}
			reflect.Indirect(result).Field(i).Set(reflect.Indirect(data))
			continue
		}else {
			mergeFunc, ok := merger.mergeFunc[mergeKey]
			if ok == false {
				continue
			}
			mergeResult, err := mergeFunc(valA.Field(i).Interface(), valB.Field(i).Interface())
			if err != nil {
				return reflect.Value{}, err
			}

			if valA.Field(i).Kind() == reflect.Slice {
				vec := reflect.MakeSlice(valA.Field(i).Type(), 0, 0)
				//sliceStructType := vec.Type().Elem()
				slice, ok := mergeResult.([]interface{}) //
				if ok != true {
					continue
				}
				for _, val := range slice {
					vec = reflect.Append(vec, reflect.ValueOf(val))
				}

				resultMergeSlice, err := merger.mergeSlice(vec, mergeKey)
				if err != nil{
					return  result,err
				}
				vecResult := reflect.MakeSlice(valA.Field(i).Type(), 0, 0)

				for i := 0; i< resultMergeSlice.Len(); i++{
					vecResult = reflect.Append(vecResult, resultMergeSlice.Index(i))
				}
				reflect.Indirect(result).Field(i).Set(vecResult)
			} else {
				reflect.Indirect(result).Field(i).Set(reflect.ValueOf(mergeResult))
			}

		}
	}
	return reflect.Indirect(result), nil
}

func (merger *MergeManager) mergeSlice(originalSlice reflect.Value, prefix string) (reflect.Value, error) {
	// do merge slice job
	if originalSlice.Kind() != reflect.Slice {
		return reflect.Value{}, errors.New("input arguments must be slice type")
	}
	inputStructType := originalSlice.Type().Elem()
	var uniqueKeyName string
	for i := 0; i < inputStructType.NumField(); i++ {
		f := inputStructType.Field(i)
		mergeTag, _ := inputStructType.Field(i).Tag.Lookup("merge")
		if mergeTag == "unique" {
			uniqueKeyName = f.Name
			break
		}
	}
	// if no unique key then return without further group merge
	if uniqueKeyName == "" {
		return reflect.Value{}, errors.New("slice struct type must have a unique key")
	}

	// group result base the same key
	groupContainer := make(map[string]reflect.Value)

	for i:= 0; i< originalSlice.Len();i++{
		key := originalSlice.Index(i).FieldByName(uniqueKeyName).String()
		if _, ok := groupContainer[key];ok != true{
			groupContainer[key] = reflect.MakeSlice(originalSlice.Type(), 0, 0 )
		}
		groupContainer[key] = reflect.Append(
			groupContainer[key], originalSlice.Index(i))
	}
	result:= reflect.MakeSlice(originalSlice.Type(), 0, 0 )

	type sortedGroup struct {
		key  string
		data reflect.Value
	}

	var groupContainerSlice []sortedGroup
	for k, v := range groupContainer{
		groupContainerSlice = append(groupContainerSlice, sortedGroup{
			key:  k,
			data: v,
		})
	}

	sort.Slice(groupContainerSlice, func(i, j int) bool{
		return groupContainerSlice[i].key < groupContainerSlice[j].key
	})


	for _, data := range groupContainerSlice{
		v := data.data
		temp := reflect.MakeSlice(originalSlice.Type(), 0, 0 )

		for i:=0;i<v.Len();i++{
			temp =reflect.Append(temp, v.Index(i))
		}

		r, err := merger.mergeContainerData(temp, prefix)
		if err != nil{
			continue
		}
		result = reflect.Append(result, r)
	}
	return result, nil
}

func (merger *MergeManager) registType(structType reflect.Type, prefix string) error {
	if structType.Kind() != reflect.Struct {
		return errors.New("RegistType Error: only support struct type")
	}

	for i := 0; i < structType.NumField(); i++ {
		varName := structType.Field(i).Name
		varType := structType.Field(i).Type
		mergeTag, _ := structType.Field(i).Tag.Lookup("merge")
		if varType.Kind() == reflect.Struct {
			appendName := varName
			if prefix != ""{
				appendName = prefix+"_"+appendName
			}
			err := merger.registType(varType, appendName)
			if err != nil{
				return  err
			}
		}else if  varType.Kind() == reflect.Slice && varType.Elem().Kind() == reflect.Struct{
			appendName := "[]"+varName
			if prefix != ""{
				appendName = prefix+"_"+appendName
			}
			merger.mergeFunc[appendName] = getMergeFunc(mergeTag)
			err := merger.registType(varType.Elem(), appendName)
			if err != nil{
				return  err
			}
		}else {
			appendName := varName
			if prefix != ""{
				appendName = prefix+"_"+varName
			}
			merger.mergeFunc[appendName] = getMergeFunc(mergeTag)
		}
	}
	merger.readyForMerge = true
	return nil
}
