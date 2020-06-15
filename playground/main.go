package main

import (
	"fmt"
	"github.com/zhangmingkai4315/mjson"
	"time"
)


type Person struct{
	Name string `json:"name" merge:"string_concat"`
	Age int  `json:"age" merge:"int_avg"`
	Locations []string `json:"locations" merge:"append_str"`
	Salary  int `json:"salary" merge:"int_plus"`
}
func main(){
	manager := mjson.NewMergeManager(time.Duration(1)*time.Second)
	manager.RegistType(Person{})
	p1 := &Person{
		Name:      "Mike",
		Age:       10,
		Locations: []string{"beijing"},
		Salary:    10000,
	}
	p2 := &Person{
		Name:      "Alice",
		Age:       20,
		Locations: []string{"shanghai"},
		Salary:    14000,
	}
	manager.Push(p1)
	manager.Push(p2)

	data := <-manager.Output
	fmt.Printf("%v", data)


	//e := reflect.ValueOf(&p1).Elem()
	//t := reflect.TypeOf(p1)
	//for i := 0; i < e.NumField(); i++ {
	//	varName := e.Type().Field(i).Name
	//	varType := e.Type().Field(i).Type
	//	f,_ := t.FieldByName(varName)
	//	mergeTag,_ := f.Tag.Lookup("merge")
	//	//varValue := e.Field(i).Interface()
	//	fmt.Printf("%v %v merge=%s\n", varName,varType,mergeTag)
	//}

}