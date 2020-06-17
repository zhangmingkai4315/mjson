package mjson

import (
	"errors"
	"reflect"
)

func floatPlusFunc(a interface{}, b interface{}) (interface{}, error) {
	aFloat64, ok := a.(float64)
	if !ok {
		return nil, errors.New("first argument cast float64 fail")
	}
	bFloat64, ok := b.(float64)
	if !ok {
		return nil, errors.New("second argument cast float64 fail")
	}
	return aFloat64 + bFloat64, nil
}

func intPlusFunc(a interface{}, b interface{}) (interface{}, error) {
	aInt, ok := a.(int)
	if !ok {
		return nil, errors.New("first argument cast int fail")
	}
	bInt, ok := b.(int)
	if !ok {
		return nil, errors.New("second argument cast int fail")
	}
	return aInt + bInt, nil
}

func float64AvgFunc(a interface{}, b interface{}) (interface{}, error) {
	aFloat64, ok := a.(float64)
	if !ok {
		return nil, errors.New("first argument cast float64 fail")
	}
	bFloat64, ok := b.(float64)
	if !ok {
		return nil, errors.New("second argument cast float64 fail")
	}
	return (aFloat64 + bFloat64) / 2, nil
}

func intAvgFunc(a interface{}, b interface{}) (interface{}, error) {
	aInt, ok := a.(int)
	if !ok {
		return nil, errors.New("first argument cast int fail")
	}
	bInt, ok := b.(int)
	if !ok {
		return nil, errors.New("second argument cast int fail")
	}
	return (aInt + bInt) / 2, nil
}

func appendStringFunc(a interface{}, b interface{}) (interface{}, error) {
	aInterfaceSlice, ok := a.([]string)
	if !ok {
		return nil, errors.New("first argument cast []string fail")
	}
	bInterfaceSlice, ok := b.([]string)
	if !ok {
		return nil, errors.New("second argument cast []string fail")
	}
	return append(aInterfaceSlice, bInterfaceSlice...), nil
}

func sliceStructFunc(a interface{}, b interface{}) (interface{}, error) {
	vA := reflect.ValueOf(a)
	vB := reflect.ValueOf(b)
	infResult := make([]interface{}, vA.Len()+vB.Len())

	for i := 0; i < vA.Len(); i++ {
		infResult[i] = vA.Index(i).Interface()
	}
	for i := 0; i < vB.Len(); i++ {
		infResult[i+vA.Len()] = vB.Index(i).Interface()
	}
	return infResult, nil
}

func stringConcatFunc(a interface{}, b interface{}) (interface{}, error) {
	aStr, ok := a.(string)
	if !ok {
		return nil, errors.New("first argument cast string fail")
	}
	bStr, ok := b.(string)
	if !ok {
		return nil, errors.New("second argument cast string fail")
	}
	return aStr + bStr, nil
}

func replaceFunc(a interface{}, b interface{}) (interface{}, error) {
	return b, nil
}

func keepFunc(a interface{}, b interface{}) (interface{}, error) {
	return a, nil
}

func getMergeFunc(tagName string) func(interface{}, interface{}) (interface{}, error) {
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
	case "[]struct":
		return sliceStructFunc
	case "replace":
	case "default":
		fallthrough
	default:
		return replaceFunc
	}
	return replaceFunc
}
