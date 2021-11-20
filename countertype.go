package kinesisdatacounter

import "strings"

//go:generate go run github.com/alvaroloes/enumer -type=CounterType -json -yaml -transform=snake
type CounterType int

const (
	Count CounterType = iota + 1
	ApproxCountDistinct
)

func CounterTypeValuesString() string {
	values := CounterTypeValues()
	strValues := make([]string, 0, len(values))
	for _, v := range values {
		strValues = append(strValues, v.String())
	}
	return strings.Join(strValues, ",")
}
