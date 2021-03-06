// Code generated by "enumer -type=CounterType -json -yaml -transform=snake"; DO NOT EDIT.

//
package kinesisdatacounter

import (
	"encoding/json"
	"fmt"
)

const _CounterTypeName = "countapprox_count_distinct"

var _CounterTypeIndex = [...]uint8{0, 5, 26}

func (i CounterType) String() string {
	i -= 1
	if i < 0 || i >= CounterType(len(_CounterTypeIndex)-1) {
		return fmt.Sprintf("CounterType(%d)", i+1)
	}
	return _CounterTypeName[_CounterTypeIndex[i]:_CounterTypeIndex[i+1]]
}

var _CounterTypeValues = []CounterType{1, 2}

var _CounterTypeNameToValueMap = map[string]CounterType{
	_CounterTypeName[0:5]:  1,
	_CounterTypeName[5:26]: 2,
}

// CounterTypeString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func CounterTypeString(s string) (CounterType, error) {
	if val, ok := _CounterTypeNameToValueMap[s]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to CounterType values", s)
}

// CounterTypeValues returns all values of the enum
func CounterTypeValues() []CounterType {
	return _CounterTypeValues
}

// IsACounterType returns "true" if the value is listed in the enum definition. "false" otherwise
func (i CounterType) IsACounterType() bool {
	for _, v := range _CounterTypeValues {
		if i == v {
			return true
		}
	}
	return false
}

// MarshalJSON implements the json.Marshaler interface for CounterType
func (i CounterType) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface for CounterType
func (i *CounterType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("CounterType should be a string, got %s", data)
	}

	var err error
	*i, err = CounterTypeString(s)
	return err
}

// MarshalYAML implements a YAML Marshaler for CounterType
func (i CounterType) MarshalYAML() (interface{}, error) {
	return i.String(), nil
}

// UnmarshalYAML implements a YAML Unmarshaler for CounterType
func (i *CounterType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	var err error
	*i, err = CounterTypeString(s)
	return err
}
