// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"math"
	bits2 "math/bits"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

var (
	errorConvertToBoolFailed                   = moerr.NewInternalError(context.Background(), "convert to the system variable bool type failed")
	errorConvertToIntFailed                    = moerr.NewInternalError(context.Background(), "convert to the system variable int type failed")
	errorConvertToUintFailed                   = moerr.NewInternalError(context.Background(), "convert to the system variable uint type failed")
	errorConvertToDoubleFailed                 = moerr.NewInternalError(context.Background(), "convert to the system variable double type failed")
	errorConvertToEnumFailed                   = moerr.NewInternalError(context.Background(), "convert to the system variable enum type failed")
	errorConvertToSetFailed                    = moerr.NewInternalError(context.Background(), "convert to the system variable set type failed")
	errorConvertToStringFailed                 = moerr.NewInternalError(context.Background(), "convert to the system variable string type failed")
	errorConvertToNullFailed                   = moerr.NewInternalError(context.Background(), "convert to the system variable null type failed")
	errorConvertFromStringToBoolFailedFormat   = "convert from string %s to the system variable bool type failed"
	errorConvertFromStringToIntFailedFormat    = "convert from string %s to the system variable int type failed"
	errorConvertFromStringToUintFailedFormat   = "convert from string %s to the system variable uint type failed"
	errorConvertFromStringToDoubleFailedFormat = "convert from string %s to the system variable double type failed"
	errorConvertFromStringToEnumFailedFormat   = "convert from string %s to the system variable enum type failed"
	errorConvertFromStringToSetFailedFormat    = "convert from string %s to the system variable set type failed"
	errorConvertFromStringToNullFailedFormat   = "convert from string %s  to the system variable null type failed"
)

func getErrorConvertFromStringToBoolFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToBoolFailedFormat, str)
}

func getErrorConvertFromStringToIntFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToIntFailedFormat, str)
}

func getErrorConvertFromStringToUintFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToUintFailedFormat, str)
}

func getErrorConvertFromStringToDoubleFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToDoubleFailedFormat, str)
}

func getErrorConvertFromStringToEnumFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToEnumFailedFormat, str)
}

func getErrorConvertFromStringToSetFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToSetFailedFormat, str)
}

func getErrorConvertFromStringToNullFailed(str string) error {
	return moerr.NewInternalError(context.Background(), errorConvertFromStringToNullFailedFormat, str)
}

func errorSystemVariableDoesNotExist() string { return "the system variable does not exist" }
func errorSystemVariableIsSession() string    { return "the system variable is session" }
func errorSystemVariableSessionEmpty() string {
	return "the value of the system variable with scope session is empty"
}
func errorSystemVariableIsGlobal() string   { return "the system variable is global" }
func errorSystemVariableIsReadOnly() string { return "the system variable is read only" }

type Scope int

const (
	ScopeGlobal       Scope = iota //it is only in global
	ScopeSession                   //it is only in session
	ScopeBoth                      //it is both in global and session
	ScopePersist                   //it is global and persisted
	ScopePersistOnly               //it is persisted without updating global and session values
	ScopeResetPersist              //to remove a persisted variable
)

func (s Scope) String() string {
	switch s {
	case ScopeGlobal:
		return "GLOBAL"
	case ScopeSession:
		return "SESSION"
	case ScopeBoth:
		return "GLOBAL, SESSION"
	case ScopePersist:
		return "GLOBAL, PERSIST"
	case ScopePersistOnly:
		return "PERSIST"
	case ScopeResetPersist:
		return "RESET PERSIST"
	default:
		return "UNKNOWN_SYSTEM_SCOPE"
	}
}

type SystemVariableType interface {
	fmt.Stringer

	// Convert the value to another value of the type
	Convert(value interface{}) (interface{}, error)

	// Type gets the type in the computation engine
	Type() types.T

	// MysqlType gets the mysql type
	MysqlType() defines.MysqlType

	// Zero gets the zero value for the type
	Zero() interface{}

	// Convert the value from string to another value of the type
	ConvertFromString(value string) (interface{}, error)
}

var _ SystemVariableType = SystemVariableBoolType{}
var _ SystemVariableType = SystemVariableIntType{}
var _ SystemVariableType = SystemVariableUintType{}
var _ SystemVariableType = SystemVariableDoubleType{}
var _ SystemVariableType = SystemVariableEnumType{}
var _ SystemVariableType = SystemVariableSetType{}
var _ SystemVariableType = SystemVariableStringType{}
var _ SystemVariableType = SystemVariableNullType{}

type SystemVariableNullType struct {
}

func (svnt SystemVariableNullType) String() string {
	return "NULL"
}

func (svnt SystemVariableNullType) Convert(value interface{}) (interface{}, error) {
	if value != nil {
		return nil, errorConvertToNullFailed
	}
	return nil, nil
}

func (svnt SystemVariableNullType) Type() types.T {
	return types.T_any
}

func (svnt SystemVariableNullType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_NULL
}

func (svnt SystemVariableNullType) Zero() interface{} {
	return nil
}

func (svnt SystemVariableNullType) ConvertFromString(value string) (interface{}, error) {
	if len(value) != 0 {
		return nil, getErrorConvertFromStringToNullFailed(value)
	}
	return nil, nil
}

type SystemVariableBoolType struct {
	name string
}

func InitSystemVariableBoolType(name string) SystemVariableBoolType {
	return SystemVariableBoolType{
		name: name,
	}
}

func (svbt SystemVariableBoolType) String() string {
	return "BOOL"
}

func (svbt SystemVariableBoolType) Convert(value interface{}) (interface{}, error) {
	cv1 := func(x int8) (interface{}, error) {
		if x == 0 || x == 1 {
			return x, nil
		}
		return nil, errorConvertToBoolFailed
	}
	cv2 := func(x float64) (interface{}, error) {
		xx := int64(x)
		rxx := float64(xx)
		if x == rxx {
			return cv1(int8(xx))
		}
		return nil, errorConvertToBoolFailed
	}
	cv3 := func(x string) (interface{}, error) {
		switch strings.ToLower(x) {
		case "on", "true", "1":
			return int8(1), nil
		case "off", "false", "0":
			return int8(0), nil
		}
		return nil, errorConvertToBoolFailed
	}
	switch v := value.(type) {
	case int:
		return cv1(int8(v))
	case uint:
		return cv1(int8(v))
	case int8:
		return cv1(v)
	case int16:
		return cv1(int8(v))
	case uint16:
		return cv1(int8(v))
	case int32:
		return cv1(int8(v))
	case uint32:
		return cv1(int8(v))
	case int64:
		return cv1(int8(v))
	case uint64:
		return cv1(int8(v))
	case bool:
		if v {
			return int8(1), nil
		} else {
			return int8(0), nil
		}
	case float32:
		return cv2(float64(v))
	case float64:
		return cv2(v)
	case string:
		return cv3(v)
	}
	return nil, errorConvertToBoolFailed
}

func (svbt SystemVariableBoolType) IsTrue(v interface{}) bool {
	switch vv := v.(type) {
	case int:
		return vv == 1
	case uint:
		return vv == uint(1)
	case int8:
		return vv == int8(1)
	case uint8:
		return vv == uint8(1)
	case int16:
		return vv == int16(1)
	case uint16:
		return vv == uint16(1)
	case int32:
		return vv == int32(1)
	case uint32:
		return vv == uint32(1)
	case int64:
		return vv == int64(1)
	case uint64:
		return vv == uint64(1)
	case bool:
		return vv
	case string:
		return strings.ToLower(vv) == "on"
	default:
		return false
	}
}

func (svbt SystemVariableBoolType) Type() types.T {
	return types.T_bool
}

func (svbt SystemVariableBoolType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_BOOL
}

func (svbt SystemVariableBoolType) Zero() interface{} {
	return int8(0)
}

func (svbt SystemVariableBoolType) ConvertFromString(value string) (interface{}, error) {
	if value == "on" {
		return int8(1), nil
	} else if value == "off" {
		return int8(0), nil
	}

	convertVal, err := strconv.ParseInt(value, 10, 8)
	if err != nil {
		return nil, getErrorConvertFromStringToBoolFailed(value)
	}
	if convertVal != 1 && convertVal != 0 {
		return nil, getErrorConvertFromStringToBoolFailed(value)
	}
	return int8(convertVal), nil

}

type SystemVariableIntType struct {
	name    string
	minimum int64
	maximum int64
	//-1 ?
	maybeMinusOne bool
}

func InitSystemVariableIntType(name string, minimum, maximum int64, maybeMinusOne bool) SystemVariableIntType {
	return SystemVariableIntType{
		name:          name,
		minimum:       minimum,
		maximum:       maximum,
		maybeMinusOne: maybeMinusOne,
	}
}

func (svit SystemVariableIntType) String() string {
	return "INT"
}

func (svit SystemVariableIntType) Convert(value interface{}) (interface{}, error) {
	cv1 := func(x int64) (interface{}, error) {
		if x >= svit.minimum && x <= svit.maximum {
			return x, nil
		} else if svit.maybeMinusOne && x == -1 {
			return x, nil
		}
		return nil, errorConvertToIntFailed
	}
	cv2 := func(x float64) (interface{}, error) {
		xx := int64(x)
		rxx := float64(xx)
		if x == rxx {
			return cv1(xx)
		}
		return nil, errorConvertToIntFailed
	}

	cv3 := func(x string) (interface{}, error) {
		convertVal, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return nil, errorConvertToIntFailed
		}
		return cv1(convertVal)
	}

	switch v := value.(type) {
	case int:
		return cv1(int64(v))
	case uint:
		return cv1(int64(v))
	case int8:
		return cv1(int64(v))
	case uint8:
		return cv1(int64(v))
	case int16:
		return cv1(int64(v))
	case uint16:
		return cv1(int64(v))
	case int32:
		return cv1(int64(v))
	case uint32:
		return cv1(int64(v))
	case int64:
		return cv1(v)
	case uint64:
		return cv1(int64(v))
	case float32:
		return cv2(float64(v))
	case float64:
		return cv2(v)
	case string:
		return cv3(v)
	}
	return nil, errorConvertToIntFailed
}

func (svit SystemVariableIntType) Type() types.T {
	return types.T_int64
}

func (svit SystemVariableIntType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_LONGLONG
}

func (svit SystemVariableIntType) Zero() interface{} {
	return int64(0)
}

func (svit SystemVariableIntType) ConvertFromString(value string) (interface{}, error) {
	convertVal, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, getErrorConvertFromStringToIntFailed(value)
	}
	return convertVal, nil
}

type SystemVariableUintType struct {
	name    string
	minimum uint64
	maximum uint64
}

func InitSystemVariableUintType(name string, minimum, maximum uint64) SystemVariableUintType {
	return SystemVariableUintType{
		name:    name,
		minimum: minimum,
		maximum: maximum,
	}
}

func (svut SystemVariableUintType) String() string {
	return "UINT"
}

func (svut SystemVariableUintType) Convert(value interface{}) (interface{}, error) {
	cv1 := func(x uint64) (interface{}, error) {
		if x >= svut.minimum && x <= svut.maximum {
			return x, nil
		}
		return nil, errorConvertToUintFailed
	}
	cv2 := func(x float64) (interface{}, error) {
		xx := uint64(x)
		rxx := float64(xx)
		if x == rxx {
			return cv1(xx)
		}
		return nil, errorConvertToUintFailed
	}

	switch v := value.(type) {
	case int:
		return cv1(uint64(v))
	case uint:
		return cv1(uint64(v))
	case int8:
		return cv1(uint64(v))
	case uint8:
		return cv1(uint64(v))
	case int16:
		return cv1(uint64(v))
	case uint16:
		return cv1(uint64(v))
	case int32:
		return cv1(uint64(v))
	case uint32:
		return cv1(uint64(v))
	case int64:
		return cv1(uint64(v))
	case uint64:
		return cv1(v)
	case float32:
		return cv2(float64(v))
	case float64:
		return cv2(v)
	}
	return nil, errorConvertToUintFailed
}

func (svut SystemVariableUintType) Type() types.T {
	return types.T_uint64
}

func (svut SystemVariableUintType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_LONGLONG
}

func (svut SystemVariableUintType) Zero() interface{} {
	return uint64(0)
}

func (svut SystemVariableUintType) ConvertFromString(value string) (interface{}, error) {
	convertVal, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, getErrorConvertFromStringToUintFailed(value)
	}
	return convertVal, nil
}

type SystemVariableDoubleType struct {
	name    string
	minimum float64
	maximum float64
}

func InitSystemVariableDoubleType(name string, minimum, maximum float64) SystemVariableDoubleType {
	return SystemVariableDoubleType{
		name:    name,
		minimum: minimum,
		maximum: maximum,
	}
}

func (svdt SystemVariableDoubleType) String() string {
	return "DOUBLE"
}

func (svdt SystemVariableDoubleType) Convert(value interface{}) (interface{}, error) {
	cv1 := func(x float64) (interface{}, error) {
		if x >= svdt.minimum && x <= svdt.maximum {
			return x, nil
		}
		return nil, errorConvertToUintFailed
	}

	switch v := value.(type) {
	case int:
		return cv1(float64(v))
	case uint:
		return cv1(float64(v))
	case int8:
		return cv1(float64(v))
	case uint8:
		return cv1(float64(v))
	case int16:
		return cv1(float64(v))
	case uint16:
		return cv1(float64(v))
	case int32:
		return cv1(float64(v))
	case uint32:
		return cv1(float64(v))
	case int64:
		return cv1(float64(v))
	case uint64:
		return cv1(float64(v))
	case float32:
		return cv1(float64(v))
	case float64:
		return cv1(v)
	case string:
		// some case '0.1' is recognized as string
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return cv1(f)
		}
	}
	return nil, errorConvertToDoubleFailed
}

func (svdt SystemVariableDoubleType) Type() types.T {
	return types.T_float64
}

func (svdt SystemVariableDoubleType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_DOUBLE
}

func (svdt SystemVariableDoubleType) Zero() interface{} {
	return float64(0)
}

func (svdt SystemVariableDoubleType) ConvertFromString(value string) (interface{}, error) {
	convertVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, getErrorConvertFromStringToDoubleFailed(value)
	}
	return convertVal, nil
}

var (
	// panic
	errorEnumHasMoreThan65535Values = moerr.NewInternalError(context.Background(), "the enum has more than 65535 values")
)

type SystemVariableEnumType struct {
	name string
	//tag name -> id
	tagName2Id map[string]int

	// id -> tag name
	id2TagName []string
}

func InitSystemSystemEnumType(name string, values ...string) SystemVariableEnumType {
	if len(values) > 65535 {
		panic(errorEnumHasMoreThan65535Values)
	}
	tagName2Id := make(map[string]int)
	for i, value := range values {
		tagName2Id[strings.ToLower(value)] = i
	}
	return SystemVariableEnumType{
		name:       name,
		tagName2Id: tagName2Id,
		id2TagName: values,
	}
}

func (svet SystemVariableEnumType) String() string {
	return "ENUM"
}

func (svet SystemVariableEnumType) Convert(value interface{}) (interface{}, error) {
	cv1 := func(x int) (interface{}, error) {
		if x >= 0 && x <= len(svet.id2TagName) {
			return svet.id2TagName[x], nil
		}
		return nil, errorConvertToEnumFailed
	}
	cv2 := func(x float64) (interface{}, error) {
		xx := int(x)
		rxx := float64(xx)
		if x == rxx {
			return cv1(xx)
		}
		return nil, errorConvertToUintFailed
	}

	switch v := value.(type) {
	case int:
		return cv1(v)
	case uint:
		return cv1(int(v))
	case int8:
		return cv1(int(v))
	case uint8:
		return cv1(int(v))
	case int16:
		return cv1(int(v))
	case uint16:
		return cv1(int(v))
	case int32:
		return cv1(int(v))
	case uint32:
		return cv1(int(v))
	case int64:
		return cv1(int(v))
	case uint64:
		return cv1(int(v))
	case float32:
		return cv2(float64(v))
	case float64:
		return cv2(v)
	case string:
		if id, ok := svet.tagName2Id[strings.ToLower(v)]; ok {
			return svet.id2TagName[id], nil
		}
	}
	return nil, errorConvertToEnumFailed
}

func (svet SystemVariableEnumType) Type() types.T {
	return types.T_varchar
}

func (svet SystemVariableEnumType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_VARCHAR
}

func (svet SystemVariableEnumType) Zero() interface{} {
	return ""
}

func (svet SystemVariableEnumType) ConvertFromString(value string) (interface{}, error) {
	lowerName := strings.ToLower(value)
	if val, ok := svet.tagName2Id[lowerName]; !ok {
		return nil, getErrorConvertFromStringToEnumFailed(value)
	} else {
		return val, nil
	}
}

const (
	//reference : https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html#data-types-storage-reqs-strings
	MaxMemberCountOfSetType = 64
)

var (
	// panic error
	errorValuesOfSetIsEmpty       = moerr.NewInternalError(context.Background(), "the count of values for set is empty")
	errorValuesOfSetGreaterThan64 = moerr.NewInternalError(context.Background(), "the count of value is greater than 64")
	errorValueHasComma            = moerr.NewInternalError(context.Background(), "the value has the comma")
	errorValueIsDuplicate         = moerr.NewInternalError(context.Background(), "the value is duplicate")
	errorValuesAreNotEnough       = moerr.NewInternalError(context.Background(), "values are not enough") // convert
	errorValueIsInvalid           = moerr.NewInternalError(context.Background(), "the value is invalid")  // convert
)

type SystemVariableSetType struct {
	// name                string
	normalized2original map[string]string
	value2BitIndex      map[string]int
	bitIndex2Value      map[int]string
}

func (svst SystemVariableSetType) String() string {
	return fmt.Sprintf("SET('%v')",
		strings.Join(svst.Values(), "','"))
}

func (svst SystemVariableSetType) Values() []string {
	bitsCount := 64 - bits2.LeadingZeros64(svst.bitmap())
	var res []string
	for i := 0; i < bitsCount; i++ {
		res = append(res, svst.bitIndex2Value[i])
	}
	return res
}

func (svst SystemVariableSetType) bitmap() uint64 {
	cnt := uint64(len(svst.value2BitIndex))
	if cnt == 64 {
		return math.MaxUint64
	}
	return uint64(1<<cnt) - 1
}

func (svst SystemVariableSetType) bits2string(bits uint64) (string, error) {
	bld := strings.Builder{}
	bitCount := 64 - bits2.LeadingZeros64(bits)
	if bitCount > len(svst.bitIndex2Value) {
		return "", errorValuesAreNotEnough
	}

	for i := 0; i < bitCount; i++ {
		mask := uint64(1 << uint64(i))
		if mask&bits != 0 {
			v, ok := svst.bitIndex2Value[i]
			if !ok {
				return "", errorValueIsInvalid
			}
			bld.WriteString(v)
			if i != 0 {
				bld.WriteByte(',')
			}
		}
	}

	bldString := bld.String()
	if len(bldString) == 0 {
		return bldString, nil
	}
	return bldString[:len(bldString)-1], nil
}

func (svst SystemVariableSetType) string2bits(s string) (uint64, error) {
	if len(s) == 0 {
		return 0, nil
	}
	ss := strings.Split(s, ",")
	bits := uint64(0)
	for _, sss := range ss {
		normalized := strings.ToLower(strings.TrimRight(sss, " "))
		if origin, ok := svst.normalized2original[normalized]; ok {
			bits |= 1 << svst.value2BitIndex[origin]
		} else {
			if x, err := strconv.ParseUint(sss, 10, 64); err == nil {
				if x == 0 {
					continue
				}
				bitsCount := bits2.TrailingZeros64(x)
				xv := 1 << uint64(bitsCount)
				if _, ok2 := svst.bitIndex2Value[xv]; ok2 {
					bits |= uint64(xv)
					continue
				}
			}
			return 0, errorValueIsInvalid
		}
	}
	return bits, nil
}

func (svst SystemVariableSetType) Convert(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	cv1 := func(x uint64) (interface{}, error) {
		if x <= svst.bitmap() {
			return svst.bits2string(x)
		}
		return nil, errorConvertToSetFailed
	}
	cv2 := func(x string) (interface{}, error) {
		bits, err := svst.string2bits(x)
		if err != nil {
			return nil, err
		}
		return svst.bits2string(bits)
	}

	switch v := value.(type) {
	case int:
		return cv1(uint64(v))
	case uint:
		return cv1(uint64(v))
	case int8:
		return cv1(uint64(v))
	case uint8:
		return cv1(uint64(v))
	case int16:
		return cv1(uint64(v))
	case uint16:
		return cv1(uint64(v))
	case int32:
		return cv1(uint64(v))
	case uint32:
		return cv1(uint64(v))
	case int64:
		return cv1(uint64(v))
	case uint64:
		return cv1(v)
	case float32:
		return cv1(uint64(v))
	case float64:
		return cv1(uint64(v))
	case string:
		return cv2(v)
	case []byte:
		return cv2(string(v))
	}
	return nil, errorConvertToSetFailed
}

func (svst SystemVariableSetType) Type() types.T {
	return types.T_any
}

func (svst SystemVariableSetType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_SET
}

func (svst SystemVariableSetType) Zero() interface{} {
	return ""
}

func (svst SystemVariableSetType) ConvertFromString(value string) (interface{}, error) {
	bits, err := svst.string2bits(value)
	if err != nil {
		return nil, getErrorConvertFromStringToSetFailed(value)
	}
	return svst.bits2string(bits)
}

func InitSystemVariableSetType(name string, values ...string) SystemVariableSetType {
	if len(values) == 0 {
		panic(errorValuesOfSetIsEmpty)
	}
	if len(values) > MaxMemberCountOfSetType {
		panic(errorValuesOfSetGreaterThan64)
	}

	normalized2original := make(map[string]string)
	value2BitIndex := make(map[string]int)
	bitIndex2Value := make(map[int]string)
	for i, value := range values {
		if strings.Contains(value, ",") {
			panic(errorValueHasComma)
		}
		v := strings.TrimRight(value, " ")
		lv := strings.ToLower(v)
		if _, ok := normalized2original[lv]; ok {
			panic(errorValueIsDuplicate)
		}
		normalized2original[lv] = v
		value2BitIndex[v] = i
		bitIndex2Value[i] = v
	}

	return SystemVariableSetType{
		normalized2original: normalized2original,
		value2BitIndex:      value2BitIndex,
		bitIndex2Value:      bitIndex2Value,
	}
}

type SystemVariableStringType struct {
	name string
}

func InitSystemVariableStringType(name string) SystemVariableStringType {
	return SystemVariableStringType{
		name: name,
	}
}

func (svst SystemVariableStringType) String() string {
	return "STRING"
}

func (svst SystemVariableStringType) Convert(value interface{}) (interface{}, error) {
	if value == nil {
		return "", nil
	}
	if v, ok := value.(string); ok {
		return v, nil
	}
	return nil, errorConvertToStringFailed
}

func (svst SystemVariableStringType) Type() types.T {
	return types.T_varchar
}

func (svst SystemVariableStringType) MysqlType() defines.MysqlType {
	return defines.MYSQL_TYPE_VARCHAR
}

func (svst SystemVariableStringType) Zero() interface{} {
	return ""
}

func (svst SystemVariableStringType) ConvertFromString(value string) (interface{}, error) {
	return value, nil
}

type SystemVariable struct {
	Name string

	// scope of the system variable includes Global,Session,Both
	Scope Scope

	// can be changed during runtime
	Dynamic bool

	//can be set for single query by SET_VAR()
	SetVarHintApplies bool

	Type SystemVariableType

	Default interface{}

	UpdateSessVar func(context.Context, *Session, map[string]interface{}, string, interface{}) error
}

func (sv SystemVariable) GetName() string {
	return sv.Name
}

func (sv SystemVariable) GetScope() Scope {
	return sv.Scope
}

func (sv SystemVariable) GetDynamic() bool {
	return sv.Dynamic
}

func (sv SystemVariable) GetSetVarHintApplies() bool {
	return sv.SetVarHintApplies
}

func (sv SystemVariable) GetType() SystemVariableType {
	return sv.Type
}

func (sv SystemVariable) GetDefault() interface{} {
	return sv.Default
}

type GlobalSystemVariables struct {
	mu sync.Mutex
	// name -> value/default
	sysVars map[string]interface{}
}

// the set of variables
var GSysVariables = &GlobalSystemVariables{
	sysVars: make(map[string]interface{}),
}

// initialize system variables from definition
func InitGlobalSystemVariables(gsv *GlobalSystemVariables) {
	if gsv.sysVars == nil {
		gsv.sysVars = make(map[string]interface{})
	}
	for _, def := range gSysVarsDefs {
		gsv.sysVars[def.GetName()] = def.GetDefault()
	}
}

// add custom system variables
func (gsv *GlobalSystemVariables) AddSysVariables(vars []SystemVariable) {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	for _, v := range vars {
		vv := v
		lname := strings.ToLower(vv.GetName())
		vv.Name = lname
		gSysVarsDefs[lname] = vv
		gsv.sysVars[lname] = vv.GetDefault()
	}
}

// set values to system variables
func (gsv *GlobalSystemVariables) SetValues(ctx context.Context, values map[string]interface{}) error {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	for name, val := range values {
		name = strings.ToLower(name)
		if sv, ok := gSysVarsDefs[name]; ok {
			cv, err := sv.GetType().Convert(val)
			if err != nil {
				return err
			}
			gsv.sysVars[name] = cv
		} else {
			return moerr.NewInternalError(ctx, errorSystemVariableDoesNotExist())
		}
	}
	return nil
}

// copy global system variable to session
func (gsv *GlobalSystemVariables) CopySysVarsToSession() map[string]interface{} {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	sesSysVars := make(map[string]interface{}, len(gsv.sysVars))
	for name, value := range gsv.sysVars {
		sesSysVars[name] = value
	}
	return sesSysVars
}

// get system variable definition ,value.
// return false, if there is no such variable.
func (gsv *GlobalSystemVariables) GetGlobalSysVar(name string) (SystemVariable, interface{}, bool) {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	name = strings.ToLower(name)
	if v, ok := gSysVarsDefs[name]; ok {
		return v, gsv.sysVars[name], true
	}
	return SystemVariable{}, nil, false
}

// get the definition of the system variable
func (gsv *GlobalSystemVariables) GetDefinitionOfSysVar(name string) (SystemVariable, bool) {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	name = strings.ToLower(name)
	if v, ok := gSysVarsDefs[name]; ok {
		return v, ok
	}
	return SystemVariable{}, false
}

// set global dynamic variable by SET GLOBAL
func (gsv *GlobalSystemVariables) SetGlobalSysVar(ctx context.Context, name string, value interface{}) error {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	name = strings.ToLower(name)
	if sv, ok := gSysVarsDefs[name]; ok {
		if sv.GetScope() == ScopeSession {
			return moerr.NewInternalError(ctx, errorSystemVariableIsSession())
		}
		if !sv.GetDynamic() {
			return moerr.NewInternalError(ctx, errorSystemVariableIsReadOnly())
		}
		val, err := sv.GetType().Convert(value)
		if err != nil {
			return err
		}
		gsv.sysVars[name] = val
	} else {
		return moerr.NewInternalError(ctx, errorSystemVariableDoesNotExist())
	}
	return nil
}

func init() {
	InitGlobalSystemVariables(GSysVariables)
}

// definitions of system variables
var gSysVarsDefs = map[string]SystemVariable{
	"port": {
		Name:              "port",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("port", 0, 65535, false),
		Default:           int64(6001),
	},
	"host": {
		Name:              "host",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("host"),
		Default:           "0.0.0.0",
	},
	"max_allowed_packet": {
		Name:              "max_allowed_packet",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_allowed_packet", 1024, 1073741824, false),
		Default:           int64(67108864),
	},
	"version_comment": {
		Name:              "version_comment",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("version_comment"),
		Default:           "MatrixOne",
	},
	"tx_isolation": {
		Name:              "tx_isolation",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("tx_isolation", "READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"),
		Default:           "REPEATABLE-READ",
	},
	"testglobalvar_dyn": {
		Name:              "testglobalvar_dyn",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("testglobalvar_dyn", 0, 100, false),
		Default:           int64(0),
	},
	"testglobalvar_nodyn": {
		Name:              "testglobalvar_nodyn",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("testglobalvar_nodyn", 0, 100, false),
		Default:           int64(0),
	},
	"testsessionvar_dyn": {
		Name:              "testsessionvar_dyn",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("testsessionvar_dyn", 0, 100, false),
		Default:           int64(0),
	},
	"testsessionvar_nodyn": {
		Name:              "testsessionvar_nodyn",
		Scope:             ScopeSession,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("testsessionvar_nodyn", 0, 100, false),
		Default:           int64(0),
	},
	"testbothvar_dyn": {
		Name:              "testbothvar_dyn",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("testbothvar_dyn", 0, 100, false),
		Default:           int64(0),
	},
	"testbotchvar_nodyn": {
		Name:              "testbotchvar_nodyn",
		Scope:             ScopeBoth,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("testbotchvar_nodyn", 0, 100, false),
		Default:           int64(0),
	},
	"character_set_client": {
		Name:              "character_set_client",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_client"),
		Default:           "utf8mb4",
	},
	"character_set_server": {
		Name:              "character_set_server",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_server"),
		Default:           "utf8mb4",
	},
	"character_set_database": {
		Name:              "character_set_database",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_database"),
		Default:           "utf8mb4",
	},
	"character_set_connection": {
		Name:              "character_set_connection",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_connection"),
		Default:           "utf8mb4",
	},
	"character_set_results": {
		Name:              "character_set_results",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_results"),
		Default:           "utf8mb4",
	},
	"collation_connection": {
		Name:              "collation_connection",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("collation_connection"),
		Default:           "default",
	},
	"collation_server": {
		Name:              "collation_server",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("collation_server"),
		Default:           "utf8mb4_bin",
	},
	"license": {
		Name:              "license",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("license"),
		Default:           "APACHE",
	},
	"autocommit": {
		Name:              "autocommit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("autocommit"),
		Default:           int64(1),
	},
	"sql_mode": {
		Name:              "sql_mode",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableSetType("sql_mode", "ANSI", "TRADITIONAL", "ALLOW_INVALID_DATES", "ANSI_QUOTES", "ERROR_FOR_DIVISION_BY_ZERO", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE", "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES", "NO_DIR_IN_CREATE", "NO_ENGINE_SUBSTITUTION", "NO_UNSIGNED_SUBTRACTION", "NO_ZERO_DATE", "NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY", "PAD_CHAR_TO_FULL_LENGTH", "PIPES_AS_CONCAT", "REAL_AS_FLOAT", "STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "TIME_TRUNCATE_FRACTIONAL"),
		Default:           "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
	},
	"completion_type": {
		Name:              "completion_type",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("completion_type", "NO_CHAIN", "CHAIN", "RELEASE"),
		Default:           "NO_CHAIN",
	},
	"time_zone": {
		Name:              "time_zone",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableStringType("time_zone"),
		Default:           "SYSTEM",
		UpdateSessVar:     updateTimeZone,
	},
	"auto_increment_increment": {
		Name:              "auto_increment_increment",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("auto_increment_increment", 1, 65535, false),
		Default:           int64(1),
	},
	"auto_increment_offset": {
		Name:              "auto_increment_offset",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("auto_increment_offset", 1, 65535, false),
		Default:           int64(1),
	},
	"init_connect": {
		Name:              "init_connect",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("init_connect"),
		Default:           "",
	},
	"interactive_timeout": {
		Name:              "interactive_timeout",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("interactive_timeout", 1, 31536000, false),
		Default:           int64(28800),
	},
	"lower_case_table_names": {
		Name:              "lower_case_table_names",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("lower_case_table_names", 0, 2, false),
		Default:           int64(1),
	},
	"net_write_timeout": {
		Name:              "net_write_timeout",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("net_write_timeout", 1, 31536000, false),
		Default:           int64(60),
	},
	"system_time_zone": {
		Name:              "system_time_zone",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("system_time_zone"),
		Default:           getSystemTimeZone(),
	},
	"transaction_isolation": {
		Name:              "transaction_isolation",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("transaction_isolation", "READ-UNCOMMITTED", "READ-COMMITTED", "REPEATABLE-READ", "SERIALIZABLE"),
		Default:           "REPEATABLE-READ",
	},
	"wait_timeout": {
		Name:              "wait_timeout",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("wait_timeout", 1, 2147483, false),
		Default:           int64(28800),
	},
	"sql_safe_updates": {
		Name:              "sql_safe_updates",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("sql_safe_updates", 0, 1, false),
		Default:           int64(0),
	},
	"profiling": {
		Name:              "profiling",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("profiling", 0, 1, false),
		Default:           int64(0),
	},
	"performance_schema": {
		Name:              "performance_schema",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("performance_schema", 0, 1, false),
		Default:           int64(0),
	},
	"transaction_read_only": {
		Name:              "transaction_read_only",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("transaction_read_only", 0, 1, false),
		Default:           int64(0),
	},
	"tx_read_only": {
		Name:              "tx_read_only",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("tx_read_only", 0, 1, false),
		Default:           int64(0),
	},
	"sql_select_limit": {
		Name:              "sql_select_limit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableUintType("sql_select_limit", 0, 18446744073709551615),
		Default:           uint64(18446744073709551615),
	},
	"save_query_result": {
		Name:              "save_query_result",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("save_query_result"),
		Default:           int64(0),
	},
	"query_result_timeout": {
		Name:              "query_result_timeout",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableUintType("query_result_timeout", 0, 18446744073709551615),
		Default:           uint64(24),
	},
	"query_result_maxsize": {
		Name:              "query_result_maxsize",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableUintType("query_result_maxsize", 0, 18446744073709551615),
		Default:           uint64(100),
	},
	//whether TN does primary key uniqueness check against transaction's workspace or not.
	"mo_pk_check_by_dn": {
		Name:              "mo_pk_check_by_dn",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("mo_pk_check_by_dn"),
		Default:           int8(0),
	},
	"syspublications": {
		Name:              "syspublications",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("syspublications"),
		Default:           "",
	},
	"net_buffer_length": {
		Name:              "net_buffer_length",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("net_buffer_length", 1024, 1048576, false),
		Default:           int64(16384),
	},
	"enable_privilege_cache": {
		Name:              "enable_privilege_cache",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("enable_privilege_cache"),
		Default:           int64(1),
	},
	"clear_privilege_cache": {
		Name:              "clear_privilege_cache",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("clear_privilege_cache"),
		Default:           int64(0),
	},
	"foreign_key_checks": {
		Name:              "foreign_key_checks",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("foreign_key_checks"),
		Default:           int64(1),
	},
	"authentication_policy": {
		Name:              "authentication_policy",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("authentication_policy"),
		Default:           "*",
	},
	"authentication_windows_log_level": {
		Name:              "authentication_windows_log_level",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("authentication_windows_log_level", 0, 4, false),
		Default:           int64(2),
	},
	"authentication_windows_use_principal_name": {
		Name:              "authentication_windows_use_principal_name",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("authentication_windows_use_principal_name"),
		Default:           int64(1),
	},
	"automatic_sp_privileges": {
		Name:              "automatic_sp_privileges",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("automatic_sp_privileges"),
		Default:           int64(1),
	},
	"auto_generate_certs": {
		Name:              "auto_generate_certs",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("auto_generate_certs"),
		Default:           int64(1),
	},
	"avoid_temporal_upgrade": {
		Name:              "avoid_temporal_upgrade",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("avoid_temporal_upgrade"),
		Default:           int64(0),
	},
	"back_log": {
		Name:              "back_log",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("back_log", 1, 65535, false),
		Default:           int64(1),
	},
	"basedir": {
		Name:              "basedir",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("basedir"),
		Default:           "",
	},
	"big_tables": {
		Name:              "big_tables",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("big_tables"),
		Default:           int64(0),
	},
	"bind_address": {
		Name:              "bind_address",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("bind_address"),
		Default:           "*",
	},
	"block_encryption_mode": {
		Name:              "block_encryption_mode",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("block_encryption_mode"),
		Default:           "aes-128-ecb",
	},
	"build_id": {
		Name:              "build_id",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("build_id"),
		Default:           "",
	},
	"bulk_insert_buffer_size": {
		Name:              "bulk_insert_buffer_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("bulk_insert_buffer_size", 0, 4294967295, false),
		Default:           int64(8388608),
	},
	"caching_sha2_password_digest_rounds": {
		Name:              "caching_sha2_password_digest_rounds",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("caching_sha2_password_digest_rounds", 5000, 4095000, false),
		Default:           int64(5000),
	},
	"caching_sha2_password_auto_generate_rsa_keys": {
		Name:              "caching_sha2_password_auto_generate_rsa_keys",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("caching_sha2_password_auto_generate_rsa_keys"),
		Default:           int64(1),
	},
	"caching_sha2_password_private_key_path": {
		Name:              "caching_sha2_password_private_key_path",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("caching_sha2_password_private_key_path"),
		Default:           "private_key.pem",
	},
	"character_set_filesystem": {
		Name:              "character_set_filesystem",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_filesystem"),
		Default:           "binary",
	},
	"character_set_system": {
		Name:              "character_set_system",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_set_system"),
		Default:           "utf8mb3",
	},
	"character_sets_dir": {
		Name:              "character_sets_dir",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("character_sets_dir"),
		Default:           "",
	},
	"check_proxy_users": {
		Name:              "check_proxy_users",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("check_proxy_users"),
		Default:           int64(0),
	},
	"collation_database": {
		Name:              "collation_database",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("collation_database"),
		Default:           "utf8mb4_0900_ai_ci",
	},
	"concurrent_insert": {
		Name:              "concurrent_insert",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("concurrent_insert", "NEVER", "AUTO", "ALWAYS", "0", "1", "2"),
		Default:           "AUTO",
	},
	"connect_timeout": {
		Name:              "connect_timeout",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("connect_timeout", 2, 31536000, false),
		Default:           int64(10),
	},
	"connection_memory_chunk_size": {
		Name:              "connection_memory_chunk_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("connection_memory_chunk_size", 0, 536870912, false),
		Default:           int64(8192),
	},
	"connection_memory_limit": {
		Name:              "connection_memory_limit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("connection_memory_limit", 0, math.MaxInt64, false),
		Default:           int64(math.MaxInt64),
	},
	"core_file": {
		Name:              "core_file",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("core_file"),
		Default:           int64(0),
	},
	"create_admin_listener_thread": {
		Name:              "create_admin_listener_thread",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("create_admin_listener_thread"),
		Default:           int64(0),
	},
	"cte_max_recursion_depth": {
		Name:              "cte_max_recursion_depth",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("cte_max_recursion_depth", 0, 4294967295, false),
		Default:           int64(1000),
	},
	"datadir": {
		Name:              "datadir",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("datadir"),
		Default:           "",
	},
	"debug": {
		Name:              "debug",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("debug"),
		Default:           "/tmp/mysqld.trace",
	},
	"debug_sync": {
		Name:              "debug_sync",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("debug_sync"),
		Default:           "",
	},
	"default_authentication_plugin": {
		Name:              "default_authentication_plugin",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("default_authentication_plugin", "mysql_native_password", "sha256_password", "caching_sha2_password"),
		Default:           "caching_sha2_password",
	},
	"default_collation_for_utf8mb4": {
		Name:              "default_collation_for_utf8mb4",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("default_collation_for_utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_general_ci"),
		Default:           "utf8mb4_0900_ai_ci",
	},
	"default_password_lifetime": {
		Name:              "default_password_lifetime",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("default_password_lifetime", 0, 65535, false),
		Default:           int64(0),
	},
	"default_storage_engine": {
		Name:              "default_storage_engine",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("default_storage_engine", "InnoDB"),
		Default:           "InnoDB",
	},
	"default_table_encryption": {
		Name:              "default_table_encryption",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableBoolType("default_table_encryption"),
		Default:           int64(0),
	},
	"default_tmp_storage_engine": {
		Name:              "default_tmp_storage_engine",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemSystemEnumType("default_tmp_storage_engine", "InnoDB"),
		Default:           "InnoDB",
	},
	"default_week_format": {
		Name:              "default_week_format",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("default_week_format", 0, 7, false),
		Default:           int64(0),
	},
	"delay_key_write": {
		Name:              "delay_key_write",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("delay_key_write", "OFF", "ON", "ALL"),
		Default:           "ON",
	},
	"delayed_insert_limit": {
		Name:              "delayed_insert_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("delayed_insert_limit", 1, math.MaxInt64, false),
		Default:           int64(100),
	},
	"delayed_insert_timeout": {
		Name:              "delayed_insert_timeout",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("delayed_insert_timeout", 1, 31536000, false),
		Default:           int64(300),
	},
	"delayed_queue_size": {
		Name:              "delayed_queue_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("delayed_queue_size", 1, math.MaxInt64, false),
		Default:           int64(1000),
	},
	"disabled_storage_engines": {
		Name:              "disabled_storage_engines",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("disabled_storage_engines"),
		Default:           "",
	},
	"disconnect_on_expired_password": {
		Name:              "disconnect_on_expired_password",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("disconnect_on_expired_password"),
		Default:           int64(1),
	},
	"div_precision_increment": {
		Name:              "div_precision_increment",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("div_precision_increment", 0, 30, false),
		Default:           int64(4),
	},
	"end_markers_in_json": {
		Name:              "end_markers_in_json",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableBoolType("end_markers_in_json"),
		Default:           int64(0),
	},
	"eq_range_index_dive_limit": {
		Name:              "eq_range_index_dive_limit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("eq_range_index_dive_limit", 0, 4294967295, false),
		Default:           int64(0),
	},
	"event_scheduler": {
		Name:              "event_scheduler",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("event_scheduler", "ON", "OFF", "DISABLED"),
		Default:           "ON",
	},
	"explain_format": {
		Name:              "explain_format",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("explain_format", "DEFAULT", "TRADITIONAL", "JSON", "TREE"),
		Default:           "TRADITIONAL",
	},
	"explicit_defaults_for_timestamp": {
		Name:              "explicit_defaults_for_timestamp",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("explicit_defaults_for_timestamp"),
		Default:           int64(1),
	},
	"external_user": {
		Name:              "external_user",
		Scope:             ScopeSession,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("external_user"),
		Default:           "",
	},
	"flush": {
		Name:              "flush",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("flush"),
		Default:           int64(0),
	},
	"flush_time": {
		Name:              "flush_time",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("flush_time", 0, 31536000, false),
		Default:           int64(0),
	},
	"ft_boolean_syntax": {
		Name:              "ft_boolean_syntax",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("ft_boolean_syntax"),
		Default:           "+ -><()~*:\"\"&|",
	},
	"ft_max_word_len": {
		Name:              "ft_max_word_len",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("ft_max_word_len", 10, 84, false),
		Default:           int64(84),
	},
	"ft_min_word_len": {
		Name:              "ft_max_word_len",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("ft_min_word_len", 1, 82, false),
		Default:           int64(4),
	},
	"ft_query_expansion_limit": {
		Name:              "ft_query_expansion_limit",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("ft_query_expansion_limit", 0, 1000, false),
		Default:           int64(20),
	},
	"ft_stopword_file": {
		Name:              "ft_stopword_file",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("ft_stopword_file"),
		Default:           "",
	},
	"general_log": {
		Name:              "general_log",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("general_log"),
		Default:           int64(0),
	},
	"general_log_file": {
		Name:              "general_log_file",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("general_log_file"),
		Default:           "host_name.log",
	},
	"generated_random_password_length": {
		Name:              "generated_random_password_length",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("generated_random_password_length", 5, 255, false),
		Default:           int64(20),
	},
	"global_connection_memory_limit": {
		Name:              "global_connection_memory_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("global_connection_memory_limit", 16777216, math.MaxInt64, false),
		Default:           int64(math.MaxInt64),
	},
	"global_connection_memory_tracking": {
		Name:              "global_connection_memory_tracking",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("global_connection_memory_tracking"),
		Default:           int64(0),
	},
	"group_concat_max_len": {
		Name:              "group_concat_max_len",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("group_concat_max_len", 4, math.MaxInt64, false),
		Default:           int64(4),
	},
	"have_ssl": {
		Name:              "have_ssl",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("have_ssl"),
		Default:           "YES",
	},
	"have_statement_timeout": {
		Name:              "have_statement_timeout",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("have_statement_timeout"),
		Default:           int64(0),
	},
	"histogram_generation_max_mem_size": {
		Name:              "histogram_generation_max_mem_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("histogram_generation_max_mem_size", 1000000, math.MaxInt64, false),
		Default:           int64(1000000),
	},
	"host_cache_size": {
		Name:              "host_cache_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("host_cache_size", 0, 65536, false),
		Default:           int64(0),
	},
	"hostname": {
		Name:              "hostname",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("hostname"),
		Default:           "",
	},
	"information_schema_stats_expiry": {
		Name:              "information_schema_stats_expiry",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("information_schema_stats_expiry", 0, 86400, false),
		Default:           int64(86400),
	},
	"init_file": {
		Name:              "init_file",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("init_file"),
		Default:           "",
	},
	"internal_tmp_disk_storage_engine": {
		Name:              "internal_tmp_disk_storage_engine",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("internal_tmp_disk_storage_engine", "MYISAM", "INNODB"),
		Default:           "INNODB",
	},
	"internal_tmp_mem_storage_engine": {
		Name:              "internal_tmp_mem_storage_engine",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemSystemEnumType("internal_tmp_mem_storage_engine", "MEMORY", "TempTable"),
		Default:           "TempTable",
	},
	"join_buffer_size": {
		Name:              "join_buffer_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("join_buffer_size", 128, 4294967168, false),
		Default:           int64(262144),
	},
	"keep_files_on_create": {
		Name:              "keep_files_on_create",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("join_buffer_size"),
		Default:           int64(0),
	},
	"key_buffer_size": {
		Name:              "key_buffer_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("key_buffer_size", 0, 4294967295, false),
		Default:           int64(8388608),
	},
	"key_cache_age_threshold": {
		Name:              "key_cache_age_threshold",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("key_cache_age_threshold", 100, 4294967295, false),
		Default:           int64(300),
	},
	"key_cache_block_size": {
		Name:              "key_cache_block_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("key_cache_block_size", 512, 16384, false),
		Default:           int64(1024),
	},
	"key_cache_division_limit": {
		Name:              "key_cache_division_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("key_cache_division_limit", 1, 100, false),
		Default:           int64(100),
	},
	"large_files_support": {
		Name:              "large_files_support",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("large_files_support"),
		Default:           int64(0),
	},
	"large_pages": {
		Name:              "large_pages",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("large_pages"),
		Default:           int64(0),
	},
	"large_page_size": {
		Name:              "large_page_size",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("large_page_size", 0, 65535, false),
		Default:           int64(0),
	},
	"lc_messages": {
		Name:              "lc_messages",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("lc_messages"),
		Default:           "en_US",
	},
	"lc_messages_dir": {
		Name:              "lc_messages_dir",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("lc_messages_dir"),
		Default:           "",
	},
	"lc_time_names": {
		Name:              "lc_time_names",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("lc_time_names"),
		Default:           "",
	},
	"local_infile": {
		Name:              "local_infile",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("local_infile"),
		Default:           int64(0),
	},
	"lock_wait_timeout": {
		Name:              "lock_wait_timeout",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("lock_wait_timeout", 1, 31536000, false),
		Default:           int64(31536000),
	},
	"locked_in_memory": {
		Name:              "locked_in_memory",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("locked_in_memory"),
		Default:           int64(0),
	},
	"log_error": {
		Name:              "log_error",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("log_error"),
		Default:           "",
	},
	"log_error_services": {
		Name:              "log_error_services",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("log_error_services"),
		Default:           "log_filter_internal",
	},
	"log_error_suppression_list": {
		Name:              "log_error_suppression_list",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("log_error_suppression_list"),
		Default:           "",
	},
	"log_error_verbosity": {
		Name:              "log_error_verbosity",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("log_error_verbosity", 1, 3, false),
		Default:           int64(2),
	},
	"log_output": {
		Name:              "log_output",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("log_output", "TABLE", "FILE", "NONE"),
		Default:           "FILE",
	},
	"log_queries_not_using_indexes": {
		Name:              "log_queries_not_using_indexes",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("log_queries_not_using_indexes"),
		Default:           int64(0),
	},
	"log_raw": {
		Name:              "log_raw",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("log_raw"),
		Default:           int64(0),
	},
	"log_slow_admin_statements": {
		Name:              "log_slow_admin_statements",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("log_slow_admin_statements"),
		Default:           int64(0),
	},
	"log_slow_extra": {
		Name:              "log_slow_extra",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("log_slow_extra"),
		Default:           int64(0),
	},
	"log_syslog": {
		Name:              "log_syslog",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("log_syslog"),
		Default:           int64(1),
	},
	"log_syslog_facility": {
		Name:              "log_syslog_facility",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("log_syslog_facility"),
		Default:           "daemon",
	},
	"log_syslog_include_pid": {
		Name:              "log_syslog_include_pid",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("log_syslog_include_pid"),
		Default:           int64(1),
	},
	"log_syslog_tag": {
		Name:              "log_syslog_tag",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("log_syslog_tag"),
		Default:           "",
	},
	"log_timestamps": {
		Name:              "log_timestamps",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("log_timestamps", "UTC", "SYSTEM"),
		Default:           "UTC",
	},
	"log_throttle_queries_not_using_indexes": {
		Name:              "log_throttle_queries_not_using_indexes",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("log_throttle_queries_not_using_indexes", 0, 4294967295, false),
		Default:           int64(0),
	},
	"long_query_time": {
		Name:              "long_query_time",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableDoubleType("long_query_time", 0, 31536000),
		Default:           float64(10),
	},
	"low_priority_updates": {
		Name:              "low_priority_updates",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("low_priority_updates"),
		Default:           int64(0),
	},
	"lower_case_file_system": {
		Name:              "lower_case_file_system",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("lower_case_file_system"),
		Default:           int64(0),
	},
	"mandatory_roles": {
		Name:              "mandatory_roles",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("mandatory_roles"),
		Default:           "",
	},
	"max_connect_errors": {
		Name:              "max_connect_errors",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_connect_errors", 1, 4294967295, false),
		Default:           int64(100),
	},
	"max_connections": {
		Name:              "max_connections",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_connections", 1, 100000, false),
		Default:           int64(151),
	},
	"max_delayed_threads": {
		Name:              "max_delayed_threads",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_delayed_threads", 0, 16384, false),
		Default:           int64(20),
	},
	"max_digest_length": {
		Name:              "max_digest_length",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_digest_length", 0, 1048576, false),
		Default:           int64(1024),
	},
	"max_error_count": {
		Name:              "max_error_count",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_error_count", 0, 65535, false),
		Default:           int64(1024),
	},
	"max_execution_time": {
		Name:              "max_execution_time",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_execution_time", 0, 4294967295, false),
		Default:           int64(0),
	},
	"max_heap_table_size": {
		Name:              "max_heap_table_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_heap_table_size", 16384, 4294966272, false),
		Default:           int64(16777216),
	},
	"max_insert_delayed_threads": {
		Name:              "max_insert_delayed_threads",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_insert_delayed_threads", 0, 16384, false),
		Default:           int64(20),
	},
	"max_join_size": {
		Name:              "max_join_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_join_size", 0, math.MaxInt64, false),
		Default:           int64(math.MaxInt64),
	},
	"max_length_for_sort_data": {
		Name:              "max_length_for_sort_data",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_length_for_sort_data", 4, 8388608, false),
		Default:           int64(4096),
	},
	"max_points_in_geometry": {
		Name:              "max_points_in_geometry",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_points_in_geometry", 3, 1048576, false),
		Default:           int64(65536),
	},
	"max_prepared_stmt_count": {
		Name:              "max_prepared_stmt_count",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_prepared_stmt_count", 0, 4194304, false),
		Default:           int64(16382),
	},
	"max_seeks_for_key": {
		Name:              "max_seeks_for_key",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_seeks_for_key", 1, 4294967295, false),
		Default:           int64(4294967295),
	},
	"max_sort_length": {
		Name:              "max_sort_length",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableIntType("max_sort_length", 4, 8388608, false),
		Default:           int64(1024),
	},
	"max_sp_recursion_depth": {
		Name:              "max_sp_recursion_depth",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_sp_recursion_depth", 0, 255, false),
		Default:           int64(0),
	},
	"max_user_connections": {
		Name:              "max_user_connections",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_user_connections", 0, 4294967295, false),
		Default:           int64(0),
	},
	"max_write_lock_count": {
		Name:              "max_write_lock_count",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("max_write_lock_count", 1, 4294967295, false),
		Default:           int64(4294967295),
	},
	"mecab_rc_file": {
		Name:              "mecab_rc_file",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("mecab_rc_file"),
		Default:           "",
	},
	"metadata_locks_cache_size": {
		Name:              "metadata_locks_cache_size",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("metadata_locks_cache_size", 1, 1048576, false),
		Default:           int64(1024),
	},
	"metadata_locks_hash_instances": {
		Name:              "metadata_locks_hash_instances",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("metadata_locks_hash_instances", 1, 1024, false),
		Default:           int64(8),
	},
	"min_examined_row_limit": {
		Name:              "min_examined_row_limit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("min_examined_row_limit", 0, 4294967295, false),
		Default:           int64(0),
	},
	"myisam_data_pointer_size": {
		Name:              "myisam_data_pointer_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("myisam_data_pointer_size", 2, 7, false),
		Default:           int64(6),
	},
	"myisam_max_sort_file_size": {
		Name:              "myisam_max_sort_file_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("myisam_max_sort_file_size", 0, 2146435072, false),
		Default:           int64(2146435072),
	},
	"myisam_mmap_size": {
		Name:              "myisam_mmap_size",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("myisam_mmap_size", 7, 4294967295, false),
		Default:           int64(4294967295),
	},
	"myisam_recover_options": {
		Name:              "myisam_recover_options",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("myisam_recover_options", "OFF", "DEFAULT", "BACKUP", "FORCE", "QUICK"),
		Default:           "OFF",
	},
	"myisam_repair_threads": {
		Name:              "myisam_repair_threads",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("myisam_repair_threads", 1, 4294967295, false),
		Default:           int64(1),
	},
	"myisam_sort_buffer_size": {
		Name:              "myisam_sort_buffer_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("myisam_sort_buffer_size", 4096, 4294967295, false),
		Default:           int64(8388608),
	},
	"myisam_stats_method": {
		Name:              "myisam_stats_method",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("myisam_stats_method", "nulls_unequal", "nulls_equal", "nulls_ignored"),
		Default:           "nulls_unequal",
	},
	"myisam_use_mmap": {
		Name:              "myisam_use_mmap",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("myisam_use_mmap"),
		Default:           int64(0),
	},
	"mysql_native_password_proxy_users": {
		Name:              "mysql_native_password_proxy_users",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("mysql_native_password_proxy_users"),
		Default:           int64(0),
	},
	"named_pipe": {
		Name:              "named_pipe",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("named_pipe"),
		Default:           int64(0),
	},
	"named_pipe_full_access_group": {
		Name:              "named_pipe_full_access_group",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("named_pipe_full_access_group"),
		Default:           "",
	},
	"net_read_timeout": {
		Name:              "net_read_timeout",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("net_read_timeout", 1, 31536000, false),
		Default:           int64(30),
	},
	"net_retry_count": {
		Name:              "net_retry_count",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("net_retry_count", 1, 4294967295, false),
		Default:           int64(10),
	},
	"new": {
		Name:              "new",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("new"),
		Default:           int64(0),
	},
	"ngram_token_size": {
		Name:              "ngram_token_size",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("ngram_token_size", 1, 10, false),
		Default:           int64(2),
	},
	"offline_mode": {
		Name:              "offline_mode",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("offline_mode"),
		Default:           int64(0),
	},
	"old": {
		Name:              "old",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("old"),
		Default:           int64(0),
	},
	"old_alter_table": {
		Name:              "old_alter_table",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("old_alter_table"),
		Default:           int64(0),
	},
	"open_files_limit": {
		Name:              "open_files_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("open_files_limit", 0, 1048576, false),
		Default:           int64(5000),
	},
	"optimizer_hints": {
		Name:              "optimizer_hints",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("optimizer_hints"),
		Default:           "",
	},
	"optimizer_prune_level": {
		Name:              "optimizer_prune_level",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("optimizer_prune_level", 0, 1, false),
		Default:           int64(1),
	},
	"optimizer_search_depth": {
		Name:              "optimizer_search_depth",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("optimizer_search_depth", 0, 62, false),
		Default:           int64(62),
	},
	"optimizer_trace": {
		Name:              "optimizer_trace",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("optimizer_trace"),
		Default:           "",
	},
	"optimizer_trace_features": {
		Name:              "optimizer_trace_features",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("optimizer_trace_features"),
		Default:           "",
	},
	"optimizer_trace_limit": {
		Name:              "optimizer_trace_limit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("optimizer_trace_limit", 0, 2147483647, false),
		Default:           int64(1),
	},
	"optimizer_trace_max_mem_size": {
		Name:              "optimizer_trace_max_mem_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("optimizer_trace_max_mem_size", 0, 4294967295, false),
		Default:           int64(1048576),
	},
	"optimizer_trace_offset": {
		Name:              "optimizer_trace_offset",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("optimizer_trace_offset", -2147483647, 2147483647, false),
		Default:           int64(-1),
	},
	"parser_max_mem_size": {
		Name:              "parser_max_mem_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("parser_max_mem_size", 10000000, 4294967295, false),
		Default:           int64(4294967295),
	},
	"partial_revokes": {
		Name:              "partial_revokes",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("partial_revokes"),
		Default:           int64(0),
	},
	"password_history": {
		Name:              "password_history",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("password_history", 0, 4294967295, false),
		Default:           int64(0),
	},
	"password_require_current": {
		Name:              "password_require_current",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("password_require_current"),
		Default:           int64(0),
	},
	"password_reuse_interval": {
		Name:              "password_reuse_interval",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("password_reuse_interval", 0, 4294967295, false),
		Default:           int64(0),
	},
	"persisted_globals_load": {
		Name:              "persisted_globals_load",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("persisted_globals_load"),
		Default:           int64(0),
	},
	"persist_only_admin_x509_subject": {
		Name:              "persist_only_admin_x509_subject",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("persist_only_admin_x509_subject"),
		Default:           "",
	},
	"persist_sensitive_variables_in_plaintext": {
		Name:              "persist_sensitive_variables_in_plaintext",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("persist_sensitive_variables_in_plaintext"),
		Default:           int64(0),
	},
	"preload_buffer_size": {
		Name:              "preload_buffer_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("preload_buffer_size", 1024, 1073741824, false),
		Default:           int64(32768),
	},
	"print_identified_with_as_hex": {
		Name:              "print_identified_with_as_hex",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("print_identified_with_as_hex"),
		Default:           int64(0),
	},
	"protocol_version": {
		Name:              "protocol_version",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("protocol_version", 0, 4294967295, false),
		Default:           int64(10),
	},
	"proxy_user": {
		Name:              "proxy_user",
		Scope:             ScopeSession,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("proxy_user"),
		Default:           "",
	},
	"pseudo_replica_mode": {
		Name:              "pseudo_replica_mode",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("pseudo_replica_mode"),
		Default:           int64(0),
	},
	"pseudo_slave_mode": {
		Name:              "pseudo_slave_mode",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("pseudo_slave_mode"),
		Default:           int64(0),
	},
	"pseudo_thread_id": {
		Name:              "pseudo_thread_id",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("pseudo_thread_id", 0, 2147483647, false),
		Default:           int64(2147483647),
	},
	"query_alloc_block_size": {
		Name:              "query_alloc_block_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("pseudo_thread_id", 1024, 4294966272, false),
		Default:           int64(8192),
	},
	"rand_seed1": {
		Name:              "rand_seed1",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("rand_seed1", 0, 4294967295, false),
		Default:           int64(0),
	},
	"rand_seed2": {
		Name:              "rand_seed2",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("rand_seed2", 0, 4294967295, false),
		Default:           int64(0),
	},
	"range_optimizer_max_mem_size": {
		Name:              "range_optimizer_max_mem_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("range_optimizer_max_mem_size", 0, 4294967295, false),
		Default:           int64(8388608),
	},
	"rbr_exec_mode": {
		Name:              "rbr_exec_mode",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("rbr_exec_mode", "STRICT", "IDEMPOTENT"),
		Default:           "STRICT",
	},
	"read_buffer_size": {
		Name:              "read_buffer_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("read_buffer_size", 8192, 2147479552, false),
		Default:           int64(131072),
	},
	"read_only": {
		Name:              "read_only",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("read_only"),
		Default:           int64(0),
	},
	"read_rnd_buffer_size": {
		Name:              "read_rnd_buffer_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("read_rnd_buffer_size", 1, 2147483647, false),
		Default:           int64(262144),
	},
	"regexp_stack_limit": {
		Name:              "regexp_stack_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("regexp_stack_limit", 1, 2147483647, false),
		Default:           int64(8000000),
	},
	"regexp_time_limit": {
		Name:              "regexp_time_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("regexp_time_limit", 0, 2147483647, false),
		Default:           int64(32),
	},
	"require_row_format": {
		Name:              "require_row_format",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("require_row_format"),
		Default:           int64(0),
	},
	"require_secure_transport": {
		Name:              "require_secure_transport",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("require_secure_transport"),
		Default:           int64(0),
	},
	"resultset_metadata": {
		Name:              "resultset_metadata",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("resultset_metadata", "FULL", "NONE"),
		Default:           "FULL",
	},
	"runtime_filter_limit_in": {
		Name:              "runtime_filter_limit_in",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("runtime_filter_limit_in", 1, 4294967295, false),
		Default:           int64(10000),
	},
	"runtime_filter_limit_bloom_filter": {
		Name:              "runtime_filter_limit_bloom_filter",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("runtime_filter_limit_bloom_filter", 1, 4294967295, false),
		Default:           int64(1000000),
	},
	"schema_definition_cache": {
		Name:              "schema_definition_cache",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("schema_definition_cache", 256, 524288, false),
		Default:           int64(256),
	},
	"secure_file_priv": {
		Name:              "secure_file_priv",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("secure_file_priv"),
		Default:           "",
	},
	"select_into_disk_sync": {
		Name:              "select_into_disk_sync",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("select_into_disk_sync"),
		Default:           int64(0),
	},
	"select_into_disk_sync_delay": {
		Name:              "select_into_disk_sync_delay",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("select_into_disk_sync_delay", 0, 31536000, false),
		Default:           int64(0),
	},
	"session_track_gtids": {
		Name:              "session_track_gtids",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("session_track_gtids", "OFF", "OWN_GTID", "ALL_GTIDS"),
		Default:           "OFF",
	},
	"session_track_schema": {
		Name:              "session_track_schema",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("session_track_schema"),
		Default:           int64(1),
	},
	"session_track_state_change": {
		Name:              "session_track_state_change",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("session_track_state_change"),
		Default:           int64(0),
	},
	"session_track_system_variables": {
		Name:              "session_track_system_variables",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("session_track_system_variables"),
		Default:           "time_zone, autocommit, character_set_client, character_set_results, character_set_connection",
	},
	"session_track_transaction_info": {
		Name:              "session_track_transaction_info",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("session_track_transaction_info", "OFF", "STATE", "CHARACTERISTICS"),
		Default:           "OFF",
	},
	"sha256_password_auto_generate_rsa_keys": {
		Name:              "sha256_password_auto_generate_rsa_keys",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sha256_password_auto_generate_rsa_keys"),
		Default:           int64(1),
	},
	"sha256_password_proxy_users": {
		Name:              "sha256_password_proxy_users",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sha256_password_proxy_users"),
		Default:           int64(0),
	},
	"shared_memory": {
		Name:              "shared_memory",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("shared_memory"),
		Default:           int64(0),
	},
	"shared_memory_base_name": {
		Name:              "shared_memory_base_name",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("session_track_system_variables"),
		Default:           "MYSQL",
	},
	"show_create_table_skip_secondary_engine": {
		Name:              "show_create_table_skip_secondary_engine",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("show_create_table_skip_secondary_engine"),
		Default:           int64(0),
	},
	"show_create_table_verbosity": {
		Name:              "show_create_table_verbosity",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("show_create_table_verbosity"),
		Default:           int64(0),
	},
	"show_gipk_in_create_table_and_information_schema": {
		Name:              "show_gipk_in_create_table_and_information_schema",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("show_gipk_in_create_table_and_information_schema"),
		Default:           int64(1),
	},
	"show_old_temporals": {
		Name:              "show_old_temporals",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("show_old_temporals"),
		Default:           int64(0),
	},
	"skip_external_locking": {
		Name:              "skip_external_locking",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("skip_external_locking"),
		Default:           int64(1),
	},
	"skip_name_resolve": {
		Name:              "skip_name_resolve",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("skip_name_resolve"),
		Default:           int64(0),
	},
	"skip_networking": {
		Name:              "skip_networking",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("skip_networking"),
		Default:           int64(0),
	},
	"skip_show_database": {
		Name:              "skip_show_database",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("skip_show_database"),
		Default:           int64(0),
	},
	"slow_query_log": {
		Name:              "slow_query_log",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("slow_query_log"),
		Default:           int64(0),
	},
	"slow_launch_time": {
		Name:              "slow_launch_time",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("slow_launch_time", 0, 31536000, false),
		Default:           int64(2),
	},
	"socket": {
		Name:              "socket",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("socket"),
		Default:           "/tmp/mysql.sock",
	},
	"sql_auto_is_null": {
		Name:              "sql_auto_is_null",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_auto_is_null"),
		Default:           int64(1),
	},
	"sql_big_selects": {
		Name:              "sql_big_selects",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_big_selects"),
		Default:           int64(0),
	},
	"sql_buffer_result": {
		Name:              "sql_buffer_result",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_buffer_result"),
		Default:           int64(0),
	},
	"sql_generate_invisible_primary_key": {
		Name:              "sql_generate_invisible_primary_key",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_generate_invisible_primary_key"),
		Default:           int64(0),
	},
	"sql_log_off": {
		Name:              "sql_log_off",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_log_off"),
		Default:           int64(0),
	},
	"sql_log_bin": {
		Name:              "sql_log_bin",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_log_bin"),
		Default:           int64(0),
	},
	"sql_notes": {
		Name:              "sql_notes",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_notes"),
		Default:           int64(1),
	},
	"sql_quote_show_create": {
		Name:              "sql_quote_show_create",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_quote_show_create"),
		Default:           int64(1),
	},
	"sql_require_primary_key": {
		Name:              "sql_require_primary_key",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_require_primary_key"),
		Default:           int64(0),
	},
	"sql_warnings": {
		Name:              "sql_warnings",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("sql_warnings"),
		Default:           int64(0),
	},
	"ssl_cipher": {
		Name:              "ssl_cipher",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("ssl_cipher"),
		Default:           "",
	},
	"ssl_session_cache_mode": {
		Name:              "ssl_session_cache_mode",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("ssl_session_cache_mode"),
		Default:           int64(1),
	},
	"ssl_session_cache_timeout": {
		Name:              "ssl_session_cache_timeout",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("ssl_session_cache_timeout", 0, 84600, false),
		Default:           int64(300),
	},
	"stored_program_cache": {
		Name:              "stored_program_cache",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("stored_program_cache", 16, 524288, false),
		Default:           int64(256),
	},
	"stored_program_definition_cache": {
		Name:              "stored_program_definition_cache",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("stored_program_definition_cache", 256, 524288, false),
		Default:           int64(256),
	},
	"super_read_only": {
		Name:              "super_read_only",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("super_read_only"),
		Default:           int64(0),
	},
	"table_definition_cache": {
		Name:              "table_definition_cache",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("table_definition_cache", 400, 524288, false),
		Default:           int64(500),
	},
	"table_encryption_privilege_check": {
		Name:              "table_encryption_privilege_check",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("table_encryption_privilege_check"),
		Default:           int64(0),
	},
	"table_open_cache": {
		Name:              "table_open_cache",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("table_definition_cache", 1, 524288, false),
		Default:           int64(4000),
	},
	"table_open_cache_instances": {
		Name:              "table_open_cache_instances",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("table_open_cache_instances", 1, 64, false),
		Default:           int64(16),
	},
	"tablespace_definition_cache": {
		Name:              "tablespace_definition_cache",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("tablespace_definition_cache", 256, 524288, false),
		Default:           int64(256),
	},
	"temptable_max_mmap": {
		Name:              "temptable_max_mmap",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("temptable_max_mmap", 256, 4294967295, false),
		Default:           int64(1073741824),
	},
	"temptable_max_ram": {
		Name:              "temptable_max_ram",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("temptable_max_ram", 2097152, 4294967295, false),
		Default:           int64(1073741824),
	},
	"temptable_use_mmap": {
		Name:              "temptable_use_mmap",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("temptable_use_mmap"),
		Default:           int64(1),
	},
	"thread_cache_size": {
		Name:              "thread_cache_size",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_cache_size", 0, 16384, false),
		Default:           int64(1),
	},
	"thread_handling": {
		Name:              "thread_handling",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("thread_handling", "no-threads", "one-thread-per-connection", "loaded-dynamically"),
		Default:           "one-thread-per-connection",
	},
	"thread_pool_algorithm": {
		Name:              "thread_pool_algorithm",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_algorithm", 0, 1, false),
		Default:           int64(0),
	},
	"thread_pool_dedicated_listeners": {
		Name:              "thread_pool_dedicated_listeners",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("thread_pool_dedicated_listeners"),
		Default:           int64(0),
	},
	"thread_pool_high_priority_connection": {
		Name:              "thread_pool_high_priority_connection",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_high_priority_connection", 0, 1, false),
		Default:           int64(0),
	},
	"thread_pool_max_active_query_threads": {
		Name:              "thread_pool_max_active_query_threads",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_max_active_query_threads", 0, 512, false),
		Default:           int64(0),
	},
	"thread_pool_max_transactions_limit": {
		Name:              "thread_pool_max_transactions_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_max_transactions_limit", 0, 1000000, false),
		Default:           int64(0),
	},
	"thread_pool_max_unused_threads": {
		Name:              "thread_pool_max_unused_threads",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_max_unused_threads", 0, 4096, false),
		Default:           int64(0),
	},
	"thread_pool_prio_kickup_timer": {
		Name:              "thread_pool_prio_kickup_timer",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_prio_kickup_timer", 0, 4294967294, false),
		Default:           int64(1000),
	},
	"thread_pool_query_threads_per_group": {
		Name:              "thread_pool_query_threads_per_group",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_query_threads_per_group", 1, 4096, false),
		Default:           int64(1),
	},
	"thread_pool_size": {
		Name:              "thread_pool_size",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_size", 1, 512, false),
		Default:           int64(1),
	},
	"thread_pool_stall_limit": {
		Name:              "thread_pool_stall_limit",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_stall_limit", 4, 600, false),
		Default:           int64(6),
	},
	"thread_pool_transaction_delay": {
		Name:              "thread_pool_transaction_delay",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("thread_pool_transaction_delay", 0, 300000, false),
		Default:           int64(0),
	},
	"transferred": {
		Name:              "transferred",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("autocommit"),
		Default:           int64(0),
	},
	"tls_ciphersuites": {
		Name:              "tls_ciphersuites",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("tls_ciphersuites"),
		Default:           "",
	},
	"tls_version": {
		Name:              "tls_version",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("tls_version"),
		Default:           "TLSv1.2,TLSv1.3",
	},
	"tmp_table_size": {
		Name:              "tmp_table_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("tmp_table_size", 1024, 4294967294, false),
		Default:           int64(16777216),
	},
	"transaction_alloc_block_size": {
		Name:              "transaction_alloc_block_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("transaction_alloc_block_size", 1024, 131072, false),
		Default:           int64(8192),
	},
	"transaction_prealloc_size": {
		Name:              "transaction_prealloc_size",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("transaction_prealloc_size", 1024, 131072, false),
		Default:           int64(4096),
	},
	"unique_checks": {
		Name:              "unique_checks",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("unique_checks"),
		Default:           int64(1),
	},
	"updatable_views_with_limit": {
		Name:              "updatable_views_with_limit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("updatable_views_with_limit"),
		Default:           int64(1),
	},
	"use_secondary_engine": {
		Name:              "use_secondary_engine",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemSystemEnumType("use_secondary_engine", "OFF", "ON", "FORCED"),
		Default:           "ON",
	},
	"version_compile_machine": {
		Name:              "version_compile_machine",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("version_compile_machine"),
		Default:           "",
	},
	"version_compile_os": {
		Name:              "version_compile_os",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("version_compile_os"),
		Default:           "",
	},
	"version_compile_zlib": {
		Name:              "version_compile_zlib",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("version_compile_zlib"),
		Default:           "",
	},
	"windowing_use_high_precision": {
		Name:              "windowing_use_high_precision",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("windowing_use_high_precision"),
		Default:           int64(1),
	},
	"xa_detach_on_prepare": {
		Name:              "xa_detach_on_prepare",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("xa_detach_on_prepare"),
		Default:           int64(1),
	},
	"version": {
		Name:              "version",
		Scope:             ScopeGlobal,
		Dynamic:           false,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("version"),
		Default:           "8.0.30-MatrixOne-v1.0.0",
	},
	"gtid_purged": {
		Name:              "gtid_purged",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableStringType("gtid_purged"),
		Default:           "",
	},
	"transaction_operator_open_log": {
		Name:              "transaction_operator_open_log",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("transaction_operator_open_log"),
		Default:           int64(0),
	},
	"disable_txn_trace": {
		Name:              "disable_txn_trace",
		Scope:             ScopeSession,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("disable_txn_trace"),
		Default:           int64(0),
	},
	"keep_user_target_list_in_result": {
		Name:              "keep_user_target_list_in_result",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableIntType("keep_user_target_list_in_result", 0, 2, false),
		Default:           int64(0),
	},
	"experimental_ivf_index": {
		Name:              "experimental_ivf_index",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("experimental_ivf_index"),
		Default:           int64(0),
	},
	"experimental_master_index": {
		Name:              "experimental_master_index",
		Scope:             ScopeGlobal,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("experimental_master_index"),
		Default:           int64(0),
	},
}

func updateTimeZone(ctx context.Context, sess *Session, vars map[string]interface{}, name string, val interface{}) error {
	tzStr := val.(string)
	tzStr = strings.TrimSpace(strings.ToLower(tzStr))
	if tzStr == "system" {
		vars[name] = "SYSTEM"
		sess.SetTimeZone(time.Local)
	} else if len(tzStr) > 0 && (tzStr[0] == '-' || tzStr[0] == '+') {
		if len(tzStr) != 5 && len(tzStr) != 6 {
			return moerr.NewWrongDatetimeSpec(ctx, tzStr)
		}

		minIdx := 3
		if tzStr[1] < '0' || tzStr[1] > '9' {
			return moerr.NewWrongDatetimeSpec(ctx, tzStr)
		}
		hour := int(tzStr[1] - '0')
		if tzStr[2] != ':' {
			if tzStr[2] < '0' || tzStr[2] > '9' {
				return moerr.NewWrongDatetimeSpec(ctx, tzStr)
			}
			hour = hour*10 + int(tzStr[2]-'0')
			minIdx = 4
			if tzStr[3] != ':' {
				return moerr.NewWrongDatetimeSpec(ctx, tzStr)
			}
		}

		if minIdx != len(tzStr)-2 {
			return moerr.NewWrongDatetimeSpec(ctx, tzStr)
		}
		if tzStr[minIdx] < '0' || tzStr[minIdx] > '9' {
			return moerr.NewWrongDatetimeSpec(ctx, tzStr)
		}
		minute := int(tzStr[minIdx]-'0') * 10
		if tzStr[minIdx+1] < '0' || tzStr[minIdx+1] > '9' {
			return moerr.NewWrongDatetimeSpec(ctx, tzStr)
		}
		minute += int(tzStr[minIdx+1] - '0')
		if minute >= 60 {
			return moerr.NewWrongDatetimeSpec(ctx, tzStr)
		}

		minute += hour * 60

		if tzStr[0] == '-' {
			if minute >= 14*60 {
				return moerr.NewWrongDatetimeSpec(ctx, tzStr)
			}
			sess.SetTimeZone(time.FixedZone("FixedZone", -minute*60))
		} else {
			if minute > 14*60 {
				return moerr.NewWrongDatetimeSpec(ctx, tzStr)
			}
			sess.SetTimeZone(time.FixedZone("FixedZone", minute*60))
		}

		vars[name] = tzStr
	} else {
		loc, err := time.LoadLocation(tzStr)
		if err != nil {
			return err
		}

		vars[name] = tzStr
		sess.SetTimeZone(loc)
	}

	return nil
}

func getSystemTimeZone() string {
	tz, _ := time.Now().Zone()
	return tz
}

func valueIsBoolTrue(value interface{}) (bool, error) {
	svbt := SystemVariableBoolType{}
	newValue, err2 := svbt.Convert(value)
	if err2 != nil {
		return false, err2
	}
	return svbt.IsTrue(newValue), nil
}

type UserDefinedVar struct {
	Value interface{}
	Sql   string
}

func autocommitValue(ctx context.Context, ses FeSession) (bool, error) {
	value, err := ses.GetSessionVar(ctx, "autocommit")
	if err != nil {
		return false, err
	}
	autocommit, err := valueIsBoolTrue(value)
	if err != nil {
		return false, err
	}
	return autocommit, err
}
