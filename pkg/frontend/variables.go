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
	errorConvertToBoolFailed             = moerr.NewInternalError(context.Background(), "convert to the system variable bool type failed")
	errorConvertToIntFailed              = moerr.NewInternalError(context.Background(), "convert to the system variable int type failed")
	errorConvertToUintFailed             = moerr.NewInternalError(context.Background(), "convert to the system variable uint type failed")
	errorConvertToDoubleFailed           = moerr.NewInternalError(context.Background(), "convert to the system variable double type failed")
	errorConvertToEnumFailed             = moerr.NewInternalError(context.Background(), "convert to the system variable enum type failed")
	errorConvertToSetFailed              = moerr.NewInternalError(context.Background(), "convert to the system variable set type failed")
	errorConvertToStringFailed           = moerr.NewInternalError(context.Background(), "convert to the system variable string type failed")
	errorConvertToNullFailed             = moerr.NewInternalError(context.Background(), "convert to the system variable null type failed")
	errorConvertFromStringToBoolFailed   = moerr.NewInternalError(context.Background(), "convert from string to the system variable bool type failed")
	errorConvertFromStringToIntFailed    = moerr.NewInternalError(context.Background(), "convert from string to the system variable int type failed")
	errorConvertFromStringToUintFailed   = moerr.NewInternalError(context.Background(), "convert from string to the system variable uint type failed")
	errorConvertFromStringToDoubleFailed = moerr.NewInternalError(context.Background(), "convert from string to the system variable double type failed")
	errorConvertFromStringToEnumFailed   = moerr.NewInternalError(context.Background(), "convert from string to the system variable enum type failed")
	errorConvertFromStringToSetFailed    = moerr.NewInternalError(context.Background(), "convert from string to the system variable set type failed")
	errorConvertFromStringToNullFailed   = moerr.NewInternalError(context.Background(), "convert from string to the system variable null type failed")
)

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
		return nil, errorConvertFromStringToNullFailed
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
		case "on", "true":
			return int8(1), nil
		case "off", "false":
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
	if vv, ok := v.(int8); ok {
		return vv == int8(1)
	} else if vv3, ok3 := v.(int64); ok3 {
		return vv3 == int64(1)
	} else if vv2, ok2 := v.(string); ok2 {
		return strings.ToLower(vv2) == "on"
	}
	return false
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
		return nil, errorConvertFromStringToBoolFailed
	}
	if convertVal != 1 && convertVal != 0 {
		return nil, errorConvertFromStringToBoolFailed
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
		return nil, errorConvertFromStringToIntFailed
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
		return nil, errorConvertFromStringToUintFailed
	}
	return convertVal, nil
}

type SystemVariableDoubleType struct {
	// Unused
	// name    string
	minimum float64
	maximum float64
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
		return nil, errorConvertFromStringToDoubleFailed
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
		return nil, errorConvertFromStringToEnumFailed
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
		return nil, errorConvertFromStringToSetFailed
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

	UpdateSessVar func(*Session, map[string]interface{}, string, interface{}) error
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
		Default:           int64(16777216),
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
		Scope:             ScopeGlobal,
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
	//whether DN does primary key uniqueness check against transaction's workspace or not.
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
}

func updateTimeZone(sess *Session, vars map[string]interface{}, name string, val interface{}) error {
	oldVal := vars[name]
	if oldVal == val {
		return nil
	}

	tzStr := val.(string)
	if tzStr == "SYSTEM" {
		vars[name] = "SYSTEM"
		sess.SetTimeZone(time.Local)
	} else if tzStr[0] == '-' || tzStr[0] == '+' {
		if len(tzStr) != 5 && len(tzStr) != 6 {
			return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
		}

		minIdx := 3
		if tzStr[1] < '0' || tzStr[1] > '9' {
			return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
		}
		hour := int(tzStr[1] - '0')
		if tzStr[2] != ':' {
			if tzStr[2] < '0' || tzStr[2] > '9' {
				return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
			}
			hour = hour*10 + int(tzStr[2]-'0')
			minIdx = 4
			if tzStr[3] != ':' {
				return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
			}
		}

		if minIdx != len(tzStr)-2 {
			return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
		}
		if tzStr[minIdx] < '0' || tzStr[minIdx] > '9' {
			return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
		}
		minute := int(tzStr[minIdx]-'0') * 10
		if tzStr[minIdx+1] < '0' || tzStr[minIdx+1] > '9' {
			return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
		}
		minute += int(tzStr[minIdx+1] - '0')
		if minute >= 60 {
			return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
		}

		minute += hour * 60

		if tzStr[0] == '-' {
			if minute >= 14*60 {
				return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
			}
			sess.SetTimeZone(time.FixedZone("FixedZone", -minute*60))
		} else {
			if minute > 14*60 {
				return moerr.NewWrongDatetimeSpec(sess.requestCtx, tzStr)
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
