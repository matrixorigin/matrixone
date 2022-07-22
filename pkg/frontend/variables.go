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
	"errors"
	"fmt"
	"math"
	bits2 "math/bits"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

var (
	errorConvertToBoolFailed        = errors.New("convert to the system variable bool type failed")
	errorConvertToIntFailed         = errors.New("convert to the system variable int type failed")
	errorConvertToUintFailed        = errors.New("convert to the system variable uint type failed")
	errorConvertToDoubleFailed      = errors.New("convert to the system variable double type failed")
	errorConvertToEnumFailed        = errors.New("convert to the system variable enum type failed")
	errorConvertToSetFailed         = errors.New("convert to the system variable set type failed")
	errorConvertToStringFailed      = errors.New("convert to the system variable string type failed")
	errorConvertToNullFailed        = errors.New("convert to the system variable null type failed")
	errorSystemVariableDoesNotExist = errors.New("the system variable does not exist")
	errorSystemVariableIsSession    = errors.New("the system variable is session")
	errorSystemVariableSessionEmpty = errors.New("the value of the system variable with scope session is empty")
	errorSystemVariableIsGlobal     = errors.New("the system variable is global")
	errorSystemVariableIsReadOnly   = errors.New("the system variable is read only")
)

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
	MysqlType() uint8

	// Zero gets the zero value for the type
	Zero() interface{}
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

func (svnt SystemVariableNullType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_NULL
}

func (svnt SystemVariableNullType) Zero() interface{} {
	return nil
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

func (svbt SystemVariableBoolType) Type() types.T {
	return types.T_bool
}

func (svbt SystemVariableBoolType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_BOOL
}

func (svbt SystemVariableBoolType) Zero() interface{} {
	return int8(0)
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

func (svit SystemVariableIntType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_LONGLONG
}

func (svit SystemVariableIntType) Zero() interface{} {
	return int64(0)
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

func (svut SystemVariableUintType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_LONGLONG
}

func (svut SystemVariableUintType) Zero() interface{} {
	return uint64(0)
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

func (svdt SystemVariableDoubleType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_DOUBLE
}

func (svdt SystemVariableDoubleType) Zero() interface{} {
	return float64(0)
}

var (
	errorEnumHasMoreThan65535Values = errors.New("the enum has more than 65535 values")
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

func (svet SystemVariableEnumType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_VARCHAR
}

func (svet SystemVariableEnumType) Zero() interface{} {
	return ""
}

const (
	//reference : https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html#data-types-storage-reqs-strings
	MaxMemberCountOfSetType = 64
)

var (
	errorValuesOfSetIsEmpty       = errors.New("the count of values for set is empty")
	errorValuesOfSetGreaterThan64 = errors.New("the count of value is greater than 64")
	errorValueHasComma            = errors.New("the value has the comma")
	errorValueIsDuplicate         = errors.New("the value is duplicate")
	errorValuesAreNotEnough       = errors.New("values are not enough")
	errorValueIsInvalid           = errors.New("the value is invalid")
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
			if i != 0 {
				bld.WriteByte(',')
			}
			bld.WriteString(v)
		}
	}
	return bld.String(), nil
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

func (svst SystemVariableSetType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_SET
}

func (svst SystemVariableSetType) Zero() interface{} {
	return ""
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

func (svst SystemVariableStringType) MysqlType() uint8 {
	return defines.MYSQL_TYPE_VARCHAR
}

func (svst SystemVariableStringType) Zero() interface{} {
	return ""
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
var gSysVariables = &GlobalSystemVariables{
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
func (gsv *GlobalSystemVariables) SetValues(values map[string]interface{}) error {
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
			return errorSystemVariableDoesNotExist
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
func (gsv *GlobalSystemVariables) SetGlobalSysVar(name string, value interface{}) error {
	gsv.mu.Lock()
	defer gsv.mu.Unlock()
	name = strings.ToLower(name)
	if sv, ok := gSysVarsDefs[name]; ok {
		if sv.GetScope() == ScopeSession {
			return errorSystemVariableIsSession
		}
		if !sv.GetDynamic() {
			return errorSystemVariableIsReadOnly
		}
		val, err := sv.GetType().Convert(value)
		if err != nil {
			return err
		}
		gsv.sysVars[name] = val
	} else {
		return errorSystemVariableDoesNotExist
	}
	return nil
}

func init() {
	InitGlobalSystemVariables(gSysVariables)
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
	"autocommit": {
		Name:              "autocommit",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: false,
		Type:              InitSystemVariableBoolType("autocommit"),
		Default:           "on",
	},
	"sql_mode": {
		Name:              "sql_mode",
		Scope:             ScopeBoth,
		Dynamic:           true,
		SetVarHintApplies: true,
		Type:              InitSystemVariableSetType("sql_mode", "ALLOW_INVALID_DATES", "ANSI_QUOTES", "ERROR_FOR_DIVISION_BY_ZERO", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE", "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES", "NO_DIR_IN_CREATE", "NO_ENGINE_SUBSTITUTION", "NO_UNSIGNED_SUBTRACTION", "NO_ZERO_DATE", "NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY", "PAD_CHAR_TO_FULL_LENGTH", "PIPES_AS_CONCAT", "REAL_AS_FLOAT", "STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "TIME_TRUNCATE_FRACTIONAL"),
		Default:           "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
	},
}
