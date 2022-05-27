package frontend

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"strings"
)

var (
	errorConvertToBoolFailed   = errors.New("convert to the system variable bool type failed")
	errorConvertToIntFailed    = errors.New("convert to the system variable int type failed")
	errorConvertToUintFailed   = errors.New("convert to the system variable uint type failed")
	errorConvertToDoubleFailed = errors.New("convert to the system variable double type failed")
	errorConvertToEnumFailed   = errors.New("convert to the system variable enum type failed")
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
	name    string
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

type SystemVariableEnumType struct {
	name string
	//tag name -> id
	tagName2Id map[string]int

	// id -> tag name
	id2TagName []string
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
)

type SystemVariableSetType struct {
	name                string
	normalized2original map[string]string
	value2BitIndex      map[string]int8
	bitIndex2Value      map[int8]string
}

func InitSystemVariableSetType(name string, values ...string) SystemVariableSetType {
	if len(values) == 0 {
		panic(errorValuesOfSetIsEmpty)
	}
	if len(values) > MaxMemberCountOfSetType {
		panic(errorValuesOfSetGreaterThan64)
	}
	//TODO: add initial
	return SystemVariableSetType{}
}
