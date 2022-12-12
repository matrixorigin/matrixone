// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/defines"
)

type Column interface {
	SetName(string)
	Name() string

	//data type: MYSQL_TYPE_XXXX
	SetColumnType(defines.MysqlType)
	ColumnType() defines.MysqlType

	//the max count of spaces
	SetLength(uint32)
	Length() uint32

	//unsigned / signed for digital types
	//default: signed
	//true: signed; false: unsigned
	SetSigned(bool)
	IsSigned() bool
}

type ColumnImpl struct {
	//the name of the column
	name string

	//the data type of the column
	columnType defines.MysqlType

	//maximum length in bytes of the field
	length uint32
}

func (ci *ColumnImpl) ColumnType() defines.MysqlType {
	return ci.columnType
}

func (ci *ColumnImpl) SetColumnType(colType defines.MysqlType) {
	ci.columnType = colType
}

func (ci *ColumnImpl) Name() string {
	return ci.name
}

func (ci *ColumnImpl) SetName(name string) {
	ci.name = name
}

func (ci *ColumnImpl) SetLength(l uint32) {
	ci.length = l
}
func (ci *ColumnImpl) Length() uint32 {
	return ci.length
}

type ResultSet interface {
	//Add a column definition
	//return the index of column (start from 0)
	AddColumn(Column) uint64

	//the Count of the Column
	GetColumnCount() uint64

	//get the i th column
	GetColumn(context.Context, uint64) (Column, error)

	//Add a data row
	//return the index of row (start from 0)
	AddRow([]interface{}) uint64

	//the count of the data row
	GetRowCount() uint64

	//get the i th data row
	GetRow(context.Context, uint64) ([]interface{}, error)

	//get the data of row i, column j
	GetValue(context.Context, uint64, uint64) (interface{}, error)

	//get the data of row i, column
	GetValueByName(context.Context, uint64, string) (interface{}, error)
}

type MysqlColumn struct {
	ColumnImpl

	//schema name
	schema string

	//virtual table name
	table string

	//physical table name
	orgTable string

	//physical column name
	orgName string

	//the column character. Actually, it is the collation id
	charset uint16

	//flags
	flag uint16

	//auto_incr
	auto_incr bool

	//max shown decimal digits
	decimal uint8

	//default value
	defaultValue []byte
}

func (mc *MysqlColumn) DefaultValue() []byte {
	return mc.defaultValue
}

func (mc *MysqlColumn) SetDefaultValue(defaultValue []byte) {
	mc.defaultValue = defaultValue
}

func (mc *MysqlColumn) Decimal() uint8 {
	return mc.decimal
}

func (mc *MysqlColumn) SetDecimal(decimal uint8) {
	if mc.columnType == defines.MYSQL_TYPE_FLOAT || mc.columnType == defines.MYSQL_TYPE_DOUBLE {
		mc.decimal = 31
		return
	}
	mc.decimal = decimal
}

func (mc *MysqlColumn) Flag() uint16 {
	return mc.flag
}

func (mc *MysqlColumn) SetFlag(flag uint16) {
	mc.flag = flag
}

func (mc *MysqlColumn) Charset() uint16 {
	return mc.charset
}

func (mc *MysqlColumn) SetCharset(charset uint16) {
	mc.charset = charset
}

func (mc *MysqlColumn) OrgName() string {
	return mc.orgName
}

func (mc *MysqlColumn) SetOrgName(orgName string) {
	mc.orgName = orgName
}

func (mc *MysqlColumn) OrgTable() string {
	return mc.orgTable
}

func (mc *MysqlColumn) SetOrgTable(orgTable string) {
	mc.orgTable = orgTable
}

func (mc *MysqlColumn) Table() string {
	return mc.table
}

func (mc *MysqlColumn) SetTable(table string) {
	mc.table = table
}

func (mc *MysqlColumn) Schema() string {
	return mc.schema
}

func (mc *MysqlColumn) SetSchema(schema string) {
	mc.schema = schema
}

func (mc *MysqlColumn) SetSigned(s bool) {
	if s {
		mc.flag = mc.flag &^ uint16(defines.UNSIGNED_FLAG)
	} else {
		mc.flag = mc.flag | uint16(defines.UNSIGNED_FLAG)
	}
}

func (mc *MysqlColumn) IsSigned() bool {
	return mc.flag&uint16(defines.UNSIGNED_FLAG) == 0
}

func (mc *MysqlColumn) SetAutoIncr(s bool) {
	mc.auto_incr = s
}

func (mc *MysqlColumn) GetAutoIncr() bool {
	return mc.auto_incr
}

// Discussion: for some MatrixOne types, the Type.Precision and Type.Scale value are needed for stringification, I think we
// need to add a field
// MoTypes []types.Type
// in this struct, what's your opinion on this matter?@Daviszhen
type MysqlResultSet struct {
	//column information
	Columns []Column

	//column name --> column index
	Name2Index map[string]uint64

	//data
	Data [][]interface{}
}

func (mrs *MysqlResultSet) AddColumn(column Column) uint64 {
	mrs.Columns = append(mrs.Columns, column)
	ret := mrs.GetColumnCount() - 1

	if mrs.Name2Index == nil {
		mrs.Name2Index = make(map[string]uint64)
	}

	name := column.Name()
	mrs.Name2Index[name] = ret

	return ret
}

func (mrs *MysqlResultSet) GetColumnCount() uint64 {
	return uint64(len(mrs.Columns))
}

func (mrs *MysqlResultSet) GetColumn(ctx context.Context, index uint64) (Column, error) {
	if index < mrs.GetColumnCount() {
		return mrs.Columns[index], nil
	} else {
		return nil, moerr.NewInternalError(ctx, "index valid column index %d ", index)
	}
}

func (mrs *MysqlResultSet) AddRow(row []interface{}) uint64 {
	mrs.Data = append(mrs.Data, row)
	return mrs.GetRowCount() - 1
}

func (mrs *MysqlResultSet) GetRowCount() uint64 {
	return uint64(len(mrs.Data))
}

func (mrs *MysqlResultSet) GetRow(ctx context.Context, index uint64) ([]interface{}, error) {
	if index < mrs.GetRowCount() {
		return mrs.Data[index], nil
	} else {
		return nil, moerr.NewInternalError(ctx, "index valid row index %d ", index)
	}
}

func (mrs *MysqlResultSet) GetValue(ctx context.Context, rindex uint64, cindex uint64) (interface{}, error) {
	if row, err := mrs.GetRow(ctx, rindex); err != nil {
		return nil, err
	} else if cindex >= uint64(len(mrs.Columns)) {
		return nil, moerr.NewInternalError(ctx, "index valid column index %d ", cindex)
	} else {
		return row[cindex], nil
	}
}

// get the index of the column with name
func (mrs *MysqlResultSet) columnName2Index(ctx context.Context, name string) (uint64, error) {
	if cindex, ok := mrs.Name2Index[name]; !ok {
		return 0, moerr.NewInternalError(ctx, "column name does not exist. %s", name)
	} else {
		return cindex, nil
	}
}

func (mrs *MysqlResultSet) GetValueByName(ctx context.Context, rindex uint64, colName string) (interface{}, error) {
	if cindex, err := mrs.columnName2Index(ctx, colName); err != nil {
		return nil, err
	} else {
		return mrs.GetValue(ctx, rindex, cindex)
	}
}

// the value in position (rindex,cindex) is null or not
// return true - null ; false - not null
func (mrs *MysqlResultSet) ColumnIsNull(ctx context.Context, rindex, cindex uint64) (bool, error) {
	if value, err := mrs.GetValue(ctx, rindex, cindex); err != nil {
		return false, err
	} else {
		return value == nil, nil
	}
}

// convert the value into int64
func (mrs *MysqlResultSet) GetInt64(ctx context.Context, rindex, cindex uint64) (int64, error) {
	value, err := mrs.GetValue(ctx, rindex, cindex)
	if err != nil {
		return 0, err
	}

	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case int:
		return int64(v), nil
	case uint:
		return int64(v), nil
	default:
		return 0, moerr.NewInternalError(ctx, "unsupported type %d ", v)
	}
}

// convert the value into uint64
func (mrs *MysqlResultSet) GetUint64(ctx context.Context, rindex, cindex uint64) (uint64, error) {
	value, err := mrs.GetValue(ctx, rindex, cindex)
	if err != nil {
		return 0, err
	}

	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	case uint8:
		return uint64(v), nil
	case uint16:
		return uint64(v), nil
	case uint32:
		return uint64(v), nil
	case uint64:
		return uint64(v), nil
	case int8:
		return uint64(v), nil
	case int16:
		return uint64(v), nil
	case int32:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case float32:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case int:
		return uint64(v), nil
	case uint:
		return uint64(v), nil
	default:
		return 0, moerr.NewInternalError(ctx, "unsupported type %d ", v)
	}
}

// convert the value into Float64
func (mrs *MysqlResultSet) GetFloat64(ctx context.Context, rindex, cindex uint64) (float64, error) {
	value, err := mrs.GetValue(ctx, rindex, cindex)
	if err != nil {
		return 0, err
	}

	switch v := value.(type) {
	case bool:
		if v {
			return 1, nil
		} else {
			return 0, nil
		}
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case int:
		return float64(v), nil
	case uint:
		return float64(v), nil
	default:
		return 0, moerr.NewInternalError(ctx, "unsupported type %d ", v)
	}
}

// convert the value into string
func (mrs *MysqlResultSet) GetString(ctx context.Context, rindex, cindex uint64) (string, error) {
	value, err := mrs.GetValue(ctx, rindex, cindex)
	if err != nil {
		return "", err
	}

	switch v := value.(type) {
	case bool:
		if v {
			return "true", nil
		} else {
			return "false", nil
		}
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(uint64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(int64(v), 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 32), nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case types.Time:
		return v.String(), nil
	case types.Datetime:
		return v.String(), nil
	case bytejson.ByteJson:
		return v.String(), nil
	case types.Uuid:
		return v.ToString(), nil
	default:
		return "", moerr.NewInternalError(ctx, "unsupported type %d ", v)
	}
}

// the result of the execution
type MysqlExecutionResult struct {
	status       uint16
	insertID     uint64
	affectedRows uint64
	warnings     uint16

	mrs *MysqlResultSet
}

func (mer *MysqlExecutionResult) Mrs() *MysqlResultSet {
	return mer.mrs
}

func (mer *MysqlExecutionResult) SetMrs(mrs *MysqlResultSet) {
	mer.mrs = mrs
}

func (mer *MysqlExecutionResult) Warnings() uint16 {
	return mer.warnings
}

func (mer *MysqlExecutionResult) SetWarnings(warnings uint16) {
	mer.warnings = warnings
}

func (mer *MysqlExecutionResult) AffectedRows() uint64 {
	return mer.affectedRows
}

func (mer *MysqlExecutionResult) SetAffectedRows(affectedRows uint64) {
	mer.affectedRows = affectedRows
}

func (mer *MysqlExecutionResult) InsertID() uint64 {
	return mer.insertID
}

func (mer *MysqlExecutionResult) SetInsertID(insertID uint64) {
	mer.insertID = insertID
}

func (mer *MysqlExecutionResult) Status() uint16 {
	return mer.status
}

func (mer *MysqlExecutionResult) SetStatus(status uint16) {
	mer.status = status
}

func NewMysqlExecutionResult(status uint16, insertid, rows uint64, warnings uint16, mrs *MysqlResultSet) *MysqlExecutionResult {
	return &MysqlExecutionResult{
		status:       status,
		insertID:     insertid,
		affectedRows: rows,
		warnings:     warnings,
		mrs:          mrs,
	}
}
