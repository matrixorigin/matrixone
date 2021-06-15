package server

import (
	"fmt"
	"strconv"
)

//information from: https://dev.mysql.com/doc/internals/en/com-query-response.html
//also in mysql 8.0.23 source code : include/field_types.h
const (
	MYSQL_TYPE_DECIMAL     uint8 = 0x00 //lenenc_str
	MYSQL_TYPE_TINY        uint8 = 0x01 //int<1> int8
	MYSQL_TYPE_SHORT       uint8 = 0x02 //int<2> int16
	MYSQL_TYPE_LONG        uint8 = 0x03 //int<4> int32
	MYSQL_TYPE_FLOAT       uint8 = 0x04 //(string.fix_len) -- (len=4) float
	MYSQL_TYPE_DOUBLE      uint8 = 0x05 //(string.fix_len) -- (len=8) double
	MYSQL_TYPE_NULL        uint8 = 0x06 //Text ResultSet: 0xFB; Binary ResultSet: The binary protocol sends NULL values as bits inside a bitmap instead of a full byte
	MYSQL_TYPE_TIMESTAMP   uint8 = 0x07 //
	MYSQL_TYPE_LONGLONG    uint8 = 0x08 //int<8> int64
	MYSQL_TYPE_INT24       uint8 = 0x09 //int<4> int32
	MYSQL_TYPE_DATE        uint8 = 0x0a //
	MYSQL_TYPE_TIME        uint8 = 0x0b
	MYSQL_TYPE_DATETIME    uint8 = 0x0c
	MYSQL_TYPE_YEAR        uint8 = 0x0d //int<2> int16
	MYSQL_TYPE_NEWDATE     uint8 = 0x0e /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_VARCHAR     uint8 = 0x0f //lenenc_str
	MYSQL_TYPE_BIT         uint8 = 0x10 //lenenc_str
	MYSQL_TYPE_TIMESTAMP2  uint8 = 0x11 //
	MYSQL_TYPE_DATETIME2   uint8 = 0x12 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_TIME2       uint8 = 0x13 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_TYPED_ARRAY uint8 = 0x14 /**< Used for replication only */

	MYSQL_TYPE_INVALID     uint8 = 243
	MYSQL_TYPE_BOOL        uint8 = 244 /**< Currently just a placeholder */
	MYSQL_TYPE_JSON        uint8 = 245
	MYSQL_TYPE_NEWDECIMAL  uint8 = 0xf6
	MYSQL_TYPE_ENUM        uint8 = 0xf7
	MYSQL_TYPE_SET         uint8 = 0xf8
	MYSQL_TYPE_TINY_BLOB   uint8 = 0xf9
	MYSQL_TYPE_MEDIUM_BLOB uint8 = 0xfa
	MYSQL_TYPE_LONG_BLOB   uint8 = 0xfb
	MYSQL_TYPE_BLOB        uint8 = 0xfc
	MYSQL_TYPE_VAR_STRING  uint8 = 0xfd //lenenc_str
	MYSQL_TYPE_STRING      uint8 = 0xfe //lenenc_str
	MYSQL_TYPE_GEOMETRY    uint8 = 0xff
)

//flags
//in mysql 8.0.23 source code : include/mysql_com.h
const (
	NOT_NULL_FLAG     uint32 = 1   /**< Field can't be NULL */
	PRI_KEY_FLAG      uint32 = 2   /**< Field is part of a primary key */
	UNIQUE_KEY_FLAG   uint32 = 4   /**< Field is part of a unique key */
	MULTIPLE_KEY_FLAG uint32 = 8   /**< Field is part of a key */
	BLOB_FLAG         uint32 = 16  /**< Field is a blob */
	UNSIGNED_FLAG     uint32 = 32  /**< Field is unsigned */
	ZEROFILL_FLAG     uint32 = 64  /**< Field is zerofill */
	BINARY_FLAG       uint32 = 128 /**< Field is binary   */

	/* The following are only sent to new clients */
	ENUM_FLAG               uint32 = 256       /**< field is an enum */
	AUTO_INCREMENT_FLAG     uint32 = 512       /**< field is a autoincrement field */
	TIMESTAMP_FLAG          uint32 = 1024      /**< Field is a timestamp */
	SET_FLAG                uint32 = 2048      /**< field is a set */
	NO_DEFAULT_VALUE_FLAG   uint32 = 4096      /**< Field doesn't have default value */
	ON_UPDATE_NOW_FLAG      uint32 = 8192      /**< Field is set to NOW on UPDATE */
	NUM_FLAG                uint32 = 32768     /**< Field is num (for clients) */
	PART_KEY_FLAG           uint32 = 16384     /**< Intern; Part of some key */
	GROUP_FLAG              uint32 = 32768     /**< Intern: Group field */
	UNIQUE_FLAG             uint32 = 65536     /**< Intern: Used by sql_yacc */
	BINCMP_FLAG             uint32 = 131072    /**< Intern: Used by sql_yacc */
	GET_FIXED_FIELDS_FLAG   uint32 = (1 << 18) /**< Used to get fields in item tree */
	FIELD_IN_PART_FUNC_FLAG uint32 = (1 << 19) /**< Field part of partition func */
	/**
	Intern: Field in TABLE object for new version of altered table,
		  which participates in a newly added index.
	*/
	FIELD_IN_ADD_INDEX             uint32 = (1 << 20)
	FIELD_IS_RENAMED               uint32 = (1 << 21) /**< Intern: Field is being renamed */
	FIELD_FLAGS_STORAGE_MEDIA      uint32 = 22        /**< Field storage media, bit 22-23 */
	FIELD_FLAGS_STORAGE_MEDIA_MASK uint32 = (3 << FIELD_FLAGS_STORAGE_MEDIA)
	FIELD_FLAGS_COLUMN_FORMAT      uint32 = 24 /**< Field column format, bit 24-25 */
	FIELD_FLAGS_COLUMN_FORMAT_MASK uint32 = (3 << FIELD_FLAGS_COLUMN_FORMAT)
	FIELD_IS_DROPPED               uint32 = (1 << 26) /**< Intern: Field is being dropped */
	EXPLICIT_NULL_FLAG             uint32 = (1 << 27) /**< Field is explicitly specified as NULL by the user */
	FIELD_IS_MARKED                uint32 = (1 << 28) /**< Intern: field is marked, general purpose */

	/** Field will not be loaded in secondary engine. */
	NOT_SECONDARY_FLAG uint32 = (1 << 29)
	/** Field is explicitly marked as invisible by the user. */
	FIELD_IS_INVISIBLE uint32 = (1 << 30)
)

type Column interface {
	SetName(string)
	Name() string

	//data type: MYSQL_TYPE_XXXX
	SetColumnType(uint8)
	ColumnType() uint8

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
	columnType uint8

	//maximum length in bytes of the field
	length uint32
}

func (ci *ColumnImpl) ColumnType() uint8 {
	return ci.columnType
}

func (ci *ColumnImpl) SetColumnType(colType uint8) {
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
	GetColumn(uint64) (Column, error)

	//Add a data row
	//return the index of row (start from 0)
	AddRow([]interface{}) uint64

	//the count of the data row
	GetRowCount() uint64

	//get the i th data row
	GetRow(uint64) ([]interface{}, error)

	//get the data of row i, column j
	GetValue(uint64, uint64) (interface{}, error)

	//get the data of row i, column
	GetValueByName(uint64, string) (interface{}, error)
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
		mc.flag = mc.flag &^ uint16(UNSIGNED_FLAG)
	} else {
		mc.flag = mc.flag | uint16(UNSIGNED_FLAG)
	}
}

func (mc *MysqlColumn) IsSigned() bool {
	return mc.flag&uint16(UNSIGNED_FLAG) != 0
}

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
	return mrs.GetColumnCount() - 1
}

func (mrs *MysqlResultSet) GetColumnCount() uint64 {
	return uint64(len(mrs.Columns))
}

func (mrs *MysqlResultSet) GetColumn(index uint64) (Column, error) {
	if index >= 0 && index < mrs.GetColumnCount() {
		return mrs.Columns[index], nil
	} else {
		return nil, fmt.Errorf("index valid column index %d ", index)
	}
}

func (mrs *MysqlResultSet) AddRow(row []interface{}) uint64 {
	mrs.Data = append(mrs.Data, row)
	return mrs.GetRowCount() - 1
}

func (mrs *MysqlResultSet) GetRowCount() uint64 {
	return uint64(len(mrs.Data))
}

func (mrs *MysqlResultSet) GetRow(index uint64) ([]interface{}, error) {
	if index >= 0 && index < mrs.GetRowCount() {
		return mrs.Data[index], nil
	} else {
		return nil, fmt.Errorf("index valid row index %d ", index)
	}
}

func (mrs *MysqlResultSet) GetValue(rindex uint64, cindex uint64) (interface{}, error) {
	if row, err := mrs.GetRow(rindex); err != nil {
		return nil, err
	} else if cindex < 0 || cindex >= uint64(len(mrs.Columns)) {
		return nil, fmt.Errorf("index valid column index %d ", cindex)
	} else {
		return row[cindex], nil
	}
}

//get the index of the column with name
func (mrs *MysqlResultSet) columnName2Index(name string) (uint64, error) {
	if cindex, ok := mrs.Name2Index[name]; !ok {
		return 0, fmt.Errorf("column name does not exist. %s", name)
	} else {
		return cindex, nil
	}
}

func (mrs *MysqlResultSet) GetValueByName(rindex uint64, colName string) (interface{}, error) {
	if cindex, err := mrs.columnName2Index(colName); err != nil {
		return nil, err
	} else {
		return mrs.GetValue(rindex, cindex)
	}
}

//the value in position (rindex,cindex) is null or not
//return true - null ; false - not null
func (mrs *MysqlResultSet) ColumnIsNull(rindex, cindex uint64) (bool, error) {
	if value, err := mrs.GetValue(rindex, cindex); err != nil {
		return false, err
	} else {
		return value == nil, nil
	}
}

//convert the value into int64
func (mrs *MysqlResultSet) GetInt64(rindex, cindex uint64) (int64, error) {
	value, err := mrs.GetValue(rindex, cindex)
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
		return 0, fmt.Errorf("unsupported type %d ", v)
	}
}

//convert the value into uint64
func (mrs *MysqlResultSet) GetUint64(rindex, cindex uint64) (uint64, error) {
	value, err := mrs.GetValue(rindex, cindex)
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
		return 0, fmt.Errorf("unsupported type %d ", v)
	}
}

//convert the value into Float64
func (mrs *MysqlResultSet) GetFloat64(rindex, cindex uint64) (float64, error) {
	value, err := mrs.GetValue(rindex, cindex)
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
		return strconv.ParseFloat(v, 10)
	case []byte:
		return strconv.ParseFloat(string(v), 10)
	case int:
		return float64(v), nil
	case uint:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("unsupported type %d ", v)
	}
}

//convert the value into string
func (mrs *MysqlResultSet) GetString(rindex, cindex uint64) (string, error) {
	value, err := mrs.GetValue(rindex, cindex)
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
	default:
		return "", fmt.Errorf("unsupported type %d ", v)
	}
}

//the result of the execution
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
