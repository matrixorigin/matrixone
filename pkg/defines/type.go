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

package defines

import (
	"math"
	"sync"
)

// information from: https://dev.mysql.com/doc/internals/en/com-query-response.html
// also in mysql 8.0.23 source code : include/field_types.h

type MysqlType uint8

const (
	MYSQL_TYPE_DECIMAL     MysqlType = 0x00 //lenenc_str
	MYSQL_TYPE_TINY        MysqlType = 0x01 //int<1> int8
	MYSQL_TYPE_SHORT       MysqlType = 0x02 //int<2> int16
	MYSQL_TYPE_LONG        MysqlType = 0x03 //int<4> int32
	MYSQL_TYPE_FLOAT       MysqlType = 0x04 //(string.fix_len) -- (len=4) float
	MYSQL_TYPE_DOUBLE      MysqlType = 0x05 //(string.fix_len) -- (len=8) double
	MYSQL_TYPE_NULL        MysqlType = 0x06 //Text ResultSet: 0xFB; Binary ResultSet: The binary protocol sends NULL values as bits inside a bitmap instead of a full byte
	MYSQL_TYPE_TIMESTAMP   MysqlType = 0x07 //
	MYSQL_TYPE_LONGLONG    MysqlType = 0x08 //int<8> int64
	MYSQL_TYPE_INT24       MysqlType = 0x09 //int<4> int32
	MYSQL_TYPE_DATE        MysqlType = 0x0a //
	MYSQL_TYPE_TIME        MysqlType = 0x0b
	MYSQL_TYPE_DATETIME    MysqlType = 0x0c
	MYSQL_TYPE_YEAR        MysqlType = 0x0d //int<2> int16
	MYSQL_TYPE_NEWDATE     MysqlType = 0x0e /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_VARCHAR     MysqlType = 0x0f //lenenc_str
	MYSQL_TYPE_BIT         MysqlType = 0x10 //lenenc_str
	MYSQL_TYPE_TIMESTAMP2  MysqlType = 0x11 //
	MYSQL_TYPE_DATETIME2   MysqlType = 0x12 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_TIME2       MysqlType = 0x13 /**< Internal to MySQL. Not used in protocol */
	MYSQL_TYPE_TYPED_ARRAY MysqlType = 0x14 /**< Used for replication only */

	MYSQL_TYPE_TEXT        MysqlType = 241 // add text to distinct blob and blob
	MYSQL_TYPE_INVALID     MysqlType = 242
	MYSQL_TYPE_UUID        MysqlType = 243
	MYSQL_TYPE_BOOL        MysqlType = 244 /**< Currently just a placeholder */
	MYSQL_TYPE_JSON        MysqlType = 0xf5
	MYSQL_TYPE_NEWDECIMAL  MysqlType = 0xf6
	MYSQL_TYPE_ENUM        MysqlType = 0xf7
	MYSQL_TYPE_SET         MysqlType = 0xf8
	MYSQL_TYPE_TINY_BLOB   MysqlType = 0xf9
	MYSQL_TYPE_MEDIUM_BLOB MysqlType = 0xfa
	MYSQL_TYPE_LONG_BLOB   MysqlType = 0xfb
	MYSQL_TYPE_BLOB        MysqlType = 0xfc
	MYSQL_TYPE_VAR_STRING  MysqlType = 0xfd //lenenc_str
	MYSQL_TYPE_STRING      MysqlType = 0xfe //lenenc_str
	MYSQL_TYPE_GEOMETRY    MysqlType = 0xff
)

func (typ *MysqlType) GetLength(width int32) uint32 {
	switch *typ {
	case MYSQL_TYPE_DECIMAL:
		return uint32(width) + 2
	case MYSQL_TYPE_BOOL:
		return 1
	case MYSQL_TYPE_TINY:
		return 8
	case MYSQL_TYPE_SHORT:
		return 16
	case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
		return 32
	case MYSQL_TYPE_LONGLONG:
		return 64
	case MYSQL_TYPE_FLOAT:
		if width != 32 && width != 0 {
			return uint32(width)
		}
		return 32
	case MYSQL_TYPE_DOUBLE:
		if width != 64 && width != 0 {
			return uint32(width)
		}
		return 64
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_STRING, MYSQL_TYPE_BLOB, MYSQL_TYPE_TEXT:
		return uint32(width) * 3
	case MYSQL_TYPE_DATE:
		return 64
	case MYSQL_TYPE_TIME:
		return 64
	case MYSQL_TYPE_DATETIME:
		return 64
	case MYSQL_TYPE_TIMESTAMP:
		return 64
	case MYSQL_TYPE_JSON:
		return math.MaxUint32
	default:
		return math.MaxUint32
	}
}

// flags
// in mysql 8.0.23 source code : include/mysql_com.h
const (
	NOT_NULL_FLAG     uint32 = 1 << 0 /**< Field can't be NULL */
	PRI_KEY_FLAG      uint32 = 1 << 1 /**< Field is part of a primary key */
	UNIQUE_KEY_FLAG   uint32 = 1 << 2 /**< Field is part of a unique key */
	MULTIPLE_KEY_FLAG uint32 = 1 << 3 /**< Field is part of a key */
	BLOB_FLAG         uint32 = 1 << 4 /**< Field is a blob */
	UNSIGNED_FLAG     uint32 = 1 << 5 /**< Field is unsigned */
	ZEROFILL_FLAG     uint32 = 1 << 6 /**< Field is zerofill */
	BINARY_FLAG       uint32 = 1 << 7 /**< Field is binary   */

	/* The following are only sent to new clients */
	ENUM_FLAG               uint32 = 1 << 8  /**< field is an enum */
	AUTO_INCREMENT_FLAG     uint32 = 1 << 9  /**< field is a autoincrement field */
	TIMESTAMP_FLAG          uint32 = 1 << 10 /**< Field is a timestamp */
	SET_FLAG                uint32 = 1 << 11 /**< field is a set */
	NO_DEFAULT_VALUE_FLAG   uint32 = 1 << 12 /**< Field doesn't have default value */
	ON_UPDATE_NOW_FLAG      uint32 = 1 << 13 /**< Field is set to NOW on UPDATE */
	NUM_FLAG                uint32 = 1 << 15 /**< Field is num (for clients) */
	PART_KEY_FLAG           uint32 = 1 << 14 /**< Intern; Part of some key */
	GROUP_FLAG              uint32 = 1 << 15 /**< Intern: Group field */
	UNIQUE_FLAG             uint32 = 1 << 16 /**< Intern: Used by sql_yacc */
	BINCMP_FLAG             uint32 = 1 << 17 /**< Intern: Used by sql_yacc */
	GET_FIXED_FIELDS_FLAG   uint32 = 1 << 18 /**< Used to get fields in item tree */
	FIELD_IN_PART_FUNC_FLAG uint32 = 1 << 19 /**< Field part of partition func */
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

// TypeInt24 bounds.
const (
	MaxUint24 = 1<<24 - 1
	MaxInt24  = 1<<23 - 1
	MinInt24  = -1 << 23
)

// use TenantIDKey{} UserIDKey{} RoleIDKey{} to get uint32 value from Context

type TenantIDKey struct{}
type UserIDKey struct{}
type RoleIDKey struct{}

// EngineKey use EngineKey{} to get engine from Context
type EngineKey struct{}

// SqlKey use SqlKey{} to get string value from Context
type SqlKey struct{}
type DatTypKey struct{}

// CarryOnCtxKeys defines keys needed to be serialized when pass context through net
var CarryOnCtxKeys = []any{TenantIDKey{}, UserIDKey{}, RoleIDKey{}}

// TemporaryDN use TemporaryDN to get temporary storage from Context
type TemporaryDN struct{}

type AutoIncrCaches struct {
	Mu             *sync.Mutex
	AutoIncrCaches map[string]AutoIncrCache
}

type AutoIncrCache struct {
	CurNum uint64
	MaxNum uint64
	Step   uint64
}
