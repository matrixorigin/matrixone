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

// information from: https://dev.mysql.com/doc/internals/en/com-query-response.html
// also in mysql 8.0.23 source code : include/field_types.h
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

	MYSQL_TYPE_TEXT        uint8 = 241 // add text to distinct blob and blob
	MYSQL_TYPE_INVALID     uint8 = 242
	MYSQL_TYPE_UUID        uint8 = 243
	MYSQL_TYPE_BOOL        uint8 = 244 /**< Currently just a placeholder */
	MYSQL_TYPE_JSON        uint8 = 0xf5
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
	ENUM_FLAG               uint32 = 256     /**< field is an enum */
	AUTO_INCREMENT_FLAG     uint32 = 512     /**< field is a autoincrement field */
	TIMESTAMP_FLAG          uint32 = 1024    /**< Field is a timestamp */
	SET_FLAG                uint32 = 2048    /**< field is a set */
	NO_DEFAULT_VALUE_FLAG   uint32 = 4096    /**< Field doesn't have default value */
	ON_UPDATE_NOW_FLAG      uint32 = 8192    /**< Field is set to NOW on UPDATE */
	NUM_FLAG                uint32 = 32768   /**< Field is num (for clients) */
	PART_KEY_FLAG           uint32 = 16384   /**< Intern; Part of some key */
	GROUP_FLAG              uint32 = 32768   /**< Intern: Group field */
	UNIQUE_FLAG             uint32 = 65536   /**< Intern: Used by sql_yacc */
	BINCMP_FLAG             uint32 = 131072  /**< Intern: Used by sql_yacc */
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

// use SqlKey{} to get string value from Context
type SqlKey struct{}

// CarryOnCtxKeys defines keys needed to be serialized when pass context through net
var CarryOnCtxKeys = []any{TenantIDKey{}, UserIDKey{}, RoleIDKey{}}
