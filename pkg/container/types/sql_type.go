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

package types

import "fmt"

// FormatSQLColumnType formats the scalar column type for SQL schema metadata and DDL.
// Enum/set values, arrays and geometry subtypes require column metadata and
// are handled by their callers. String remains the internal type name.
func FormatSQLColumnType(t Type) (string, bool) {
	name := t.String()
	switch t.Oid {
	case T_bool:
		return "BOOLEAN", true
	case T_text:
		switch t.Width {
		case MaxTinyTextLen:
			return "TINYTEXT", true
		case MaxMediumTextLen:
			return "MEDIUMTEXT", true
		case MaxLongTextLen:
			return "LONGTEXT", true
		default:
			return "TEXT", true
		}
	case T_blob:
		switch {
		case t.Width == 0:
			return "LONGBLOB", true
		case t.Width > 0 && t.Width <= MaxTinyTextLen:
			return "TINYBLOB", true
		case t.Width > MaxStringSize && t.Width <= MaxMediumTextLen:
			return "MEDIUMBLOB", true
		case t.Width > MaxMediumTextLen:
			return "LONGBLOB", true
		default:
			return "BLOB", true
		}
	case T_decimal64, T_decimal128, T_decimal256:
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Width, t.Scale), true
	case T_timestamp, T_datetime, T_time:
		if t.Width > 0 {
			return fmt.Sprintf("%s(%d)", name, t.Width), true
		}
	case T_float32, T_float64:
		if t.Width > 0 && t.Scale != -1 {
			return fmt.Sprintf("%s(%d,%d)", name, t.Width, t.Scale), true
		}
	case T_bit, T_char, T_varchar, T_binary, T_varbinary:
		return fmt.Sprintf("%s(%d)", name, t.Width), true
	case T_int8, T_int16, T_int32, T_int64, T_uint8, T_uint16, T_uint32, T_uint64,
		T_date, T_year, T_json, T_uuid, T_datalink:
		return name, true
	default:
		return "", false
	}
	return name, true
}
