// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// makeStatementColumnDefData keeps prepared metadata (including rebuilt caches)
// on the same presentation contract as ordinary execution. The local protocol
// value is only a payload encoder: it owns no connection or session resources.
func makeStatementColumnDefData(
	ctx context.Context, protocol MysqlRrWr, stmt tree.Statement, columns []*plan.ColDef, directIntegerLengths ...uint32,
) ([][]byte, error) {
	if _, ok := stmt.(*tree.ShowColumns); !ok {
		return protocol.MakeColumnDefData(ctx, columns, directIntegerLengths...)
	}
	wireColumns, err := showColumnsWireMetadata(ctx, len(columns))
	if err != nil {
		return nil, err
	}
	encoder := MysqlProtocolImpl{io: NewIOPackage(true)}
	packets := make([][]byte, len(wireColumns))
	for i, column := range wireColumns {
		packets[i] = encoder.makeColumnDefinition41Payload(column.(*MysqlColumn), int(COM_STMT_PREPARE))
	}
	return packets, nil
}

// showColumnsWireMetadata owns the presentation of SHOW [FULL] COLUMNS and
// DESC, independently of the expressions producing their rows. MySQL 8.4 uses
// different types and bounded lengths for these columns; an unbounded varchar
// for every field overflows JDBC precision. Keep MO's existing Comment column
// on ordinary SHOW, and use MO's supported utf8mb4 collation, not MySQL's 0900.
func showColumnsWireMetadata(ctx context.Context, count int) ([]interface{}, error) {
	if count != 7 && count != 9 {
		return nil, moerr.NewInternalErrorf(ctx, "unexpected SHOW COLUMNS result width %d", count)
	}
	const (
		notNull   = uint16(defines.NOT_NULL_FLAG)
		text      = uint16(defines.BLOB_FLAG | defines.BINARY_FLAG)
		noDefault = uint16(defines.NO_DEFAULT_VALUE_FLAG)
	)
	fields := [...]struct {
		name     string
		typ      defines.MysqlType
		length   uint32
		flags    uint16
		fullOnly bool
	}{
		{"Field", defines.MYSQL_TYPE_VAR_STRING, 256, 0, false},
		{"Type", defines.MYSQL_TYPE_BLOB, 67108860, notNull | text | noDefault, false},
		{"Collation", defines.MYSQL_TYPE_VAR_STRING, 256, 0, true},
		{"Null", defines.MYSQL_TYPE_VAR_STRING, 12, notNull, false},
		{"Key", defines.MYSQL_TYPE_STRING, 12, notNull | uint16(defines.BINARY_FLAG|defines.ENUM_FLAG) | noDefault, false},
		{"Default", defines.MYSQL_TYPE_BLOB, 262140, text, false},
		{"Extra", defines.MYSQL_TYPE_VAR_STRING, 1024, 0, false},
		{"Privileges", defines.MYSQL_TYPE_VAR_STRING, 616, 0, true},
		{"Comment", defines.MYSQL_TYPE_BLOB, 24576, notNull | text, false},
	}
	columns := make([]interface{}, 0, count)
	for _, field := range fields {
		if field.fullOnly && count == 7 {
			continue
		}
		col := new(MysqlColumn)
		col.SetName(field.name)
		col.SetColumnType(field.typ)
		col.SetLength(field.length)
		col.SetFlag(field.flags)
		col.SetCharset(uint16(Utf8mb4CollationID))
		col.SetDecimal(0)
		columns = append(columns, col)
	}
	return columns, nil
}
