// Copyright 2026 Matrix Origin
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
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func enumSetCursorMaterializes(kind plan.MySQLSpecialResultType_Kind) bool {
	return kind == plan.MySQLSpecialResultType_PREPARE_ONLY || kind == plan.MySQLSpecialResultType_SCALAR_SUBQUERY ||
		kind == plan.MySQLSpecialResultType_ROLLUP
}

// A cursor materializes its schema even when a static empty plan lets ordinary
// execution publish unmaterialized CHAR metadata. Do not change the ColDefs or
// the statement's prepare packets: those remain valid for subsequent executes.
func applyCursorEnumSetResultMetadata(columns []any, defs []*plan.ColDef) {
	for i, def := range defs {
		result := def.MysqlSpecialResultType
		if result != nil && enumSetCursorMaterializes(result.Kind) {
			columns[i].(*MysqlColumn).SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
		}
	}
}

// MySQL publishes ENUM/SET labels as STRING, with field flags only for column
// results. Both text and binary row encoders keep consuming the same string
// vectors; only their column metadata changes.
func setEnumSetResultMetadata(ctx context.Context, result *plan.MySQLSpecialResultType, displayCharset uint8, col *MysqlColumn, preparing bool) error {
	source := &result.Typ
	var width int32
	var flag uint32
	switch {
	case source.Id == int32(types.T_enum):
		flag = defines.ENUM_FLAG
		for label := range strings.SplitSeq(source.Enumvalues, ",") {
			width = max(width, int32(utf8.RuneCountInString(label)))
		}
	case source.Id == int32(types.T_uint64) && source.Enumvalues != "":
		flag = defines.SET_FLAG
		labels := types.DecodeSetValues(source.Enumvalues)
		width = int32(len(labels) - 1) // separators between selected members
		for _, label := range labels {
			width += int32(utf8.RuneCountInString(label))
		}
	default:
		return moerr.NewInternalError(ctx, "invalid ENUM/SET result presentation")
	}
	// The display expression, not the ordinal/bitmap storage domain, owns the
	// label encoding. Retain its charset and express the label width in bytes.
	if result.Kind == plan.MySQLSpecialResultType_VARCHAR_LABEL ||
		(result.Kind == plan.MySQLSpecialResultType_PREPARE_ONLY && !preparing && !result.MaterializationElided) {
		col.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	} else {
		col.SetColumnType(defines.MYSQL_TYPE_STRING)
	}
	col.SetLength(mysqlStringColumnLength(width, mysqlTextMaxBytesPerCharacter(displayCharset)))
	col.SetDecimal(0)
	switch result.Kind {
	case plan.MySQLSpecialResultType_COLUMN:
		col.SetFlag(col.Flag() | uint16(flag))
	case plan.MySQLSpecialResultType_AGGREGATE, plan.MySQLSpecialResultType_SCALAR_SUBQUERY,
		plan.MySQLSpecialResultType_ROLLUP, plan.MySQLSpecialResultType_ROLLUP_AGGREGATE, plan.MySQLSpecialResultType_CHAR_LABEL,
		plan.MySQLSpecialResultType_PREPARE_ONLY, plan.MySQLSpecialResultType_VARCHAR_LABEL:
		// Expression results retain label widths, but not field flags.
	default:
		return moerr.NewInternalError(ctx, "invalid ENUM/SET result presentation kind")
	}
	return nil
}
