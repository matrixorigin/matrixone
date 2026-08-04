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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type legacyTinyTextColumn struct {
	ordinal int
	name    string
}

// RecoverLegacyTinyTextFromCreateSQL recovers the subtype that old catalog
// writers lost. Before TINYTEXT had a distinct width marker, both TINYTEXT and
// TEXT were persisted as T_text/Width=0; the original CREATE statement is the
// only durable discriminator left in those catalogs.
//
// Recovery is intentionally metadata-only. Oversized values written by an old
// binary remain readable, while every future assignment uses the recovered
// 255-byte contract. The function never guesses: every parseable SQL mode must
// identify the same columns, and each recovered ordinal, name, and base type
// must agree with the structured TableDef.
func RecoverLegacyTinyTextFromCreateSQL(ctx context.Context, tableDef *planpb.TableDef) error {
	if tableDef == nil || tableDef.Createsql == "" ||
		!isLegacyTinyTextTableKind(tableDef.TableType) {
		return nil
	}
	hasLegacyText := false
	for _, column := range tableDef.Cols {
		if column != nil && !column.Hidden &&
			types.T(column.Typ.Id) == types.T_text && column.Typ.Width == 0 {
			hasLegacyText = true
			break
		}
	}
	if !hasLegacyText || !strings.Contains(strings.ToLower(tableDef.Createsql), "tinytext") {
		return nil
	}

	recovered, err := parseLegacyTinyTextColumns(ctx, tableDef.Createsql)
	if err != nil {
		return moerr.NewInvalidInputf(
			ctx,
			"cannot recover legacy TINYTEXT metadata for %s.%s: %v",
			tableDef.DbName,
			tableDef.Name,
			err,
		)
	}
	if len(recovered) == 0 {
		return nil
	}

	indexes := make([]int, 0, len(recovered))
	for _, column := range recovered {
		index := column.ordinal - 1
		if index < 0 || index >= len(tableDef.Cols) {
			return legacyTinyTextMismatch(ctx, tableDef, column, "catalog ordinal is missing")
		}
		catalogColumn := tableDef.Cols[index]
		if catalogColumn == nil || catalogColumn.Hidden ||
			!strings.EqualFold(catalogColumn.GetOriginCaseName(), column.name) {
			return legacyTinyTextMismatch(ctx, tableDef, column, "catalog column name does not match")
		}
		if types.T(catalogColumn.Typ.Id) != types.T_text {
			return legacyTinyTextMismatch(ctx, tableDef, column, "catalog base type is not TEXT")
		}
		switch catalogColumn.Typ.Width {
		case 0:
			indexes = append(indexes, index)
		case types.MaxTinyTextLen:
			// Already written by a fixed binary.
		default:
			return legacyTinyTextMismatch(ctx, tableDef, column, "catalog TEXT width is incompatible")
		}
	}

	// Clone only the columns that change. Resolve returns a planner-owned Cols
	// slice, but the ColDef pointers may still be shared with the catalog cache.
	for _, index := range indexes {
		column := *tableDef.Cols[index]
		column.Typ.Width = types.MaxTinyTextLen
		tableDef.Cols[index] = &column
	}
	return nil
}

func isLegacyTinyTextTableKind(tableType string) bool {
	switch tableType {
	case catalog.SystemOrdinaryRel, catalog.SystemClusterRel, catalog.SystemPartitionRel:
		return true
	default:
		return false
	}
}

func legacyTinyTextMismatch(
	ctx context.Context,
	tableDef *planpb.TableDef,
	column legacyTinyTextColumn,
	reason string,
) error {
	return moerr.NewInvalidInputf(
		ctx,
		"cannot recover legacy TINYTEXT metadata for %s.%s column %s at ordinal %d: %s",
		tableDef.DbName,
		tableDef.Name,
		column.name,
		column.ordinal,
		reason,
	)
}

func parseLegacyTinyTextColumns(ctx context.Context, createSQL string) ([]legacyTinyTextColumn, error) {
	var recovered []legacyTinyTextColumn
	var firstParseErr error
	successfulModes := 0
	for _, sqlMode := range mysql.ParserSQLModeCombinations() {
		stmt, err := parsers.ParseOneWithSQLMode(ctx, dialect.MYSQL, createSQL, 0, sqlMode)
		if err != nil {
			if firstParseErr == nil {
				firstParseErr = err
			}
			continue
		}

		createTable, ok := stmt.(*tree.CreateTable)
		if !ok {
			stmt.Free()
			return nil, moerr.NewInvalidInput(ctx, "stored SQL is not a CREATE TABLE statement")
		}
		columns := tinyTextColumnsFromLegacyCreate(createTable)
		stmt.Free()

		if successfulModes == 0 {
			recovered = columns
		} else if !equalLegacyTinyTextColumns(recovered, columns) {
			return nil, moerr.NewInvalidInput(ctx, "stored SQL is ambiguous across SQL modes")
		}
		successfulModes++
	}
	if successfulModes == 0 {
		return nil, firstParseErr
	}
	return recovered, nil
}

func tinyTextColumnsFromLegacyCreate(stmt *tree.CreateTable) []legacyTinyTextColumn {
	columns := make([]legacyTinyTextColumn, 0)
	ordinal := 0
	for _, def := range stmt.Defs {
		column, ok := def.(*tree.ColumnTableDef)
		if !ok {
			continue
		}
		ordinal++
		typ, ok := column.Type.(*tree.T)
		if !ok || defines.MysqlType(typ.InternalType.Oid) != defines.MYSQL_TYPE_TEXT ||
			!strings.EqualFold(typ.InternalType.FamilyString, "tinytext") {
			continue
		}
		columns = append(columns, legacyTinyTextColumn{
			ordinal: ordinal,
			name:    column.Name.ColName(),
		})
	}
	return columns
}

func equalLegacyTinyTextColumns(left, right []legacyTinyTextColumn) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ordinal != right[i].ordinal || left[i].name != right[i].name {
			return false
		}
	}
	return true
}
