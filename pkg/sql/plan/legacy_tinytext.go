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

	"github.com/gogo/protobuf/proto"
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

const maxLegacyTinyTextLineageDepth = 64

type legacyTinyTextColumn struct {
	ordinal int
	name    string
	width   int32
}

type legacyTinyTextCreateEvidence struct {
	columns  []legacyTinyTextColumn
	likeDB   string
	likeName string
}

// LegacyTinyTextTableResolver returns a raw catalog definition for a table in
// the same transaction/snapshot as the definition being normalized. A missing
// table is represented by (nil, nil).
type LegacyTinyTextTableResolver func(
	ctx context.Context,
	databaseName string,
	tableName string,
) (*planpb.TableDef, error)

// RecoverLegacyTinyTextFromCreateSQL retains the original direct-CREATE API.
// Callers that can resolve tables should use RecoverLegacyTinyText so catalog
// definitions created through a legacy CREATE TABLE ... LIKE can also recover.
func RecoverLegacyTinyTextFromCreateSQL(ctx context.Context, tableDef *planpb.TableDef) error {
	return RecoverLegacyTinyText(ctx, tableDef, nil)
}

// RecoverLegacyTinyText restores the subtype marker lost by catalog writers
// that persisted both TINYTEXT and TEXT as T_text/Width=0.
//
// Historical CREATE SQL is authoritative only while the durable catalog
// identity proves that the current columns still belong to the original
// physical table: TblId must equal its non-zero LogicalId. Copy-table ALTER
// (including MODIFY and DROP/ADD) preserves the old LogicalId under a new TblId,
// so recovery deliberately leaves those tables as unbounded TEXT. In-place
// changes such as RENAME retain the physical identity; the durable Seqnum match
// below then recovers the renamed original column without trusting its old name.
//
// For an unaltered explicit CREATE, the original declaration ordinal is matched
// only to the column's durable Seqnum. For an unaltered legacy CREATE TABLE ...
// LIKE, which contains no subtype declarations, recovery follows the source
// relation and copies recovered TEXT-family capacity markers after the complete
// visible ColDef structures match.
//
// Recovery is metadata-only: oversized values written before upgrade remain
// readable, while future assignments observe the recovered 255-byte limit.
// Stale, missing, cyclic, or ambiguous historical SQL is a safe no-op instead
// of making an otherwise valid current table unresolvable.
func RecoverLegacyTinyText(
	ctx context.Context,
	tableDef *planpb.TableDef,
	resolve LegacyTinyTextTableResolver,
) error {
	return recoverLegacyTinyText(ctx, tableDef, resolve, make(map[string]struct{}), 0)
}

func recoverLegacyTinyText(
	ctx context.Context,
	tableDef *planpb.TableDef,
	resolve LegacyTinyTextTableResolver,
	visited map[string]struct{},
	depth int,
) error {
	if tableDef == nil || !hasAuthoritativeLegacyCreate(tableDef) || tableDef.Createsql == "" ||
		!isLegacyTinyTextTableKind(tableDef.TableType) ||
		!hasLegacyUnboundedText(tableDef) || depth >= maxLegacyTinyTextLineageDepth {
		return nil
	}

	lowerCreateSQL := strings.ToLower(tableDef.Createsql)
	if !containsLegacyTextFamily(lowerCreateSQL) && !strings.Contains(lowerCreateSQL, "like") {
		return nil
	}
	evidence, err := parseLegacyTinyTextEvidence(ctx, tableDef.Createsql)
	if err != nil {
		return nil
	}
	if evidence.likeName == "" {
		recoverLegacyTinyTextColumns(tableDef, evidence.columns)
		return nil
	}
	if resolve == nil {
		return nil
	}

	databaseName := evidence.likeDB
	if databaseName == "" {
		databaseName = tableDef.DbName
	}
	key := strings.ToLower(databaseName) + "\x00" + strings.ToLower(evidence.likeName)
	if _, ok := visited[key]; ok {
		return nil
	}
	visited[key] = struct{}{}
	defer delete(visited, key)

	source, err := resolve(ctx, databaseName, evidence.likeName)
	if err != nil {
		return err
	}
	if source == nil {
		return nil
	}
	source = CloneTableDefForPlan(source, true)
	if source.DbName == "" {
		source.DbName = databaseName
	}
	if err := recoverLegacyTinyText(ctx, source, resolve, visited, depth+1); err != nil {
		return err
	}
	recoverLegacyTinyTextFromLike(tableDef, source)
	return nil
}

func hasAuthoritativeLegacyCreate(tableDef *planpb.TableDef) bool {
	return tableDef.TblId != 0 &&
		tableDef.LogicalId != 0 && tableDef.TblId == tableDef.LogicalId
}

// LegacyTinyTextCreateSQLNeedsRebuild reports whether rel_createsql can no
// longer describe the current TEXT-family subtype safely. Schema consumers
// that emit executable DDL should reconstruct it from the structured TableDef
// in this case instead of replaying historical TEXT-family or CREATE LIKE
// lineage.
func LegacyTinyTextCreateSQLNeedsRebuild(tableDef *planpb.TableDef) bool {
	if tableDef == nil || (tableDef.Version == 0 && hasAuthoritativeLegacyCreate(tableDef)) ||
		tableDef.Createsql == "" || !isLegacyTinyTextTableKind(tableDef.TableType) ||
		!hasLegacyUnboundedText(tableDef) {
		return false
	}
	lowerCreateSQL := strings.ToLower(tableDef.Createsql)
	return containsLegacyTextFamily(lowerCreateSQL) ||
		strings.Contains(lowerCreateSQL, "like")
}

func containsLegacyTextFamily(createSQL string) bool {
	return strings.Contains(createSQL, "tinytext") ||
		strings.Contains(createSQL, "mediumtext") ||
		strings.Contains(createSQL, "longtext")
}

func hasLegacyUnboundedText(tableDef *planpb.TableDef) bool {
	for _, column := range tableDef.Cols {
		if column != nil && !column.Hidden &&
			types.T(column.Typ.Id) == types.T_text && column.Typ.Width == 0 {
			return true
		}
	}
	return false
}

func recoverLegacyTinyTextColumns(tableDef *planpb.TableDef, columns []legacyTinyTextColumn) {
	for _, historical := range columns {
		var candidateIndex = -1
		sequenceNumber := uint32(historical.ordinal - 1)
		for index, column := range tableDef.Cols {
			if column == nil || column.Hidden || column.Seqnum != sequenceNumber {
				continue
			}
			if candidateIndex != -1 {
				candidateIndex = -1
				break
			}
			candidateIndex = index
		}
		if candidateIndex == -1 {
			continue
		}
		candidate := tableDef.Cols[candidateIndex]
		if types.T(candidate.Typ.Id) != types.T_text || candidate.Typ.Width != 0 {
			continue
		}
		cloned := *candidate
		cloned.Typ.Width = historical.width
		tableDef.Cols[candidateIndex] = &cloned
	}
}

func recoverLegacyTinyTextFromLike(target, source *planpb.TableDef) {
	targetColumns := visibleLegacyTinyTextColumns(target)
	sourceColumns := visibleLegacyTinyTextColumns(source)
	if len(targetColumns) != len(sourceColumns) {
		return
	}
	for index := range targetColumns {
		if !legacyLikeColumnsCompatible(targetColumns[index], sourceColumns[index]) {
			return
		}
	}
	for index, sourceColumn := range sourceColumns {
		targetColumn := targetColumns[index]
		if types.T(sourceColumn.Typ.Id) != types.T_text ||
			!isLegacyTextWidth(sourceColumn.Typ.Width) ||
			types.T(targetColumn.Typ.Id) != types.T_text || targetColumn.Typ.Width != 0 {
			continue
		}
		for tableIndex, column := range target.Cols {
			if column == targetColumn {
				cloned := *column
				cloned.Typ.Width = sourceColumn.Typ.Width
				target.Cols[tableIndex] = &cloned
				break
			}
		}
	}
}

func visibleLegacyTinyTextColumns(tableDef *planpb.TableDef) []*planpb.ColDef {
	columns := make([]*planpb.ColDef, 0, len(tableDef.Cols))
	for _, column := range tableDef.Cols {
		if column != nil && !column.Hidden {
			columns = append(columns, column)
		}
	}
	return columns
}

func legacyLikeColumnsCompatible(target, source *planpb.ColDef) bool {
	targetClone := proto.Clone(target).(*planpb.ColDef)
	sourceClone := proto.Clone(source).(*planpb.ColDef)
	for _, column := range []*planpb.ColDef{targetClone, sourceClone} {
		column.ColId = 0
		column.Seqnum = 0
		column.TblName = ""
		column.DbName = ""
		column.Typ.Table = ""
		// Legacy CREATE LIKE materialized an implicit nullable default as an
		// explicit DEFAULT NULL expression. Those encodings are semantically
		// identical and must not invalidate otherwise exact lineage evidence.
		if column.Default != nil && column.Default.NullAbility &&
			(column.Default.Expr == nil || column.Default.Expr.GetLit().GetIsnull()) {
			column.Default.Expr = nil
			column.Default.OriginString = ""
		}
	}
	if types.T(targetClone.Typ.Id) == types.T_text && targetClone.Typ.Width == 0 &&
		types.T(sourceClone.Typ.Id) == types.T_text && isLegacyTextWidth(sourceClone.Typ.Width) {
		sourceClone.Typ.Width = 0
	}
	return proto.Equal(targetClone, sourceClone)
}

func isLegacyTinyTextTableKind(tableType string) bool {
	switch tableType {
	case catalog.SystemOrdinaryRel, catalog.SystemClusterRel, catalog.SystemPartitionRel:
		return true
	default:
		return false
	}
}

func isLegacyTextWidth(width int32) bool {
	return width == types.MaxTinyTextLen ||
		width == types.MaxMediumTextLen ||
		width == types.MaxLongTextLen
}

func parseLegacyTinyTextEvidence(ctx context.Context, createSQL string) (legacyTinyTextCreateEvidence, error) {
	var recovered legacyTinyTextCreateEvidence
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
			return legacyTinyTextCreateEvidence{}, moerr.NewInvalidInput(ctx, "stored SQL is not a CREATE TABLE statement")
		}
		evidence := legacyTinyTextEvidenceFromCreate(createTable)
		stmt.Free()

		if successfulModes == 0 {
			recovered = evidence
		} else if !equalLegacyTinyTextEvidence(recovered, evidence) {
			return legacyTinyTextCreateEvidence{}, moerr.NewInvalidInput(ctx, "stored SQL is ambiguous across SQL modes")
		}
		successfulModes++
	}
	if successfulModes == 0 {
		return legacyTinyTextCreateEvidence{}, firstParseErr
	}
	return recovered, nil
}

func legacyTinyTextEvidenceFromCreate(stmt *tree.CreateTable) legacyTinyTextCreateEvidence {
	if stmt.IsAsLike {
		return legacyTinyTextCreateEvidence{
			likeDB:   string(stmt.LikeTableName.Schema()),
			likeName: string(stmt.LikeTableName.Name()),
		}
	}
	return legacyTinyTextCreateEvidence{columns: tinyTextColumnsFromLegacyCreate(stmt)}
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
		if !ok || defines.MysqlType(typ.InternalType.Oid) != defines.MYSQL_TYPE_TEXT {
			continue
		}
		var width int32
		switch strings.ToLower(typ.InternalType.FamilyString) {
		case "tinytext":
			width = types.MaxTinyTextLen
		case "mediumtext":
			width = types.MaxMediumTextLen
		case "longtext":
			width = types.MaxLongTextLen
		default:
			continue
		}
		columns = append(columns, legacyTinyTextColumn{
			ordinal: ordinal,
			name:    column.Name.ColName(),
			width:   width,
		})
	}
	return columns
}

func equalLegacyTinyTextEvidence(left, right legacyTinyTextCreateEvidence) bool {
	if !strings.EqualFold(left.likeDB, right.likeDB) ||
		!strings.EqualFold(left.likeName, right.likeName) ||
		len(left.columns) != len(right.columns) {
		return false
	}
	for index := range left.columns {
		if left.columns[index].ordinal != right.columns[index].ordinal ||
			!strings.EqualFold(left.columns[index].name, right.columns[index].name) ||
			left.columns[index].width != right.columns[index].width {
			return false
		}
	}
	return true
}
