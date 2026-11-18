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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// indexRawProjection belongs only to a planner-generated DML old image.
// The result position is assigned after user ORDER BY has been bound.
type indexRawProjection struct {
	alias     string
	column    string
	tableID   uint64
	schema    string
	tableName string
	columnPos int32
	seqnum    uint32
	typ       planpb.Type
	resultPos int32
}

func tableHasNewEnumIndex(table *planpb.TableDef) (bool, error) {
	if table == nil {
		return false, moerr.NewInvalidInputNoCtx("cannot determine ENUM index old-image owner")
	}
	usesNew := false
	for _, index := range table.Indexes {
		uses, err := EnumIndexUsesNewIdentity(table, index)
		if err != nil {
			return false, err
		}
		usesNew = usesNew || uses
	}
	return usesNew, nil
}

func prepareEnumIndexRawProjections(ctx *BindContext, dml *DMLContext, onlyUpdated bool) (map[string]*indexRawProjection, error) {
	invalid := func() error { return moerr.NewInvalidInputNoCtx("cannot prove ENUM index old-image source") }
	result := make(map[string]*indexRawProjection)
	if ctx == nil || dml == nil || len(dml.aliases) != len(dml.tableDefs) || len(dml.objRefs) != len(dml.tableDefs) || (onlyUpdated && len(dml.updateCol2Expr) != len(dml.tableDefs)) {
		return nil, invalid()
	}
	for i, table := range dml.tableDefs {
		if table == nil {
			return nil, invalid()
		}
		if onlyUpdated && len(dml.updateCol2Expr[i]) == 0 {
			continue
		}
		usesNew, err := tableHasNewEnumIndex(table)
		if err != nil {
			return nil, err
		}
		if !usesNew {
			continue
		}
		if table.TblId == 0 || dml.objRefs[i] == nil || uint64(dml.objRefs[i].Obj) != table.TblId {
			return nil, invalid()
		}
		for colPos, col := range table.Cols {
			if col == nil {
				return nil, invalid()
			}
			if !isEnumOrSetPlanType(&col.Typ) {
				continue
			}
			key := strings.ToLower(dml.aliases[i] + "." + col.Name)
			if result[key] != nil {
				return nil, invalid()
			}
			request := &indexRawProjection{
				alias: dml.aliases[i], column: col.Name, tableID: table.TblId,
				schema: table.DbName, tableName: table.Name, columnPos: int32(colPos),
				seqnum: col.Seqnum, typ: col.Typ, resultPos: -1,
			}
			result[key] = request
			ctx.indexRawProjections = append(ctx.indexRawProjections, request)
		}
	}
	return result, nil
}

// bindEnumIndexRawProjections runs only after user projection and ORDER BY
// binding. It uses the already resolved target relation, never a user AST.
func bindEnumIndexRawProjections(builder *QueryBuilder, ctx *BindContext, resultLen int) ([]int32, error) {
	invalid := func() error { return moerr.NewInvalidInputNoCtx("cannot prove ENUM index old-image binding") }
	positions := make([]int32, 0, len(ctx.indexRawProjections))
	for i, request := range ctx.indexRawProjections {
		binding := ctx.bindingByTable[request.alias]
		if binding == nil || binding.tableID != request.tableID || !strings.EqualFold(binding.table, request.alias) || !strings.EqualFold(binding.db, request.schema) {
			return nil, invalid()
		}
		if builder == nil || binding.nodeId < 0 || int(binding.nodeId) >= len(builder.qry.Nodes) {
			return nil, invalid()
		}
		source := builder.qry.Nodes[binding.nodeId]
		if source == nil || source.NodeType != planpb.Node_TABLE_SCAN || source.TableDef == nil || source.ObjRef == nil ||
			source.TableDef.TblId != request.tableID || uint64(source.ObjRef.Obj) != request.tableID ||
			!strings.EqualFold(source.TableDef.Name, request.tableName) || !strings.EqualFold(source.TableDef.DbName, request.schema) {
			return nil, invalid()
		}
		pos := binding.FindColumn(strings.ToLower(request.column))
		if pos != request.columnPos || pos < 0 || int(pos) >= len(binding.types) || int(pos) >= len(source.TableDef.Cols) ||
			binding.types[pos] == nil || source.TableDef.Cols[pos] == nil || source.TableDef.Cols[pos].Seqnum != request.seqnum ||
			!strings.EqualFold(source.TableDef.Cols[pos].Name, request.column) ||
			!sameEnumIndexDomain(binding.types[pos], &request.typ) || !sameEnumIndexDomain(&source.TableDef.Cols[pos].Typ, &request.typ) {
			return nil, invalid()
		}
		request.resultPos = int32(resultLen + i)
		positions = append(positions, int32(len(ctx.projects)))
		ctx.projects = append(ctx.projects, GetColExpr(*binding.types[pos], binding.tag, pos))
	}
	return positions, nil
}

// enumIndexOldValuePos preserves the branch's existing lookup for a legacy
// index; new-identity indexes use a separately proven physical old image.
func enumIndexOldValuePos(table *planpb.TableDef, useRaw bool, raw map[string]*indexRawProjection, alias, name string, legacy int32) (int32, error) {
	if !useRaw {
		return legacy, nil
	}
	for _, col := range table.Cols {
		if col != nil && strings.EqualFold(col.Name, name) {
			if !isEnumOrSetPlanType(&col.Typ) {
				return legacy, nil
			}
			req := raw[strings.ToLower(alias+"."+col.Name)]
			if req == nil || req.resultPos < 0 || req.tableID != table.TblId || !sameEnumIndexDomain(&req.typ, &col.Typ) {
				return 0, moerr.NewInvalidInputNoCtx("cannot prove ENUM index old-image position")
			}
			return req.resultPos, nil
		}
	}
	return 0, moerr.NewInvalidInputNoCtx("cannot resolve ENUM index old-image column")
}

// The legacy recursive DML planner has a positional display-value source and
// cannot safely maintain a new-identity index. Reject rather than re-entering
// that planner with the ErrUnsupportedDML fallback sentinel.
func rejectLegacyNewEnumIndexMutation(table *planpb.TableDef) error {
	usesNew, err := tableHasNewEnumIndex(table)
	if err != nil {
		return err
	}
	if usesNew {
		return moerr.NewNotSupportedNoCtx("legacy DML or foreign-key action on a new ENUM collation index")
	}
	return nil
}
