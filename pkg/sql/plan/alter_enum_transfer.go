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

package plan

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type alterEnumTransferKey struct{}

type alterEnumTransferColumn struct {
	source *planpb.ColDef
	target *planpb.ColDef
}

type alterEnumTransferBinding struct {
	spec            *executor.AlterEnumTransferSpec
	insert          *tree.Insert
	rows            *tree.Select
	sourceNode      *tree.TableName
	columns         map[string]alterEnumTransferColumn
	sourceValidated bool
	targetValidated bool
}

// NeedsAlterEnumTransfer identifies COPY plans that may require physical ENUM
// transfer. The parsed statement and resolved relations are checked separately.
func NeedsAlterEnumTransfer(source, target *planpb.TableDef) bool {
	if source == nil || target == nil {
		return false
	}
	// Canonicalize each complete ordered domain once. A legacy and framed
	// representation can encode the same members; comparing every pair by
	// decoding each domain repeatedly makes unrelated ALTERs quadratic in both
	// column count and allocations.
	type domain struct {
		value    string
		encoding uint32
	}
	type cachedDomain struct {
		key   domain
		valid bool
	}
	type identity struct {
		collation uint32
		encoding  uint32
	}
	type identities struct {
		first  identity
		varied bool
	}
	cache := make(map[domain]cachedDomain)
	canonical := func(typ *planpb.Type) (domain, bool) {
		raw := domain{value: typ.Enumvalues, encoding: typ.EnumEncoding}
		if result, ok := cache[raw]; ok {
			return result.key, result.valid
		}
		members, err := types.DecodeEnumMembers(raw.value, raw.encoding)
		if err != nil {
			cache[raw] = cachedDomain{}
			return domain{}, false
		}
		value, encoding, err := types.EncodeEnumMembers(members)
		result := cachedDomain{key: domain{value: value, encoding: encoding}, valid: err == nil}
		cache[raw] = result
		return result.key, result.valid
	}
	oldDomains := make(map[domain]identities)
	for _, col := range source.Cols {
		if col == nil || col.Hidden || !isEnumPlanType(&col.Typ) {
			continue
		}
		key, ok := canonical(&col.Typ)
		if !ok {
			continue
		}
		id := identity{collation: col.Typ.EnumCollation, encoding: col.Typ.EnumEncoding}
		if prior, exists := oldDomains[key]; exists {
			prior.varied = prior.varied || prior.first != id
			oldDomains[key] = prior
		} else {
			oldDomains[key] = identities{first: id}
		}
	}
	for _, col := range target.Cols {
		if col == nil || col.Hidden || col.GeneratedCol != nil || !isEnumPlanType(&col.Typ) {
			continue
		}
		key, ok := canonical(&col.Typ)
		if !ok {
			continue
		}
		if prior, exists := oldDomains[key]; exists && (prior.varied || prior.first != (identity{collation: col.Typ.EnumCollation, encoding: col.Typ.EnumEncoding})) {
			return true
		}
	}
	return false
}

func uniqueAlterEnumColumn(table *planpb.TableDef, name string) (*planpb.ColDef, error) {
	var found *planpb.ColDef
	for _, col := range table.Cols {
		if col == nil {
			return nil, moerr.NewInvalidInputNoCtx("invalid ALTER COPY column metadata")
		}
		if strings.EqualFold(col.Name, name) {
			if found != nil {
				return nil, moerr.NewInvalidInputNoCtx("ambiguous ALTER COPY column")
			}
			found = col
		}
	}
	return found, nil
}

func alterEnumDirectTable(expr tree.TableExpr, database, name string, allowSnapshot bool) (*tree.TableName, error) {
	if join, ok := expr.(*tree.JoinTableExpr); ok {
		if join.Right != nil || join.Cond != nil {
			return nil, moerr.NewInvalidInputNoCtx("invalid ALTER COPY source")
		}
		expr = join.Left
	}
	if alias, ok := expr.(*tree.AliasedTableExpr); ok {
		if alias.As.Alias != "" || len(alias.IndexHints) != 0 {
			return nil, moerr.NewInvalidInputNoCtx("invalid ALTER COPY table alias")
		}
		expr = alias.Expr
	}
	tbl, ok := expr.(*tree.TableName)
	if !ok || (!allowSnapshot && tbl.AtTsExpr != nil) || tbl.IcebergRef != nil || !strings.EqualFold(string(tbl.SchemaName), database) || !strings.EqualFold(string(tbl.ObjectName), name) {
		return nil, moerr.NewInvalidInputNoCtx("invalid ALTER COPY table")
	}
	return tbl, nil
}

// WithAlterEnumTransfer marks only the exact generated INSERT and its direct
// original-column projections. No user statement can install this binding.
func WithAlterEnumTransfer(ctx context.Context, stmt tree.Statement, spec *executor.AlterEnumTransferSpec) (context.Context, error) {
	invalid := func() error { return moerr.NewInvalidInputNoCtx("invalid internal ALTER ENUM transfer") }
	if spec == nil || spec.Source == nil || spec.Target == nil || spec.SourceID == 0 || spec.TargetID == 0 || spec.Database == "" {
		return nil, invalid()
	}
	account, err := defines.GetAccountId(ctx)
	if err != nil || account != spec.AccountID {
		return nil, invalid()
	}
	ins, ok := stmt.(*tree.Insert)
	if !ok || ins.With != nil || ins.Overwrite || ins.HasReturning() || len(ins.OnDuplicateUpdate) != 0 || len(ins.Columns) == 0 || ins.Rows == nil {
		return nil, invalid()
	}
	if _, err = alterEnumDirectTable(ins.Table, spec.Database, spec.Target.Name, false); err != nil {
		return nil, err
	}
	rows := ins.Rows
	if rows.With != nil || rows.IsPerform || rows.TimeWindow != nil || len(rows.OrderBy) != 0 || rows.Limit != nil || rows.RankOption != nil || rows.Ep != nil || rows.SelectLockInfo != nil {
		return nil, invalid()
	}
	selectClause, ok := rows.Select.(*tree.SelectClause)
	if !ok || selectClause.Distinct || selectClause.Option != 0 || selectClause.From == nil || len(selectClause.From.Tables) != 1 || selectClause.Where != nil || selectClause.GroupBy != nil || selectClause.Having != nil || len(selectClause.Exprs) != len(ins.Columns) {
		return nil, invalid()
	}
	sourceNode, err := alterEnumDirectTable(selectClause.From.Tables[0], spec.Database, spec.Source.Name, true)
	if err != nil {
		return nil, err
	}
	state := &alterEnumTransferBinding{spec: spec, insert: ins, rows: rows, sourceNode: sourceNode, columns: make(map[string]alterEnumTransferColumn)}
	seenTargets := make(map[string]bool, len(ins.Columns))
	for i, targetName := range ins.Columns {
		name := strings.ToLower(string(targetName))
		if seenTargets[name] {
			return nil, invalid()
		}
		seenTargets[name] = true
		target, err := uniqueAlterEnumColumn(spec.Target, name)
		if err != nil {
			return nil, err
		}
		if target == nil || target.Hidden || target.GeneratedCol != nil || !isEnumPlanType(&target.Typ) {
			continue
		}
		ref, direct := selectClause.Exprs[i].Expr.(*tree.UnresolvedName)
		if !direct || ref.Star || ref.NumParts != 1 || (selectClause.Exprs[i].As != nil && !selectClause.Exprs[i].As.Empty()) {
			continue
		}
		source, err := uniqueAlterEnumColumn(spec.Source, ref.ColName())
		if err != nil {
			return nil, err
		}
		if source == nil || source.Hidden || !isEnumPlanType(&source.Typ) {
			continue
		}
		if err := types.ValidateEnumCollation(source.Typ.EnumCollation); err != nil {
			return nil, err
		}
		if err := types.ValidateEnumCollation(target.Typ.EnumCollation); err != nil {
			return nil, err
		}
		equal, err := types.EqualEnumMembers(source.Typ.Enumvalues, source.Typ.EnumEncoding, target.Typ.Enumvalues, target.Typ.EnumEncoding)
		if err != nil {
			return nil, err
		}
		if !equal || (source.Typ.EnumCollation == target.Typ.EnumCollation && source.Typ.EnumEncoding == target.Typ.EnumEncoding) {
			continue
		}
		if source.Seqnum != target.Seqnum {
			return nil, moerr.NewInvalidInputNoCtx("ambiguous ALTER ENUM column source")
		}
		state.columns[name] = alterEnumTransferColumn{source: source, target: target}
	}
	return context.WithValue(ctx, alterEnumTransferKey{}, state), nil
}

func isAlterEnumTransfer(ctx context.Context) bool {
	state, _ := ctx.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	return state != nil && len(state.columns) != 0
}

// ValidateAlterEnumTransferPlan follows each target write slot through the
// executable projections to the exact physical source column. A cast found in
// some other projection or write slot is not evidence for this transfer.
func ValidateAlterEnumTransferPlan(ctx context.Context, p *planpb.Plan) error {
	state, _ := ctx.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	if state == nil || len(state.columns) == 0 {
		return nil
	}
	invalid := func() error { return moerr.NewInvalidInputNoCtx("invalid optimized ALTER ENUM transfer plan") }
	if !state.sourceValidated || !state.targetValidated || p == nil || p.GetQuery() == nil || len(p.GetQuery().Steps) == 0 {
		return invalid()
	}
	query := p.GetQuery()
	nodeAt := func(id int32) *planpb.Node {
		if id < 0 || int(id) >= len(query.Nodes) {
			return nil
		}
		return query.Nodes[id]
	}
	// The table COPY has one write to the replacement table. Index-table writes
	// may occur in the same MULTI_UPDATE and are deliberately not treated as it.
	var writer *planpb.Node
	var target *planpb.UpdateCtx
	var writerStep int32
	for step, root := range query.Steps {
		node := nodeAt(root)
		if node == nil {
			return invalid()
		}
		if node.NodeType != planpb.Node_MULTI_UPDATE {
			continue
		}
		for _, upd := range node.UpdateCtxList {
			if upd != nil && upd.ObjRef != nil && uint64(upd.ObjRef.Obj) == state.spec.TargetID {
				if target != nil || upd.TableDef == nil || upd.TableDef.TblId != state.spec.TargetID || len(upd.DeleteCols) != 0 {
					return invalid()
				}
				writer, target, writerStep = node, upd, int32(step)
			}
		}
	}
	if target == nil || writer == nil {
		return invalid()
	}
	for _, pair := range state.columns {
		targetIdx := -1
		for i, col := range target.TableDef.Cols {
			if col != nil && strings.EqualFold(col.Name, pair.target.Name) {
				if targetIdx >= 0 || col.Hidden || col.GeneratedCol != nil || !sameEnumIndexDomain(&col.Typ, &pair.target.Typ) {
					return invalid()
				}
				targetIdx = i
			}
		}
		if targetIdx < 0 || targetIdx >= len(target.InsertCols) {
			return invalid()
		}
		writeRef := target.InsertCols[targetIdx]
		// MULTI_UPDATE consumes only child 0 and InsertCols.ColPos; it does
		// not use RelPos to select an input.
		if len(writer.Children) != 1 || writeRef.RelPos != 0 {
			return invalid()
		}
		id, pos := writer.Children[0], writeRef.ColPos
		currentStep := writerStep
		sawCast := false
		visitedPath := make(map[int32]bool)
		for {
			node := nodeAt(id)
			if node == nil || visitedPath[id] || pos < 0 || int(pos) >= len(node.ProjectList) {
				return invalid()
			}
			visitedPath[id] = true
			expr := node.ProjectList[pos]
			if expr == nil {
				return invalid()
			}
			if node.NodeType == planpb.Node_TABLE_SCAN {
				if !sawCast || node.ObjRef == nil || uint64(node.ObjRef.Obj) != state.spec.SourceID || node.TableDef == nil || node.TableDef.TblId != state.spec.SourceID || node.IndexScanInfo.IsIndexScan {
					return invalid()
				}
				ref := expr.GetCol()
				if ref == nil || ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(node.TableDef.Cols) {
					return invalid()
				}
				source := node.TableDef.Cols[ref.ColPos]
				if source == nil || source.Hidden || !strings.EqualFold(source.Name, pair.source.Name) || source.Seqnum != pair.source.Seqnum || !sameEnumIndexDomain(&source.Typ, &pair.source.Typ) || !sameEnumIndexDomain(&expr.Typ, &pair.source.Typ) {
					return invalid()
				}
				break
			}
			switch node.NodeType {
			case planpb.Node_PROJECT, planpb.Node_LOCK_OP, planpb.Node_FILTER:
			case planpb.Node_JOIN:
				// Dedup FAIL returns the new-row side. The other side is filled
				// with NULL even if its projection metadata names a source column.
				ref := expr.GetCol()
				if node.JoinType != planpb.Node_DEDUP || node.OnDuplicateAction != planpb.Node_FAIL || node.DedupJoinCtx != nil || len(node.Children) != 2 || ref == nil || ref.ColPos < 0 || (!node.IsRightJoin && ref.RelPos != 1) || (node.IsRightJoin && ref.RelPos != 0) || !sameAlterEnumTransferPathType(expr, pair, sawCast) {
					return invalid()
				}
				id, pos = node.Children[ref.RelPos], ref.ColPos
				continue
			case planpb.Node_SINK_SCAN:
				// A sink scan applies its projection to the batch produced by the
				// referenced earlier step. Follow that exact physical slot.
				ref := expr.GetCol()
				if ref == nil || ref.RelPos != 0 || ref.ColPos < 0 || len(node.Children) != 0 || len(node.SourceStep) != 1 || node.RecursiveSink || node.RecursiveCte {
					return invalid()
				}
				step := node.SourceStep[0]
				if step < 0 || int(step) >= len(query.Steps) || step >= currentStep {
					return invalid()
				}
				sink := nodeAt(query.Steps[step])
				if sink == nil || sink.NodeType != planpb.Node_SINK || sink.RecursiveSink || sink.RecursiveCte || len(sink.Children) != 1 || !sameAlterEnumTransferPathType(expr, pair, sawCast) {
					return invalid()
				}
				id, pos, currentStep = query.Steps[step], ref.ColPos, step
				continue
			case planpb.Node_SINK:
				// SINK dispatches its child's batch without executing ProjectList.
				// Only accept identity metadata and follow the physical slot.
				ref := expr.GetCol()
				if len(node.Children) != 1 || ref == nil || ref.RelPos != 0 || ref.ColPos != pos || !sameAlterEnumTransferPathType(expr, pair, sawCast) {
					return invalid()
				}
				id = node.Children[0]
				continue
			case planpb.Node_PRE_INSERT:
				// PRE_INSERT does not execute its ProjectList: ordinary fields are
				// passed by physical slot, while auto-increment and generated keys
				// may be changed. Prove this is the target's ordinary ENUM slot.
				pre := node.PreInsertCtx
				ref := expr.GetCol()
				if pre == nil || pre.Ref == nil || uint64(pre.Ref.Obj) != state.spec.TargetID || pre.TableDef == nil || pre.TableDef.TblId != state.spec.TargetID || pre.IsOldUpdate || pre.IsNewUpdate || pre.ColOffset != 0 || len(node.Children) != 1 || ref == nil || ref.RelPos != 0 || ref.ColPos != pos || !sameAlterEnumTransferPathType(expr, pair, sawCast) {
					return invalid()
				}
				var slot int32
				found := false
				for i, col := range pre.TableDef.Cols {
					if col == nil {
						return invalid()
					}
					if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
						continue
					}
					if strings.EqualFold(col.Name, pair.target.Name) {
						if found || slot != pos || int32(i) != slot || col.Typ.AutoIncr || col.GeneratedCol != nil || !sameEnumIndexDomain(&col.Typ, &pair.target.Typ) {
							return invalid()
						}
						found = true
					}
					slot++
				}
				if !found {
					return invalid()
				}
				id = node.Children[0]
				continue
			default:
				return invalid()
			}
			if f := expr.GetF(); f != nil {
				if sawCast || f.Func == nil || !strings.EqualFold(f.Func.ObjName, "cast") || len(f.Args) != 2 || f.Args[0] == nil || f.Args[1] == nil || f.Args[1].GetT() == nil || !sameEnumIndexDomain(&f.Args[0].Typ, &pair.source.Typ) || !sameEnumIndexDomain(&f.Args[1].Typ, &pair.target.Typ) || !sameEnumIndexDomain(&expr.Typ, &pair.target.Typ) {
					return invalid()
				}
				overload, err := function.GetFunctionByName(ctx, "cast", []types.Type{makeTypeByPlan2Type(pair.source.Typ), makeTypeByPlan2Type(pair.target.Typ)})
				if err != nil || f.Func.Obj != overload.GetEncodedOverloadID() {
					return invalid()
				}
				sawCast = true
				expr = f.Args[0]
			} else if sawCast {
				if !sameEnumIndexDomain(&expr.Typ, &pair.source.Typ) {
					return invalid()
				}
			} else if !sameEnumIndexDomain(&expr.Typ, &pair.target.Typ) {
				return invalid()
			}
			ref := expr.GetCol()
			// PROJECT, FILTER and LOCK_OP compile only child 0; an arbitrary
			// RelPos cannot authorize following some other plan child.
			if ref == nil || len(node.Children) != 1 || ref.RelPos != 0 {
				return invalid()
			}
			id, pos = node.Children[0], ref.ColPos
		}
	}
	return nil
}

func sameAlterEnumTransferPathType(expr *planpb.Expr, pair alterEnumTransferColumn, sawCast bool) bool {
	if sawCast {
		return sameEnumIndexDomain(&expr.Typ, &pair.source.Typ)
	}
	return sameEnumIndexDomain(&expr.Typ, &pair.target.Typ)
}

func WithoutAlterEnumTransfer(ctx context.Context) context.Context {
	if ctx == nil || ctx.Value(alterEnumTransferKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, alterEnumTransferKey{}, (*alterEnumTransferBinding)(nil))
}

func validateAlterEnumTarget(ctx context.Context, stmt *tree.Insert, def *planpb.TableDef, obj *planpb.ObjectRef) error {
	state, _ := ctx.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	if state == nil || state.insert != stmt {
		return nil
	}
	if def == nil || obj == nil || def.TblId != state.spec.TargetID || uint64(obj.Obj) != state.spec.TargetID || !strings.EqualFold(def.Name, state.spec.Target.Name) {
		return moerr.NewInvalidInputNoCtx("ALTER ENUM target changed")
	}
	for name, pair := range state.columns {
		actual, err := uniqueAlterEnumColumn(def, name)
		if err != nil || actual == nil || actual.Hidden || actual.GeneratedCol != nil || !sameEnumIndexDomain(&actual.Typ, &pair.target.Typ) {
			return moerr.NewInvalidInputNoCtx("ALTER ENUM target schema changed")
		}
	}
	state.targetValidated = true
	return nil
}

func validateAlterEnumSource(ctx context.Context, node *tree.TableName, def *planpb.TableDef, obj *planpb.ObjectRef) error {
	state, _ := ctx.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	if state == nil || state.sourceNode != node {
		return nil
	}
	if def == nil || obj == nil || def.TblId != state.spec.SourceID || uint64(obj.Obj) != state.spec.SourceID || !strings.EqualFold(def.Name, state.spec.Source.Name) {
		return moerr.NewInvalidInputNoCtx("ALTER ENUM source changed")
	}
	for _, pair := range state.columns {
		actual, err := uniqueAlterEnumColumn(def, pair.source.Name)
		if err != nil || actual == nil || actual.Hidden || actual.Seqnum != pair.source.Seqnum || !sameEnumIndexDomain(&actual.Typ, &pair.source.Typ) {
			return moerr.NewInvalidInputNoCtx("ALTER ENUM source schema changed")
		}
	}
	state.sourceValidated = true
	return nil
}

func alterEnumTransferExpr(ctx context.Context, rows *tree.Select, column string, raw *planpb.Expr, target planpb.Type) (*planpb.Expr, bool, error) {
	state, _ := ctx.Value(alterEnumTransferKey{}).(*alterEnumTransferBinding)
	if state == nil || state.rows != rows {
		return raw, false, nil
	}
	pair, ok := state.columns[strings.ToLower(column)]
	if !ok {
		return raw, false, nil
	}
	if !state.sourceValidated || !state.targetValidated || raw == nil || !sameEnumIndexDomain(&raw.Typ, &pair.source.Typ) || !sameEnumIndexDomain(&target, &pair.target.Typ) {
		return nil, false, moerr.NewInvalidInputNoCtx("ALTER ENUM transfer proof changed")
	}
	overload, err := function.GetFunctionByName(ctx, "cast", []types.Type{makeTypeByPlan2Expr(raw), makeTypeByPlan2Type(target)})
	if err != nil {
		return nil, false, err
	}
	targetExpr := &planpb.Expr{Typ: target, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
	result := &planpb.Expr{Typ: target, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "cast"},
		Args: []*planpb.Expr{raw, targetExpr},
	}}}
	return result, true, nil
}
