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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// A writable rewrite must keep the identity and values of each selected base
// row. The DML projection still carries the full physical row for maintenance.
type dmlTargetRewrite struct {
	source     *tree.TableName
	alias      string
	tableIndex int
	tableID    uint64
	key        string
	chain      []*tree.Rewrite
	layers     []dmlRewriteLayer
	visible    map[string]int32
}

type dmlRewriteLayer struct {
	qualifier string
	input     map[string]int32
	where     *tree.Where
}

func resolveDMLTargetRewrites(
	ctx CompilerContext,
	option *tree.RewriteOption,
	sources tree.TableExprs,
	with *tree.With,
	aliases map[string]int,
	tableDefs []*plan.TableDef,
) map[string]*dmlTargetRewrite {
	if option == nil || len(option.Rewrites) == 0 {
		return nil
	}

	defaultDatabase := ctx.DefaultDatabase()
	lower := ctx.GetLowerCaseTableNames()
	names := make(map[string]*tree.TableName)
	for _, source := range sources {
		walkDMLSourceAliases(source, "", func(alias string, table *tree.TableName, _ bool) {
			names[alias] = table
		})
	}
	var targets map[string]*dmlTargetRewrite
	for alias, source := range names {
		idx, writable := aliases[alias]
		if !writable {
			continue
		}
		database := string(source.SchemaName)
		if database == "" {
			database = defaultDatabase
		}
		table := string(source.ObjectName)
		key := rewriteTableKey(database, table, lower)
		chain := option.Rewrites[key]
		if len(chain) == 0 {
			continue
		}
		def := tableDefs[idx]
		target := &dmlTargetRewrite{
			source:     source,
			alias:      alias,
			tableIndex: idx,
			tableID:    def.TblId,
			key:        key,
			chain:      chain,
		}
		policyDefault := defaultDatabase
		if with != nil {
			for _, cte := range with.CTEs {
				if tree.NewCStr(string(cte.Name.Alias), lower).Compare() == tree.NewCStr(table, lower).Compare() {
					// An unqualified rule source could resolve to the caller CTE.
					policyDefault = ""
					break
				}
			}
		}
		if !def.IsTemporary && def.TableType == catalog.SystemOrdinaryRel {
			target.layers, target.visible = writableRewriteLayers(chain, def, database, table, policyDefault, lower)
		}
		if targets == nil {
			targets = make(map[string]*dmlTargetRewrite)
		}
		targets[alias] = target
	}
	return targets
}

func writableRewriteLayers(
	chain []*tree.Rewrite, def *plan.TableDef, database, table, defaultDatabase string, lower int64,
) ([]dmlRewriteLayer, map[string]int32) {
	visible := make(map[string]int32, len(def.Cols))
	for i, col := range def.Cols {
		if !col.Hidden {
			visible[col.Name] = int32(i)
		}
	}

	layers := make([]dmlRewriteLayer, 0, len(chain))
	for _, rewrite := range chain {
		if rewrite == nil {
			return nil, nil
		}
		stmt, ok := rewrite.Stmt.(*tree.Select)
		if !ok || stmt.IsPerform || stmt.With != nil || stmt.RewriteOption != nil || stmt.TimeWindow != nil ||
			stmt.RankOption != nil || len(stmt.OrderBy) != 0 || stmt.Limit != nil ||
			stmt.Ep != nil || len(stmt.IntoVars) != 0 || stmt.SelectLockInfo != nil {
			return nil, nil
		}
		clause, ok := stmt.Select.(*tree.SelectClause)
		if !ok || clause.Distinct || clause.Option&^tree.QuerySpecOptionAll != 0 || clause.GroupBy != nil || clause.Having != nil ||
			len(clause.Windows) != 0 || len(clause.IntoVars) != 0 || clause.IntoExport != nil || len(clause.Exprs) == 0 {
			return nil, nil
		}
		qualifier, aliased, ok := rewriteSingleTableSource(clause, database, table, defaultDatabase, lower)
		if !ok {
			return nil, nil
		}
		names, all, ok := rewriteProjectionColumns(clause.Exprs, database, qualifier, aliased, lower)
		if !ok || (all && len(clause.Exprs) != 1) || (!all && len(names) != len(clause.Exprs)) {
			return nil, nil
		}

		layers = append(layers, dmlRewriteLayer{qualifier: qualifier, input: visible, where: clause.Where})
		if all {
			continue
		}
		projected := make(map[string]int32, len(names))
		for name := range names {
			pos, ok := visible[name]
			if !ok {
				return nil, nil
			}
			projected[name] = pos
		}
		visible = projected
	}
	return layers, visible
}

func (builder *QueryBuilder) enableDMLTargetRewrites(
	ctx *BindContext, targets map[string]*dmlTargetRewrite, assignments []map[string]tree.Expr,
) error {
	for _, target := range targets {
		if assignments != nil && len(assignments[target.tableIndex]) == 0 {
			continue
		}
		if len(target.layers) == 0 {
			return rewritePrivilegeError(builder.GetContext())
		}
		if builder.dmlRewriteTargets == nil {
			builder.dmlRewriteTargets = make(map[*tree.TableName]*dmlTargetRewrite, len(targets))
			ctx.dmlPhysicalRefs = make(map[*tree.UnresolvedName]struct{})
		}
		builder.dmlRewriteTargets[target.source] = target
	}
	return nil
}

func (builder *QueryBuilder) enableDeleteTargetRewrites(
	ctx *BindContext, stmt *tree.Delete, aliases map[string]int, tableDefs []*plan.TableDef,
) error {
	sources := stmt.TableRefs
	if len(sources) == 0 {
		sources = stmt.Tables
	}
	targets := resolveDMLTargetRewrites(builder.compCtx, stmt.RewriteOption, sources, stmt.With, aliases, tableDefs)
	return builder.enableDMLTargetRewrites(ctx, targets, nil)
}

func (ctx *BindContext) markDMLPhysicalRef(ref *tree.UnresolvedName) {
	if ctx.dmlPhysicalRefs != nil {
		ctx.dmlPhysicalRefs[ref] = struct{}{}
	}
}

func (ctx *BindContext) restrictDMLBinding(binding *Binding, visible map[string]int32) {
	binding.dmlStorageColumns = binding.colIdByName
	binding.colIdByName = make(map[string]int32, len(visible))
	for i, name := range binding.cols {
		if _, allowed := visible[name]; allowed {
			binding.colIdByName[name] = binding.dmlStorageColumns[name]
			continue
		}
		binding.colIsHidden[i] = true
		if ctx.bindingByCol[name] == binding {
			delete(ctx.bindingByCol, name)
		}
	}
}

func (builder *QueryBuilder) dmlRewriteTarget(source tree.TableExpr) *dmlTargetRewrite {
	if len(builder.dmlRewriteTargets) == 0 {
		return nil
	}
	for {
		paren, ok := source.(*tree.ParenTableExpr)
		if !ok {
			break
		}
		source = paren.Expr
	}
	table, _ := source.(*tree.TableName)
	return builder.dmlRewriteTargets[table]
}

func (builder *QueryBuilder) applyDMLTargetRewrite(
	nodeID int32, scan *plan.Node, ctx *BindContext, target *dmlTargetRewrite,
) (int32, error) {
	binding := ctx.bindingByTable[target.alias]
	if scan.NodeType != plan.Node_TABLE_SCAN || scan.ScanSnapshot != nil ||
		scan.GetObjRef().GetPubInfo() != nil || binding == nil || binding.tableID != target.tableID {
		return 0, rewritePrivilegeError(builder.GetContext())
	}
	scan.NotCacheable = true

	for i, layer := range target.layers {
		if layer.where == nil {
			continue
		}
		// Bind each rule in its own namespace over the preceding projection.
		// As in a SELECT rewrite, only the older layers of this table apply
		// to nested reads inside the rule.
		scope := NewBindContext(builder, nil)
		scope.defaultDatabase = ctx.defaultDatabase
		if i != 0 {
			scope.remapOption = &tree.RewriteOption{Rewrites: map[string][]*tree.Rewrite{
				target.key: target.chain[:i],
			}}
		}
		ruleBinding := *binding
		ruleBinding.table = layer.qualifier
		ruleBinding.colIsHidden = slices.Clone(binding.colIsHidden)
		scope.bindings = append(scope.bindings, &ruleBinding)
		scope.bindingTree = &BindingTreeNode{binding: &ruleBinding}
		scope.bindingByTable[layer.qualifier] = &ruleBinding
		scope.bindingByTag[binding.tag] = &ruleBinding
		for name := range layer.input {
			scope.bindingByCol[name] = &ruleBinding
		}
		scope.restrictDMLBinding(&ruleBinding, layer.input)
		scope.binder = NewWhereBinder(builder, scope)
		where := *layer.where
		where.Expr = cloneTreeExpr(where.Expr)
		inputID, filters, _, err := builder.bindWhere(scope, &where, nodeID, false)
		if err != nil {
			return 0, err
		}
		filterID := builder.appendNode(&plan.Node{
			NodeType:        plan.Node_FILTER,
			Children:        []int32{inputID},
			FilterList:      filters,
			FilterIsBarrier: true,
			NotCacheable:    true,
		}, ctx)
		// Total predicates may narrow the scan beneath another total predicate.
		// Other predicates remain above the policy to preserve error and side
		// effect behavior for rows the policy excludes.
		if inputID == nodeID && areTruncationSafePredicates(filters) {
			if builder.dmlRewriteFilters == nil {
				builder.dmlRewriteFilters = make(map[int32]struct{})
			}
			builder.dmlRewriteFilters[filterID] = struct{}{}
		}
		nodeID = filterID
	}
	ctx.restrictDMLBinding(binding, target.visible)
	return nodeID, nil
}

func (builder *QueryBuilder) dmlReturningColumnVisible(name string) bool {
	for _, target := range builder.dmlRewriteTargets {
		if target.tableID == builder.returningTableDef.TblId {
			_, visible := target.visible[name]
			return visible
		}
	}
	return true
}
