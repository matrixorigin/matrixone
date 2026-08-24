// Copyright 2022 Matrix Origin
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
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func normalizeGroupByName(name *tree.UnresolvedName) {
	for i := 0; i < name.NumParts; i++ {
		if name.CStrParts[i] != nil {
			name.CStrParts[i] = tree.NewCStr(name.CStrParts[i].Compare(), 0)
		}
	}
}

func resolveGroupByColumnIdentity(ctx *BindContext, name *tree.UnresolvedName) (*Binding, int32, bool) {
	if ctx == nil || name == nil || name.Star || name.NumParts == 0 {
		return nil, 0, false
	}

	col := name.ColName()
	var binding *Binding
	if table := name.TblName(); table != "" {
		binding = ctx.bindingByTable[table]
		if binding == nil && name.DbName() != "" {
			binding = ctx.bindingByTable[name.DbName()+"."+table]
		}
	} else {
		binding = ctx.bindingByCol[col]
	}
	if binding == nil {
		return nil, 0, false
	}
	// An explicit database qualifier is part of column identity. Do not let a
	// table-name match discard a typo or a reference to a different database.
	// The regular binder performs the same validation when it resolves the
	// column; canonicalization must not collapse the qualifier before then.
	if db := name.DbName(); db != "" && !strings.EqualFold(db, binding.db) {
		return nil, 0, false
	}

	colPos := binding.FindColumn(col)
	if colPos == NotFound || colPos == AmbiguousName {
		return nil, 0, false
	}
	return binding, colPos, true
}

// resolveMedianColumnIdentity resolves a column in the current query block or
// one of its parents.  The scope depth is part of the identity: an inner
// column and a correlated outer column may have the same relation/column name
// but still denote different expressions.
func resolveMedianColumnIdentity(ctx *BindContext, name *tree.UnresolvedName) (*Binding, int32, int32, bool) {
	if name == nil || name.Star || name.NumParts == 0 {
		return nil, 0, 0, false
	}
	col := name.ColName()
	for depth, current := int32(0), ctx; current != nil; depth, current = depth+1, current.parent {
		var binding *Binding
		if table := name.TblName(); table != "" {
			binding = current.bindingByTable[table]
			if binding == nil && name.DbName() != "" {
				binding = current.bindingByTable[name.DbName()+"."+table]
			}
			if binding == nil {
				continue
			}
			if db := name.DbName(); db != "" && !strings.EqualFold(db, binding.db) {
				return nil, 0, 0, false
			}
		} else {
			binding = current.bindingByCol[col]
			if binding == nil {
				// A nil entry records an ambiguous local column.  It must
				// not silently resolve to an outer scope.
				if _, exists := current.bindingByCol[col]; exists {
					return nil, 0, 0, false
				}
				continue
			}
		}

		colPos := binding.FindColumn(col)
		if colPos == AmbiguousName {
			return nil, 0, 0, false
		}
		if colPos != NotFound {
			return binding, colPos, depth, true
		}
		if name.TblName() != "" {
			// An explicit table qualifier that resolved to a visible
			// binding but not to a column is a binding error, not a
			// correlated outer reference.
			return nil, 0, 0, false
		}
	}
	return nil, 0, 0, false
}

func medianNameHasMismatchedDatabase(ctx *BindContext, name *tree.UnresolvedName) bool {
	if name == nil || name.DbName() == "" || name.TblName() == "" {
		return false
	}
	for current := ctx; current != nil; current = current.parent {
		binding := current.bindingByTable[name.TblName()]
		if binding == nil {
			binding = current.bindingByTable[name.DbName()+"."+name.TblName()]
		}
		if binding != nil {
			return !strings.EqualFold(name.DbName(), binding.db)
		}
	}
	return false
}

func medianResolvedColumnMarker(binding *Binding, colPos, depth int32) string {
	return fmt.Sprintf("__mo_resolved_%s_%s_%d_%d",
		strings.ToLower(binding.db), strings.ToLower(binding.table), colPos, depth)
}

// canonicalizeMedianAstValue normalizes identifiers in a cloned AST.  When a
// scalar subquery is encountered, it is rebound with a fresh builder/context
// so its local columns are resolved in that query block instead of against the
// outer MEDIAN context.  The normalized AST is then formatted into the
// equality key by canonicalMedianWithinGroupAstKey.
func canonicalizeMedianAstValue(
	value reflect.Value,
	ctx *BindContext,
	builder *QueryBuilder,
	valid *bool,
	visited map[treeClonePointer]struct{},
) {
	if !value.IsValid() {
		return
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return
		}
		canonicalizeMedianAstValue(value.Elem(), ctx, builder, valid, visited)
		return
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return
		}
		key := treeClonePointer{typ: value.Type(), ptr: value.Pointer()}
		if _, seen := visited[key]; seen {
			return
		}
		visited[key] = struct{}{}
		if value.CanInterface() {
			if expr, ok := value.Interface().(tree.Expr); ok {
				switch node := expr.(type) {
				case *tree.Subquery:
					canonicalizeMedianSubquery(node, ctx, builder, valid, visited)
					return
				case *tree.UnresolvedName:
					normalizeGroupByName(node)
					if medianNameHasMismatchedDatabase(ctx, node) {
						*valid = false
					}
					if binding, colPos, depth, ok := resolveMedianColumnIdentity(ctx, node); ok {
						marker := medianResolvedColumnMarker(binding, colPos, depth)
						node.NumParts = 1
						node.CStrParts = tree.CStrParts{}
						node.CStrParts[0] = tree.NewCStr(marker, 0)
					}
					return
				case *tree.FuncExpr:
					if node.FuncName != nil {
						node.FuncName = tree.NewCStr(node.FuncName.Compare(), 0)
					}
				}
			}
		}
		canonicalizeMedianAstValue(value.Elem(), ctx, builder, valid, visited)
		return
	}

	switch value.Kind() {
	case reflect.Struct:
		valueType := value.Type()
		for i := 0; i < value.NumField(); i++ {
			field := valueType.Field(i)
			if field.PkgPath != "" {
				continue
			}
			// Func is parser metadata naming the called function, not an
			// expression.  Walking it would canonicalize function references
			// as columns and could alter the equality contract.
			if valueType == groupingOrderFuncExprType && field.Name == "Func" {
				continue
			}
			canonicalizeMedianAstValue(value.Field(i), ctx, builder, valid, visited)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			canonicalizeMedianAstValue(value.Index(i), ctx, builder, valid, visited)
		}
	}
}

func canonicalizeMedianSubquery(
	node *tree.Subquery,
	parentCtx *BindContext,
	parentBuilder *QueryBuilder,
	valid *bool,
	visited map[treeClonePointer]struct{},
) {
	if node == nil || node.Select == nil {
		return
	}
	if parentBuilder == nil || parentBuilder.compCtx == nil {
		canonicalizeMedianAstValue(reflect.ValueOf(node.Select), parentCtx, nil, valid, visited)
		return
	}

	// Bind only the cloned subquery with an isolated builder.  This populates
	// the local binding maps without appending temporary nodes to the real
	// query, and starts tags after the real builder's current range so parent
	// and child identities cannot collide while the scope is inspected.
	tempBuilder := NewQueryBuilder(plan.Query_SELECT, parentBuilder.compCtx,
		parentBuilder.isPrepareStatement, parentBuilder.skipStats)
	tempBuilder.nextBindTag = parentBuilder.nextBindTag
	subCtx := NewBindContext(tempBuilder, parentCtx)
	var err error
	switch subquery := node.Select.(type) {
	case *tree.Select:
		_, err = tempBuilder.bindSelect(subquery, subCtx, false)
	case *tree.ParenSelect:
		_, err = tempBuilder.bindSelect(subquery.Select, subCtx, false)
	}
	if err != nil {
		*valid = false
		return
	}
	canonicalizeMedianAstValue(reflect.ValueOf(node.Select), subCtx, tempBuilder, valid, visited)
}

func canonicalMedianNodeKey(node tree.NodeFormatter) string {
	display := tree.String(node, dialect.MYSQL)
	identity := tree.StringWithOpts(node, dialect.MYSQL, tree.WithParamExprOffset())
	if identity == display {
		return display
	}
	return identity + "\x00" + display
}

// canonicalGroupByAstKey folds identifiers and function names through their
// comparison form. Resolved columns are represented by relation tag and column
// position, so col, tbl.col, and db.tbl.col match only when they resolve to the
// same source column. String literals and all other case-sensitive values retain
// their original spelling.
func canonicalGroupByAstKey(ctx *BindContext, astExpr tree.Expr) string {
	normalized := cloneTreeExpr(astExpr)
	var resolvedColumns strings.Builder
	functionNames := make(map[*tree.UnresolvedName]struct{})
	walkGroupingSetOrderByExpr(normalized, func(expr tree.Expr) bool {
		switch node := expr.(type) {
		case *tree.UnresolvedName:
			normalizeGroupByName(node)
			if _, isFunctionName := functionNames[node]; isFunctionName {
				return true
			}
			if binding, colPos, ok := resolveGroupByColumnIdentity(ctx, node); ok {
				node.NumParts = 1
				node.CStrParts = tree.CStrParts{}
				node.CStrParts[0] = tree.NewCStr("__mo_resolved_group_by_column", 0)
				resolvedColumns.WriteByte('#')
				resolvedColumns.WriteString(strconv.FormatInt(int64(binding.tag), 10))
				resolvedColumns.WriteByte(':')
				resolvedColumns.WriteString(strconv.FormatInt(int64(colPos), 10))
				resolvedColumns.WriteByte(';')
			}
		case *tree.Subquery:
			// A scalar subquery owns a separate bind context. Resolving its
			// columns against ctx can confuse an inner column with a correlated
			// outer column and make distinct MEDIAN expressions compare equal.
			return false
		case *tree.FuncExpr:
			if node.FuncName != nil {
				node.FuncName = tree.NewCStr(node.FuncName.Compare(), 0)
			}
			if name, ok := node.Func.FunctionReference.(*tree.UnresolvedName); ok {
				normalizeGroupByName(name)
				functionNames[name] = struct{}{}
			}
		}
		return true
	})
	return semanticAstKey(normalized) + "\x00resolved-columns" + resolvedColumns.String()
}

// canonicalMedianWithinGroupAstKey compares expressions after resolving each
// query block in its own scope.  This keeps equivalent local references such
// as a and y.a equal while retaining a different identity for correlated outer
// references.
func canonicalMedianWithinGroupAstKey(
	ctx *BindContext,
	builder *QueryBuilder,
	astExpr tree.Expr,
) (string, bool) {
	normalized := cloneTreeExpr(astExpr)
	valid := true
	canonicalizeMedianAstValue(
		reflect.ValueOf(normalized),
		ctx,
		builder,
		&valid,
		make(map[treeClonePointer]struct{}),
	)
	return canonicalMedianNodeKey(normalized), valid
}

func lookupGroupByAst(ctx *BindContext, astExpr tree.Expr, astKey string) (int32, bool) {
	if pos, ok := ctx.groupByAst[astKey]; ok {
		return pos, true
	}
	pos, ok := ctx.groupByCanonicalAst[canonicalGroupByAstKey(ctx, astExpr)]
	return pos, ok
}

func NewGroupBinder(builder *QueryBuilder, ctx *BindContext, selectList tree.SelectExprs) *GroupBinder {
	b := &GroupBinder{}
	b.sysCtx = builder.GetContext()
	b.builder = builder
	b.ctx = ctx
	b.impl = b
	b.selectList = selectList
	b.projectionExprPos = -1

	return b
}

func (b *GroupBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	var numericTarget *plan.Type
	reusesProjection := false
	if isRoot && b.projectionExprPos >= 0 {
		pos := b.projectionExprPos
		astExpr = b.selectList[pos].Expr
		reusesProjection = true
		if int(pos) < len(b.ctx.numericProjectionTypes) {
			target := b.ctx.numericProjectionTypes[pos]
			if target.Id != 0 {
				numericTarget = &target
			}
		}
	}
	if isRoot {
		if numVal, ok := astExpr.(*tree.NumVal); ok {
			switch numVal.Kind() {
			case tree.Int:
				reusesProjection = true
				colPos, _ := numVal.Int64()
				if colPos < 1 || int(colPos) > len(b.selectList) {
					return nil, moerr.NewSyntaxErrorf(b.GetContext(), "GROUP BY position %v is not in select list", colPos)
				}

				astExpr = b.selectList[colPos-1].Expr
				if int(colPos) <= len(b.ctx.numericProjectionTypes) {
					target := b.ctx.numericProjectionTypes[colPos-1]
					if target.Id != 0 {
						numericTarget = &target
					}
				}

			case tree.Unknown:
				if numVal.ValType != tree.P_null {
					return nil, moerr.NewSyntaxError(b.GetContext(), "non-integer constant in GROUP BY")
				}

			default:
				return nil, moerr.NewSyntaxError(b.GetContext(), "non-integer constant in GROUP BY")
			}
		}
	}

	var expr *plan.Expr
	var err error
	if numericTarget != nil {
		expr, err = b.bindNumericExprWithContext(astExpr, depth, numericTarget)
	} else {
		expr, err = b.baseBindExpr(astExpr, depth, isRoot)
	}
	if err != nil {
		return nil, err
	}

	if isRoot && !b.ctx.isGroupingSet {
		astStr := semanticAstKey(astExpr)
		// Independently written prepared expressions have different parameter
		// identities even when their formatted SQL is identical. Ordinal and alias
		// GROUP BY references are guaranteed to reuse the SELECT expression itself.
		hasParam := containsDynamicParam(expr)
		registerAst := reusesProjection || !hasParam
		if registerAst {
			if _, ok := b.ctx.groupByAst[astStr]; ok {
				return nil, nil
			}
			pos := int32(len(b.ctx.groups))
			b.ctx.groupByAst[astStr] = pos
			canonicalKey := canonicalGroupByAstKey(b.ctx, astExpr)
			if _, ok := b.ctx.groupByCanonicalAst[canonicalKey]; !ok {
				b.ctx.groupByCanonicalAst[canonicalKey] = pos
			}
		}
		if hasParam {
			key := parameterizedGroupByKey(astStr, expr)
			if _, ok := b.ctx.groupByParamAst[key]; ok {
				return nil, nil
			}
			b.ctx.groupByParamAst[key] = int32(len(b.ctx.groups))
		}
		if !registerAst {
			b.ctx.groups = append(b.ctx.groups, expr)
			return nil, nil
		}
		b.ctx.groups = append(b.ctx.groups, expr)
	}

	if isRoot && b.ctx.isGroupingSet {
		astStr := semanticAstKey(astExpr)
		pos, ok := lookupGroupByAst(b.ctx, astExpr, astStr)
		if containsDynamicParam(expr) {
			pos, ok = b.ctx.groupByParamAst[parameterizedGroupByKey(astStr, expr)]
		}
		if !ok || int(pos) >= len(b.ctx.groupingFlag) {
			return nil, moerr.NewInternalErrorf(b.GetContext(), "grouping expression position not found: %s", astStr)
		}
		b.ctx.groupingFlag[pos] = true
	}

	return expr, err
}

func parameterizedGroupByKey(ast string, expr *plan.Expr) string {
	positions := make([]int32, 0, 2)
	collectGroupByParamPositions(expr, &positions)
	var key strings.Builder
	key.WriteString(ast)
	for _, pos := range positions {
		key.WriteByte('#')
		key.WriteString(strconv.FormatInt(int64(pos), 10))
	}
	return key.String()
}

func collectGroupByParamPositions(expr *plan.Expr, positions *[]int32) {
	if expr == nil {
		return
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_P:
		*positions = append(*positions, item.P.Pos)
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			collectGroupByParamPositions(arg, positions)
		}
	}
}

func (b *GroupBinder) BindProjectionExpr(pos int32) (*plan.Expr, error) {
	b.projectionExprPos = pos
	defer func() { b.projectionExprPos = -1 }()
	return b.BindExpr(b.selectList[pos].Expr, 0, true)
}

func (b *GroupBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	expr, err := b.baseBindColRef(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}

	if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
		return nil, moerr.NewNYI(b.GetContext(), "correlated columns in GROUP BY clause")
	}

	return expr, nil
}

func (b *GroupBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "GROUP BY clause cannot contain aggregate functions")
}

func (b *GroupBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput(b.GetContext(), "GROUP BY clause cannot contain window functions")
}

func (b *GroupBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI(b.GetContext(), "subquery in GROUP BY clause")
}

func (b *GroupBinder) BindTimeWindowFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInputf(b.GetContext(), "cannot bind time window functions '%s'", funcName)
}
