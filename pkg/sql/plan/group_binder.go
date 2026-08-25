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

func (b *baseBinder) medianValidationBaseBinder() *baseBinder {
	return b
}

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
	return fmt.Sprintf("__mo_resolved_%d_%d_%d", binding.tag, colPos, depth)
}

func canonicalizeMedianAliasReferences(expr tree.Expr, aliases map[string]string) {
	if expr == nil || len(aliases) == 0 {
		return
	}
	walkGroupingSetOrderByExpr(expr, func(current tree.Expr) bool {
		switch node := current.(type) {
		case *tree.Subquery:
			return false
		case *tree.UnresolvedName:
			if node.Star || node.NumParts != 1 {
				return true
			}
			marker, ok := aliases[node.ColName()]
			if !ok {
				return true
			}
			node.CStrParts = tree.CStrParts{}
			node.CStrParts[0] = tree.NewCStr(marker, 0)
		}
		return true
	})
}

func canonicalizeMedianCTEValue(
	value reflect.Value,
	aliases map[string]string,
	path string,
	visited map[treeClonePointer]struct{},
) {
	if !value.IsValid() {
		return
	}
	if value.Kind() == reflect.Interface {
		if !value.IsNil() {
			canonicalizeMedianCTEValue(value.Elem(), aliases, path, visited)
		}
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
			switch node := value.Interface().(type) {
			case *tree.Select:
				canonicalizeMedianSelectCTEs(node, aliases, path, visited)
				return
			case *tree.TableName:
				if !node.ExplicitCatalog && !node.ExplicitSchema {
					if marker, ok := aliases[strings.ToLower(string(node.ObjectName))]; ok {
						node.ObjectName = tree.Identifier(marker)
					}
				}
			}
		}
		canonicalizeMedianCTEValue(value.Elem(), aliases, path, visited)
		return
	}

	switch value.Kind() {
	case reflect.Struct:
		valueType := value.Type()
		for i := 0; i < value.NumField(); i++ {
			if valueType.Field(i).PkgPath == "" {
				canonicalizeMedianCTEValue(value.Field(i), aliases, path, visited)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < value.Len(); i++ {
			canonicalizeMedianCTEValue(value.Index(i), aliases, path, visited)
		}
	}
}

func canonicalizeMedianSelectCTEs(
	node *tree.Select,
	inherited map[string]string,
	path string,
	visited map[treeClonePointer]struct{},
) {
	if node == nil {
		return
	}
	aliases := cloneMedianMap(inherited)
	if aliases == nil {
		aliases = make(map[string]string)
	}
	if node.With != nil {
		for i, cte := range node.With.CTEs {
			if cte == nil || cte.Name == nil {
				continue
			}
			marker := fmt.Sprintf("__mo_cte_%s_%d", path, i)
			aliases[strings.ToLower(string(cte.Name.Alias))] = marker
			cte.Name.Alias = tree.Identifier(marker)
			for col := range cte.Name.Cols {
				cte.Name.Cols[col] = tree.Identifier(fmt.Sprintf("__mo_cte_col_%s_%d_%d", path, i, col))
			}
		}
		for i, cte := range node.With.CTEs {
			if cte != nil {
				canonicalizeMedianCTEValue(
					reflect.ValueOf(cte.Stmt), aliases, fmt.Sprintf("%s_%d", path, i), visited,
				)
			}
		}
	}

	canonicalizeMedianCTEValue(reflect.ValueOf(node.Select), aliases, path+"_body", visited)
	value := reflect.ValueOf(node).Elem()
	valueType := value.Type()
	for i := 0; i < value.NumField(); i++ {
		field := valueType.Field(i)
		if field.PkgPath != "" || field.Name == "With" || field.Name == "Select" {
			continue
		}
		canonicalizeMedianCTEValue(value.Field(i), aliases, path+"_tail", visited)
	}
}

func canonicalizeMedianSelectAliases(node *tree.Select) {
	if node == nil {
		return
	}
	var clause *tree.SelectClause
	switch selectStmt := node.Select.(type) {
	case *tree.SelectClause:
		clause = selectStmt
	case *tree.ParenSelect:
		canonicalizeMedianSelectAliases(selectStmt.Select)
		return
	default:
		return
	}

	aliases := make(map[string]string)
	for i := range clause.Exprs {
		alias := clause.Exprs[i].As
		if alias == nil || alias.Empty() {
			continue
		}
		aliases[alias.Compare()] = fmt.Sprintf("__mo_result_%d", i)
		clause.Exprs[i].As = nil
	}
	for _, order := range node.OrderBy {
		if order != nil {
			canonicalizeMedianAliasReferences(order.Expr, aliases)
		}
	}
	if clause.GroupBy != nil {
		for _, group := range clause.GroupBy.GroupByExprsList {
			for _, expr := range group {
				canonicalizeMedianAliasReferences(expr, aliases)
			}
		}
		for _, expr := range clause.GroupBy.GroupingSet {
			canonicalizeMedianAliasReferences(expr, aliases)
		}
	}
	if clause.Having != nil {
		canonicalizeMedianAliasReferences(clause.Having.Expr, aliases)
	}
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
			switch node := value.Interface().(type) {
			case *tree.Select:
				canonicalizeMedianSelectAliases(node)
			case *tree.SelectClause:
				// A scalar SELECT result alias is a heading, not part of the
				// value produced by the scalar subquery.
				for i := range node.Exprs {
					node.Exprs[i].As = nil
				}
			case *tree.AliasedTableExpr:
				// Column references have already been resolved to binding tags.
				// The declaration spelling of a local table alias is therefore
				// non-semantic and must not affect the equality key.
				node.As = tree.AliasClause{}
			}
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
	isolatedParent := cloneMedianBindScope(parentCtx, tempBuilder)
	subCtx := NewBindContext(tempBuilder, isolatedParent)
	var err error
	var selectNode *tree.Select
	switch subquery := node.Select.(type) {
	case *tree.Select:
		selectNode = subquery
		_, err = tempBuilder.bindSelect(subquery, subCtx, false)
	case *tree.ParenSelect:
		selectNode = subquery.Select
		_, err = tempBuilder.bindSelect(subquery.Select, subCtx, false)
	}
	if err != nil {
		*valid = false
		return
	}
	canonicalizeMedianSelectCTEs(
		selectNode, nil, "root", make(map[treeClonePointer]struct{}),
	)
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

func cloneMedianMap[K comparable, V any](values map[K]V) map[K]V {
	if values == nil {
		return nil
	}
	cloned := make(map[K]V, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneMedianSlice[V any](values []V) []V {
	if values == nil {
		return nil
	}
	return append([]V(nil), values...)
}

func cloneMedianBinder(original Binder, ctx *BindContext, builder *QueryBuilder) Binder {
	if original == nil {
		return nil
	}
	value := reflect.ValueOf(original)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return nil
	}
	clonedValue := reflect.New(value.Elem().Type())
	clonedValue.Elem().Set(value.Elem())
	cloned, ok := clonedValue.Interface().(Binder)
	if !ok {
		return nil
	}
	provider, ok := cloned.(interface{ medianValidationBaseBinder() *baseBinder })
	if !ok {
		return nil
	}
	base := provider.medianValidationBaseBinder()
	base.builder = builder
	base.ctx = ctx
	base.impl = cloned
	base.boundCols = cloneMedianSlice(base.boundCols)
	return cloned
}

// cloneMedianBindScope detaches every parent query block reachable during the
// validation bind. Containers and binders are copied before the temporary
// subquery is bound, so correlation, CTE/view bookkeeping, time-boundary
// resolution, and full-group-by checks cannot mutate the real plan state.
func cloneMedianBindScope(root *BindContext, builder *QueryBuilder) *BindContext {
	contexts := make(map[*BindContext]*BindContext)
	ctes := make(map[*CTERef]*CTERef)

	var cloneContext func(*BindContext) *BindContext
	var cloneCTE func(*CTERef) *CTERef
	cloneCTE = func(original *CTERef) *CTERef {
		if original == nil {
			return nil
		}
		if cloned, ok := ctes[original]; ok {
			return cloned
		}
		cloned := *original
		ctes[original] = &cloned
		if original.ast != nil {
			cloned.ast = cloneTreeValue(
				reflect.ValueOf(original.ast),
				make(map[treeClonePointer]reflect.Value),
			).Interface().(*tree.CTE)
		}
		cloned.maskedCTEs = cloneMedianMap(original.maskedCTEs)
		cloned.occurrences = cloneMedianSlice(original.occurrences)
		for i := range cloned.occurrences {
			cloned.occurrences[i].ctx = cloneContext(original.occurrences[i].ctx)
			cloned.occurrences[i].headings = cloneMedianSlice(original.occurrences[i].headings)
			cloned.occurrences[i].types = cloneMedianSlice(original.occurrences[i].types)
		}
		cloned.declarationCtx = cloneContext(original.declarationCtx)
		return &cloned
	}
	cloneContext = func(original *BindContext) *BindContext {
		if original == nil {
			return nil
		}
		if cloned, ok := contexts[original]; ok {
			return cloned
		}
		cloned := *original
		contexts[original] = &cloned

		cloned.outputColumnProvenance = cloneMedianMap(original.outputColumnProvenance)
		cloned.mysqlSpecialOrderTypes = cloneMedianMap(original.mysqlSpecialOrderTypes)
		cloned.mysqlSpecialCanonicalTypes = cloneMedianMap(original.mysqlSpecialCanonicalTypes)
		cloned.mysqlSpecialRawProjectPositions = cloneMedianMap(original.mysqlSpecialRawProjectPositions)
		cloned.groupingSetOrderAliases = cloneMedianMap(original.groupingSetOrderAliases)
		for key, exprs := range cloned.groupingSetOrderAliases {
			clonedExprs := make([]tree.Expr, len(exprs))
			for i, expr := range exprs {
				clonedExprs[i] = cloneTreeExpr(expr)
			}
			cloned.groupingSetOrderAliases[key] = clonedExprs
		}
		cloned.groupingSetOrderSourceProbes = cloneMedianMap(original.groupingSetOrderSourceProbes)
		for key, probe := range cloned.groupingSetOrderSourceProbes {
			if probe != nil {
				clonedProbe := *probe
				cloned.groupingSetOrderSourceProbes[key] = &clonedProbe
			}
		}
		cloned.headings = cloneMedianSlice(original.headings)
		cloned.expandedSelectLists = cloneMedianMap(original.expandedSelectLists)
		for key, exprs := range cloned.expandedSelectLists {
			cloned.expandedSelectLists[key] = cloneMedianSlice(exprs)
		}
		cloned.groups = cloneMedianSlice(original.groups)
		cloned.aggregates = cloneMedianSlice(original.aggregates)
		cloned.projects = cloneMedianSlice(original.projects)
		cloned.results = cloneMedianSlice(original.results)
		cloned.windows = cloneMedianSlice(original.windows)
		cloned.times = cloneMedianSlice(original.times)
		cloned.groupByAst = cloneMedianMap(original.groupByAst)
		cloned.groupByCanonicalAst = cloneMedianMap(original.groupByCanonicalAst)
		cloned.groupByParamAst = cloneMedianMap(original.groupByParamAst)
		cloned.aggregateByAst = cloneMedianMap(original.aggregateByAst)
		cloned.sampleByAst = cloneMedianMap(original.sampleByAst)
		cloned.windowByAst = cloneMedianMap(original.windowByAst)
		cloned.projectByExpr = cloneMedianMap(original.projectByExpr)
		cloned.timeByAst = cloneMedianMap(original.timeByAst)
		cloned.whereFilters = cloneMedianSlice(original.whereFilters)
		cloned.flattenedVolatileExprs = cloneMedianMap(original.flattenedVolatileExprs)
		cloned.gapFillWhereFilters = cloneMedianSlice(original.gapFillWhereFilters)
		cloned.projectColByAst = cloneMedianMap(original.projectColByAst)
		cloned.projectByAst = cloneMedianSlice(original.projectByAst)
		cloned.projectSemanticKeys = cloneMedianSlice(original.projectSemanticKeys)
		cloned.numericProjectionTypes = cloneMedianSlice(original.numericProjectionTypes)
		cloned.numericTableProjectionTypes = cloneMedianMap(original.numericTableProjectionTypes)
		for key, types := range cloned.numericTableProjectionTypes {
			cloned.numericTableProjectionTypes[key] = cloneMedianSlice(types)
		}
		cloned.numericTableProjectionAmbiguous = cloneMedianMap(original.numericTableProjectionAmbiguous)
		for key, ambiguous := range cloned.numericTableProjectionAmbiguous {
			cloned.numericTableProjectionAmbiguous[key] = cloneMedianSlice(ambiguous)
		}
		cloned.numericCteByName = cloneMedianMap(original.numericCteByName)
		for name, cte := range cloned.numericCteByName {
			if cte != nil {
				cloned.numericCteByName[name] = cloneTreeValue(
					reflect.ValueOf(cte),
					make(map[treeClonePointer]reflect.Value),
				).Interface().(*tree.CTE)
			}
		}
		cloned.timeAsts = cloneMedianSlice(original.timeAsts)
		cloned.aliasMap = make(map[string]*aliasItem, len(original.aliasMap))
		for name, item := range original.aliasMap {
			if item == nil {
				cloned.aliasMap[name] = nil
				continue
			}
			clonedItem := *item
			clonedItem.astExpr = cloneTreeExpr(item.astExpr)
			cloned.aliasMap[name] = &clonedItem
		}
		cloned.aliasFrequency = cloneMedianMap(original.aliasFrequency)
		cloned.bindings = cloneMedianSlice(original.bindings)
		cloned.bindingByTag = cloneMedianMap(original.bindingByTag)
		cloned.bindingByTable = cloneMedianMap(original.bindingByTable)
		cloned.bindingByCol = cloneMedianMap(original.bindingByCol)
		cloned.outerUsingCols = cloneMedianMap(original.outerUsingCols)
		for key, cols := range cloned.outerUsingCols {
			cloned.outerUsingCols[key] = cloneMedianSlice(cols)
		}
		cloned.sqlUdfArgs = cloneMedianMap(original.sqlUdfArgs)
		cloned.sampleFunc.columns = cloneMedianSlice(original.sampleFunc.columns)
		cloned.views = cloneMedianSlice(original.views)
		cloned.boundViews = cloneMedianMap(original.boundViews)
		for key, view := range cloned.boundViews {
			if view != nil {
				cloned.boundViews[key] = cloneTreeValue(
					reflect.ValueOf(view),
					make(map[treeClonePointer]reflect.Value),
				).Interface().(*tree.CreateView)
			}
		}
		cloned.viewChain = cloneMedianSlice(original.viewChain)
		cloned.groupingFlag = cloneMedianSlice(original.groupingFlag)
		if original.orderResolution != nil {
			orderResolution := *original.orderResolution
			orderResolution.bindAsts = cloneMedianSlice(original.orderResolution.bindAsts)
			orderResolution.semanticKeysByTag = cloneMedianMap(original.orderResolution.semanticKeysByTag)
			for key, values := range orderResolution.semanticKeysByTag {
				orderResolution.semanticKeysByTag[key] = cloneMedianSlice(values)
			}
			cloned.orderResolution = &orderResolution
		}

		cloned.parent = cloneContext(original.parent)
		cloned.queryBlockOwner = cloneContext(original.queryBlockOwner)
		cloned.aggregateInputParent = cloneContext(original.aggregateInputParent)
		cloned.cteByName = make(map[string]*CTERef, len(original.cteByName))
		for name, cte := range original.cteByName {
			cloned.cteByName[name] = cloneCTE(cte)
		}
		cloned.boundCtes = make(map[string]*CTERef, len(original.boundCtes))
		for name, cte := range original.boundCtes {
			cloned.boundCtes[name] = cloneCTE(cte)
		}
		cloned.cteState.cte = cloneCTE(original.cteState.cte)
		cloned.cteState.recursiveRefQueryBlock = cloneContext(original.cteState.recursiveRefQueryBlock)
		cloned.binder = cloneMedianBinder(original.binder, &cloned, builder)
		return &cloned
	}
	return cloneContext(root)
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
