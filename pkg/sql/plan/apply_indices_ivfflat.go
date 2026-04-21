// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

type ivfIndexContext struct {
	vecCtx          *vectorSortContext
	metaDef         *plan.IndexDef
	idxDef          *plan.IndexDef
	entriesDef      *plan.IndexDef
	vecLitArg       *plan.Expr
	origFuncName    string
	partPos         int32
	partType        plan.Type
	pkPos           int32
	pkType          plan.Type
	params          string
	nThread         int64
	nProbe          int64
	pushdownEnabled bool

	// Auto mode support.
	isAutoMode      bool   // Whether in auto mode
	initialStrategy string // Initial strategy selected in auto mode ("pre" or "post")
}

// shouldUseForceMode determines if force mode (full table scan) should be used
// based on table size and LIMIT value.
//
// Rule: Use force mode when table_rows < LIMIT × 2
//
// Rationale:
//   - For very small datasets, index overhead (metadata reading, distance calculation)
//     exceeds the benefit of using an index
//   - Full table scan is faster and guarantees 100% recall
//   - Does not rely on filter selectivity estimation (which may be inaccurate)
//
// Returns true if force mode should be used, false otherwise.
func (builder *QueryBuilder) shouldUseForceMode(vecCtx *vectorSortContext) bool {
	scanNode := vecCtx.scanNode
	stats := scanNode.Stats

	// Get table row count and selectivity from statistics
	var tableCnt float64
	var selectivity float64 = 1.0

	if stats != nil {
		tableCnt = stats.TableCnt
		if stats.Selectivity > 0 && stats.Selectivity < 1 {
			selectivity = stats.Selectivity
		}
	}

	// If no statistics available, conservatively use post mode
	if tableCnt <= 0 {
		return false
	}

	// Get LIMIT value
	limitExpr := vecCtx.limit
	if limitExpr == nil {
		return false
	}

	limitConst := limitExpr.GetLit()
	if limitConst == nil {
		return false
	}

	limitVal := float64(limitConst.GetU64Val())
	if limitVal <= 0 {
		return false
	}

	// Rule: Estimated rows after filtering < LIMIT × 2
	// For small result sets, brute force (force mode) is more reliable and often faster
	estimatedRows := tableCnt * selectivity
	threshold := limitVal * 2.0

	if tableCnt < threshold || estimatedRows < threshold {
		logutil.Debugf(
			"Auto mode: small dataset or high selectivity detected, table_rows=%.0f, selectivity=%.4f, estimated_rows=%.0f, limit=%.0f, threshold=%.0f",
			tableCnt, selectivity, estimatedRows, limitVal, threshold,
		)
		return true
	}

	return false
}

// resolveVectorSearchMode resolves the vector search mode based on user input and configuration.
// Returns:
//   - mode: The actual mode to use ("pre", "post", or "force")
//   - isAutoMode: Whether auto mode is enabled
//   - shouldDisableIndex: Whether vector index should be disabled
func (builder *QueryBuilder) resolveVectorSearchMode(
	vecCtx *vectorSortContext,
	enableVectorPrefilterByDefault bool,
	enableVectorAutoModeByDefault bool,
) (mode string, isAutoMode bool, shouldDisableIndex bool) {

	// 1. Parse user-specified mode
	var userMode string
	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode != "" {
		userMode = vecCtx.rankOption.Mode
	}

	// 2. Handle force mode: disable vector index
	if userMode == "force" {
		return "force", false, true
	}

	// 3. Handle auto mode
	if userMode == "auto" || (userMode == "" && enableVectorAutoModeByDefault) {
		isAutoMode = true

		// Check if this is a very small dataset.
		if builder.shouldUseForceMode(vecCtx) {
			logutil.Debugf("Auto mode: small dataset, selected 'force'")
			return "force", isAutoMode, true
		}

		// Default to post mode for normal cases
		logutil.Debugf("Auto mode: normal case, selected 'post'")
		mode = "post"
		return mode, isAutoMode, false
	}

	// 4. Handle explicitly specified pre/post mode
	if userMode == "pre" || userMode == "post" {
		return userMode, false, false
	}

	// 5. No mode specified: use default behavior
	if enableVectorPrefilterByDefault {
		mode = "pre"
	} else {
		mode = "post"
	}

	return mode, false, false
}

func (builder *QueryBuilder) calculateAdaptiveNprobe(baseNprobe int64, stats *plan.Stats, totalLists int64) int64 {
	// 1. If no statistics or invalid selectivity, keep as is
	if stats == nil || stats.Selectivity <= 0 || stats.Selectivity >= 1 {
		return baseNprobe
	}

	// 2. Calculate compensation factor (square root smoothing)
	// Square root is used to prevent nprobe from growing too fast and causing excessive overhead
	compensation := math.Sqrt(1.0 / stats.Selectivity)

	// 3. Calculate adaptive nprobe
	adaptiveNprobe := int64(math.Ceil(float64(baseNprobe) * compensation))

	// 4. Boundary handling: not less than base value, not more than total lists
	adaptiveNprobe = max(adaptiveNprobe, baseNprobe)
	adaptiveNprobe = min(adaptiveNprobe, totalLists)

	return adaptiveNprobe
}

var ivfTreeEmptyLocale = ""

func parseIvfIncludeColumns(indexAlgoParams string) ([]string, error) {
	return getIvfIncludeColumnNamesFromParams(indexAlgoParams)
}

func buildIvfTableFuncArgs(tblCfgStr string, vecLitArg *plan.Expr, pushdownFilterSQL string, searchRoundLimit uint64, bucketExpandStep uint64) []*plan.Expr {
	args := []*plan.Expr{
		{
			Typ: plan.Type{
				Id: int32(types.T_varchar),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_Sval{
						Sval: tblCfgStr,
					},
				},
			},
		},
		DeepCopyExpr(vecLitArg),
	}

	if pushdownFilterSQL == "" && searchRoundLimit == 0 && bucketExpandStep == 0 {
		return args
	}

	args = append(args,
		makePlan2StringConstExprWithType(pushdownFilterSQL),
		makePlan2Uint64ConstExprWithType(searchRoundLimit),
		makePlan2Uint64ConstExprWithType(bucketExpandStep),
	)
	return args
}

func buildIvfChildProjectionMap(childNode *plan.Node) map[[2]int32]*plan.Expr {
	if childNode == nil || len(childNode.BindingTags) == 0 {
		return nil
	}

	childMap := make(map[[2]int32]*plan.Expr, len(childNode.ProjectList))
	for i, expr := range childNode.ProjectList {
		childMap[[2]int32{childNode.BindingTags[0], int32(i)}] = DeepCopyExpr(expr)
	}
	return childMap
}

func collectRequiredColumns(
	projNode, childNode, scanNode *plan.Node,
	orderExpr *plan.Expr,
	partPos int32,
) map[string]struct{} {
	required := make(map[string]struct{})
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return required
	}

	scanTag := scanNode.BindingTags[0]
	childMap := buildIvfChildProjectionMap(childNode)

	if projNode != nil {
		for _, expr := range projNode.ProjectList {
			resolved := replaceColumnsForExpr(DeepCopyExpr(expr), childMap)
			collectScanColumnsFromExpr(resolved, scanTag, partPos, scanNode.TableDef, required)
		}
	}

	if orderExpr != nil {
		resolved := replaceColumnsForExpr(DeepCopyExpr(orderExpr), childMap)
		collectScanColumnsFromExpr(resolved, scanTag, partPos, scanNode.TableDef, required)
	}

	for _, expr := range scanNode.FilterList {
		collectScanColumnsFromExpr(DeepCopyExpr(expr), scanTag, partPos, scanNode.TableDef, required)
	}

	return required
}

func collectProjectedColumns(
	projNode, childNode, scanNode *plan.Node,
	partPos int32,
) map[string]struct{} {
	projected := make(map[string]struct{})
	if projNode == nil || scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return projected
	}

	scanTag := scanNode.BindingTags[0]
	childMap := buildIvfChildProjectionMap(childNode)
	for _, expr := range projNode.ProjectList {
		resolved := replaceColumnsForExpr(DeepCopyExpr(expr), childMap)
		collectScanColumnsFromExpr(resolved, scanTag, partPos, scanNode.TableDef, projected)
	}
	return projected
}

func collectScanColumnsFromExpr(expr *plan.Expr, scanTag, partPos int32, tableDef *plan.TableDef, out map[string]struct{}) {
	if expr == nil || tableDef == nil {
		return
	}

	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos != scanTag {
			return
		}
		if int(impl.Col.ColPos) >= len(tableDef.Cols) {
			return
		}
		out[tableDef.Cols[impl.Col.ColPos].Name] = struct{}{}
	case *plan.Expr_F:
		if isIvfDistanceExpr(expr, scanTag, partPos) {
			return
		}
		for _, arg := range impl.F.Args {
			collectScanColumnsFromExpr(arg, scanTag, partPos, tableDef, out)
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			collectScanColumnsFromExpr(sub, scanTag, partPos, tableDef, out)
		}
	}
}

func isIvfDistanceExpr(expr *plan.Expr, scanTag, partPos int32) bool {
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if _, ok := metric.DistFuncOpTypes[fn.Func.ObjName]; !ok {
		return false
	}
	if len(fn.Args) != 2 {
		return false
	}

	for _, arg := range fn.Args {
		col := arg.GetCol()
		if col != nil && col.RelPos == scanTag && col.ColPos == partPos {
			return true
		}
	}
	return false
}

func canDoIndexOnlyScan(requiredCols map[string]struct{}, tableDef *plan.TableDef, includeColumns []string) bool {
	if tableDef == nil || tableDef.Pkey == nil {
		return false
	}

	covered := make(map[string]struct{}, len(includeColumns)+1)
	for _, col := range includeColumns {
		covered[col] = struct{}{}
	}
	if len(tableDef.Pkey.Names) == 1 {
		covered[tableDef.Pkey.PkeyColName] = struct{}{}
	}

	for col := range requiredCols {
		if _, ok := covered[col]; !ok {
			return false
		}
	}
	return true
}

func splitFiltersByIncludeColumns(
	filters []*plan.Expr,
	scanNode *plan.Node,
	includeColumns []string,
	partPos int32,
) (pushdownFilters, remainingFilters []*plan.Expr) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil, filters
	}

	covered := make(map[string]struct{}, len(includeColumns)+1)
	for _, col := range includeColumns {
		covered[col] = struct{}{}
	}
	if scanNode.TableDef.Pkey != nil && len(scanNode.TableDef.Pkey.Names) == 1 {
		covered[scanNode.TableDef.Pkey.PkeyColName] = struct{}{}
	}

	scanTag := scanNode.BindingTags[0]
	for _, expr := range filters {
		if exprRefsOnlyCoveredColumns(expr, scanTag, partPos, scanNode.TableDef, covered) {
			pushdownFilters = append(pushdownFilters, expr)
		} else {
			remainingFilters = append(remainingFilters, expr)
		}
	}
	return pushdownFilters, remainingFilters
}

func exprRefsOnlyCoveredColumns(expr *plan.Expr, scanTag, partPos int32, tableDef *plan.TableDef, covered map[string]struct{}) bool {
	if expr == nil {
		return true
	}

	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos != scanTag {
			return false
		}
		if int(impl.Col.ColPos) >= len(tableDef.Cols) {
			return false
		}
		colName := tableDef.Cols[impl.Col.ColPos].Name
		_, ok := covered[colName]
		return ok
	case *plan.Expr_F:
		if isIvfDistanceExpr(expr, scanTag, partPos) {
			return false
		}
		for _, arg := range impl.F.Args {
			if !exprRefsOnlyCoveredColumns(arg, scanTag, partPos, tableDef, covered) {
				return false
			}
		}
		return true
	case *plan.Expr_Lit:
		return true
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !exprRefsOnlyCoveredColumns(sub, scanTag, partPos, tableDef, covered) {
				return false
			}
		}
		return true
	case *plan.Expr_T:
		return true
	default:
		return false
	}
}

func serializeFiltersToSQL(filters []*plan.Expr, scanNode *plan.Node, includeColumns []string, partPos int32) (string, []*plan.Expr, []*plan.Expr, error) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return "", nil, filters, nil
	}

	covered := make(map[string]struct{}, len(includeColumns)+1)
	for _, col := range includeColumns {
		covered[col] = struct{}{}
	}
	if scanNode.TableDef.Pkey != nil && len(scanNode.TableDef.Pkey.Names) == 1 {
		covered[scanNode.TableDef.Pkey.PkeyColName] = struct{}{}
	}

	scanTag := scanNode.BindingTags[0]
	sqlParts := make([]string, 0, len(filters))
	pushdownFilters := make([]*plan.Expr, 0, len(filters))
	remainingFilters := make([]*plan.Expr, 0, len(filters))
	for _, expr := range filters {
		astExpr, ok, err := serializeFilterExprToAST(expr, scanNode, scanTag, partPos, covered)
		if err != nil {
			return "", nil, nil, err
		}
		if !ok {
			remainingFilters = append(remainingFilters, expr)
			continue
		}
		sql := tree.StringWithOpts(astExpr, dialect.MYSQL, tree.WithQuoteString(true))
		sqlParts = append(sqlParts, "("+sql+")")
		pushdownFilters = append(pushdownFilters, expr)
	}

	return strings.Join(sqlParts, " AND "), pushdownFilters, remainingFilters, nil
}

func serializeFilterExprToAST(expr *plan.Expr, scanNode *plan.Node, scanTag, partPos int32, covered map[string]struct{}) (tree.Expr, bool, error) {
	if !exprRefsOnlyCoveredColumns(expr, scanTag, partPos, scanNode.TableDef, covered) {
		return nil, false, nil
	}

	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colExpr, err := ivfFilterColumnToAST(impl.Col, scanNode)
		if err != nil {
			return nil, false, err
		}
		return colExpr, true, nil
	case *plan.Expr_Lit:
		litExpr, err := ivfLiteralToAST(impl.Lit, expr.Typ)
		if err != nil {
			return nil, false, err
		}
		return litExpr, true, nil
	case *plan.Expr_List:
		items, ok, err := serializeFilterExprListToAST(impl.List.List, scanNode, scanTag, partPos, covered)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		return tree.NewTuple(items), true, nil
	case *plan.Expr_F:
		return serializeFilterFuncToAST(expr, impl.F, scanNode, scanTag, partPos, covered)
	default:
		return nil, false, nil
	}
}

func serializeFilterExprListToAST(exprs []*plan.Expr, scanNode *plan.Node, scanTag, partPos int32, covered map[string]struct{}) (tree.Exprs, bool, error) {
	items := make(tree.Exprs, 0, len(exprs))
	for _, expr := range exprs {
		item, ok, err := serializeFilterExprToAST(expr, scanNode, scanTag, partPos, covered)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		items = append(items, item)
	}
	return items, true, nil
}

func serializeFilterFuncToAST(parentExpr *plan.Expr, fn *plan.Function, scanNode *plan.Node, scanTag, partPos int32, covered map[string]struct{}) (tree.Expr, bool, error) {
	if fn == nil || fn.Func == nil || fn.Func.ObjName == "" {
		return nil, false, nil
	}

	fnName := strings.ToLower(fn.Func.ObjName)
	args := fn.Args

	switch fnName {
	case "and", "or", "xor":
		if len(args) != 2 {
			return nil, false, nil
		}
		left, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		right, ok, err := serializeFilterExprToAST(args[1], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		switch fnName {
		case "and":
			return tree.NewAndExpr(left, right), true, nil
		case "or":
			return tree.NewOrExpr(left, right), true, nil
		default:
			return tree.NewXorExpr(left, right), true, nil
		}
	case "not":
		if len(args) != 1 {
			return nil, false, nil
		}
		subExpr, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		return tree.NewNotExpr(subExpr), true, nil
	case "=", "!=", "<>", "<", "<=", ">", ">=", "like", "not_like", "ilike", "not_ilike", "reg_match", "not_reg_match", "<=>":
		if len(args) != 2 {
			return nil, false, nil
		}
		left, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		right, ok, err := serializeFilterExprToAST(args[1], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		op, ok := ivfComparisonOpForFunc(fnName)
		if !ok {
			return nil, false, nil
		}
		return tree.NewComparisonExpr(op, left, right), true, nil
	case "between":
		if len(args) != 3 {
			return nil, false, nil
		}
		target, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		fromExpr, ok, err := serializeFilterExprToAST(args[1], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		toExpr, ok, err := serializeFilterExprToAST(args[2], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		return tree.NewRangeCond(false, target, fromExpr, toExpr), true, nil
	case "in", "not_in", "partition_in":
		if len(args) < 2 {
			return nil, false, nil
		}
		target, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		listExpr, ok, err := serializeInListToAST(args[1:], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		op := tree.IN
		if fnName == "not_in" {
			op = tree.NOT_IN
		}
		return tree.NewComparisonExpr(op, target, listExpr), true, nil
	case "isnull", "is_null":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsNullExpr(expr)
		})
	case "isnotnull", "is_not_null":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsNotNullExpr(expr)
		})
	case "isunknown", "is_unknown":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsUnknownExpr(expr)
		})
	case "isnotunknown", "is_not_unknown":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsNotUnknownExpr(expr)
		})
	case "istrue", "is_true":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsTrueExpr(expr)
		})
	case "isnottrue", "is_not_true":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsNotTrueExpr(expr)
		})
	case "isfalse", "is_false":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsFalseExpr(expr)
		})
	case "isnotfalse", "is_not_false":
		return serializeFilterIsExprToAST(args, scanNode, scanTag, partPos, covered, func(expr tree.Expr) tree.Expr {
			return tree.NewIsNotFalseExpr(expr)
		})
	case "+", "-", "*", "/", "div", "%", "&", "|", "^", "<<", ">>":
		if len(args) != 2 {
			return nil, false, nil
		}
		left, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		right, ok, err := serializeFilterExprToAST(args[1], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		op, ok := ivfBinaryOpForFunc(fnName)
		if !ok {
			return nil, false, nil
		}
		return tree.NewBinaryExpr(op, left, right), true, nil
	case "unary_minus", "unary_plus", "unary_tilde", "unary_mark":
		if len(args) != 1 {
			return nil, false, nil
		}
		child, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		op, ok := ivfUnaryOpForFunc(fnName)
		if !ok {
			return nil, false, nil
		}
		return tree.NewUnaryExpr(op, child), true, nil
	case "cast":
		if len(args) < 1 {
			return nil, false, nil
		}
		child, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		targetType := parentExpr.Typ
		if len(args) > 1 && !args[1].Typ.IsEmpty() {
			targetType = args[1].Typ
		}
		treeType, err := ivfPlanTypeToTreeType(targetType)
		if err != nil {
			return nil, false, err
		}
		return tree.NewCastExpr(child, treeType), true, nil
	case "case":
		return serializeFilterCaseExprToAST(args, scanNode, scanTag, partPos, covered)
	default:
		if !ivfCanFormatAsFunctionName(fn.Func.ObjName) {
			return nil, false, nil
		}
		astArgs, ok, err := serializeFilterExprListToAST(args, scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		return &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(fn.Func.ObjName)),
			Type:  tree.FUNC_TYPE_DEFAULT,
			Exprs: astArgs,
		}, true, nil
	}
}

func serializeFilterIsExprToAST(args []*plan.Expr, scanNode *plan.Node, scanTag, partPos int32, covered map[string]struct{}, build func(tree.Expr) tree.Expr) (tree.Expr, bool, error) {
	if len(args) != 1 {
		return nil, false, nil
	}
	target, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
	if err != nil || !ok {
		return nil, ok, err
	}
	return build(target), true, nil
}

func serializeInListToAST(args []*plan.Expr, scanNode *plan.Node, scanTag, partPos int32, covered map[string]struct{}) (tree.Expr, bool, error) {
	if len(args) == 1 {
		listExpr, ok, err := serializeFilterExprToAST(args[0], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		if _, ok := listExpr.(*tree.Tuple); !ok {
			return nil, false, nil
		}
		return listExpr, true, nil
	}

	items, ok, err := serializeFilterExprListToAST(args, scanNode, scanTag, partPos, covered)
	if err != nil || !ok {
		return nil, ok, err
	}
	return tree.NewTuple(items), true, nil
}

func serializeFilterCaseExprToAST(args []*plan.Expr, scanNode *plan.Node, scanTag, partPos int32, covered map[string]struct{}) (tree.Expr, bool, error) {
	if len(args) == 0 || len(args)%2 == 0 {
		return nil, false, nil
	}

	whens := make([]*tree.When, 0, len(args)/2)
	for i := 0; i < len(args)-1; i += 2 {
		cond, ok, err := serializeFilterExprToAST(args[i], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		val, ok, err := serializeFilterExprToAST(args[i+1], scanNode, scanTag, partPos, covered)
		if err != nil || !ok {
			return nil, ok, err
		}
		whens = append(whens, tree.NewWhen(cond, val))
	}

	elseExpr, ok, err := serializeFilterExprToAST(args[len(args)-1], scanNode, scanTag, partPos, covered)
	if err != nil || !ok {
		return nil, ok, err
	}
	return tree.NewCaseExpr(nil, whens, elseExpr), true, nil
}

func ivfComparisonOpForFunc(fnName string) (tree.ComparisonOp, bool) {
	switch fnName {
	case "=":
		return tree.EQUAL, true
	case "!=", "<>":
		return tree.NOT_EQUAL, true
	case "<":
		return tree.LESS_THAN, true
	case "<=":
		return tree.LESS_THAN_EQUAL, true
	case ">":
		return tree.GREAT_THAN, true
	case ">=":
		return tree.GREAT_THAN_EQUAL, true
	case "like":
		return tree.LIKE, true
	case "not_like":
		return tree.NOT_LIKE, true
	case "ilike":
		return tree.ILIKE, true
	case "not_ilike":
		return tree.NOT_ILIKE, true
	case "reg_match":
		return tree.REG_MATCH, true
	case "not_reg_match":
		return tree.NOT_REG_MATCH, true
	case "<=>":
		return tree.NULL_SAFE_EQUAL, true
	default:
		return tree.ComparisonOp(0), false
	}
}

func ivfBinaryOpForFunc(fnName string) (tree.BinaryOp, bool) {
	switch fnName {
	case "+":
		return tree.PLUS, true
	case "-":
		return tree.MINUS, true
	case "*":
		return tree.MULTI, true
	case "/":
		return tree.DIV, true
	case "div":
		return tree.INTEGER_DIV, true
	case "%":
		return tree.MOD, true
	case "&":
		return tree.BIT_AND, true
	case "|":
		return tree.BIT_OR, true
	case "^":
		return tree.BIT_XOR, true
	case "<<":
		return tree.LEFT_SHIFT, true
	case ">>":
		return tree.RIGHT_SHIFT, true
	default:
		return tree.BinaryOp(0), false
	}
}

func ivfUnaryOpForFunc(fnName string) (tree.UnaryOp, bool) {
	switch fnName {
	case "unary_minus":
		return tree.UNARY_MINUS, true
	case "unary_plus":
		return tree.UNARY_PLUS, true
	case "unary_tilde":
		return tree.UNARY_TILDE, true
	case "unary_mark":
		return tree.UNARY_MARK, true
	default:
		return tree.UnaryOp(0), false
	}
}

func ivfCanFormatAsFunctionName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if r == '_' || (r >= '0' && r <= '9' && i > 0) || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		return false
	}
	return true
}

func ivfFilterColumnToAST(col *plan.ColRef, scanNode *plan.Node) (tree.Expr, error) {
	if scanNode == nil || scanNode.TableDef == nil || scanNode.TableDef.Pkey == nil {
		return nil, moerr.NewInternalErrorNoCtx("invalid ivf filter scan node")
	}
	if int(col.ColPos) >= len(scanNode.TableDef.Cols) {
		return nil, moerr.NewInternalErrorNoCtx("invalid ivf filter column position")
	}

	colName := scanNode.TableDef.Cols[col.ColPos].Name
	mappedName := catalog.SystemSI_IVFFLAT_IncludeColPrefix + colName
	if len(scanNode.TableDef.Pkey.Names) == 1 && colName == scanNode.TableDef.Pkey.PkeyColName {
		mappedName = catalog.SystemSI_IVFFLAT_TblCol_Entries_pk
	}

	colExpr := tree.NewUnresolvedColName(mappedName)
	colExpr.CStrParts[0] = tree.NewCStr("`"+mappedName+"`", 0)
	return colExpr, nil
}

func ivfLiteralToAST(lit *plan.Literal, typ plan.Type) (tree.Expr, error) {
	if lit == nil || lit.Isnull {
		return tree.NewNumVal("", "", false, tree.P_null), nil
	}

	switch v := lit.Value.(type) {
	case *plan.Literal_I8Val:
		val := int64(int8(v.I8Val))
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_int64), nil
	case *plan.Literal_I16Val:
		val := int64(int16(v.I16Val))
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_int64), nil
	case *plan.Literal_I32Val:
		val := int64(v.I32Val)
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_int64), nil
	case *plan.Literal_I64Val:
		return tree.NewNumVal(v.I64Val, fmt.Sprintf("%d", v.I64Val), false, tree.P_int64), nil
	case *plan.Literal_U8Val:
		val := uint64(uint8(v.U8Val))
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_uint64), nil
	case *plan.Literal_U16Val:
		val := uint64(uint16(v.U16Val))
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_uint64), nil
	case *plan.Literal_U32Val:
		val := uint64(v.U32Val)
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_uint64), nil
	case *plan.Literal_U64Val:
		return tree.NewNumVal(v.U64Val, fmt.Sprintf("%d", v.U64Val), false, tree.P_uint64), nil
	case *plan.Literal_Fval:
		val := float64(v.Fval)
		return tree.NewNumVal(val, fmt.Sprintf("%g", val), false, tree.P_float64), nil
	case *plan.Literal_Dval:
		return tree.NewNumVal(v.Dval, fmt.Sprintf("%g", v.Dval), false, tree.P_float64), nil
	case *plan.Literal_Sval:
		return tree.NewNumVal(v.Sval, v.Sval, false, tree.P_char), nil
	case *plan.Literal_Bval:
		return tree.NewNumVal(v.Bval, "", false, tree.P_bool), nil
	case *plan.Literal_Dateval:
		str := types.Date(v.Dateval).String()
		return tree.NewNumVal(str, str, false, tree.P_char), nil
	case *plan.Literal_Timeval:
		str := types.Time(v.Timeval).String2(typ.Scale)
		return tree.NewNumVal(str, str, false, tree.P_char), nil
	case *plan.Literal_Datetimeval:
		str := types.Datetime(v.Datetimeval).String2(typ.Scale)
		return tree.NewNumVal(str, str, false, tree.P_char), nil
	case *plan.Literal_Timestampval:
		str := types.Timestamp(v.Timestampval).String2(time.UTC, typ.Scale)
		return tree.NewNumVal(str, str, false, tree.P_char), nil
	case *plan.Literal_Decimal64Val:
		str := types.Decimal64(v.Decimal64Val.A).Format(typ.Scale)
		return tree.NewNumVal(str, str, false, tree.P_decimal), nil
	case *plan.Literal_Decimal128Val:
		str := types.Decimal128{
			B0_63:   uint64(v.Decimal128Val.A),
			B64_127: uint64(v.Decimal128Val.B),
		}.Format(typ.Scale)
		return tree.NewNumVal(str, str, false, tree.P_decimal), nil
	case *plan.Literal_Jsonval:
		str := string(v.Jsonval)
		return tree.NewNumVal(str, str, false, tree.P_char), nil
	case *plan.Literal_EnumVal:
		val := uint64(v.EnumVal)
		return tree.NewNumVal(val, fmt.Sprintf("%d", val), false, tree.P_uint64), nil
	case *plan.Literal_VecVal:
		return tree.NewNumVal(v.VecVal, v.VecVal, false, tree.P_char), nil
	default:
		return nil, moerr.NewNotSupportedNoCtx("unsupported ivf filter literal")
	}
}

func ivfPlanTypeToTreeType(typ plan.Type) (*tree.T, error) {
	treeType := &tree.T{
		InternalType: tree.InternalType{
			Locale: &ivfTreeEmptyLocale,
		},
	}

	switch types.T(typ.Id) {
	case types.T_bool:
		treeType.InternalType.Family = tree.BoolFamily
		treeType.InternalType.FamilyString = "bool"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_BOOL)
	case types.T_bit:
		treeType.InternalType.Family = tree.BitFamily
		treeType.InternalType.FamilyString = "bit"
		treeType.InternalType.DisplayWith = max(typ.Width, int32(1))
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_BIT)
	case types.T_int8:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "tinyint"
		treeType.InternalType.Width = 8
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_TINY)
	case types.T_uint8:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "tinyint"
		treeType.InternalType.Width = 8
		treeType.InternalType.Unsigned = true
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_TINY)
	case types.T_int16:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "smallint"
		treeType.InternalType.Width = 16
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_SHORT)
	case types.T_uint16:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "smallint"
		treeType.InternalType.Width = 16
		treeType.InternalType.Unsigned = true
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_SHORT)
	case types.T_int32:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "int"
		treeType.InternalType.Width = 32
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_LONG)
	case types.T_uint32:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "int"
		treeType.InternalType.Width = 32
		treeType.InternalType.Unsigned = true
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_LONG)
	case types.T_int64:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "bigint"
		treeType.InternalType.Width = 64
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_LONGLONG)
	case types.T_uint64:
		treeType.InternalType.Family = tree.IntFamily
		treeType.InternalType.FamilyString = "bigint"
		treeType.InternalType.Width = 64
		treeType.InternalType.Unsigned = true
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_LONGLONG)
	case types.T_float32:
		treeType.InternalType.Family = tree.FloatFamily
		treeType.InternalType.FamilyString = "float"
		treeType.InternalType.Width = 32
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_FLOAT)
	case types.T_float64:
		treeType.InternalType.Family = tree.FloatFamily
		treeType.InternalType.FamilyString = "double"
		treeType.InternalType.Width = 64
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_DOUBLE)
	case types.T_decimal64, types.T_decimal128:
		treeType.InternalType.Family = tree.FloatFamily
		treeType.InternalType.FamilyString = "decimal"
		treeType.InternalType.DisplayWith = max(typ.Width, int32(0))
		treeType.InternalType.Scale = max(typ.Scale, int32(0))
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_NEWDECIMAL)
	case types.T_date:
		treeType.InternalType.Family = tree.DateFamily
		treeType.InternalType.FamilyString = "date"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_DATE)
	case types.T_time:
		treeType.InternalType.Family = tree.TimeFamily
		treeType.InternalType.FamilyString = "time"
		treeType.InternalType.DisplayWith = max(typ.Scale, int32(0))
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_TIME)
	case types.T_datetime:
		treeType.InternalType.Family = tree.TimestampFamily
		treeType.InternalType.FamilyString = "datetime"
		treeType.InternalType.DisplayWith = max(typ.Scale, int32(0))
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_DATETIME)
	case types.T_timestamp:
		treeType.InternalType.Family = tree.TimestampFamily
		treeType.InternalType.FamilyString = "timestamp"
		treeType.InternalType.DisplayWith = max(typ.Scale, int32(0))
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_TIMESTAMP)
	case types.T_char:
		treeType.InternalType.Family = tree.StringFamily
		treeType.InternalType.FamilyString = "char"
		treeType.InternalType.DisplayWith = ivfStringTypeDisplayWidth(typ.Width)
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_STRING)
	case types.T_varchar:
		treeType.InternalType.Family = tree.StringFamily
		treeType.InternalType.FamilyString = "varchar"
		treeType.InternalType.DisplayWith = ivfStringTypeDisplayWidth(typ.Width)
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_VARCHAR)
	case types.T_binary:
		treeType.InternalType.Family = tree.StringFamily
		treeType.InternalType.FamilyString = "binary"
		treeType.InternalType.Binary = true
		treeType.InternalType.DisplayWith = ivfStringTypeDisplayWidth(typ.Width)
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_VARCHAR)
	case types.T_varbinary:
		treeType.InternalType.Family = tree.StringFamily
		treeType.InternalType.FamilyString = "varbinary"
		treeType.InternalType.Binary = true
		treeType.InternalType.DisplayWith = ivfStringTypeDisplayWidth(typ.Width)
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_VARCHAR)
	case types.T_text:
		treeType.InternalType.Family = tree.BlobFamily
		treeType.InternalType.FamilyString = "text"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_TEXT)
	case types.T_blob:
		treeType.InternalType.Family = tree.BlobFamily
		treeType.InternalType.FamilyString = "blob"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_BLOB)
	case types.T_json:
		treeType.InternalType.Family = tree.JsonFamily
		treeType.InternalType.FamilyString = "json"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_JSON)
	case types.T_uuid:
		treeType.InternalType.Family = tree.UuidFamily
		treeType.InternalType.FamilyString = "uuid"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_UUID)
	case types.T_enum:
		treeType.InternalType.Family = tree.EnumFamily
		treeType.InternalType.FamilyString = "enum"
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_ENUM)
		if typ.Enumvalues != "" {
			treeType.InternalType.EnumValues = strings.Split(typ.Enumvalues, ",")
		}
	case types.T_array_float32:
		treeType.InternalType.Family = tree.ArrayFamily
		treeType.InternalType.FamilyString = "vecf32"
		treeType.InternalType.DisplayWith = ivfStringTypeDisplayWidth(typ.Width)
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_VARCHAR)
	case types.T_array_float64:
		treeType.InternalType.Family = tree.ArrayFamily
		treeType.InternalType.FamilyString = "vecf64"
		treeType.InternalType.DisplayWith = ivfStringTypeDisplayWidth(typ.Width)
		treeType.InternalType.Oid = uint32(defines.MYSQL_TYPE_VARCHAR)
	default:
		return nil, moerr.NewNotSupportedNoCtxf("unsupported ivf filter cast target type %s", types.T(typ.Id).String())
	}

	return treeType, nil
}

func ivfStringTypeDisplayWidth(width int32) int32 {
	if width > 0 {
		return width
	}
	return -1
}

func buildIvfScanToTableFuncMap(tableFuncTag int32, tableFuncIncludeColumns []string, scanNode *plan.Node) map[[2]int32]*plan.Expr {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil
	}

	scanTag := scanNode.BindingTags[0]
	projMap := make(map[[2]int32]*plan.Expr, len(tableFuncIncludeColumns)+1)
	if scanNode.TableDef.Pkey != nil && len(scanNode.TableDef.Pkey.Names) == 1 {
		pkPos := scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
		projMap[[2]int32{scanTag, pkPos}] = &plan.Expr{
			Typ: scanNode.TableDef.Cols[pkPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
					Name:   "pkid",
				},
			},
		}
	}

	for i, colName := range tableFuncIncludeColumns {
		colPos, ok := scanNode.TableDef.Name2ColIndex[colName]
		if !ok {
			continue
		}
		projMap[[2]int32{scanTag, colPos}] = &plan.Expr{
			Typ: scanNode.TableDef.Cols[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: int32(2 + i),
					Name:   catalog.SystemSI_IVFFLAT_IncludeColPrefix + colName,
				},
			},
		}
	}
	return projMap
}

func firstIvfSearchRoundLimit(limit *plan.Expr, hasFilterPressure bool) uint64 {
	if limit == nil || limit.GetLit() == nil {
		return 0
	}

	originalLimit := limit.GetLit().GetU64Val()
	if !hasFilterPressure {
		return originalLimit
	}

	overFetchFactor := calculateFilteredPostModeOverFetchFactor(originalLimit)
	return max(uint64(float64(originalLimit)*overFetchFactor), originalLimit+10)
}

func (builder *QueryBuilder) prepareIvfIndexContext(vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex) (*ivfIndexContext, error) {
	if vecCtx == nil || multiTableIndex == nil {
		return nil, nil
	}
	if vecCtx.distFnExpr == nil {
		return nil, nil
	}

	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "force" {
		return nil, nil
	}

	rewriteAllowed, err := builder.validateVectorIndexSortRewrite(vecCtx)
	if err != nil || !rewriteAllowed {
		return nil, err
	}

	// Check if vector pre-filter pushdown should be enabled by default
	// This session variable changes the default vector search behavior
	var enableVectorPrefilterByDefault bool
	if val, err := builder.compCtx.ResolveVariable("enable_vector_prefilter_by_default", true, false); err == nil && val != nil {
		if v, ok := val.(int8); ok && v == 1 {
			enableVectorPrefilterByDefault = true
		}
	}

	var enableVectorAutoModeByDefault bool
	if val, err := builder.compCtx.ResolveVariable("enable_vector_auto_mode_by_default", true, false); err == nil && val != nil {
		if v, ok := val.(int8); ok && v == 1 {
			enableVectorAutoModeByDefault = true
		}
	}

	// Resolve vector search mode
	mode, isAutoMode, shouldDisableIndex := builder.resolveVectorSearchMode(
		vecCtx,
		enableVectorPrefilterByDefault,
		enableVectorAutoModeByDefault,
	)

	// If index should be disabled (force mode), return nil
	if shouldDisableIndex {
		return nil, nil
	}

	// Log auto mode activation
	if isAutoMode {
		logutil.Debugf("Vector search auto mode enabled, initial strategy: %s", mode)
	}

	metaDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	idxDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	entriesDef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]
	if metaDef == nil || idxDef == nil || entriesDef == nil {
		return nil, nil
	}

	opTypeAst, err := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamOpType)
	if err != nil {
		return nil, nil
	}
	opType, err := opTypeAst.StrictString()
	if err != nil {
		return nil, nil
	}

	// Get total lists for nprobe boundary handling
	var totalLists int64 = -1
	if listsAst, err2 := sonic.Get([]byte(metaDef.IndexAlgoParams), catalog.IndexAlgoParamLists); err2 == nil {
		if lists, err3 := listsAst.Int64(); err3 == nil {
			totalLists = lists
		}
	}

	origFuncName := vecCtx.distFnExpr.Func.ObjName
	if opType != metric.DistFuncOpTypes[origFuncName] {
		return nil, nil
	}

	keyPart := idxDef.Parts[0]
	partPos := vecCtx.scanNode.TableDef.Name2ColIndex[keyPart]
	_, vecLitArg, found := builder.getArgsFromDistFn(vecCtx.distFnExpr, partPos)
	if !found {
		return nil, nil
	}

	nThread, err := builder.compCtx.ResolveVariable("ivf_threads_search", true, false)
	if err != nil {
		return nil, err
	}

	nProbe := int64(5)
	if nProbeIf, err := builder.compCtx.ResolveVariable("probe_limit", true, false); err != nil {
		return nil, err
	} else if nProbeIf != nil {
		val, ok := nProbeIf.(int64)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("ResolveVariable: probe_limit is not int64")
		}
		nProbe = val
	}

	// Dynamically amplify nprobe for auto mode.
	// Only applied if mode is "post" (pushdown disabled) and totalLists is available
	if isAutoMode && mode == "post" && totalLists > 0 {
		oldNProbe := nProbe
		nProbe = builder.calculateAdaptiveNprobe(
			nProbe,
			vecCtx.scanNode.Stats,
			totalLists,
		)
		if nProbe != oldNProbe {
			logutil.Debugf("Auto mode: adjusted nprobe from %d to %d (selectivity: %.4f)",
				oldNProbe, nProbe, vecCtx.scanNode.Stats.Selectivity)
		}
	}

	pkPos := vecCtx.scanNode.TableDef.Name2ColIndex[vecCtx.scanNode.TableDef.Pkey.PkeyColName]
	pkType := vecCtx.scanNode.TableDef.Cols[pkPos].Typ
	partType := vecCtx.scanNode.TableDef.Cols[partPos].Typ

	return &ivfIndexContext{
		vecCtx:          vecCtx,
		metaDef:         metaDef,
		idxDef:          idxDef,
		entriesDef:      entriesDef,
		vecLitArg:       vecLitArg,
		origFuncName:    origFuncName,
		partPos:         partPos,
		partType:        partType,
		pkPos:           pkPos,
		pkType:          pkType,
		params:          idxDef.IndexAlgoParams,
		nThread:         nThread.(int64),
		nProbe:          nProbe,
		pushdownEnabled: (mode == "pre"),

		// Auto mode fields.
		isAutoMode:      isAutoMode,
		initialStrategy: mode,
	}, nil
}

func (builder *QueryBuilder) getDistRangeFromFilters(filters []*plan.Expr, ivfCtx *ivfIndexContext) ([]*plan.Expr, *plan.DistRange) {
	var distRange *plan.DistRange

	currIdx := 0
	for _, filter := range filters {
		var (
			vecLit string
			fdist  *plan.Function
		)

		f := filter.GetF()
		if f == nil || len(f.Args) != 2 {
			goto NO_RANGE
		}

		fdist = f.Args[0].GetF()
		if fdist == nil || len(fdist.Args) != 2 {
			goto NO_RANGE
		}

		if partCol := fdist.Args[0].GetCol(); partCol == nil || partCol.ColPos != ivfCtx.partPos {
			goto NO_RANGE
		}

		if fdist.Func.ObjName != ivfCtx.origFuncName {
			goto NO_RANGE
		}

		vecLit = fdist.Args[1].GetLit().GetVecVal()
		if vecLit == "" || vecLit != ivfCtx.vecLitArg.GetLit().GetVecVal() {
			goto NO_RANGE
		}

		switch f.Func.ObjName {
		case "<":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.UpperBoundType = plan.BoundType_EXCLUSIVE
			distRange.UpperBound = f.Args[1]

		case "<=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.UpperBoundType = plan.BoundType_INCLUSIVE
			distRange.UpperBound = f.Args[1]

		case ">":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.LowerBoundType = plan.BoundType_EXCLUSIVE
			distRange.LowerBound = f.Args[1]

		case ">=":
			if distRange == nil {
				distRange = &plan.DistRange{}
			}
			distRange.LowerBoundType = plan.BoundType_INCLUSIVE
			distRange.LowerBound = f.Args[1]

		default:
			goto NO_RANGE
		}

		continue

	NO_RANGE:
		filters[currIdx] = filter
		currIdx++
	}

	return filters[:currIdx], distRange
}

func (builder *QueryBuilder) applyIndicesForSortUsingIvfflat(nodeID int32, vecCtx *vectorSortContext, multiTableIndex *MultiTableIndex, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	if vecCtx == nil || vecCtx.sortNode == nil || vecCtx.scanNode == nil {
		return nodeID, nil
	}

	ctx := builder.ctxByNode[nodeID]
	projNode := vecCtx.projNode
	sortNode := vecCtx.sortNode
	scanNode := vecCtx.scanNode
	childNode := vecCtx.childNode
	orderExpr := vecCtx.orderExpr
	limit := vecCtx.limit

	ivfCtx, err := builder.prepareIvfIndexContext(vecCtx, multiTableIndex)
	if err != nil || ivfCtx == nil {
		return nodeID, err
	}

	// Persist the inferred auto mode back to the plan nodes so downstream
	// adaptive-vector-search checks see the same mode consistently.
	if ivfCtx.isAutoMode && (vecCtx.rankOption == nil || vecCtx.rankOption.Mode == "") {
		if vecCtx.rankOption == nil {
			vecCtx.rankOption = &plan.RankOption{}
		}
		vecCtx.rankOption.Mode = "auto"

		// Sync back to nodes
		if sortNode.RankOption == nil {
			sortNode.RankOption = vecCtx.rankOption
		}
		if scanNode.RankOption == nil {
			scanNode.RankOption = vecCtx.rankOption
		}
		if projNode.RankOption == nil {
			projNode.RankOption = vecCtx.rankOption
		}
	}

	newFilterList, distRange := builder.getDistRangeFromFilters(scanNode.FilterList, ivfCtx)
	includeColumns, err := parseIvfIncludeColumns(ivfCtx.entriesDef.IndexAlgoParams)
	if err != nil {
		return nodeID, err
	}
	includeAwareColumns := includeColumns
	if vecCtx.rankOption != nil {
		switch vecCtx.rankOption.Mode {
		case "", "include", "auto":
		default:
			includeAwareColumns = nil
		}
	}
	requiredCols := collectRequiredColumns(projNode, childNode, scanNode, orderExpr, ivfCtx.partPos)
	projectedCols := collectProjectedColumns(projNode, childNode, scanNode, ivfCtx.partPos)
	coveragePushdown, _ := splitFiltersByIncludeColumns(newFilterList, scanNode, includeAwareColumns, ivfCtx.partPos)
	pushdownFilterSQL, serializedPushdownFilters, _, err := serializeFiltersToSQL(coveragePushdown, scanNode, includeAwareColumns, ivfCtx.partPos)
	if err != nil {
		return nodeID, err
	}
	remainingFilters := make([]*plan.Expr, 0, len(newFilterList)-len(serializedPushdownFilters))
	serializedSet := make(map[*plan.Expr]struct{}, len(serializedPushdownFilters))
	for _, expr := range serializedPushdownFilters {
		serializedSet[expr] = struct{}{}
	}
	for _, expr := range newFilterList {
		if _, ok := serializedSet[expr]; ok {
			continue
		}
		remainingFilters = append(remainingFilters, expr)
	}
	canIndexOnly := canDoIndexOnlyScan(requiredCols, scanNode.TableDef, includeAwareColumns) && len(remainingFilters) == 0
	needsOffsetCompensation := len(newFilterList) > 0
	tableFuncIncludeColumns := make([]string, 0, len(includeAwareColumns))
	if canIndexOnly {
		for _, col := range includeAwareColumns {
			if _, ok := projectedCols[col]; ok {
				tableFuncIncludeColumns = append(tableFuncIncludeColumns, col)
			}
		}
	}

	includeColumnTypes := make([]int32, 0, len(includeColumns))
	for _, colName := range includeColumns {
		colIdx, ok := scanNode.TableDef.Name2ColIndex[colName]
		if !ok {
			continue
		}
		includeColumnTypes = append(includeColumnTypes, scanNode.TableDef.Cols[colIdx].Typ.Id)
	}

	tblCfg := vectorindex.IndexTableConfig{
		DbName:             scanNode.ObjRef.SchemaName,
		SrcTable:           scanNode.TableDef.Name,
		MetadataTable:      ivfCtx.metaDef.IndexTableName,
		IndexTable:         ivfCtx.idxDef.IndexTableName,
		ThreadsSearch:      ivfCtx.nThread,
		EntriesTable:       ivfCtx.entriesDef.IndexTableName,
		Nprobe:             uint(ivfCtx.nProbe),
		PKeyType:           ivfCtx.pkType.Id,
		PKey:               scanNode.TableDef.Pkey.PkeyColName,
		KeyPart:            ivfCtx.idxDef.Parts[0],
		KeyPartType:        ivfCtx.partType.Id,
		OrigFuncName:       ivfCtx.origFuncName,
		IncludeColumns:     includeColumns,
		IncludeColumnTypes: includeColumnTypes,
	}
	tblCfgBytes, err := json.Marshal(tblCfg)
	if err != nil {
		return nodeID, err
	}
	tblCfgStr := string(tblCfgBytes)
	firstRoundLimit := uint64(0)
	bucketExpandStep := uint64(0)
	if vecCtx.rankOption != nil && vecCtx.rankOption.Mode == "include" {
		firstRoundLimit = firstIvfSearchRoundLimit(limit, len(serializedPushdownFilters) > 0 || len(remainingFilters) > 0)
		if firstRoundLimit == 0 && limit != nil {
			firstRoundLimit = 1
		}
		bucketExpandStep = uint64(ivfCtx.nProbe)
		if bucketExpandStep == 0 {
			bucketExpandStep = 1
		}
	}

	// build ivf_search table function node
	tableFuncTag := builder.genNewBindTag()
	tableFuncNode := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  kIVFSearchFuncName,
				Param: []byte(ivfCtx.params),
			},
			Cols: buildIvfSearchColDefs(tableFuncIncludeColumns, scanNode.TableDef),
		},
		BindingTags:     []int32{tableFuncTag},
		TblFuncExprList: buildIvfTableFuncArgs(tblCfgStr, ivfCtx.vecLitArg, pushdownFilterSQL, firstRoundLimit, bucketExpandStep),
	}
	tableFuncNodeID := builder.appendNode(tableFuncNode, ctx)

	err = builder.addBinding(tableFuncNodeID, tree.AliasClause{Alias: tree.Identifier("mo_ivf_alias_0")}, ctx)
	if err != nil {
		return 0, err
	}

	// change doc_id type to the primary type here
	tableFuncNode.TableDef.Cols[0].Typ = ivfCtx.pkType

	// push down the candidate limit to the table function.
	// If the outer sort has OFFSET, the index reader must fetch limit+offset
	// candidates so the final sort can still return the requested window.
	limitExpr := DeepCopyExpr(limit)
	if limitExpr != nil && sortNode.Offset != nil && needsOffsetCompensation {
		limitExpr, err = bindFuncExprAndConstFold(
			builder.GetContext(),
			builder.compCtx.GetProcess(),
			"+",
			[]*Expr{limitExpr, DeepCopyExpr(sortNode.Offset)},
		)
		if err != nil {
			return 0, err
		}
	}

	// When there are filters, over-fetch to get more candidates.
	// This ensures we have enough candidates after filtering.
	if len(remainingFilters) > 0 && !ivfCtx.pushdownEnabled {
		// Over-fetch strategy: dynamically adjust factor based on limit size
		// Smaller limits need more over-fetching due to higher variance
		if limitConst := limitExpr.GetLit(); limitConst != nil {
			originalLimit := limitConst.GetU64Val()

			// Filtered post mode needs a larger candidate budget than the historical
			// default, but we keep it as fixed buckets so the plan is predictable.
			overFetchFactor := calculateFilteredPostModeOverFetchFactor(originalLimit)

			newLimit := max(uint64(float64(originalLimit)*overFetchFactor), originalLimit+10)

			if ivfCtx.isAutoMode {
				logutil.Debugf(
					"Auto mode over-fetch: original_limit=%d, factor=%.2f, filter_count=%d",
					originalLimit, overFetchFactor, len(remainingFilters),
				)
				logutil.Debugf(
					"Auto mode over-fetch result: original_limit=%d, new_limit=%d",
					originalLimit, newLimit,
				)
			} else {
				logutil.Debugf(
					"Vector mode over-fetch: mode=post, original_limit=%d, factor=%.2f, filter_count=%d, new_limit=%d",
					originalLimit, overFetchFactor, len(remainingFilters), newLimit,
				)
			}

			limitExpr = &Expr{
				Typ: limit.Typ,
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{
						Isnull: false,
						Value: &plan.Literal_U64Val{
							U64Val: newLimit,
						},
					},
				},
			}
		}
	}

	tableFuncNode.IndexReaderParam = &plan.IndexReaderParam{
		Limit:        limitExpr,
		OrigFuncName: ivfCtx.origFuncName,
		DistRange:    distRange,
	}
	tableFuncNode.Limit = DeepCopyExpr(limitExpr)

	// Determine join structure based on rankOption.mode:
	//   mode != "pre": JOIN( scanNode, ivf_search )
	//   mode == "pre": JOIN( scanNode, JOIN(ivf_search, secondScan) )
	var joinRootID int32

	pushdownEnabled := ivfCtx.pushdownEnabled && len(remainingFilters) > 0
	scanNode.FilterList = remainingFilters

	if canIndexOnly {
		joinRootID = tableFuncNodeID
	} else if pushdownEnabled {
		// secondScanNode: copy original scanNode for JOIN(ivf, table)
		secondScanNodeID := builder.copyNode(ctx, scanNode.NodeId)
		secondScanNode := builder.qry.Nodes[secondScanNodeID]
		oldTag := secondScanNode.BindingTags[0]
		builder.rebindScanNode(secondScanNode)
		newTag := secondScanNode.BindingTags[0]

		// Update colRefCnt and idxColMap to reflect the new binding tag
		// This is essential for index optimization to work correctly on the rebound node
		if oldTag != newTag {
			for key, value := range colRefCnt {
				if key[0] == oldTag {
					colRefCnt[[2]int32{newTag, key[1]}] = value
				}
			}
			for key, value := range idxColMap {
				if key[0] == oldTag {
					idxColMap[[2]int32{newTag, key[1]}] = DeepCopyExpr(value)
				}
			}
		}

		if builder.canApplyRegularIndex(secondScanNode) {
			// Remove filters that reference the vector column (e.g. "embedding IS NOT NULL").
			// The copied second scan only needs to produce PKs for the inner BloomFilter join;
			// the original outer scan still keeps the full filter list as the safety net.
			partPos := ivfCtx.partPos
			var cleanedFilters []*plan.Expr
			for _, expr := range secondScanNode.FilterList {
				if refsColumn(expr, newTag, partPos) {
					continue
				}
				cleanedFilters = append(cleanedFilters, expr)
			}
			secondScanNode.FilterList = cleanedFilters

			// Build a minimal colRefCnt for the copied scan so index-only planning is still
			// possible after removing vector-column-only filters.
			secondColRefCnt := make(map[[2]int32]int)
			secondColRefCnt[[2]int32{newTag, ivfCtx.pkPos}] = 1
			for _, expr := range secondScanNode.FilterList {
				extractColRefs(expr, newTag, secondColRefCnt)
			}
			optimizedSecondScanID := builder.applyIndicesForFilters(secondScanNodeID, secondScanNode, secondColRefCnt, idxColMap)
			secondScanNodeID = optimizedSecondScanID
		}

		// Otherwise BloomFilter will only see the truncated primary key set, causing data loss.
		clearLimitOffsetInSubtree(builder.qry, secondScanNodeID)

		// Add a PROJECT node above secondScanNode to output only the primary key column
		secondProjectTag := builder.genNewBindTag()
		secondPkExpr := builder.buildPkExprFromNode(secondScanNodeID, ivfCtx.pkType, scanNode.TableDef.Pkey.PkeyColName)
		if secondPkExpr == nil {
			// If an optimized second-scan subtree can't provide a stable PK expression,
			// skip IVF rewrite to avoid wiring stale bindings into join/runtime-filter paths.
			return nodeID, nil
		}
		secondProjectNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{secondScanNodeID},
			ProjectList: []*plan.Expr{secondPkExpr},
			BindingTags: []int32{secondProjectTag},
		}, ctx)

		// inner join: (ivf_search table function JOIN second table project)
		innerJoinOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: secondProjectTag,
						ColPos: 0, // only pk column from second scan
					},
				},
			},
		})

		innerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{tableFuncNodeID, secondProjectNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{innerJoinOn},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// Construct BloomFilter type runtime filter for inner join + table function
		rfTag := builder.genNewMsgTag()

		// build side: primary key from secondScanNode (consistent with BloomFilter build column)
		buildExpr := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: secondProjectTag,
					ColPos: 0,
				},
			},
		}
		buildSpec := MakeRuntimeFilter(rfTag, false, 0, buildExpr, false)
		buildSpec.UseBloomFilter = true
		innerJoinNode := builder.qry.Nodes[innerJoinNodeID]
		innerJoinNode.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{buildSpec}

		// probe side: pkid column from table function
		probeExpr := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tableFuncTag,
					ColPos: 0,
				},
			},
		}
		probeSpec := MakeRuntimeFilter(rfTag, false, 0, probeExpr, false)
		probeSpec.UseBloomFilter = true
		tableFuncNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{probeSpec}

		// The original scan was guarded during the recursive planner pass so the vector rewrite
		// could see the raw table scan shape. Once the IVF subtree is constructed, we can
		// temporarily suspend that protection and apply regular secondary-index optimization
		// to the row-fetch side of the outer join.
		outerScanNodeID := scanNode.NodeId
		if builder.canApplyRegularIndex(scanNode) {
			builder.withSuspendedScanProtection(scanNode.NodeId, func() {
				outerScanNodeID = builder.applyIndicesForFilters(scanNode.NodeId, scanNode, colRefCnt, idxColMap)
			})
		}

		outerPkExpr := builder.buildPkExprFromNode(outerScanNodeID, ivfCtx.pkType, scanNode.TableDef.Pkey.PkeyColName)
		if outerPkExpr == nil && outerScanNodeID != scanNode.NodeId {
			// If a future regular-index rewrite produces an unsupported subtree shape,
			// fall back to the original scan instead of wiring stale bindings into the IVF join.
			logutil.Debugf("IVF outer PK fallback: optimized node %d -> original scan %d", outerScanNodeID, scanNode.NodeId)
			outerScanNodeID = scanNode.NodeId
			outerPkExpr = builder.buildPkExprFromNode(outerScanNodeID, ivfCtx.pkType, scanNode.TableDef.Pkey.PkeyColName)
		}
		if outerPkExpr == nil {
			return nodeID, nil
		}

		// outer join: optimized outer subtree JOIN (inner ivf join)
		outerOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			DeepCopyExpr(outerPkExpr),
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag, // tf pkid from inner join subtree
						ColPos: 0,
					},
				},
			},
		})

		outerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{outerScanNodeID, innerJoinNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{outerOn},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// Manually construct a runtime filter for outer join:
		//   - build side: right child inner join (smaller set, contains actual pkid)
		//   - probe side: left child table scan (original table), performs block/row pruning at scan stage.
		// Note:
		//   1) We don't use BloomFilter here, but use the existing IN-list runtime filter pipeline;
		//   2) UpperLimit is set to avoid all filters being degraded to PASS due to 0.
		rfTag2 := builder.genNewMsgTag()

		outerHasProbeRuntimeFilter := false
		outerProbeNodeID := builder.findScanNodeByTag(outerScanNodeID, outerPkExpr.GetCol().RelPos)
		if outerProbeNodeID >= 0 {
			probeSpec2 := MakeRuntimeFilter(rfTag2, false, 0, DeepCopyExpr(outerPkExpr), false)
			builder.qry.Nodes[outerProbeNodeID].RuntimeFilterProbeList = append(builder.qry.Nodes[outerProbeNodeID].RuntimeFilterProbeList, probeSpec2)
			outerHasProbeRuntimeFilter = true
		}

		// build: placeholder column, HashBuild will generate IN-list based on build side join key's UniqueJoinKeys[0]
		buildExpr2 := &plan.Expr{
			Typ: ivfCtx.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 0,
				},
			},
		}

		// Set inLimit to "unlimited" to ensure this runtime filter won't be disabled due to upper limit.
		// Use int32 max value directly here.
		const unlimitedInFilterCard = int32(1<<31 - 1)
		buildSpec2 := MakeRuntimeFilter(rfTag2, false, unlimitedInFilterCard, buildExpr2, false)

		if outerHasProbeRuntimeFilter {
			outerJoinNode := builder.qry.Nodes[outerJoinNodeID]
			outerJoinNode.RuntimeFilterBuildList = append(outerJoinNode.RuntimeFilterBuildList, buildSpec2)
		}

		// Outer join doesn't add extra project, let global column pruning optimizer handle it
		joinRootID = outerJoinNodeID
	} else {
		// JOIN( table, ivf )
		wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: ivfCtx.pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: ivfCtx.pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 0, // tf.pkid
					},
				},
			},
		})

		joinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, tableFuncNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{wherePkEqPk},
			// Don't set Limit/Offset on JOIN - they should be applied after SORT
		}, ctx)

		// In non-nested mode, outer join also doesn't add extra project, let optimizer handle column pruning
		joinRootID = joinNodeID
	}

	if !canIndexOnly {
		// Keep residual filters on scanNode so they are applied during table scan.
		scanNode.Limit = nil
		scanNode.Offset = nil
	}

	// Create SortBy, still sort directly by table function's score, let remap map ColRef to corresponding output column
	orderByScore := []*OrderBySpec{
		{
			Expr: &plan.Expr{
				Typ: tableFuncNode.TableDef.Cols[1].Typ, // score column
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tableFuncTag,
						ColPos: 1, // score column
					},
				},
			},
			Flag: vecCtx.sortDirection,
		},
	}

	sortByID := builder.appendNode(&plan.Node{
		NodeType:   plan.Node_SORT,
		Children:   []int32{joinRootID},
		OrderBy:    orderByScore,
		Limit:      limit,                         // Apply LIMIT after sorting
		Offset:     DeepCopyExpr(sortNode.Offset), // Apply OFFSET after sorting
		RankOption: DeepCopyRankOption(vecCtx.rankOption),
	}, ctx)

	projNode.Children[0] = sortByID

	if canIndexOnly {
		scanToTFMap := buildIvfScanToTableFuncMap(tableFuncTag, tableFuncIncludeColumns, scanNode)
		if childNode != nil {
			sortIdx := orderExpr.GetCol().ColPos
			projMap := make(map[[2]int32]*plan.Expr)
			for i, proj := range childNode.ProjectList {
				if i == int(sortIdx) {
					projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = DeepCopyExpr(orderByScore[0].Expr)
					continue
				}
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = replaceColumnsForExpr(DeepCopyExpr(proj), scanToTFMap)
			}
			replaceColumnsForNode(projNode, projMap)
		} else {
			replaceColumnsForNode(projNode, scanToTFMap)
		}
	} else if childNode != nil {
		sortIdx := orderExpr.GetCol().ColPos
		projMap := make(map[[2]int32]*plan.Expr)
		for i, proj := range childNode.ProjectList {
			if i == int(sortIdx) {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = DeepCopyExpr(orderByScore[0].Expr)
			} else {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = proj
			}
		}

		replaceColumnsForNode(projNode, projMap)
	}

	return nodeID, nil
}

func (builder *QueryBuilder) buildPkExprFromNode(nodeID int32, pkType plan.Type, pkName string) *plan.Expr {
	if builder == nil || nodeID < 0 {
		return nil
	}
	node := builder.qry.Nodes[nodeID]
	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		if node.TableDef == nil || len(node.BindingTags) == 0 {
			return nil
		}
		colIdx, ok := node.TableDef.Name2ColIndex[pkName]
		if !ok {
			if node.IndexScanInfo.IsIndexScan {
				colIdx, ok = node.TableDef.Name2ColIndex[catalog.IndexTablePrimaryColName]
				if !ok {
					logutil.Debugf("IVF buildPkExprFromNode: index primary column %q missing in table %q for node %d", catalog.IndexTablePrimaryColName, node.TableDef.Name, nodeID)
					return nil
				}
			} else {
				if node.TableDef.Pkey == nil {
					return nil
				}
				colIdx = node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
			}
		}
		return &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: node.BindingTags[0],
					ColPos: colIdx,
					Name:   pkName,
				},
			},
		}
	case plan.Node_PROJECT:
		for _, expr := range node.ProjectList {
			if col := expr.GetCol(); col != nil {
				if builder.getColName(col) == pkName {
					return DeepCopyExpr(expr)
				}
			}
		}
		// If PROJECT doesn't expose PK, don't recurse to child: using child's binding tag here
		// would produce stale ColRef(RelPos) for joins/runtime filters above this PROJECT.
		return nil
	case plan.Node_JOIN:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	default:
		if len(node.Children) > 0 {
			return builder.buildPkExprFromNode(node.Children[0], pkType, pkName)
		}
	}
	return nil
}

func (builder *QueryBuilder) findScanNodeByTag(nodeID, tag int32) int32 {
	return builder.findScanNodeByTagWithVisited(nodeID, tag, make(map[int32]struct{}))
}

func (builder *QueryBuilder) findScanNodeByTagWithVisited(nodeID, tag int32, visited map[int32]struct{}) int32 {
	if builder == nil || nodeID < 0 {
		return -1
	}
	if _, seen := visited[nodeID]; seen {
		return -1
	}
	visited[nodeID] = struct{}{}
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN && len(node.BindingTags) > 0 && node.BindingTags[0] == tag {
		return nodeID
	}
	for _, childID := range node.Children {
		if found := builder.findScanNodeByTagWithVisited(childID, tag, visited); found >= 0 {
			return found
		}
	}
	return -1
}

func (builder *QueryBuilder) getColName(col *plan.ColRef) string {
	if col == nil {
		return ""
	}
	if builder == nil || builder.nameByColRef == nil {
		return col.Name
	}
	if name := builder.nameByColRef[[2]int32{col.RelPos, col.ColPos}]; name != "" {
		return name
	}
	return col.Name
}

func (builder *QueryBuilder) rebindScanNode(scanNode *plan.Node) {
	if scanNode == nil || len(scanNode.BindingTags) == 0 {
		return
	}
	oldTag := scanNode.BindingTags[0]
	newTag := builder.genNewBindTag()
	scanNode.BindingTags[0] = newTag
	builder.addNameByColRef(newTag, scanNode.TableDef)
	for _, expr := range scanNode.FilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
	// Also update BlockFilterList, which was copied from the original scanNode
	// and still contains references to the old binding tag
	for _, expr := range scanNode.BlockFilterList {
		replaceColRefTag(expr, oldTag, newTag)
	}
}

func replaceColRefTag(expr *plan.Expr, oldTag, newTag int32) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == oldTag {
			impl.Col.RelPos = newTag
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			replaceColRefTag(arg, oldTag, newTag)
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			replaceColRefTag(sub, oldTag, newTag)
		}
	}
}

func (builder *QueryBuilder) canApplyRegularIndex(node *plan.Node) bool {
	if node == nil || node.TableDef == nil {
		return false
	}
	colCnt := len(node.TableDef.Cols)
	if colCnt == 0 {
		return false
	}
	for _, expr := range node.FilterList {
		if !colRefsWithin(expr, colCnt) {
			return false
		}
	}
	return len(node.FilterList) > 0
}

func clearLimitOffsetInSubtree(qry *plan.Query, nodeID int32) {
	if qry == nil || nodeID < 0 {
		return
	}
	node := qry.Nodes[nodeID]
	node.Limit = nil
	node.Offset = nil
	for _, childID := range node.Children {
		clearLimitOffsetInSubtree(qry, childID)
	}
}

func colRefsWithin(expr *plan.Expr, colCnt int) bool {
	if expr == nil {
		return true
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return int(impl.Col.ColPos) < colCnt
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if !colRefsWithin(arg, colCnt) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !colRefsWithin(sub, colCnt) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func extractColRefs(expr *plan.Expr, tag int32, colRefCnt map[[2]int32]int) {
	if expr == nil {
		return
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == tag {
			colRefCnt[[2]int32{tag, impl.Col.ColPos}]++
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			extractColRefs(arg, tag, colRefCnt)
		}
	case *plan.Expr_Sub:
		return
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			extractColRefs(sub, tag, colRefCnt)
		}
	}
}

func refsColumn(expr *plan.Expr, tag int32, colPos int32) bool {
	if expr == nil {
		return false
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return impl.Col.RelPos == tag && impl.Col.ColPos == colPos
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if refsColumn(arg, tag, colPos) {
				return true
			}
		}
	case *plan.Expr_Sub:
		return false
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if refsColumn(sub, tag, colPos) {
				return true
			}
		}
	}
	return false
}
