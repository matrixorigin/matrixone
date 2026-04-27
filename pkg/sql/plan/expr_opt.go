// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (builder *QueryBuilder) mergeFiltersOnCompositeKey(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.mergeFiltersOnCompositeKey(childID)
		}

		return
	}

	if node.TableDef.Pkey == nil {
		return
	}

	newFilterList := builder.doMergeFiltersOnCompositeKey(node.TableDef, node.BindingTags[0], node.FilterList...)
	node.FilterList = newFilterList
	node.Stats = calcScanStats(node, builder)
	resetHashMapStats(node.Stats)
}

func (builder *QueryBuilder) rewriteInDomainNotInFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.rewriteInDomainNotInFilters(childID)
		}
		return
	}
	if len(node.FilterList) < 2 {
		return
	}

	domains := collectInFilterDomains(node.FilterList)
	if len(domains) == 0 {
		return
	}
	for idx, filter := range node.FilterList {
		node.FilterList[idx] = builder.rewriteExprByInDomains(filter, domains)
	}
}

type inFilterDomain struct {
	colExpr *plan.Expr
	values  []*plan.Expr
}

func collectInFilterDomains(filters []*plan.Expr) map[[2]int32]*inFilterDomain {
	domains := make(map[[2]int32]*inFilterDomain)
	for _, filter := range filters {
		colExpr, values, ok := extractInListFilter(filter)
		if !ok {
			continue
		}
		colRef := colExpr.GetCol()
		colKey := [2]int32{colRef.RelPos, colRef.ColPos}
		if _, exists := domains[colKey]; exists {
			continue
		}
		uniqueValues, _, ok := constLiteralListValues(values)
		if !ok || len(uniqueValues) == 0 {
			continue
		}
		domains[colKey] = &inFilterDomain{
			colExpr: DeepCopyExpr(colExpr),
			values:  uniqueValues,
		}
	}
	return domains
}

func extractInListFilter(expr *plan.Expr) (*plan.Expr, []*plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 {
		return nil, nil, false
	}
	colExpr := fn.Args[0]
	if colExpr.GetCol() == nil {
		return nil, nil, false
	}
	list := fn.Args[1].GetList()
	if list == nil {
		return nil, nil, false
	}
	return colExpr, list.List, true
}

func constLiteralListValues(values []*plan.Expr) ([]*plan.Expr, map[string]struct{}, bool) {
	keys := make(map[string]struct{}, len(values))
	uniqueValues := make([]*plan.Expr, 0, len(values))
	for _, value := range values {
		key, ok := constLiteralKey(value)
		if !ok {
			return nil, nil, false
		}
		if _, exists := keys[key]; exists {
			continue
		}
		keys[key] = struct{}{}
		uniqueValues = append(uniqueValues, DeepCopyExpr(value))
	}
	return uniqueValues, keys, true
}

func constLiteralKey(expr *plan.Expr) (string, bool) {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return "", false
	}
	return fmt.Sprintf("%d/%d/%d/%s", expr.Typ.Id, expr.Typ.Width, expr.Typ.Scale, lit.String()), true
}

func (builder *QueryBuilder) rewriteExprByInDomains(
	expr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	fn := expr.GetF()
	if fn == nil {
		return expr
	}

	switch fn.Func.ObjName {
	case "not":
		if len(fn.Args) != 1 {
			return expr
		}
		if rewritten := builder.rewriteNotInByDomains(fn.Args[0], domains); rewritten != nil {
			return rewritten
		}
		fn.Args[0] = builder.rewriteExprByInDomains(fn.Args[0], domains)
		return expr
	case "and":
		for idx, arg := range fn.Args {
			fn.Args[idx] = builder.rewriteExprByInDomains(arg, domains)
		}
		return builder.rewriteAndNotEqualByDomains(expr, domains)
	default:
		for idx, arg := range fn.Args {
			fn.Args[idx] = builder.rewriteExprByInDomains(arg, domains)
		}
		return expr
	}
}

func (builder *QueryBuilder) rewriteNotInByDomains(
	inExpr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	colExpr, values, ok := extractInListFilter(inExpr)
	if !ok {
		return nil
	}
	colRef := colExpr.GetCol()
	domain := domains[[2]int32{colRef.RelPos, colRef.ColPos}]
	if domain == nil {
		return nil
	}
	_, excludeKeys, ok := constLiteralListValues(values)
	if !ok {
		return nil
	}
	return builder.buildDomainDifferenceInExpr(domain, excludeKeys)
}

func (builder *QueryBuilder) rewriteAndNotEqualByDomains(
	expr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	var conjuncts []*plan.Expr
	flattenLogicalExpressions(expr, "and", &conjuncts)
	if len(conjuncts) < 2 {
		return expr
	}

	type notEqualGroup struct {
		positions []int
		keys      map[string]struct{}
	}
	groups := make(map[[2]int32]*notEqualGroup)
	groupOrder := make([][2]int32, 0)
	for idx, conjunct := range conjuncts {
		colExpr, value, ok := extractNotEqualConst(conjunct)
		if !ok {
			continue
		}
		key, ok := constLiteralKey(value)
		if !ok {
			continue
		}
		colRef := colExpr.GetCol()
		colKey := [2]int32{colRef.RelPos, colRef.ColPos}
		if domains[colKey] == nil {
			continue
		}
		group := groups[colKey]
		if group == nil {
			group = &notEqualGroup{keys: make(map[string]struct{})}
			groups[colKey] = group
			groupOrder = append(groupOrder, colKey)
		}
		group.positions = append(group.positions, idx)
		group.keys[key] = struct{}{}
	}

	replaced := false
	skip := make(map[int]struct{})
	for _, colKey := range groupOrder {
		group := groups[colKey]
		rewritten := builder.buildDomainDifferenceInExpr(domains[colKey], group.keys)
		if rewritten == nil {
			continue
		}
		for _, pos := range group.positions {
			skip[pos] = struct{}{}
		}
		conjuncts = append(conjuncts, rewritten)
		replaced = true
	}
	if !replaced {
		return expr
	}

	newConjuncts := make([]*plan.Expr, 0, len(conjuncts))
	for idx, conjunct := range conjuncts {
		if _, ok := skip[idx]; ok {
			continue
		}
		newConjuncts = append(newConjuncts, conjunct)
	}
	if len(newConjuncts) == 0 {
		return makePlan2BoolConstExprWithType(true)
	}
	newExpr, err := combinePlanConjunction(builder.GetContext(), newConjuncts)
	if err != nil {
		return expr
	}
	return newExpr
}

func extractNotEqualConst(expr *plan.Expr) (*plan.Expr, *plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return nil, nil, false
	}
	if fn.Func.ObjName != "!=" && fn.Func.ObjName != "<>" {
		return nil, nil, false
	}
	if fn.Args[0].GetCol() != nil && fn.Args[1].GetLit() != nil {
		return fn.Args[0], fn.Args[1], true
	}
	if fn.Args[1].GetCol() != nil && fn.Args[0].GetLit() != nil {
		return fn.Args[1], fn.Args[0], true
	}
	return nil, nil, false
}

func (builder *QueryBuilder) buildDomainDifferenceInExpr(
	domain *inFilterDomain,
	excludeKeys map[string]struct{},
) *plan.Expr {
	diffValues := make([]*plan.Expr, 0, len(domain.values))
	overlap := false
	for _, value := range domain.values {
		key, ok := constLiteralKey(value)
		if !ok {
			return nil
		}
		if _, excluded := excludeKeys[key]; excluded {
			overlap = true
			continue
		}
		diffValues = append(diffValues, DeepCopyExpr(value))
	}
	if !overlap {
		return nil
	}
	if len(diffValues) == 0 {
		return MakeFalseExpr()
	}

	listExpr := &plan.Expr{
		Typ: domain.colExpr.Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: diffValues,
			},
		},
	}
	rewritten, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "in", []*plan.Expr{
		DeepCopyExpr(domain.colExpr),
		listExpr,
	})
	if err != nil {
		return nil
	}
	return rewritten
}

func (builder *QueryBuilder) doMergeFiltersOnCompositeKey(tableDef *plan.TableDef, tableTag int32, filters ...*plan.Expr) []*plan.Expr {
	sortkeyIdx := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	col2filter := make(map[int32]int)
	Parts := tableDef.Pkey.Names
	numParts := len(Parts)
	if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		sortkeyIdx = tableDef.Name2ColIndex[tableDef.ClusterBy.Name]
		Parts = util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
		numParts = len(Parts)
	}

	for i, expr := range filters {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		funcName := fn.Func.ObjName
		if funcName == "=" {
			if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
				fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
			}

			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) {
				continue
			}

			col2filter[col.ColPos] = i
		} else if funcName == "between" {
			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) || !isRuntimeConstExpr(fn.Args[2]) {
				continue
			}

			col2filter[col.ColPos] = i
		} else if funcName == "in" {
			if fn.Args[0].GetCol() == nil {
				continue
			}

			col := fn.Args[0].GetCol()
			if _, ok := col2filter[col.ColPos]; ok {
				continue
			}

			col2filter[col.ColPos] = i
		} else if funcName == "or" {
			var orArgs []*plan.Expr
			flattenLogicalExpressions(expr, "or", &orArgs)

			newOrArgs := make([]*plan.Expr, 0, len(orArgs))
			var inArgs []*plan.Expr
			var firstEquiExpr *plan.Expr
			pkFnName := "in"

			currColPos := int32(-1)
			for _, subExpr := range orArgs {
				subFn := subExpr.GetF()
				if subFn == nil {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if subFn.Func.ObjName == "=" || subFn.Func.ObjName == "in" {
					if numParts > 1 {
						newArgs := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, subExpr)
						subExpr = newArgs[0]
					}
				} else if subFn.Func.ObjName == "and" {
					var andArgs []*plan.Expr
					flattenLogicalExpressions(subExpr, "and", &andArgs)

					newArgs := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, andArgs...)
					if len(newArgs) == 1 {
						subExpr = newArgs[0]
					} else {
						subFn.Args = newArgs
					}
				}

				mergedFn := subExpr.GetF()
				if mergedFn == nil || len(mergedFn.Args) != 2 || mergedFn.Args[0].GetCol() == nil || !isRuntimeConstExpr(mergedFn.Args[1]) {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if currColPos == -1 {
					currColPos = mergedFn.Args[0].GetCol().ColPos
				} else if currColPos != mergedFn.Args[0].GetCol().ColPos {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if len(inArgs) == 0 {
					firstEquiExpr = subExpr
				}

				switch mergedFn.Func.ObjName {
				case "=":
					inArgs = append(inArgs, mergedFn.Args[1])

				case "prefix_eq":
					inArgs = append(inArgs, mergedFn.Args[1])
					pkFnName = "prefix_in"

				case "in":
					inArgs = append(inArgs, mergedFn.Args[1].GetList().List...)

				default:
					newOrArgs = append(newOrArgs, subExpr)
				}
			}

			if len(inArgs) == 1 {
				newOrArgs = append(newOrArgs, firstEquiExpr)
			} else if len(inArgs) > 1 {
				leftExpr := firstEquiExpr.GetF().Args[0]
				leftType := makeTypeByPlan2Expr(leftExpr)
				argsType := []types.Type{leftType, leftType}
				fGet, _ := function.GetFunctionByName(builder.GetContext(), pkFnName, argsType)

				funcID := fGet.GetEncodedOverloadID()
				returnType := fGet.GetReturnType()
				exprType := makePlan2Type(&returnType)
				args := []*plan.Expr{
					leftExpr,
					{
						Typ: leftExpr.Typ,
						Expr: &plan.Expr_List{
							List: &plan.ExprList{
								List: inArgs,
							},
						},
					},
				}
				exprType.NotNullable = function.DeduceNotNullable(funcID, args)
				inExpr := &plan.Expr{
					Typ: exprType,
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: getFunctionObjRef(funcID, pkFnName),
							Args: args,
						},
					},
				}

				newOrArgs = append(newOrArgs, inExpr)
			}

			if len(newOrArgs) == 1 {
				filters[i] = newOrArgs[0]
				colPos := firstEquiExpr.GetF().Args[0].GetCol().ColPos
				if colPos != sortkeyIdx {
					col2filter[colPos] = i
				}
			} else {
				fn.Args = newOrArgs
			}
		}
	}

	if numParts == 1 {
		return filters
	}

	filterIdx := make([]int, 0, numParts)
	for _, part := range Parts {
		colIdx := tableDef.Name2ColIndex[part]
		idx, ok := col2filter[colIdx]
		if !ok {
			break
		}

		filterIdx = append(filterIdx, idx)
		funcName := filters[idx].GetF().Func.ObjName
		if funcName == "in" || funcName == "between" {
			break
		}
	}

	if len(filterIdx) == 0 {
		return filters
	}

	var compositePKFilter *plan.Expr
	pkExpr := &plan.Expr{
		Typ: tableDef.Cols[sortkeyIdx].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tableTag,
				ColPos: sortkeyIdx,
			},
		},
	}

	compositePKFilterSel := 1.0
	for i := range filterIdx {
		compositePKFilterSel *= (filters[filterIdx[i]]).Selectivity
	}

	lastFilter := filters[filterIdx[len(filterIdx)-1]]
	lastFuncName := lastFilter.GetF().Func.ObjName
	if lastFuncName == "in" {
		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		inArgs := make([]*plan.Expr, len(lastFilter.GetF().Args[1].GetList().List))
		for i, lastArg := range lastFilter.GetF().Args[1].GetList().List {
			tmpSerialArgs := DeepCopyExprList(serialArgs)
			tmpSerialArgs = append(tmpSerialArgs, lastArg)
			rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", tmpSerialArgs)
			inArgs[i] = rightArg
		}

		funcName := "in"
		if len(filterIdx) < numParts {
			funcName = "prefix_in"
		}

		compositePKFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
			pkExpr,
			{
				Typ: pkExpr.Typ,
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: inArgs,
					},
				},
			},
		})
	} else if lastFuncName == "between" {
		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		tmpSerialArgs := DeepCopyExprList(serialArgs)
		tmpSerialArgs = append(tmpSerialArgs, lastFilter.GetF().Args[1])
		leftArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", tmpSerialArgs)

		tmpSerialArgs = serialArgs
		tmpSerialArgs = append(tmpSerialArgs, lastFilter.GetF().Args[2])
		rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", tmpSerialArgs)

		funcName := "between"
		if len(filterIdx) < numParts {
			funcName = "prefix_between"
		}

		compositePKFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
			pkExpr,
			leftArg,
			rightArg,
		})
	} else {
		serialArgs := make([]*plan.Expr, len(filterIdx))
		for i := range filterIdx {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}
		rightArg, _ := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", serialArgs)

		funcName := "="
		if len(filterIdx) < numParts {
			funcName = "prefix_eq"
		}

		compositePKFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
			pkExpr,
			rightArg,
		})
	}
	compositePKFilter.Selectivity = compositePKFilterSel

	hitFilterSet := make(map[int]bool)
	for i := range filterIdx {
		hitFilterSet[filterIdx[i]] = true
	}

	newFilterList := make([]*plan.Expr, 0, len(filters)-len(filterIdx)+1)
	newFilterList = append(newFilterList, compositePKFilter)
	for i, filter := range filters {
		if !hitFilterSet[i] {
			newFilterList = append(newFilterList, filter)
		}
	}

	return newFilterList
}

func flattenLogicalExpressions(expr *plan.Expr, opName string, args *[]*plan.Expr) {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != opName {
		*args = append(*args, expr)
		return
	}

	for _, arg := range fn.Args {
		flattenLogicalExpressions(arg, opName, args)
	}
}
