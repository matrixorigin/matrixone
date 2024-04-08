// Copyright 2023 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (builder *QueryBuilder) partitionPrune(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.partitionPrune(childID)
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_EXTERNAL_SCAN:
		if node.TableDef.GetPartition() != nil && len(node.FilterList) != 0 {
			partitionByDef := node.TableDef.Partition
			switch partitionByDef.Type {
			case plan.PartitionType_KEY, plan.PartitionType_LINEAR_KEY, plan.PartitionType_HASH, plan.PartitionType_LINEAR_HASH:
				pruner := &KeyHashPartitionPruner{
					node:    node,
					process: builder.compCtx.GetProcess(),
				}
				pruner.init()
				pruner.prune()
			case plan.PartitionType_LIST:
				// XXX unimplement
			case plan.PartitionType_LIST_COLUMNS:
				// XXX unimplement
			case plan.PartitionType_RANGE:
				// XXX unimplement
			case plan.PartitionType_RANGE_COLUMNS:
				// XXX unimplement
			}
		}
	}
}

// KEY and HASH Partition Pruner
type KeyHashPartitionPruner struct {
	partitionKeysMap map[string]int
	partitionByDef   *plan.PartitionByDef
	node             *Node
	process          *process.Process
}

type PartitionPruneResult struct {
	usedPartitions map[int32]bool
	isUnablePrune  bool
	needPushUp     bool
}

func (p *KeyHashPartitionPruner) init() {
	partitionByDef := p.node.TableDef.Partition
	p.partitionByDef = partitionByDef
	p.partitionKeysMap = make(map[string]int)

	switch partitionByDef.Type {
	case plan.PartitionType_KEY, plan.PartitionType_LINEAR_KEY:
		for _, partitionCol := range partitionByDef.PartitionColumns.PartitionColumns {
			if _, ok := p.partitionKeysMap[partitionCol]; !ok {
				p.partitionKeysMap[partitionCol] = 1
			}
		}
	case plan.PartitionType_HASH, plan.PartitionType_LINEAR_HASH:
		extractColumnsFromExpression(partitionByDef.PartitionExpr.Expr, p.partitionKeysMap)
	}
}

// detachAndPrune Detach of filter conditions and partition prune
func (p *KeyHashPartitionPruner) detachAndPrune() *PartitionPruneResult {
	if len(p.node.FilterList) == 1 {
		if exprF, ok := p.node.FilterList[0].Expr.(*plan.Expr_F); ok && exprF.F.Func.ObjName == "or" {
			return p.detachDNFCondAndBuildPrune(p.node.FilterList[0])
		}
	}
	return p.detachCNFCondAndBuildPrune(p.node.FilterList)
}

func (p *KeyHashPartitionPruner) prune() bool {
	pruneResult := p.detachAndPrune()
	if pruneResult.isUnablePrune {
		return false
	}

	p.node.PartitionPrune = &plan.PartitionPrune{
		IsPruned:           true,
		SelectedPartitions: make([]*plan.PartitionItem, 0, len(pruneResult.usedPartitions)),
	}

	for pid := range pruneResult.usedPartitions {
		partitionItem := p.partitionByDef.Partitions[pid]
		partition := &plan.PartitionItem{
			PartitionName:      partitionItem.PartitionName,
			OrdinalPosition:    partitionItem.OrdinalPosition,
			Description:        partitionItem.Description,
			Comment:            partitionItem.Comment,
			LessThan:           DeepCopyExprList(partitionItem.LessThan),
			InValues:           DeepCopyExprList(partitionItem.InValues),
			PartitionTableName: partitionItem.PartitionTableName,
		}
		p.node.PartitionPrune.SelectedPartitions = append(p.node.PartitionPrune.SelectedPartitions, partition)
	}
	return true
}

func (p *KeyHashPartitionPruner) detachDNFCondAndBuildPrune(orExpr *plan.Expr) *PartitionPruneResult {
	unablePruneResult := &PartitionPruneResult{
		isUnablePrune: true,
	}

	// split disjunctive expression
	dnfItems := SplitDNFItems(orExpr)
	if isAllColEqualConstExpr(dnfItems) {
		if len(p.partitionKeysMap) == 1 {
			usedPartitions := make(map[int32]bool)
			for _, expr := range dnfItems {
				// 1. extract all ColRef equals const value from expression
				colEqValMap := make(map[string]*plan.Expr)
				extractColEqValFromEqualExpr(expr, colEqValMap)

				// 2. Check if all column equivalence expressions contain all partition keys
				if !exprColsIncludePartitionKeys(p.partitionKeysMap, colEqValMap) {
					return unablePruneResult
				}

				if ok, pidx := p.getUsedPartition(colEqValMap); ok {
					// if pidx=-1, it means that no existing partitions can be selected
					if pidx != -1 {
						usedPartitions[pidx] = true
					}
				} else {
					return unablePruneResult
				}
			}
			return &PartitionPruneResult{
				isUnablePrune:  false,
				usedPartitions: usedPartitions,
			}
		} else {
			return unablePruneResult
		}
	} else {
		hitPartitions := make(map[int32]bool)
		for i := range dnfItems {
			if isLogicExpr(dnfItems[i], "and") {
				exprs := SplitCNFItems(dnfItems[i])
				tmp := p.detachCNFCondAndBuildPrune(exprs)
				if tmp.needPushUp || tmp.isUnablePrune {
					return unablePruneResult
				} else {
					hitPartitions = union(hitPartitions, tmp.usedPartitions)
				}
			} else if isExprColRefEqualConst(dnfItems[i]) {
				// 2. extract all colRef to const value from filters
				colEqValMap := make(map[string]*plan.Expr)
				extractColEqValFromEqualExpr(dnfItems[i], colEqValMap)
				if !exprColsIncludePartitionKeys(p.partitionKeysMap, colEqValMap) {
					return unablePruneResult
				}

				if ok, pidx := p.getUsedPartition(colEqValMap); ok {
					// if pidx=-1, it means that no existing partitions can be selected
					if pidx != -1 {
						hitPartitions[pidx] = true
					}
				} else {
					return unablePruneResult
				}
			} else {
				return unablePruneResult
			}
		}
		return &PartitionPruneResult{
			usedPartitions: hitPartitions,
			isUnablePrune:  false,
		}
	}
}

func (p *KeyHashPartitionPruner) detachCNFCondAndBuildPrune(conditions []*Expr) *PartitionPruneResult {
	if isAllSimpleExpr(conditions) {
		// 1. Collect equivalent expressions
		if ok, colEqValMap := extractColEqValFromExprs(conditions, p.partitionKeysMap); ok {
			return p.buildPruneResult(colEqValMap)
		} else {
			return &PartitionPruneResult{
				isUnablePrune: true,
			}
		}
	} else if isAllLogicExpr(conditions, "or") {
		return p.buildPruneResultForOrConditions(conditions)
	} else {
		return &PartitionPruneResult{
			isUnablePrune: true,
		}
	}
}

// buildPruneResult Get hit partitions based on the set of equivalent expressions
func (p *KeyHashPartitionPruner) buildPruneResult(colEqValMap map[string]*plan.Expr) *PartitionPruneResult {
	// Check if the conditions meet the partitioning key
	if len(colEqValMap) != len(p.partitionKeysMap) {
		return &PartitionPruneResult{
			isUnablePrune: true,
			needPushUp:    true,
		}
	}

	if ok, pid := p.getUsedPartition(colEqValMap); ok {
		hitPartitions := make(map[int32]bool)
		if pid != -1 {
			hitPartitions[pid] = true
		}
		result := &PartitionPruneResult{
			isUnablePrune:  false,
			usedPartitions: hitPartitions,
		}
		return result
	} else {
		return &PartitionPruneResult{
			isUnablePrune: true,
		}
	}
}

// buildPruneResultForOrConditions Get hit partitions based on the set of disjunction expressions
func (p *KeyHashPartitionPruner) buildPruneResultForOrConditions(conditions []*Expr) *PartitionPruneResult {
	hitPartitions := make(map[int32]bool)
	for i, cond := range conditions {
		tmp := p.detachDNFCondAndBuildPrune(cond)
		if tmp.isUnablePrune {
			return &PartitionPruneResult{
				isUnablePrune: true,
			}
		}
		if i == 0 {
			hitPartitions = tmp.usedPartitions
		} else {
			hitPartitions = intersection(hitPartitions, tmp.usedPartitions)
		}
	}
	return &PartitionPruneResult{
		usedPartitions: hitPartitions,
		isUnablePrune:  false,
	}
}

// getUsedPartition Calculate the partition based on the constant expression of the partition key column
func (p *KeyHashPartitionPruner) getUsedPartition(cnfColEqVal map[string]*plan.Expr) (bool, int32) {
	// 1.evaluate the partition expr where the colRef assigned with const
	inputBat := batch.NewWithSize(len(p.node.TableDef.GetCols()))
	inputBat.SetRowCount(1)
	defer inputBat.Clean(p.process.Mp())

	for i, colDef := range p.node.TableDef.GetCols() {
		if valueExpr, ok := cnfColEqVal[colDef.GetName()]; ok {
			colVec, err := colexec.EvalExpressionOnce(p.process, valueExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			if err != nil {
				return false, -1
			}
			inputBat.SetVector(int32(i), colVec)
		} else {
			typ := types.New(types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale)
			colVec := vector.NewConstNull(typ, 1, p.process.Mp())
			inputBat.SetVector(int32(i), colVec)
		}
	}

	// 2. calculate partition expression
	resVec, err := colexec.EvalExpressionOnce(p.process, p.partitionByDef.PartitionExpression, []*batch.Batch{inputBat})
	if err != nil {
		return false, -1
	}
	defer resVec.Free(p.process.Mp())

	// 3. prune the partition
	if resVec.IsConstNull() {
		return false, -1
	} else {
		return true, vector.MustFixedCol[int32](resVec)[0]
	}
}

// intersection Finding the Intersection of Two Map[int32]bool Sets
func intersection(left, right map[int32]bool) map[int32]bool {
	result := make(map[int32]bool)
	for key, value := range left {
		if _, ok := right[key]; ok {
			result[key] = value
		}
	}
	return result
}

// union Finding the Union of Two Map[int32]bool Sets
func union(left, right map[int32]bool) map[int32]bool {
	result := make(map[int32]bool)
	for key, value := range left {
		result[key] = value
	}
	for key, value := range right {
		result[key] = value
	}
	return result
}

// SplitCNFItems splits CNF items.
// CNF means conjunctive normal form, such as: "a and b and c".
func SplitCNFItems(onExpr *Expr) []*Expr {
	return splitNormalFormItems(onExpr, "and")
}

// SplitDNFItems splits DNF items.
// DNF means disjunctive normal form, such as: "a or b or c".
func SplitDNFItems(onExpr *Expr) []*Expr {
	return splitNormalFormItems(onExpr, "or")
}

// splitNormalFormItems split CNF(conjunctive normal form) like "a and b and c", or DNF(disjunctive normal form) like "a or b or c"
func splitNormalFormItems(onExpr *Expr, funcName string) []*Expr {
	// nolint: revive
	switch v := onExpr.Expr.(type) {
	case *plan.Expr_F:
		if v.F.Func.ObjName == funcName {
			var ret []*Expr
			for _, arg := range v.F.GetArgs() {
				ret = append(ret, splitNormalFormItems(arg, funcName)...)
			}
			return ret
		}
	}
	return []*Expr{onExpr}
}

// ----------------------------------------------------------------------------------------------------------------------
// extract column equivalent pairs from a equality comparison expression
func extractColEqValFromEqualExpr(expr *plan.Expr, colEqValMap map[string]*plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "=" {
			if isColExpr(exprImpl.F.Args[0]) {
				exprCol := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
				colEqValMap[exprCol.Col.Name] = exprImpl.F.Args[1]
			} else if isColExpr(exprImpl.F.Args[1]) {
				exprCol := exprImpl.F.Args[1].Expr.(*plan.Expr_Col)
				colEqValMap[exprCol.Col.Name] = exprImpl.F.Args[0]
			}
		}
	}
}

// extract column equivalent pairs from some expressions
func extractColEqValFromExprs(cnfExprs []*Expr, partKeysMap map[string]int) (bool, map[string]*plan.Expr) {
	colEqValMap := make(map[string]*plan.Expr)
	for i := range cnfExprs {
		switch exprImpl := cnfExprs[i].Expr.(type) {
		case *plan.Expr_F:
			if exprImpl.F.Func.ObjName == "=" {
				if isColExpr(exprImpl.F.Args[0]) && isConstExpr(exprImpl.F.Args[1]) {
					exprCol := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
					if _, ok := partKeysMap[exprCol.Col.Name]; ok {
						colEqValMap[exprCol.Col.Name] = exprImpl.F.Args[1]
					}
				} else if isConstExpr(exprImpl.F.Args[0]) && isColExpr(exprImpl.F.Args[1]) {
					exprCol := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
					if _, ok := partKeysMap[exprCol.Col.Name]; ok {
						colEqValMap[exprCol.Col.Name] = exprImpl.F.Args[0]
					}
				} else {
					continue
				}
			} else {
				if checkExprContainPartitionKey(cnfExprs[i], partKeysMap) {
					return false, nil
				}
			}
		default:
			if checkExprContainPartitionKey(cnfExprs[i], partKeysMap) {
				return false, nil
			}
		}
	}
	return true, colEqValMap
}

// Extract columns used in partition expressions
func extractColumnsFromExpression(expr *plan.Expr, usedColumns map[string]int) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if v, ok := usedColumns[e.Col.Name]; ok {
			usedColumns[e.Col.Name] = v + 1
		} else {
			usedColumns[e.Col.Name] = 1
		}
	case *plan.Expr_F:
		for _, args := range e.F.Args {
			extractColumnsFromExpression(args, usedColumns)
		}
	case *plan.Expr_List:
		for _, exprl := range e.List.List {
			extractColumnsFromExpression(exprl, usedColumns)
		}
	default:
		return
	}
}

// Check if all column equivalence expressions contain all partition keys
func exprColsIncludePartitionKeys(partitionKeys map[string]int, exprCols map[string]*plan.Expr) bool {
	for key := range partitionKeys {
		if !keyIsInExprCols(key, exprCols) {
			return false
		}
	}
	return true
}

func keyIsInExprCols(key string, exprCols map[string]*plan.Expr) bool {
	for c1 := range exprCols {
		if strings.EqualFold(key, c1) {
			return true
		}
	}
	return false
}

func isAllSimpleExpr(exprs []*Expr) bool {
	for _, expr := range exprs {
		if !isSimpleExpr(expr) {
			return false
		}
	}
	return true
}

// checkExprContainPartitionKey Check if the expression contains partitioning keys
func checkExprContainPartitionKey(expr *Expr, partitionKeys map[string]int) bool {
	switch v := expr.Expr.(type) {
	case *plan.Expr_Col:
		if _, ok := partitionKeys[v.Col.Name]; ok {
			return true
		}
	case *plan.Expr_F:
		for _, arg := range v.F.GetArgs() {
			if checkExprContainPartitionKey(arg, partitionKeys) {
				return true
			}
		}
	}
	return false
}

func isAllColEqualConstExpr(exprs []*Expr) bool {
	for _, expr := range exprs {
		if !isExprColRefEqualConst(expr) {
			return false
		}
	}
	return true
}

func isAllLogicExpr(exprs []*Expr, funcName string) bool {
	for _, expr := range exprs {
		if !isLogicExpr(expr, funcName) {
			return false
		}
	}
	return true
}

func isSimpleExpr(expr *Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if !isFactorExpr(arg) {
				return false
			}
		}
	}
	return true
}

func isFactorExpr(expr *Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_Lit, *plan.Expr_Max, *plan.Expr_T:
		return true
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "cast" {
			return isFactorExpr(exprImpl.F.Args[0])
		} else {
			return false
		}
	default:
		return false
	}
}

func isLogicExpr(expr *Expr, funcName string) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == funcName {
			return true
		}
	}
	return false
}

func isExprColRefEqualConst(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "=" {
			if isColExpr(exprImpl.F.Args[0]) && isConstExpr(exprImpl.F.Args[1]) ||
				isConstExpr(exprImpl.F.Args[0]) && isColExpr(exprImpl.F.Args[1]) {
				return true
			}
		}
	}
	return false
}

func isColExpr(expr *plan.Expr) bool {
	switch expr.Expr.(type) {
	case *plan.Expr_Col:
		return true
	}
	return false
}

func isConstExpr(expr *plan.Expr) bool {
	switch expr.Expr.(type) {
	case *plan.Expr_Lit:
		return true
	}
	return false
}
