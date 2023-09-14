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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

func (builder *QueryBuilder) partitionPrune(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	for _, childID := range node.Children {
		builder.partitionPrune(childID)
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_EXTERNAL_SCAN:
		// if target table does not has partition, return
		if node.TableDef.GetPartition() == nil || len(node.TableDef.GetPartition().Partitions) == 0 {
			return
		}
		if node.FilterList == nil || len(node.FilterList) == 0 {
			return
		}
		// Analysis of Matching Partitioning and Filtering Conditions
		analyzePartKeyAndFilters(builder.compCtx.GetProcess(), node)
	default:
		return
	}
}

// Analyze partition expressions and filter conditions, and perform partition pruning
func analyzePartKeyAndFilters(process *process.Process, node *Node) {
	partitionByDef := node.TableDef.Partition
	switch partitionByDef.Type {
	case plan.PartitionType_KEY, plan.PartitionType_LINEAR_KEY:
		pruner := &KeyPartitionPruner{
			node:    node,
			process: process,
		}
		pruner.prune()
	case plan.PartitionType_HASH, plan.PartitionType_LINEAR_HASH:
		//partitionPruneCondExactMatch(process, node.FilterList, node)
		pruner := &HashPartitionPruner{
			node:    node,
			process: process,
		}
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
	return
}

// isExprColRefEqualConst
func isExprColRefEqualConst(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "=" {
			if isColRef(exprImpl.F.Args[0]) && isConst(exprImpl.F.Args[1]) ||
				isConst(exprImpl.F.Args[0]) && isColRef(exprImpl.F.Args[1]) {
				return true
			}
		}
	}
	return false
}

func isColRef(expr *plan.Expr) bool {
	switch expr.Expr.(type) {
	case *plan.Expr_Col:
		return true
	}
	return false
}

func isConst(expr *plan.Expr) bool {
	switch expr.Expr.(type) {
	case *plan.Expr_C:
		return true
	}
	return false
}

// Extracting Column Information from Equivalent Expressions
func extractCol2ValFromEqualExpr(expr *plan.Expr, colEqValMap map[string]*plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "=" {
			if isColRef(exprImpl.F.Args[0]) {
				exprCol := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
				colEqValMap[exprCol.Col.Name] = exprImpl.F.Args[1]
			} else if isColRef(exprImpl.F.Args[1]) {
				exprCol := exprImpl.F.Args[1].Expr.(*plan.Expr_Col)
				colEqValMap[exprCol.Col.Name] = exprImpl.F.Args[0]
			}
		}
	}
}

// exprColsIncludePartKey checks the partitioning key is included in the expression.
func exprColsIncludePartKey(partitionKeys map[string]int, exprCols map[string]*plan.Expr) bool {
	for key := range partitionKeys {
		if !keyIsInExprCols(key, exprCols) {
			return false
		}
	}
	return true
}

func keyIsInExprCols(c string, exprCols map[string]*plan.Expr) bool {
	for c1 := range exprCols {
		if strings.EqualFold(c, c1) {
			return true
		}
	}
	return false
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
	}
	return
}

type KeyPartitionPruner struct {
	conditions       []*Expr
	colEqValMap      map[string]*plan.Expr
	partitionKeysMap map[string]int
	partitionByDef   *plan.PartitionByDef
	node             *Node
	process          *process.Process
}

func (p *KeyPartitionPruner) init() {
	partitionByDef := p.node.TableDef.Partition
	p.partitionByDef = partitionByDef
	p.partitionKeysMap = make(map[string]int)
	for _, partitionCol := range partitionByDef.PartitionColumns.PartitionColumns {
		if _, ok := p.partitionKeysMap[partitionCol]; !ok {
			p.partitionKeysMap[partitionCol] = 1
		}
	}
}

func (p *KeyPartitionPruner) collectConditions() bool {
	// 1. Check if the components of the conjunctive normal form are simple equivalent expressions
	for _, expr := range p.node.GetFilterList() {
		if !isExprColRefEqualConst(expr) {
			return false
		}
	}
	// 2. extract all colRef to const value from filters
	colEqValMap := make(map[string]*plan.Expr)
	for _, filter := range p.node.FilterList {
		extractCol2ValFromEqualExpr(filter, colEqValMap)
	}

	p.colEqValMap = colEqValMap
	return true
}

func (p *KeyPartitionPruner) checkPartitionPruneMatch() bool {
	if len(p.partitionKeysMap) > len(p.colEqValMap) {
		return false
	} else {
		if !exprColsIncludePartKey(p.partitionKeysMap, p.colEqValMap) {
			return false
		}
	}
	return true
}

func (p *KeyPartitionPruner) tryPrunePartition() bool {
	// 1.evaluate the partition expr where the colRef assigned with const
	inputBat := batch.NewWithSize(len(p.node.TableDef.GetCols()))
	inputBat.SetRowCount(1)
	defer inputBat.Clean(p.process.Mp())

	for i, colDef := range p.node.TableDef.GetCols() {
		if valueExpr, ok := p.colEqValMap[colDef.GetName()]; ok {
			colVec, err := colexec.EvalExpressionOnce(p.process, valueExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			if err != nil {
				return false
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
		return false
	}
	defer resVec.Free(p.process.Mp())

	// 3. prune the partition
	var partitionId int32
	if resVec.IsConstNull() {
		return false
	} else {
		partitionId = vector.MustFixedCol[int32](resVec)[0]

		p.node.PartitionPrune = &plan.PartitionPrune{
			IsPruned: true,
		}
		if partitionId != -1 {
			p.node.PartitionPrune.SelectedPartitions = make([]*plan.PartitionItem, 1)
			partitionItem := p.partitionByDef.Partitions[partitionId]
			p.node.PartitionPrune.SelectedPartitions[0] = &plan.PartitionItem{
				PartitionName:      partitionItem.PartitionName,
				OrdinalPosition:    partitionItem.OrdinalPosition,
				Description:        partitionItem.Description,
				Comment:            partitionItem.Comment,
				LessThan:           DeepCopyExprList(partitionItem.LessThan),
				InValues:           DeepCopyExprList(partitionItem.InValues),
				PartitionTableName: partitionItem.PartitionTableName,
			}
		}
		return true
	}
}

func (p *KeyPartitionPruner) prune() bool {
	p.init()
	if !p.collectConditions() {
		return false
	}
	if !p.checkPartitionPruneMatch() {
		return false
	}
	if !p.tryPrunePartition() {
		return false
	}
	return true
}

type HashPartitionPruner struct {
	conditions       []*Expr
	colEqValMap      map[string]*plan.Expr
	partitionKeysMap map[string]int
	partitionByDef   *plan.PartitionByDef
	node             *Node
	process          *process.Process
}

func (p *HashPartitionPruner) init() {
	partitionByDef := p.node.TableDef.Partition
	p.partitionByDef = partitionByDef
	p.partitionKeysMap = make(map[string]int)
	extractColumnsFromExpression(partitionByDef.PartitionExpr.Expr, p.partitionKeysMap)
}

func (p *HashPartitionPruner) collectConditions() bool {
	// 1. Check if the components of the conjunctive normal form are simple equivalent expressions
	for _, expr := range p.node.GetFilterList() {
		if !isExprColRefEqualConst(expr) {
			return false
		}
	}
	// 2. extract all colRef to const value from filters
	colEqValMap := make(map[string]*plan.Expr)
	for _, filter := range p.node.FilterList {
		extractCol2ValFromEqualExpr(filter, colEqValMap)
	}
	p.colEqValMap = colEqValMap
	return true
}

func (p *HashPartitionPruner) checkPartitionPruneMatch() bool {
	if len(p.partitionKeysMap) > len(p.colEqValMap) {
		return false
	} else {
		if !exprColsIncludePartKey(p.partitionKeysMap, p.colEqValMap) {
			return false
		}
	}
	return true
}

func (p *HashPartitionPruner) tryPrunePartition() bool {
	inputBat := batch.NewWithSize(len(p.node.TableDef.GetCols()))
	inputBat.SetRowCount(1)
	defer inputBat.Clean(p.process.Mp())

	for i, colDef := range p.node.TableDef.GetCols() {
		if valueExpr, ok := p.colEqValMap[colDef.GetName()]; ok {
			colVec, err := colexec.EvalExpressionOnce(p.process, valueExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			if err != nil {
				return false
			}
			inputBat.SetVector(int32(i), colVec)
		} else {
			typ := types.New(types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale)
			colVec := vector.NewConstNull(typ, 1, p.process.Mp())
			inputBat.SetVector(int32(i), colVec)
		}
	}

	resVec, err := colexec.EvalExpressionOnce(p.process, p.partitionByDef.PartitionExpression, []*batch.Batch{inputBat})
	if err != nil {
		return false
	}
	defer resVec.Free(p.process.Mp())

	if resVec.IsConstNull() {
		return false
	} else {
		partitionId := vector.GetFixedAt[int32](resVec, 0)
		p.node.PartitionPrune = &plan.PartitionPrune{
			IsPruned: true,
		}

		if partitionId != -1 {
			p.node.PartitionPrune.SelectedPartitions = make([]*plan.PartitionItem, 1)
			partitionItem := p.partitionByDef.Partitions[partitionId]
			p.node.PartitionPrune.SelectedPartitions[0] = &plan.PartitionItem{
				PartitionName:      partitionItem.PartitionName,
				OrdinalPosition:    partitionItem.OrdinalPosition,
				Description:        partitionItem.Description,
				Comment:            partitionItem.Comment,
				LessThan:           DeepCopyExprList(partitionItem.LessThan),
				InValues:           DeepCopyExprList(partitionItem.InValues),
				PartitionTableName: partitionItem.PartitionTableName,
			}
		}
		return true
	}
}

func (p *HashPartitionPruner) prune() bool {
	p.init()
	if !p.collectConditions() {
		return false
	}
	if !p.checkPartitionPruneMatch() {
		return false
	}
	if !p.tryPrunePartition() {
		return false
	}
	return true
}
