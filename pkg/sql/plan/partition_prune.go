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
	"fmt"
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
		analyzeFilters(builder.compCtx.GetProcess(), node)
	default:
		return
	}
}

func analyzeFilters(process *process.Process, node *Node) {
	// if target table does not has partition, return
	if node.TableDef.GetPartition() == nil || len(node.TableDef.GetPartition().Partitions) == 0 {
		return
	}
	if node.FilterList == nil || len(node.FilterList) == 0 {
		return
	}
	partitionPruneCondExactMatch(process, node.FilterList, node)
	return
}

func partitionPruneCondExactMatch(proc *process.Process, filterList []*Expr, node *Node) (bool, error) {
	// 1. Check if the components of the conjunctive normal form are simple equivalent expressions
	for _, expr := range filterList {
		isEqualValueComp := isExprWithOneColRefOneConst(expr)
		if isEqualValueComp {
			continue
		} else {
			return false, nil
		}
	}
	// 2. extract all colRef from filters
	cols := make(map[string]*plan.Expr)
	for _, filter := range node.FilterList {
		getAllCols(filter, cols)
	}

	for col := range cols {
		fmt.Println("+++>colFromFilter ", col)
	}

	partitionByDef := node.TableDef.Partition

	switch partitionByDef.Type {
	case plan.PartitionType_KEY, plan.PartitionType_LINEAR_KEY:
		partitionColumns := partitionByDef.PartitionColumns.GetPartitionColumns()
		partitionKeys := make(map[string]int)
		stringSliceToMap(partitionColumns, partitionKeys)

		if len(partitionKeys) != len(cols) {
			return false, nil
		} else {
			if !exprColsIncludePartKey(partitionKeys, cols) {
				return false, nil
			}

			//--------------------------------------------------------------------------------
			//step 5: evaluate the partition expr where the colRef assigned with const
			inputBat := batch.NewWithSize(len(node.TableDef.GetCols()))
			inputBat.SetRowCount(1)
			defer inputBat.Clean(proc.Mp())

			for i, colDef := range node.TableDef.GetCols() {
				if valueExpr, ok := cols[colDef.GetName()]; ok {
					colVec, err := colexec.EvalExpressionOnce(proc, valueExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					if err != nil {
						return false, nil
					}
					inputBat.SetVector(int32(i), colVec)
				} else {
					typ := types.New(types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale)
					colVec := vector.NewConstNull(typ, 1, proc.Mp())
					inputBat.SetVector(int32(i), colVec)
				}
			}

			resVec, err := colexec.EvalExpressionOnce(proc, partitionByDef.PartitionExpression, []*batch.Batch{inputBat})
			if err != nil {
				return false, nil
			}
			defer resVec.Free(proc.Mp())

			//step 7: prune the partition
			var partitionId int32
			if resVec.IsConstNull() {
				fmt.Println("***> partitionId is null")
				return false, err
			} else {
				partitionId = vector.MustFixedCol[int32](resVec)[0]
				fmt.Println("***> partitionId ", partitionId)

				node.PartitionPrune = &plan.PartitionPrune{
					IsPruned: true,
				}
				if partitionId != -1 {
					node.PartitionPrune.SelectedPartitions = make([]*plan.PartitionItem, 1)
					partitionItem := partitionByDef.Partitions[partitionId]
					node.PartitionPrune.SelectedPartitions[0] = &plan.PartitionItem{
						PartitionName:      partitionItem.PartitionName,
						OrdinalPosition:    partitionItem.OrdinalPosition,
						Description:        partitionItem.Description,
						Comment:            partitionItem.Comment,
						LessThan:           DeepCopyExprList(partitionItem.LessThan),
						InValues:           DeepCopyExprList(partitionItem.InValues),
						PartitionTableName: partitionItem.PartitionTableName,
					}
				}
			}
		}
	case plan.PartitionType_HASH, plan.PartitionType_LINEAR_HASH:
		//extractColumnsFromExpression
		partitionKeys := make(map[string]int)
		extractColumnsFromExpression(partitionByDef.PartitionExpr.Expr, partitionKeys)
		if len(partitionKeys) > len(cols) {
			return false, nil
		} else {
			if !exprColsIncludePartKey(partitionKeys, cols) {
				return false, nil
			}

			inputBat := batch.NewWithSize(len(node.TableDef.GetCols()))
			inputBat.SetRowCount(1)
			defer inputBat.Clean(proc.Mp())

			for i, colDef := range node.TableDef.GetCols() {
				if valueExpr, ok := cols[colDef.GetName()]; ok {
					colVec, err := colexec.EvalExpressionOnce(proc, valueExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					if err != nil {
						return false, nil
					}
					inputBat.SetVector(int32(i), colVec)
				} else {
					typ := types.New(types.T(colDef.Typ.Id), colDef.Typ.Width, colDef.Typ.Scale)
					colVec := vector.NewConstNull(typ, 1, proc.Mp())
					inputBat.SetVector(int32(i), colVec)
				}
			}

			resVec, err := colexec.EvalExpressionOnce(proc, partitionByDef.PartitionExpression, []*batch.Batch{inputBat})
			if err != nil {
				return false, nil
			}
			defer resVec.Free(proc.Mp())

			if resVec.IsConstNull() {
				fmt.Println("***> partitionId is null")
				return false, err
			} else {
				partitionId := vector.GetFixedAt[int32](resVec, 0)
				fmt.Println("***> partitionId ", partitionId)

				node.PartitionPrune = &plan.PartitionPrune{
					IsPruned: true,
				}

				if partitionId != -1 {
					node.PartitionPrune.SelectedPartitions = make([]*plan.PartitionItem, 1)
					partitionItem := partitionByDef.Partitions[partitionId]
					node.PartitionPrune.SelectedPartitions[0] = &plan.PartitionItem{
						PartitionName:      partitionItem.PartitionName,
						OrdinalPosition:    partitionItem.OrdinalPosition,
						Description:        partitionItem.Description,
						Comment:            partitionItem.Comment,
						LessThan:           DeepCopyExprList(partitionItem.LessThan),
						InValues:           DeepCopyExprList(partitionItem.InValues),
						PartitionTableName: partitionItem.PartitionTableName,
					}
				}
			}
		}

	case plan.PartitionType_LIST:
	case plan.PartitionType_LIST_COLUMNS:
	case plan.PartitionType_RANGE:
	case plan.PartitionType_RANGE_COLUMNS:
	}

	return true, nil
}

func partitionPruneCondRangeMatch(filterList []*Expr, partitionByDef *PartitionByDef) bool {
	return false
}

func partitionPruneCondListMatch(filterList []*Expr, partitionByDef *PartitionByDef) bool {
	return false
}

func partitionPruneCondCompositeConditions(filterList []*Expr, partitionByDef *PartitionByDef) bool {
	return false
}

// isExprWithOneColRefOneConst
func isExprWithOneColRefOneConst(expr *plan.Expr) bool {
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

func saveCol(expr *plan.Expr, value *plan.Expr, colRefs map[string]*plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefs[exprImpl.Col.Name] = value
	}
}

func getAllCols(expr *plan.Expr, colRefs map[string]*plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "=" {
			if isColRef(exprImpl.F.Args[0]) {
				saveCol(exprImpl.F.Args[0], exprImpl.F.Args[1], colRefs)
			} else if isColRef(exprImpl.F.Args[1]) {
				saveCol(exprImpl.F.Args[1], exprImpl.F.Args[0], colRefs)
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
