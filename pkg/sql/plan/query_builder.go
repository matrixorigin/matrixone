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
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

func NewQueryBuilder(queryType plan.Query_StatementType, ctx CompilerContext, isPrepareStatement bool, skipStats bool) *QueryBuilder {
	//
	// There is a class of variables that controls SQL behavior.  To add such a variable, first
	// add it to the frontend/variables.go file.  Like the following sql_mode and agg_spill_mem.
	//
	// Once added, user can issue
	//     SET @@sql_mode = '';
	// then it will be effedctive within this session.   Note that if, sql result or plan is cached,
	// query does not go through this query builder and this won't be applied.
	//
	// sql_mode is easy -- it only needs to apply at plan time.
	//
	// agg_spill_mem is more complicated.  It needs to apply at run time, well, we call it "compile time",
	// and exec time, but anyway, we need to send agg_spill_mem to multi CNs.
	// See plan.proto and pipeline.proto.   Of course, you still need to change code to copy this value.
	//
	// Our terminology is a total mess.  Session, Process, Plan time, Compile time, Scope, Dispatch,
	// FUBAR.

	var mysqlCompatible bool

	mode, err := ctx.ResolveVariable("sql_mode", true, false)
	if err == nil {
		if modeStr, ok := mode.(string); ok {
			if !strings.Contains(modeStr, "ONLY_FULL_GROUP_BY") {
				mysqlCompatible = true
			}
		}
	}

	var aggSpillMem int64
	aggSpillMemInt, err := ctx.ResolveVariable("agg_spill_mem", true, false)
	if err == nil {
		if aggSpillMemVal, ok := aggSpillMemInt.(int64); ok {
			aggSpillMem = aggSpillMemVal
		}
	}

	var maxDop int64
	maxDopInt, err := ctx.ResolveVariable("max_dop", true, false)
	if err == nil {
		if maxDopVal, ok := maxDopInt.(int64); ok {
			maxDop = maxDopVal
		}
	}

	return &QueryBuilder{
		qry: &Query{
			StmtType: queryType,
			MaxDop:   int64(maxDop),
		},
		compCtx:              ctx,
		ctxByNode:            []*BindContext{},
		nameByColRef:         make(map[[2]int32]string),
		protectedScans:       make(map[int32]int),
		projectSpecialGuards: make(map[int32]*specialIndexGuard),
		nextBindTag:          0,
		mysqlCompatible:      mysqlCompatible,
		aggSpillMem:          aggSpillMem,
		tag2Table:            make(map[int32]*TableDef),
		tag2NodeID:           make(map[int32]int32),
		isPrepareStatement:   isPrepareStatement,
		deleteNode:           make(map[uint64]int32),
		skipStats:            skipStats,
		optimizationHistory:  make([]string, 0),
	}
}

// buildRemapErrorMessage constructs a human-readable error message for column remapping failures.
// This function provides detailed context to help debug issues where a column reference cannot
// be found in the remapping context.
//
// Example error scenario:
//   - A filter expression like "o.id = cast(d.id AS INT)" is incorrectly deduced during join
//   - The deduced predicate "m.id = cast(d.id AS INT)" references d.id which is not available
//   - in the m-side context, causing remap failure
//
// Debugging tips:
//  1. Check if the expression contains columns from multiple tables (cross-table reference)
//  2. Verify that predicate deduction (deduceNewFilterList) is correct
//  3. Ensure column pruning (remapAllColRefs) hasn't removed necessary columns
//  4. Review the node type and tip to understand the remapping context
//
// Parameters:
//   - missingCol: The column reference that cannot be found [RelPos, ColPos]
//   - colName: The name of the missing column (for readability)
//   - colMap: The available column mapping in the current context
//   - remapInfo: Context information about the remapping operation
//   - builder: QueryBuilder instance for accessing column name mappings
//
// Returns:
//
//	A formatted error message string with:
//	- Clear error description
//	- Missing column information (name and position)
//	- Available columns list (formatted)
//	- Related expression (if available)
//	- Node type and context information
func (builder *QueryBuilder) buildRemapErrorMessage(
	missingCol [2]int32,
	colName string,
	colMap map[[2]int32][2]int32,
	remapInfo *RemapInfo,
) string {
	var sb strings.Builder

	// Header: Error description
	sb.WriteString("Column remapping failed: cannot find column reference\n")

	// Missing column information
	sb.WriteString("❌ Missing Column:\n")
	sb.WriteString(fmt.Sprintf("   Position: [RelPos=%d, ColPos=%d]\n", missingCol[0], missingCol[1]))
	sb.WriteString(fmt.Sprintf("   Name: %s\n", colName))
	sb.WriteString("\n")

	// Context information
	if remapInfo != nil && remapInfo.node != nil {
		sb.WriteString("📍 Context:\n")
		sb.WriteString(fmt.Sprintf("   Step: %d\n", remapInfo.step))
		sb.WriteString(fmt.Sprintf("   Node ID: %d\n", remapInfo.node.NodeId))
		sb.WriteString(fmt.Sprintf("   Node Type: %s\n", remapInfo.node.NodeType))
		if remapInfo.tip != "" {
			sb.WriteString(fmt.Sprintf("   Tip: %s\n", remapInfo.tip))
		}
		if remapInfo.srcExprIdx >= 0 {
			sb.WriteString(fmt.Sprintf("   Expression Index: %d\n", remapInfo.srcExprIdx))
		}
		sb.WriteString("\n")

		// Related expression
		var exprStr string
		if remapInfo.node != nil {
			var expr *plan.Expr
			switch remapInfo.node.NodeType {
			case plan.Node_FILTER:
				if remapInfo.srcExprIdx >= 0 && remapInfo.srcExprIdx < len(remapInfo.node.FilterList) {
					expr = remapInfo.node.FilterList[remapInfo.srcExprIdx]
				}
			case plan.Node_PROJECT:
				if remapInfo.srcExprIdx >= 0 && remapInfo.srcExprIdx < len(remapInfo.node.ProjectList) {
					expr = remapInfo.node.ProjectList[remapInfo.srcExprIdx]
				}
			case plan.Node_AGG:
				if remapInfo.srcExprIdx >= 0 && remapInfo.srcExprIdx < len(remapInfo.node.AggList) {
					expr = remapInfo.node.AggList[remapInfo.srcExprIdx]
				}
			}
			if expr != nil {
				// Format the expression using FormatExpr for better readability
				option := FormatOption{
					ExpandVec:       false,
					ExpandVecMaxLen: 0,
					MaxDepth:        7, // Limit depth to avoid overly long output
				}
				exprStr = FormatExpr(expr, option)
			}
		}
		if exprStr != "" {
			sb.WriteString("🔍 Related Expression:\n")
			sb.WriteString(fmt.Sprintf("   %s\n", exprStr))
			sb.WriteString("\n")
		}
	}

	// Optimization history
	if len(builder.optimizationHistory) > 0 {
		sb.WriteString("🔧 Optimization History:\n")
		for _, hist := range builder.optimizationHistory {
			sb.WriteString(fmt.Sprintf("   - %s\n", hist))
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString("🔧 Optimization History: (no associatelaw applied)\n\n")
	}

	// Available columns
	if len(colMap) > 0 {
		sb.WriteString("✅ Available Columns in Context:\n")
		var keyPairs []string
		for k := range colMap {
			name := builder.nameByColRef[k]
			if name == "" {
				name = "<unknown>"
			}
			keyPairs = append(keyPairs, fmt.Sprintf("   [RelPos=%d, ColPos=%d] -> %s", k[0], k[1], name))
		}
		slices.Sort(keyPairs)
		for _, pair := range keyPairs {
			sb.WriteString(pair)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	} else {
		sb.WriteString("⚠️  No columns available in context\n\n")
	}

	return sb.String()
}

func (builder *QueryBuilder) remapSingleColRef(col *plan.ColRef, colMap map[[2]int32][2]int32, remapInfo *RemapInfo) error {
	mapID := [2]int32{col.RelPos, col.ColPos}
	if mappedCol, ok := colMap[mapID]; ok {
		col.RelPos = mappedCol[0]
		col.ColPos = mappedCol[1]
		col.Name = builder.nameByColRef[mapID]
	} else {
		colName := "<unknown>"
		if builder != nil {
			colName = builder.nameByColRef[mapID]
		}
		errorMsg := builder.buildRemapErrorMessage(mapID, colName, colMap, remapInfo)
		return moerr.NewParseErrorf(builder.GetContext(), "%s", errorMsg)
	}
	return nil
}

func (builder *QueryBuilder) remapColRefForExpr(expr *Expr, colMap map[[2]int32][2]int32, remapInfo *RemapInfo) error {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		return builder.remapSingleColRef(ne.Col, colMap, remapInfo)

	case *plan.Expr_F:
		for _, arg := range ne.F.GetArgs() {
			err := builder.remapColRefForExpr(arg, colMap, remapInfo)
			if err != nil {
				return err
			}
		}
	case *plan.Expr_W:
		err := builder.remapColRefForExpr(ne.W.WindowFunc, colMap, remapInfo)
		if err != nil {
			return err
		}
		for _, order := range ne.W.OrderBy {
			err = builder.remapColRefForExpr(order.Expr, colMap, remapInfo)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type ColRefRemapping struct {
	globalToLocal map[[2]int32][2]int32
	localToGlobal [][2]int32
}

func (m *ColRefRemapping) addColRef(colRef [2]int32) {
	m.globalToLocal[colRef] = [2]int32{0, int32(len(m.localToGlobal))}
	m.localToGlobal = append(m.localToGlobal, colRef)
}

func (m *ColRefRemapping) String() string {
	if m == nil {
		return "empty ColRefRemapping"
	}
	sb := strings.Builder{}
	sb.WriteString("ColRefRemapping{")
	sb.WriteString("globalToLocal")
	for k, v := range m.globalToLocal {
		sb.WriteString(fmt.Sprintf("[%v : %v]", k, v))
	}
	sb.WriteString("localToGlobal")
	for k, v := range m.localToGlobal {
		sb.WriteString(fmt.Sprintf("[%v : %v]", k, v))
	}
	sb.WriteString("}")
	return sb.String()
}

// XXX: It's dangerous to copy binding/message tags if both old and new nodes are eventually used.
func (builder *QueryBuilder) copyNode(ctx *BindContext, nodeId int32) int32 {
	node := builder.qry.Nodes[nodeId]
	newNode := DeepCopyNode(node)
	newNode.Children = make([]int32, 0, len(node.Children))
	for _, child := range node.Children {
		newNode.Children = append(newNode.Children, builder.copyNode(ctx, child))
	}
	newNodeId := builder.appendNode(newNode, ctx)
	return newNodeId
}

func (builder *QueryBuilder) remapAllColRefs(nodeID int32, step int32, colRefCnt map[[2]int32]int, colRefBool map[[2]int32]bool, sinkColRef map[[2]int32]int) (*ColRefRemapping, error) {
	node := builder.qry.Nodes[nodeID]

	remapping := &ColRefRemapping{
		globalToLocal: make(map[[2]int32][2]int32),
	}
	remapInfo := RemapInfo{
		step:       step,
		node:       node,
		colRefCnt:  colRefCnt,
		colRefBool: colRefBool,
		sinkColRef: sinkColRef,
		remapping:  remapping,
	}

	switch node.NodeType {
	case plan.Node_FUNCTION_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := DeepCopyTableDef(node.TableDef, false)

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)

			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(col))
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(node.TableDef.Cols[0]))
		}

		node.TableDef = newTableDef

		remapInfo.tip = "FilterList"
		remapInfo.interRemapping = internalRemapping
		for idx, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, internalRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
				continue
			}

			remapping.addColRef(internalRemapping.localToGlobal[i])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   col.Name,
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.TableDef.Cols) == 0 {
				globalRef := [2]int32{tag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   node.TableDef.Cols[0].Name,
						},
					},
				})
			} else {
				remapping.addColRef(internalRemapping.localToGlobal[0])
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   node.TableDef.Cols[0].Name,
						},
					},
				})
			}
		}

		if len(node.Children) == 0 {
			break
		}

		childId := node.Children[0]
		for _, expr := range node.TblFuncExprList {
			increaseRefCnt(expr, 1, colRefCnt)
		}
		childMap, err := builder.remapAllColRefs(childId, step, colRefCnt, colRefBool, sinkColRef)

		if err != nil {
			return nil, err
		}

		remapInfo.tip = "TblFuncExprList"
		remapInfo.interRemapping = internalRemapping
		for idx, expr := range node.TblFuncExprList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err = builder.remapColRefForExpr(expr, childMap.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN,
		plan.Node_EXTERNAL_SCAN, plan.Node_SOURCE_SCAN, plan.Node_TABLE_CLONE:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, expr := range node.BlockFilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, rfSpec := range node.RuntimeFilterProbeList {
			if rfSpec.Expr != nil {
				increaseRefCnt(rfSpec.Expr, 1, colRefCnt)
			}
		}

		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, 1, colRefCnt)
		}

		if node.IndexReaderParam != nil {
			for _, orderBy := range node.IndexReaderParam.OrderBy {
				increaseRefCnt(orderBy.Expr, 1, colRefCnt)
			}
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
			localToGlobal: make([][2]int32, 0),
		}

		colTag := node.BindingTags[0]
		newTableDef := DeepCopyTableDef(node.TableDef, false)

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{colTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)
			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(col))
		}

		if len(node.BindingTags) > 1 {
			orderFuncTag := node.BindingTags[1]
			internalRemapping.addColRef([2]int32{orderFuncTag, 0})

		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{colTag, 0})
			newTableDef.Cols = append(newTableDef.Cols, DeepCopyColDef(node.TableDef.Cols[0]))
		}

		node.TableDef = newTableDef

		colMap := maps.Clone(internalRemapping.globalToLocal)
		for localIdx, global := range internalRemapping.localToGlobal {
			colMap[[2]int32{0, int32(localIdx)}] = global
		}

		remapInfo.tip = "FilterList"
		remapInfo.interRemapping = internalRemapping
		for idx, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, colMap, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		remapInfo.tip = "BlockFilterList"
		for idx, expr := range node.BlockFilterList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, colMap, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		remapInfo.tip = "RuntimeFilterProbeList"
		for idx, rfSpec := range node.RuntimeFilterProbeList {
			if rfSpec.Expr != nil {
				increaseRefCnt(rfSpec.Expr, -1, colRefCnt)
				remapInfo.srcExprIdx = idx
				err := builder.remapColRefForExpr(rfSpec.Expr, colMap, &remapInfo)
				if err != nil {
					return nil, err
				}
				col := rfSpec.Expr.GetCol()
				if len(col.Name) == 0 {
					col.Name = node.TableDef.Cols[col.ColPos].Name
				}
			}
		}

		remapInfo.tip = "OrderBy"
		for idx, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(orderBy.Expr, colMap, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		remapInfo.tip = "IndexOrderBy"
		if node.IndexReaderParam != nil {
			for idx, orderBy := range node.IndexReaderParam.OrderBy {
				increaseRefCnt(orderBy.Expr, -1, colRefCnt)
				remapInfo.srcExprIdx = idx
				err := builder.remapColRefForExpr(orderBy.Expr, colMap, &remapInfo)
				if err != nil {
					return nil, err
				}
			}
		}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
				continue
			}

			remapping.addColRef(internalRemapping.localToGlobal[i])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[internalRemapping.localToGlobal[i]],
					},
				},
			})
		}

		if len(node.BindingTags) > 1 {
			orderFuncTag := node.BindingTags[1]

			if colRefCnt[[2]int32{orderFuncTag, 0}] > 0 {
				remapping.addColRef([2]int32{orderFuncTag, 0})

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.IndexReaderParam.OrderBy[0].Expr.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(len(node.TableDef.Cols)),
							Name:   "__dist_func__",
						},
					},
				})
			}
		}

		if len(node.ProjectList) == 0 && len(node.TableDef.Cols) > 0 {
			remapping.addColRef(internalRemapping.localToGlobal[0])
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.TableDef.Cols[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   builder.nameByColRef[internalRemapping.localToGlobal[0]],
					},
				},
			})
		}

	case plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
		plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_MINUS, plan.Node_MINUS_ALL:

		thisTag := node.BindingTags[0]
		leftID := node.Children[0]
		rightID := node.Children[1]
		for i, expr := range node.ProjectList {
			increaseRefCnt(expr, 1, colRefCnt)
			globalRef := [2]int32{thisTag, int32(i)}
			remapping.addColRef(globalRef)
		}

		rightNode := builder.qry.Nodes[rightID]
		if rightNode.NodeType == plan.Node_PROJECT {
			projectTag := rightNode.BindingTags[0]
			for i := range rightNode.ProjectList {
				increaseRefCnt(&plan.Expr{
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: projectTag,
							ColPos: int32(i),
						},
					}}, 1, colRefCnt)
			}
		}

		leftRemapping, err := builder.remapAllColRefs(leftID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		_, err = builder.remapAllColRefs(rightID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		remapInfo.tip = "ProjectList"
		for idx, expr := range node.ProjectList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, leftRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_JOIN:
		for _, expr := range node.OnList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		if node.DedupJoinCtx != nil {
			for _, col := range node.DedupJoinCtx.OldColList {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]++
			}

			for _, expr := range node.DedupJoinCtx.UpdateColExprList {
				increaseRefCnt(expr, 1, colRefCnt)
			}
		}

		leftID := node.Children[0]
		leftRemapping, err := builder.remapAllColRefs(leftID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		internalMap := maps.Clone(leftRemapping.globalToLocal)

		rightID := node.Children[1]
		rightRemapping, err := builder.remapAllColRefs(rightID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for k, v := range rightRemapping.globalToLocal {
			internalMap[k] = [2]int32{1, v[1]}
		}

		remapInfo.tip = "OnList"
		for idx, expr := range node.OnList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, internalMap, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		remapInfo.tip = "DedupJoinCtx"
		if node.DedupJoinCtx != nil {
			for i, col := range node.DedupJoinCtx.OldColList {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]--
				err := builder.remapSingleColRef(&node.DedupJoinCtx.OldColList[i], internalMap, &remapInfo)
				if err != nil {
					return nil, err
				}
			}

			for idx, expr := range node.DedupJoinCtx.UpdateColExprList {
				increaseRefCnt(expr, -1, colRefCnt)
				remapInfo.srcExprIdx = idx
				err := builder.remapColRefForExpr(expr, internalMap, &remapInfo)
				if err != nil {
					return nil, err
				}
			}
		}

		if node.JoinType == plan.Node_DEDUP && len(node.DedupColTypes) == 0 {
			node.DedupColTypes = []plan.Type{node.OnList[0].GetF().Args[0].Typ}
		}

		childProjList := builder.qry.Nodes[leftID].ProjectList
		for i, globalRef := range leftRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)
			if node.JoinType == plan.Node_RIGHT {
				childProjList[i].Typ.NotNullable = false
			}
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if node.JoinType == plan.Node_MARK {
			globalRef := [2]int32{node.BindingTags[0], 0}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: false,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		} else {
			childProjList = builder.qry.Nodes[rightID].ProjectList
			for i, globalRef := range rightRemapping.localToGlobal {
				if colRefCnt[globalRef] == 0 {
					continue
				}

				remapping.addColRef(globalRef)

				if node.JoinType == plan.Node_LEFT {
					childProjList[i].Typ.NotNullable = false
				}

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: childProjList[i].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(i),
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			}
		}

		if len(node.ProjectList) == 0 && len(leftRemapping.localToGlobal) > 0 {
			globalRef := leftRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: builder.qry.Nodes[leftID].ProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_AGG:
		for _, expr := range node.GroupBy {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, expr := range node.AggList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]
		groupSize := int32(len(node.GroupBy))

		for _, expr := range node.FilterList {
			builder.remapHavingClause(expr, groupTag, aggregateTag, groupSize)
		}

		remapInfo.tip = "GroupBy"
		for idx, expr := range node.GroupBy {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		remapInfo.tip = "AggList"
		for idx, expr := range node.AggList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{aggregateTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: int32(idx) + groupSize,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if groupSize > 0 {
				globalRef := [2]int32{groupTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.GroupBy[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			} else {
				globalRef := [2]int32{aggregateTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.AggList[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -2,
							ColPos: 0,
							Name:   builder.nameByColRef[globalRef],
						},
					},
				})
			}
		}

		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_TABLE_SCAN && len(child.FilterList) == 0 && len(node.GroupBy) == 0 && child.Limit == nil && child.Offset == nil {
			child.AggList = make([]*Expr, 0, len(node.AggList))
			for _, agg := range node.AggList {
				switch agg.GetF().Func.ObjName {
				case "starcount", "count", "min", "max":
					child.AggList = append(child.AggList, DeepCopyExpr(agg))
				default:
					child.AggList = nil
				}
				if child.AggList == nil {
					break
				}
			}
		}

	case plan.Node_SAMPLE:
		groupTag := node.BindingTags[0]
		sampleTag := node.BindingTags[1]
		increaseRefCntForExprList(node.GroupBy, 1, colRefCnt)
		increaseRefCntForExprList(node.AggList, 1, colRefCnt)

		// the result order of sample will follow [group by columns, sample columns, other columns].
		// and the projection list needs to be based on the result order.
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			builder.remapHavingClause(expr, groupTag, sampleTag, int32(len(node.GroupBy)))
		}

		remapInfo.tip = "GroupBy"
		// deal with group col and sample col.
		for i, expr := range node.GroupBy {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = i
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(len(node.ProjectList)),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		offsetSize := int32(len(node.GroupBy))
		remapInfo.tip = "AggList"
		for i, expr := range node.AggList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = i
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{sampleTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: int32(i) + offsetSize,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		offsetSize += int32(len(node.AggList))
		childProjectionList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjectionList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i) + offsetSize,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_TIME_WINDOW:
		for _, expr := range node.AggList {
			increaseRefCnt(expr, 1, colRefCnt)
		}
		increaseRefCnt(node.OrderBy[0].Expr, 1, colRefCnt)

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		timeTag := node.BindingTags[0]
		groupTag := node.BindingTags[1]

		// order by
		idx := 0
		increaseRefCnt(node.OrderBy[0].Expr, -1, colRefCnt)
		remapInfo.tip = "OrderBy[0].Expr"
		remapInfo.srcExprIdx = 0
		err = builder.remapColRefForExpr(node.OrderBy[0].Expr, childRemapping.globalToLocal, &remapInfo)
		if err != nil {
			return nil, err
		}
		globalRef := [2]int32{groupTag, int32(0)}
		if colRefCnt[globalRef] != 0 {
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.OrderBy[0].Expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
			idx++
		}

		var wstart, wend *plan.Expr
		var i, j int
		remapInfo.tip = "AggList"
		for k, expr := range node.AggList {
			if e, ok := expr.Expr.(*plan.Expr_Col); ok {
				if e.Col.Name == TimeWindowStart {
					wstart = expr
					i = k
				}
				if e.Col.Name == TimeWindowEnd {
					wend = expr
					j = k
				}
				continue
			}
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = k
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{timeTag, int32(k)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
			idx++
		}

		if wstart != nil {
			increaseRefCnt(wstart, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				break
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.Timestamp.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
			idx++
		}

		if wend != nil {
			increaseRefCnt(wend, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(j)}
			if colRefCnt[globalRef] == 0 {
				break
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.Timestamp.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_WINDOW:
		for _, expr := range node.WinSpecList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		// remap children node
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		// append children projection list
		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		windowTag := node.BindingTags[0]
		l := len(childProjList)

		// In the window function node,
		// the filtering conditions also need to be remapped
		for _, expr := range node.FilterList {
			// get col pos from remap info
			err = builder.remapWindowClause(
				expr,
				windowTag,
				int32(l),
				childRemapping.globalToLocal,
				&remapInfo)
			if err != nil {
				return nil, err
			}
		}

		// remap all window function
		remapInfo.tip = "WinSpecList"
		for idx, expr := range node.WinSpecList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{windowTag, node.GetWindowIdx()}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(l),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_FILL:

		for _, expr := range node.FillVal {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FillVal {
			increaseRefCnt(expr, -1, colRefCnt)
			err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		//for _, expr := range node.AggList {
		//	increaseRefCnt(expr, -1, colRefCnt)
		//	err = builder.remapColRefForExpr(expr, childRemapping.globalToLocal)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

	case plan.Node_SORT, plan.Node_PARTITION:
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		remapInfo.tip = "OrderBy"
		for idx, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(orderBy.Expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 && len(childRemapping.localToGlobal) > 0 {
			globalRef := childRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_FILTER:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		remapInfo.tip = "FilterList"
		for idx, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		tag := node.BindingTags[0]
		var newProjList []*plan.Expr

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			newProjList = append(newProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
			remapping.addColRef(globalRef)
			for _, sourceStep := range node.SourceStep {
				if sourceStep >= step {
					continue
				}
				colRefBool[[2]int32{sourceStep, int32(i)}] = true
			}

		}
		node.ProjectList = newProjList

	case plan.Node_SINK:
		childNode := builder.qry.Nodes[node.Children[0]]
		resultTag := childNode.BindingTags[0]
		for i := range childNode.ProjectList {
			if colRefBool[[2]int32{step, int32(i)}] {
				colRefCnt[[2]int32{resultTag, int32(i)}] = 1
			}
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}
		var newProjList []*plan.Expr
		for i, expr := range node.ProjectList {
			if !colRefBool[[2]int32{step, int32(i)}] {
				continue
			}
			sinkColRef[[2]int32{step, int32(i)}] = len(newProjList)
			newProjList = append(newProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: 0,
						ColPos: childRemapping.globalToLocal[[2]int32{resultTag, int32(i)}][1],
						// Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		node.ProjectList = newProjList

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]

		var neededProj []int32

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			neededProj = append(neededProj, int32(i))
			increaseRefCnt(expr, 1, colRefCnt)
		}

		if len(neededProj) == 0 {
			increaseRefCnt(node.ProjectList[0], 1, colRefCnt)
			neededProj = append(neededProj, 0)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		var newProjList []*plan.Expr
		remapInfo.tip = "neededProj"
		for idx, needed := range neededProj {
			expr := node.ProjectList[needed]
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, childRemapping.globalToLocal, &remapInfo)
			if err != nil {
				return nil, err
			}

			switch ne := expr.Expr.(type) {
			case *plan.Expr_Col:
				expr.Typ.NotNullable = childProjList[ne.Col.ColPos].Typ.NotNullable
			}

			globalRef := [2]int32{projectTag, needed}
			remapping.addColRef(globalRef)

			newProjList = append(newProjList, expr)
		}

		node.ProjectList = newProjList

	case plan.Node_DISTINCT:
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		// Rewrite DISTINCT to AGG
		node.NodeType = plan.Node_AGG
		preNode := builder.qry.Nodes[node.Children[0]]
		node.GroupBy = make([]*Expr, len(preNode.ProjectList))
		node.ProjectList = make([]*Expr, len(preNode.ProjectList))
		node.SpillMem = builder.aggSpillMem

		for i, prjExpr := range preNode.ProjectList {
			node.GroupBy[i] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			}

			node.ProjectList[i] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(i),
					},
				},
			}
		}

		remapping = childRemapping

	case plan.Node_VALUE_SCAN:
		node.NotCacheable = true
		// VALUE_SCAN always have one column now
		if node.TableDef == nil { // like select 1,2
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}},
			})
		} else {
			tag := node.BindingTags[0]
			newCols := make([]*plan.ColDef, 0, len(node.TableDef.Cols))
			newData := make([]*plan.ColData, 0, len(node.RowsetData.Cols))

			for i, col := range node.TableDef.Cols {
				globalRef := [2]int32{tag, int32(i)}
				if colRefCnt[globalRef] == 0 {
					continue
				}

				remapping.addColRef(globalRef)

				newCols = append(newCols, node.TableDef.Cols[i])
				newData = append(newData, node.RowsetData.Cols[i])
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(len(node.ProjectList)),
							Name:   col.Name,
						},
					},
				})
			}

			node.TableDef.Cols = newCols
			node.RowsetData.Cols = newData
		}

	case plan.Node_LOCK_OP:
		preNode := builder.qry.Nodes[node.Children[0]]

		var pkExprs []*plan.Expr
		var oldPkPos [][2]int32
		var partitionExprs []*plan.Expr
		var oldPartitionPos [][2]int32
		for _, lockTarget := range node.LockTargets {
			if lockTarget.HasPartitionCol {
				partitionExpr := &plan.Expr{
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: lockTarget.PrimaryColRelPos,
							ColPos: lockTarget.PartitionColIdxInBat,
						},
					},
				}
				increaseRefCnt(partitionExpr, 1, colRefCnt)
				partitionExprs = append(partitionExprs, partitionExpr)
				oldPartitionPos = append(oldPartitionPos, [2]int32{lockTarget.PrimaryColRelPos, lockTarget.PartitionColIdxInBat})
			} else {
				partitionExprs = append(partitionExprs, nil)
				oldPartitionPos = append(oldPartitionPos, [2]int32{})
			}
			pkExpr := &plan.Expr{
				// Typ: node.LockTargets[0].GetPrimaryColTyp(),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: lockTarget.PrimaryColRelPos,
						ColPos: lockTarget.PrimaryColIdxInBat,
					},
				},
			}
			increaseRefCnt(pkExpr, 1, colRefCnt)
			pkExprs = append(pkExprs, pkExpr)
			oldPkPos = append(oldPkPos, [2]int32{lockTarget.PrimaryColRelPos, lockTarget.PrimaryColIdxInBat})
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for idx, lockTarget := range node.LockTargets {
			if lockTarget.HasPartitionCol {
				if newPos, ok := childRemapping.globalToLocal[oldPartitionPos[idx]]; ok {
					lockTarget.PartitionColIdxInBat = newPos[1]
				}
				increaseRefCnt(partitionExprs[idx], -1, colRefCnt)
			}
			if newPos, ok := childRemapping.globalToLocal[oldPkPos[idx]]; ok {
				lockTarget.PrimaryColRelPos = newPos[0]
				lockTarget.PrimaryColIdxInBat = newPos[1]
			}
			increaseRefCnt(pkExprs[idx], -1, colRefCnt)
		}

		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: preNode.ProjectList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_APPLY:
		internalMap := make(map[[2]int32][2]int32)

		right := builder.qry.Nodes[node.Children[1]]
		rightTag := right.BindingTags[0]

		for _, expr := range right.TblFuncExprList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		leftID := node.Children[0]
		leftRemapping, err := builder.remapAllColRefs(leftID, step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		childProjList := builder.qry.Nodes[leftID].ProjectList
		for i, globalRef := range leftRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		for i, col := range right.TableDef.Cols {
			globalRef := [2]int32{rightTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		for idx, expr := range right.TblFuncExprList {
			increaseRefCnt(expr, -1, colRefCnt)
			remapInfo.srcExprIdx = idx
			err := builder.remapColRefForExpr(expr, internalMap, &remapInfo)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_INSERT, plan.Node_DELETE:
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_MULTI_UPDATE:
		for _, updateCtx := range node.UpdateCtxList {
			for _, col := range updateCtx.InsertCols {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]++
			}

			for _, col := range updateCtx.DeleteCols {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]++
			}

			for _, col := range updateCtx.PartitionCols {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]++
			}
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		remapInfo.tip = "UpdateCtxList"
		for idx, updateCtx := range node.UpdateCtxList {
			remapInfo.srcExprIdx = idx
			for i, col := range updateCtx.InsertCols {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]--
				err := builder.remapSingleColRef(&updateCtx.InsertCols[i], childRemapping.globalToLocal, &remapInfo)
				if err != nil {
					return nil, err
				}
			}

			for i, col := range updateCtx.DeleteCols {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]--
				err := builder.remapSingleColRef(&updateCtx.DeleteCols[i], childRemapping.globalToLocal, &remapInfo)
				if err != nil {
					return nil, err
				}
			}

			for i, col := range updateCtx.PartitionCols {
				colRefCnt[[2]int32{col.RelPos, col.ColPos}]--
				err := builder.remapSingleColRef(&updateCtx.PartitionCols[i], childRemapping.globalToLocal, &remapInfo)
				if err != nil {
					return nil, err
				}
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_PRE_INSERT:
		childRemapping, err := builder.remapAllColRefs(node.Children[0], step, colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   builder.nameByColRef[globalRef],
					},
				},
			})
		}

		if node.PreInsertCtx.CompPkeyExpr != nil {
			globalRef := [2]int32{node.BindingTags[0], 0}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.PreInsertCtx.CompPkeyExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
					},
				},
			})
		} else if node.PreInsertCtx.ClusterByExpr != nil {
			globalRef := [2]int32{node.BindingTags[0], 0}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.PreInsertCtx.ClusterByExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
					},
				},
			})
		}

	default:
		return nil, moerr.NewInternalError(builder.GetContext(), "unsupport node type")
	}

	node.BindingTags = nil

	return remapping, nil
}

func (builder *QueryBuilder) markSinkProject(nodeID int32, step int32, colRefBool map[[2]int32]bool) {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		for _, i := range node.SourceStep {
			if i >= step {
				for _, expr := range node.ProjectList {
					colRefBool[[2]int32{i, expr.GetCol().ColPos}] = true
				}
			}
		}
	default:
		for i := range node.Children {
			builder.markSinkProject(node.Children[i], step, colRefBool)
		}
	}
}

func (builder *QueryBuilder) rewriteStarApproxCount(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_AGG:
		if len(node.GroupBy) == 0 && len(node.AggList) == 1 {
			if agg := node.AggList[0].GetF(); agg != nil && agg.Func.ObjName == "approx_count" {
				if len(node.Children) == 1 {
					child := builder.qry.Nodes[node.Children[0]]
					if child.NodeType == plan.Node_TABLE_SCAN && len(child.FilterList) == 0 {
						agg.Func.ObjName = "sum"
						fr, _ := function.GetFunctionByName(context.TODO(), "sum", []types.Type{types.T_int64.ToType()})
						agg.Func.Obj = fr.GetEncodedOverloadID()
						agg.Args[0] = &plan.Expr{
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: 0,
									ColPos: Metadata_Rows_Cnt_Pos,
								},
							},
						}

						var exprs []*plan.Expr
						str := child.ObjRef.SchemaName + "." + child.TableDef.Name
						exprs = append(exprs, &plan.Expr{
							Typ: Type{
								Id:          int32(types.T_varchar),
								NotNullable: true,
								Width:       int32(len(str)),
							},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Sval{
										Sval: str,
									},
								},
							},
						})
						str = child.TableDef.Cols[0].Name
						exprs = append(exprs, &plan.Expr{
							Typ: Type{
								Id:          int32(types.T_varchar),
								NotNullable: true,
								Width:       int32(len(str)),
							},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_Sval{
										Sval: str,
									},
								},
							},
						})
						scanNode := &plan.Node{
							NodeType: plan.Node_VALUE_SCAN,
						}
						childId := builder.appendNode(scanNode, nil)
						children := []int32{childId}
						node.Children[0] = builder.buildMetadataScan(nil, nil, exprs, children)
						child = builder.qry.Nodes[node.Children[0]]
						switch expr := agg.Args[0].Expr.(type) {
						case *plan.Expr_Col:
							expr.Col.RelPos = child.BindingTags[0]
							agg.Args[0].Typ = child.TableDef.Cols[expr.Col.ColPos].Typ
						}
					}
				}
			}
		}
	default:
		for i := range node.Children {
			builder.rewriteStarApproxCount(node.Children[i])
		}
	}
}

func (builder *QueryBuilder) removeUnnecessaryProjections(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]
	if len(node.Children) == 0 {
		return nodeID
	}

	for i, childID := range node.Children {
		node.Children[i] = builder.removeUnnecessaryProjections(childID)
	}

	if node.NodeType != plan.Node_PROJECT {
		return nodeID
	}
	childNodeID := node.Children[0]
	childNode := builder.qry.Nodes[childNodeID]
	if len(childNode.ProjectList) != 0 {
		return nodeID
	}
	if childNode.NodeType == plan.Node_JOIN {
		return nodeID
	}
	childNode.ProjectList = node.ProjectList
	return childNodeID
}

func (builder *QueryBuilder) createQuery() (*Query, error) {
	var err error
	colRefBool := make(map[[2]int32]bool)
	sinkColRef := make(map[[2]int32]int)

	builder.parseOptimizeHints()
	for i, rootID := range builder.qry.Steps {
		builder.skipStats = builder.canSkipStats()
		builder.rewriteDistinctToAGG(rootID)
		builder.rewriteEffectlessAggToProject(rootID)
		rootID = builder.optimizeFilters(rootID)
		builder.pushdownLimitToTableScan(rootID)

		colRefCnt := make(map[[2]int32]int)
		builder.countColRefs(rootID, colRefCnt)
		builder.removeSimpleProjections(rootID, plan.Node_UNKNOWN, false, colRefCnt)
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.determineBuildAndProbeSide(rootID, true)
		determineHashOnPK(rootID, builder)
		tagCnt := make(map[int32]int)
		rootID = builder.removeEffectlessLeftJoins(rootID, tagCnt)
		builder.pushdownTopThroughLeftJoin(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)

		rootID = builder.aggPushDown(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)
		rootID = builder.determineJoinOrder(rootID)
		colMap := make(map[[2]int32]int)
		colGroup := make([]int, 0)
		builder.removeRedundantJoinCond(rootID, colMap, colGroup)
		ReCalcNodeStats(rootID, builder, true, false, true)
		rootID = builder.applyAssociativeLaw(rootID)
		builder.determineBuildAndProbeSide(rootID, true)
		rootID = builder.aggPullup(rootID, rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)
		rootID = builder.pushdownSemiAntiJoins(rootID)
		builder.optimizeDistinctAgg(rootID)
		ReCalcNodeStats(rootID, builder, true, false, true)
		builder.determineBuildAndProbeSide(rootID, true)

		builder.qry.Steps[i] = rootID

		// XXX: This will be removed soon, after merging implementation of all hash-join operators
		builder.swapJoinChildren(rootID)
		ReCalcNodeStats(rootID, builder, true, true, true)

		determineHashOnPK(rootID, builder)
		determineShuffleMethod(rootID, builder)
		determineShuffleMethod2(rootID, -1, builder)
		if builder.optimizerHints != nil && builder.optimizerHints.printShuffle == 1 && HasShuffleInPlan(builder.qry) {
			logutil.Infof("has shuffle node in plan! sql: %v", builder.compCtx.GetRootSql())
		}
		// after determine shuffle, be careful when calling ReCalcNodeStats again.
		// needResetHashMapStats should always be false from here
		builder.prepareSpecialIndexGuards(rootID)
		rootID, err = builder.applyIndices(rootID, colRefCnt, make(map[[2]int32]*plan.Expr))
		builder.resetSpecialIndexGuards()
		if err != nil {
			return nil, err
		}
		ReCalcNodeStats(rootID, builder, true, false, false)

		builder.generateRuntimeFilters(rootID)
		builder.pushdownVectorIndexTopToTableScan(rootID)
		builder.removeSimpleProjections(rootID, plan.Node_UNKNOWN, false, colRefCnt)
		ReCalcNodeStats(rootID, builder, true, false, false)
		builder.forceJoinOnOneCN(rootID, false)
		// after this ,never call ReCalcNodeStats again !!!

		if builder.isForUpdate {
			reCheckifNeedLockWholeTable(builder)
		}

		builder.handleMessages(rootID)

		builder.rewriteStarApproxCount(rootID)

		if builder.qry.StmtType != plan.Query_SELECT {
			builder.updateLocksOnDemand(rootID)
		}
		rootNode := builder.qry.Nodes[rootID]

		for j := range rootNode.ProjectList {
			colRefBool[[2]int32{int32(i), int32(j)}] = false
			if i == len(builder.qry.Steps)-1 {
				colRefBool[[2]int32{int32(i), int32(j)}] = true
			}
		}
	}

	for i := range builder.qry.Steps {
		rootID := builder.qry.Steps[i]
		builder.markSinkProject(rootID, int32(i), colRefBool)
	}

	for i := len(builder.qry.Steps) - 1; i >= 0; i-- {
		rootID := builder.qry.Steps[i]
		rootNode := builder.qry.Nodes[rootID]
		colRefCnt := make(map[[2]int32]int)
		if len(rootNode.BindingTags) > 0 {
			resultTag := rootNode.BindingTags[0]
			for j := range rootNode.ProjectList {
				colRefCnt[[2]int32{resultTag, int32(j)}] = 1
			}
		}
		_, err = builder.remapAllColRefs(rootID, int32(i), colRefCnt, colRefBool, sinkColRef)
		if err != nil {
			return nil, err
		}
		builder.qry.Steps[i] = builder.removeUnnecessaryProjections(rootID)
	}

	err = builder.lockTableIfLockNoRowsAtTheEndForDelAndUpdate()
	if err != nil {
		return nil, err
	}

	//for i := 1; i < len(builder.qry.Steps); i++ {
	//	builder.remapSinkScanColRefs(builder.qry.Steps[i], int32(i), sinkColRef)
	//}
	builder.hintQueryType()
	return builder.qry, nil
}

func (builder *QueryBuilder) buildUnion(stmt *tree.UnionClause, astOrderBy tree.OrderBy, astLimit *tree.Limit, astRankOption *tree.RankOption, ctx *BindContext, isRoot bool) (int32, error) {
	if builder.isForUpdate {
		return 0, moerr.NewInternalError(builder.GetContext(), "not support select union for update")
	}

	var selectStmts []tree.Statement
	var unionTypes []plan.Node_NodeType

	// get Union selectStmts
	err := getUnionSelects(builder.GetContext(), stmt, &selectStmts, &unionTypes)
	if err != nil {
		return 0, err
	}

	if len(selectStmts) == 1 {
		switch sltStmt := selectStmts[0].(type) {
		case *tree.Select:
			if sltClause, ok := sltStmt.Select.(*tree.SelectClause); ok {
				sltClause.Distinct = true
				return builder.bindSelect(sltStmt, ctx, isRoot)
			} else {
				// rewrite sltStmt to select distinct * from (sltStmt) a
				tmpSltStmt := &tree.Select{
					Select: &tree.SelectClause{
						Distinct: true,

						Exprs: []tree.SelectExpr{
							{Expr: tree.StarExpr()},
						},
						From: &tree.From{
							Tables: tree.TableExprs{
								&tree.AliasedTableExpr{
									Expr: &tree.ParenTableExpr{
										Expr: sltStmt,
									},
									As: tree.AliasClause{
										Alias: "a",
									},
								},
							},
						},
					},
					Limit:   astLimit,
					OrderBy: astOrderBy,
				}
				return builder.bindSelect(tmpSltStmt, ctx, isRoot)
			}

		case *tree.SelectClause:
			if !sltStmt.Distinct {
				sltStmt.Distinct = true
			}
			return builder.bindSelect(&tree.Select{Select: sltStmt, Limit: astLimit, OrderBy: astOrderBy}, ctx, isRoot)
		}
	}

	// build selects
	var projectTypList [][]types.Type
	selectStmtLength := len(selectStmts)
	nodes := make([]int32, selectStmtLength)
	subCtxList := make([]*BindContext, selectStmtLength)
	var projectLength int
	var nodeID int32
	for idx, sltStmt := range selectStmts {
		subCtx := NewBindContext(builder, ctx)
		if slt, ok := sltStmt.(*tree.Select); ok {
			nodeID, err = builder.bindSelect(slt, subCtx, isRoot)
		} else {
			nodeID, err = builder.bindSelect(&tree.Select{Select: sltStmt}, subCtx, isRoot)
		}
		if err != nil {
			return 0, err
		}

		if idx == 0 {
			projectLength = len(builder.qry.Nodes[nodeID].ProjectList)
			projectTypList = make([][]types.Type, projectLength)
			for i := 0; i < projectLength; i++ {
				projectTypList[i] = make([]types.Type, selectStmtLength)
			}
		} else {
			if projectLength != len(builder.qry.Nodes[nodeID].ProjectList) {
				return 0, moerr.NewParseError(builder.GetContext(), "SELECT statements have different number of columns")
			}
		}

		for i, expr := range subCtx.results {
			projectTypList[i][idx] = makeTypeByPlan2Expr(expr)
		}
		subCtxList[idx] = subCtx
		nodes[idx] = nodeID
	}

	// reset all select's return Projection(type cast up)
	// we use coalesce function's type check&type cast rule
	for columnIdx, argsType := range projectTypList {
		// we don't cast null as any type in function
		// but we will cast null as some target type in union/intersect/minus
		var tmpArgsType []types.Type
		for _, typ := range argsType {
			if typ.Oid != types.T_any {
				tmpArgsType = append(tmpArgsType, typ)
			}
		}

		if len(tmpArgsType) > 0 {
			fGet, err := function.GetFunctionByName(builder.GetContext(), "coalesce", tmpArgsType)
			if err != nil {
				return 0, moerr.NewParseErrorf(builder.GetContext(), "the %d column cann't cast to a same type", columnIdx)
			}
			argsCastType, _ := fGet.ShouldDoImplicitTypeCast()

			if len(argsCastType) > 0 && int(argsCastType[0].Oid) == int(types.T_datetime) {
				for i := 0; i < len(argsCastType); i++ {
					argsCastType[i].Scale = 0
				}
			}
			var targetType plan.Type
			var targetArgType types.Type
			if len(argsCastType) == 0 {
				targetArgType = tmpArgsType[0]
				// if string union string, different length may cause error.
				if targetArgType.Oid == types.T_varchar || targetArgType.Oid == types.T_char {
					for _, typ := range argsType {
						if targetArgType.Width < typ.Width {
							targetArgType.Width = typ.Width
						}
					}
				}
			} else {
				targetArgType = argsCastType[0]
			}

			if targetArgType.Oid == types.T_binary || targetArgType.Oid == types.T_varbinary {
				targetArgType = types.T_blob.ToType()
			}
			targetType = makePlan2Type(&targetArgType)

			for idx, tmpID := range nodes {
				if !argsType[idx].Eq(targetArgType) {
					node := builder.qry.Nodes[tmpID]
					if argsType[idx].Oid == types.T_any {
						node.ProjectList[columnIdx].Typ = targetType
					} else {
						node.ProjectList[columnIdx], err = appendCastBeforeExpr(builder.GetContext(), node.ProjectList[columnIdx], targetType)
						if err != nil {
							return 0, err
						}
					}
				}
			}
		}
	}

	firstSelectProjectNode := builder.qry.Nodes[nodes[0]]
	// set ctx's headings  projects  results
	ctx.headings = append(ctx.headings, subCtxList[0].headings...)

	getProjectList := func(tag int32, thisTag int32) []*plan.Expr {
		projectList := make([]*plan.Expr, len(firstSelectProjectNode.ProjectList))
		for i, expr := range firstSelectProjectNode.ProjectList {
			projectList[i] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tag,
						ColPos: int32(i),
					},
				},
			}
			builder.nameByColRef[[2]int32{thisTag, int32(i)}] = ctx.headings[i]
		}
		return projectList
	}

	// build intersect node first.  because intersect has higher precedence then UNION and MINUS
	var newNodes []int32
	var newUnionType []plan.Node_NodeType
	var lastTag int32
	newNodes = append(newNodes, nodes[0])
	for i := 1; i < len(nodes); i++ {
		utIdx := i - 1
		lastNewNodeIdx := len(newNodes) - 1
		if unionTypes[utIdx] == plan.Node_INTERSECT || unionTypes[utIdx] == plan.Node_INTERSECT_ALL {
			lastTag = builder.genNewBindTag()
			leftNodeTag := builder.qry.Nodes[newNodes[lastNewNodeIdx]].BindingTags[0]
			newNodeID := builder.appendNode(&plan.Node{
				NodeType:    unionTypes[utIdx],
				Children:    []int32{newNodes[lastNewNodeIdx], nodes[i]},
				BindingTags: []int32{lastTag},
				ProjectList: getProjectList(leftNodeTag, lastTag),
			}, ctx)
			newNodes[lastNewNodeIdx] = newNodeID
		} else {
			newNodes = append(newNodes, nodes[i])
			newUnionType = append(newUnionType, unionTypes[utIdx])
		}
	}

	// build UNION/MINUS node one by one
	lastNodeID := newNodes[0]
	for i := 1; i < len(newNodes); i++ {
		utIdx := i - 1
		lastTag = builder.genNewBindTag()
		leftNodeTag := builder.qry.Nodes[lastNodeID].BindingTags[0]

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    newUnionType[utIdx],
			Children:    []int32{lastNodeID, newNodes[i]},
			BindingTags: []int32{lastTag},
			ProjectList: getProjectList(leftNodeTag, lastTag),
		}, ctx)
	}

	// set ctx base on selects[0] and it's ctx
	ctx.groupTag = builder.genNewBindTag()
	ctx.aggregateTag = builder.genNewBindTag()
	ctx.projectTag = builder.genNewBindTag()
	for i, v := range ctx.headings {
		ctx.aliasMap[v] = &aliasItem{
			idx: int32(i),
		}
		ctx.aliasFrequency[v]++
		builder.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = v
	}
	for i, expr := range firstSelectProjectNode.ProjectList {
		ctx.projects = append(ctx.projects, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: lastTag,
					ColPos: int32(i),
				},
			},
		})
	}
	havingBinder := NewHavingBinder(builder, ctx)
	projectionBinder := NewProjectionBinder(builder, ctx, havingBinder)

	// Track the original number of columns before ORDER BY binding
	// ORDER BY may add new expressions to ctx.projects, but these should not be in the final output
	resultLen := len(ctx.projects)

	// bind orderBy BEFORE creating PROJECT node, so that any new expressions
	// added to ctx.projects by ORDER BY are included in the PROJECT node
	var orderBys []*plan.OrderBySpec
	if astOrderBy != nil {
		orderBinder := NewOrderBinder(projectionBinder, nil)
		orderBys = make([]*plan.OrderBySpec, 0, len(astOrderBy))

		for _, order := range astOrderBy {
			expr, err := orderBinder.BindExpr(order.Expr)
			if err != nil {
				return 0, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderBy.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
			}

			orderBys = append(orderBys, orderBy)
		}
	}

	// append a project node (after ORDER BY binding to include any new expressions)
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.projects,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{ctx.projectTag},
	}, ctx)

	// append orderBy (SORT node)
	if len(orderBys) > 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{lastNodeID},
			OrderBy:  orderBys,
		}, ctx)
	}

	// append limit / rank option
	var unionRankOption *plan.RankOption
	if astRankOption != nil {
		if unionRankOption, err = parseRankOption(astRankOption.Option, builder.GetContext()); err != nil {
			return 0, err
		}
	}

	if astLimit != nil {
		node := builder.qry.Nodes[lastNodeID]

		limitBinder := NewLimitBinder(builder, ctx)
		if astLimit.Offset != nil {
			node.Offset, err = limitBinder.BindExpr(astLimit.Offset, 0, true)
			if err != nil {
				return 0, err
			}
		}
		if astLimit.Count != nil {
			node.Limit, err = limitBinder.BindExpr(astLimit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := node.Limit.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					ctx.hasSingleRow = c.U64Val == 1
				}
			}
		}

		if unionRankOption != nil && unionRankOption.Mode != "" {
			node.RankOption = unionRankOption
		}
	} else if unionRankOption != nil && unionRankOption.Mode != "" {
		node := builder.qry.Nodes[lastNodeID]
		node.RankOption = unionRankOption
	}

	// append result PROJECT node
	// Use resultLen to exclude ORDER BY expressions from the final output
	if builder.qry.Nodes[lastNodeID].NodeType != plan.Node_PROJECT {
		for i := 0; i < resultLen; i++ {
			ctx.results = append(ctx.results, &plan.Expr{
				Typ: ctx.projects[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.projectTag,
						ColPos: int32(i),
					},
				},
			})
		}
		ctx.resultTag = builder.genNewBindTag()

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: ctx.results,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{ctx.resultTag},
		}, ctx)
	} else {
		ctx.results = ctx.projects[:resultLen]
	}

	// set heading
	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return lastNodeID, nil
}

const NameGroupConcat = "group_concat"
const NameClusterCenters = "cluster_centers"

func (builder *QueryBuilder) bindNoRecursiveCte(
	ctx *BindContext,
	s *tree.Select,
	cteRef *CTERef,
	table string) (nodeID int32, err error) {
	subCtx := NewBindContext(builder, ctx)
	subCtx.cteName = table
	subCtx.snapshot = cteRef.snapshot
	subCtx.recordCteInBinding(table,
		CteBindState{
			cteBindType:   CteBindTypeNonRecur,
			cte:           cteRef,
			recScanNodeId: -1})
	cteRef.isRecursive = false

	oldSnapshot := builder.compCtx.GetSnapshot()
	builder.compCtx.SetSnapshot(subCtx.snapshot)
	nodeID, err = builder.bindSelect(s, subCtx, false)
	builder.compCtx.SetSnapshot(oldSnapshot)
	if err != nil {
		return
	}

	if subCtx.hasSingleRow {
		ctx.hasSingleRow = true
	}

	cols := cteRef.ast.Name.Cols

	if len(cols) > 0 && (len(cols) != len(builder.qry.Nodes[nodeID].ProjectList) ||
		len(cols) != len(subCtx.headings)) {
		return 0, moerr.NewSyntaxErrorf(builder.GetContext(), "table %q has %d columns available but %d columns specified", table, len(builder.qry.Nodes[nodeID].ProjectList), len(cteRef.ast.Name.Cols))
	}

	for i, col := range cols {
		subCtx.headings[i] = string(col)
	}
	return nodeID, nil
}

func (builder *QueryBuilder) bindRecursiveCte(
	ctx *BindContext,
	s *tree.Select,
	cteRef *CTERef,
	table string,
	left *tree.SelectStatement,
	stmts []tree.SelectStatement,
	checkOnly bool,
) (nodeID int32, err error) {
	if len(s.OrderBy) > 0 {
		return 0, moerr.NewParseError(builder.GetContext(), "not support ORDER BY in recursive cte")
	}
	//1. bind initial statement
	initCtx := NewBindContext(builder, ctx)
	initCtx.cteName = table
	initCtx.recordCteInBinding(table,
		CteBindState{
			cteBindType:   CteBindTypeInitStmt,
			cte:           cteRef,
			recScanNodeId: -1})
	initCtx.sinkTag = builder.genNewBindTag()
	initLastNodeID, err1 := builder.bindSelect(&tree.Select{Select: *left}, initCtx, false)
	if err1 != nil {
		err = err1
		return
	}

	//2. add Sink Node on top of initial statement
	initLastNodeID = appendSinkNodeWithTag(builder, initCtx, initLastNodeID, initCtx.sinkTag)
	builder.qry.Nodes[initLastNodeID].RecursiveCte = false

	projects := builder.qry.Nodes[builder.qry.Nodes[initLastNodeID].Children[0]].ProjectList
	// recursive statement
	recursiveLastNodeID := initLastNodeID
	initSourceStep := int32(len(builder.qry.Steps))
	recursiveSteps := make([]int32, len(stmts))
	recursiveNodeIDs := make([]int32, len(stmts))

	//3. bind recursive parts
	for i, r := range stmts {
		subCtx := NewBindContext(builder, ctx)
		subCtx.cteName = table
		subCtx.sinkTag = initCtx.sinkTag
		//3.0 add initial statement as table binding into the subCtx of recursive part
		err = builder.addBinding(initLastNodeID, *cteRef.ast.Name, subCtx)
		if err != nil {
			return
		}
		//3.1 add recursive cte Node
		_ = builder.appendStep(recursiveLastNodeID)
		recScanId := appendRecursiveScanNode(builder, subCtx, initSourceStep, subCtx.sinkTag)
		recursiveNodeIDs[i] = recScanId
		recursiveSteps[i] = int32(len(builder.qry.Steps))

		subCtx.recordCteInBinding(table,
			CteBindState{
				cteBindType:   CteBindTypeRecurStmt,
				cte:           cteRef,
				recScanNodeId: recScanId})

		recursiveLastNodeID, err = builder.bindSelect(&tree.Select{Select: r}, subCtx, false)
		if err != nil {
			return
		}

		//3.2 add Sink Node on the top of single recursive part
		recursiveLastNodeID = appendSinkNodeWithTag(builder, subCtx, recursiveLastNodeID, subCtx.sinkTag)
		builder.qry.Nodes[recursiveLastNodeID].RecursiveCte = true
		if !checkOnly {
			// some check
			n := builder.qry.Nodes[builder.qry.Nodes[recursiveLastNodeID].Children[0]]
			if len(projects) != len(n.ProjectList) {
				return 0, moerr.NewParseErrorf(builder.GetContext(), "recursive cte %s projection error", table)
			}
			for i := range n.ProjectList {
				projTyp := projects[i].GetTyp()
				n.ProjectList[i], err = makePlan2CastExpr(builder.GetContext(), n.ProjectList[i], projTyp)
				if err != nil {
					return
				}
			}
			if subCtx.hasSingleRow {
				ctx.hasSingleRow = true
			}

			cols := cteRef.ast.Name.Cols

			if len(cols) > len(subCtx.headings) {
				return 0, moerr.NewSyntaxErrorf(builder.GetContext(), "table %q has %d columns available but %d columns specified", table, len(subCtx.headings), len(cols))
			}

			for i, col := range cols {
				subCtx.headings[i] = string(col)
			}
		}
	}
	if checkOnly {
		builder.qry.Steps = builder.qry.Steps[:0]
		return
	}

	// union all statement
	var limitExpr *Expr
	var offsetExpr *Expr
	if s.Limit != nil {
		limitBinder := NewLimitBinder(builder, ctx)
		if s.Limit.Offset != nil {
			offsetExpr, err = limitBinder.BindExpr(s.Limit.Offset, 0, true)
			if err != nil {
				return 0, err
			}
		}
		if s.Limit.Count != nil {
			limitExpr, err = limitBinder.BindExpr(s.Limit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := limitExpr.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					ctx.hasSingleRow = c.U64Val == 1
				}
			}
		}
	}

	//4. add CTE Scan Node
	_ = builder.appendStep(recursiveLastNodeID)
	nodeID = appendCTEScanNode(builder, ctx, initSourceStep, initCtx.sinkTag)
	if limitExpr != nil || offsetExpr != nil {
		node := builder.qry.Nodes[nodeID]
		node.Limit = limitExpr
		node.Offset = offsetExpr
	}
	//4.1 make recursive parts as the source step of the CTE Scan Node besides initSourceStep of initial statement
	for i := 0; i < len(recursiveSteps); i++ {
		builder.qry.Nodes[nodeID].SourceStep = append(builder.qry.Nodes[nodeID].SourceStep, recursiveSteps[i])
	}

	//4.2 make CTE scan as the source step of the Recursive cte Node
	curStep := int32(len(builder.qry.Steps))
	for _, id := range recursiveNodeIDs {
		builder.qry.Nodes[id].SourceStep[0] = curStep
	}

	//4.3 add Sink Node on top of CTE Scan Node
	unionAllLastNodeID := appendSinkNodeWithTag(builder, ctx, nodeID, ctx.sinkTag)
	builder.qry.Nodes[unionAllLastNodeID].RecursiveSink = true

	//5. bind final statement
	ctx.sinkTag = initCtx.sinkTag
	//5.0 add initial statement as table binding into the ctx of main query
	err = builder.addBinding(initLastNodeID, *cteRef.ast.Name, ctx)
	if err != nil {
		return
	}
	//5.1 add Sink Scan Node as the scan node of the recursive cte
	sourceStep := builder.appendStep(unionAllLastNodeID)
	nodeID = appendSinkScanNodeWithTag(builder, ctx, sourceStep, initCtx.sinkTag)
	return
}

// check if binding cte currently
func (bc *BindContext) bindingCte() bool {
	return bc.cteState.cteBindType != CteBindTypeNone
}

// check if binding non recursive cte currently
func (bc *BindContext) bindingNonRecurCte() bool {
	return bc.cteState.cteBindType == CteBindTypeNonRecur
}

// check if binding recursive cte currently
func (bc *BindContext) bindingRecurCte() bool {
	return bc.cteState.cteBindType == CteBindTypeInitStmt ||
		bc.cteState.cteBindType == CteBindTypeRecurStmt
}

// check if binding recursive part of recursive cte currently
func (bc *BindContext) bindingRecurStmt() bool {
	return bc.cteState.cteBindType == CteBindTypeRecurStmt
}

func (builder *QueryBuilder) bindCte(
	ctx *BindContext,
	stmt tree.NodeFormatter,
	cteRef *CTERef,
	table string,
	checkOnly bool,
) (nodeID int32, err error) {
	var s *tree.Select
	switch stmt := cteRef.ast.Stmt.(type) {
	case *tree.Select:
		s = getSelectTree(stmt)
	case *tree.ParenSelect:
		s = getSelectTree(stmt.Select)
	default:
		err = moerr.NewParseErrorf(builder.GetContext(), "unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL))
		return
	}

	var left *tree.SelectStatement
	var stmts []tree.SelectStatement
	left, err = builder.splitRecursiveMember(&s.Select, table, &stmts)
	if err != nil {
		return 0, err
	}
	isR := len(stmts) > 0

	if isR && !cteRef.isRecursive {
		err = moerr.NewParseErrorf(builder.GetContext(), "not declare RECURSIVE: '%v'", tree.String(stmt, dialect.MYSQL))
	} else if !isR {
		nodeID, err = builder.bindNoRecursiveCte(ctx, s, cteRef, table)
		if err != nil {
			return 0, err
		}
	} else {
		nodeID, err = builder.bindRecursiveCte(ctx, s, cteRef, table, left, stmts, checkOnly)
		if err != nil {
			return 0, err
		}
	}
	return
}

func (builder *QueryBuilder) preprocessCte(stmt *tree.Select, ctx *BindContext) error {
	// preprocess CTEs
	if stmt.With != nil {
		ctx.cteByName = make(map[string]*CTERef)
		maskedNames := make([]string, len(stmt.With.CTEs))

		for i := range stmt.With.CTEs {
			idx := len(stmt.With.CTEs) - i - 1
			cte := stmt.With.CTEs[idx]

			name := string(cte.Name.Alias)
			if _, ok := ctx.cteByName[name]; ok {
				return moerr.NewSyntaxErrorf(builder.GetContext(), "WITH query name %q specified more than once", name)
			}

			var maskedCTEs map[string]bool
			if len(maskedNames) > 0 {
				maskedCTEs = make(map[string]bool)
				for _, mask := range maskedNames {
					maskedCTEs[mask] = true
				}
			}

			maskedNames[i] = name

			ctx.cteByName[name] = &CTERef{
				ast:         cte,
				isRecursive: stmt.With.IsRecursive,
				maskedCTEs:  maskedCTEs,
			}
		}

		/*
			Try to do binding for CTE at declaration.

			CORNER CASE:

				create table t2 (a int, b int);
				create table t3 (a int);

				//mo and postgrsql, oracle, sqlserver, mysql will report error about t3 not in FROM
				//but duckdb will not report error. duckdb treat it as related subquery on t3.
				with qn as (select * from t2 where t2.b=t3.a)
				select * from t3 where exists (select * from qn);
		*/
		for _, cte := range stmt.With.CTEs {

			table := string(cte.Name.Alias)
			cteRef := ctx.cteByName[table]

			_, err := builder.bindCte(ctx, stmt, cteRef, table, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (builder *QueryBuilder) bindSelect(stmt *tree.Select, ctx *BindContext, isRoot bool) (nodeID int32, err error) {
	if stmt.RewriteOption != nil {
		ctx.remapOption = stmt.RewriteOption
	}

	// preprocess CTEs
	if err = builder.preprocessCte(stmt, ctx); err != nil {
		return
	}

	var projectionBinder *ProjectionBinder
	var havingBinder *HavingBinder
	var boundHavingList []*plan.Expr
	var selectList tree.SelectExprs
	var resultLen int
	var lockNode *plan.Node
	var notCacheable bool
	var helpFunc *helpFunc
	var boundTimeWindowGroupBy *plan.Expr
	var rollupFilter bool

	astOrderBy := stmt.OrderBy
	astLimit := stmt.Limit
	astRankOption := stmt.RankOption
	astTimeWindow := stmt.TimeWindow

	ctx.groupTag = builder.genNewBindTag()
	ctx.aggregateTag = builder.genNewBindTag()
	ctx.projectTag = builder.genNewBindTag()
	ctx.windowTag = builder.genNewBindTag()
	ctx.sampleTag = builder.genNewBindTag()
	if astTimeWindow != nil {
		ctx.timeTag = builder.genNewBindTag() // ctx.timeTag > 0
		if astTimeWindow.Sliding != nil {
			ctx.sliding = true
		}

		if helpFunc, err = makeHelpFuncForTimeWindow(astTimeWindow); err != nil {
			return
		}
	}

	if stmt.SelectLockInfo != nil && stmt.SelectLockInfo.LockType == tree.SelectLockForUpdate {
		builder.isForUpdate = true
	}

	// strip parentheses
	// ((select a from t1)) order by b  [ is equal ] select a from t1 order by b
	// (((select a from t1)) order by b) [ is equal ] select a from t1 order by b
	//
	// (select a from t1 union/union all select aa from t2) order by a
	//       => we will strip parentheses, but order by only can use 'a' column from the union's output projectlist
	for {
		if selectClause, ok := stmt.Select.(*tree.ParenSelect); ok {
			if selectClause.Select.OrderBy != nil {
				if astOrderBy != nil {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "multiple ORDER BY clauses not allowed")
				}
				astOrderBy = selectClause.Select.OrderBy
			}
			if selectClause.Select.Limit != nil {
				if astLimit != nil {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "multiple LIMIT clauses not allowed")
				}
				astLimit = selectClause.Select.Limit
			}
			if selectClause.Select.RankOption != nil {
				if astRankOption != nil {
					return 0, moerr.NewSyntaxError(builder.GetContext(), "multiple BY RANK clauses not allowed")
				}
				astRankOption = selectClause.Select.RankOption
			}
			stmt = selectClause.Select

			//stmt may be replaced above.
			//do preprocess CTEs again
			err = builder.preprocessCte(stmt, ctx)
			if err != nil {
				return 0, err
			}
		} else {
			break
		}
	}

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		if selectClause.GroupBy != nil {
			if selectClause.GroupBy.Rollup {
				for i := len(selectClause.GroupBy.GroupByExprsList[0]) - 1; i > 0; i-- {
					selectClause.GroupBy.GroupByExprsList = append(selectClause.GroupBy.GroupByExprsList, selectClause.GroupBy.GroupByExprsList[0][0:i])
				}
				selectClause.GroupBy.GroupByExprsList = append(selectClause.GroupBy.GroupByExprsList, nil)
			}
			if selectClause.GroupBy.Cube {
				subsets := func(Exprs []tree.Expr) [][]tree.Expr {
					result := [][]tree.Expr{}
					var backtrack func(start int, current []tree.Expr)
					backtrack = func(start int, current []tree.Expr) {
						result = append(result, append([]tree.Expr{}, current...))
						for i := start; i < len(Exprs); i++ {
							current = append(current, Exprs[i])
							backtrack(i+1, current)
							current = current[:len(current)-1]
						}
					}
					backtrack(0, []tree.Expr{})
					return result
				}
				Exprs := selectClause.GroupBy.GroupByExprsList[0]
				selectClause.GroupBy.GroupByExprsList = nil
				for _, subset := range subsets(Exprs) {
					selectClause.GroupBy.GroupByExprsList = append(selectClause.GroupBy.GroupByExprsList, subset)
				}
			}
			if len(selectClause.GroupBy.GroupByExprsList) > 1 && !selectClause.GroupBy.Apart {
				groupingCount := len(selectClause.GroupBy.GroupByExprsList)
				selectStmts := make([]*tree.SelectClause, groupingCount)
				if groupingCount > 1 {
					for i, list := range selectClause.GroupBy.GroupByExprsList {
						if selectClause.Having != nil {
							selectClause.Having.RollupHaving = true
						}
						selectStmts[i] = &tree.SelectClause{
							Distinct: selectClause.Distinct,
							Exprs:    selectClause.Exprs,
							From:     selectClause.From,
							Where:    selectClause.Where,
							GroupBy: &tree.GroupByClause{
								GroupByExprsList: selectClause.GroupBy.GroupByExprsList,
								GroupingSet:      list,
								Apart:            true,
								Cube:             false,
								Rollup:           false,
							},
							Having: selectClause.Having,
							Option: selectClause.Option,
						}
					}
				}
				leftClause := &tree.UnionClause{Type: tree.UNION, Left: selectStmts[0], Right: selectStmts[1], All: true}
				for i, stmt := range selectStmts {
					if i == 0 || i == 1 {
						continue
					}
					leftClause = &tree.UnionClause{Type: tree.UNION, Left: leftClause, Right: stmt, All: true}
				}
				return builder.buildUnion(leftClause, astOrderBy, astLimit, astRankOption, ctx, isRoot)
			}
		}

		if nodeID, selectList, lockNode, notCacheable, boundTimeWindowGroupBy, havingBinder, boundHavingList, err = builder.bindSelectClause(
			ctx,
			selectClause,
			astLimit,
			astTimeWindow,
			helpFunc,
			isRoot,
		); err != nil {
			return
		}
		if selectClause.Having != nil {
			rollupFilter = selectClause.Having.RollupHaving
		}
	case *tree.UnionClause:
		return builder.buildUnion(selectClause, astOrderBy, astLimit, astRankOption, ctx, isRoot)
	case *tree.ValuesClause:
		if nodeID, selectList, err = builder.bindValues(ctx, selectClause); err != nil {
			return
		}
	default:
		return 0, moerr.NewNYIf(builder.GetContext(), "statement '%s'", tree.String(stmt, dialect.MYSQL))
	}

	// bind SELECT clause (Projection List)
	projectionBinder = NewProjectionBinder(builder, ctx, havingBinder)
	if resultLen, notCacheable, err = builder.bindProjection(ctx, projectionBinder, selectList, notCacheable); err != nil {
		return
	}

	// bind TIME WINDOW
	var fillType plan.Node_FillType
	var fillVals, fillCols []*Expr
	var interval, sliding, ts, wEnd *Expr
	var boundTimeWindowOrderBy *plan.OrderBySpec
	if astTimeWindow != nil {
		if fillType, fillVals, fillCols, interval, sliding, ts, wEnd, boundTimeWindowOrderBy, err = builder.bindTimeWindow(
			ctx,
			projectionBinder,
			astTimeWindow,
			boundTimeWindowGroupBy,
			helpFunc,
		); err != nil {
			return
		}
	}

	// bind ORDER BY clause
	var boundOrderBys []*plan.OrderBySpec
	if astOrderBy != nil {
		if boundOrderBys, err = builder.bindOrderBy(ctx, astOrderBy, projectionBinder, selectList); err != nil {
			return
		}
	}

	// bind limit/offset clause
	var boundOffsetExpr *Expr
	var boundCountExpr *Expr
	var rankOption *plan.RankOption
	if astLimit != nil || astRankOption != nil {
		if boundOffsetExpr, boundCountExpr, rankOption, err = builder.bindLimit(ctx, astLimit, astRankOption); err != nil {
			return
		}

		if astLimit != nil && builder.isForUpdate {
			lockNode.Children[0] = nodeID
			nodeID = builder.appendNode(lockNode, ctx)
		}
	}

	if ctx.sampleFunc.hasSampleFunc {
		// return err if it's not a legal SAMPLE function.
		if err = validSample(ctx, builder); err != nil {
			return 0, err
		}
	} else {
		// sample can ignore these check because it supports the sql like 'select a, b, sample(c) from t group by a' whose b is not in group by clause.
		if (len(ctx.groups) > 0 || len(ctx.aggregates) > 0 || len(ctx.times) > 0) && len(projectionBinder.boundCols) > 0 {
			if !builder.mysqlCompatible {
				return 0, moerr.NewSyntaxErrorf(builder.GetContext(), "column %q must appear in the GROUP BY clause or be used in an aggregate function", projectionBinder.boundCols[0])
			}

			// For MySQL compatibility, wrap bare ColRefs in any_value()
			for i, proj := range ctx.projects {
				ctx.projects[i] = builder.wrapBareColRefsInAnyValue(proj, ctx)
			}
		}
	}

	// FIXME: delete this when SINGLE join is ready
	if len(ctx.groups) == 0 && len(ctx.aggregates) > 0 {
		ctx.hasSingleRow = true
	}

	// For group_concat with ORDER BY, we need to sort the data before aggregation.
	// The sort key should be: GROUP BY columns (if any) + ORDER BY columns.
	// This ensures that within each group, the data arrives in the correct order.
	if len(ctx.groupConcatOrderBys) > 0 && (len(ctx.groups) > 0 || len(ctx.aggregates) > 0) {
		var sortSpecs []*plan.OrderBySpec

		// First, add GROUP BY columns to sort key (ASC by default)
		// This ensures data is grouped together before applying group_concat order
		for _, groupExpr := range ctx.groups {
			sortSpecs = append(sortSpecs, &plan.OrderBySpec{
				Expr: DeepCopyExpr(groupExpr),
				Flag: plan.OrderBySpec_ASC,
			})
		}

		// Then, add group_concat ORDER BY columns
		sortSpecs = append(sortSpecs, ctx.groupConcatOrderBys...)

		// Insert Sort node before Agg
		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{nodeID},
			OrderBy:  sortSpecs,
		}, ctx)
	}

	// append AGG node or SAMPLE node
	if ctx.sampleFunc.hasSampleFunc {
		if nodeID, err = builder.appendSampleNode(ctx, nodeID, boundHavingList); err != nil {
			return
		}
	} else if len(ctx.groups) > 0 || len(ctx.aggregates) > 0 {
		if nodeID, err = builder.appendAggNode(ctx, nodeID, boundHavingList, rollupFilter); err != nil {
			return
		}
	}

	// append TIME WINDOW node
	if len(ctx.times) > 0 {
		if nodeID, err = builder.appendTimeWindowNode(
			ctx,
			nodeID,
			boundTimeWindowOrderBy,
			boundTimeWindowGroupBy,
			fillType,
			fillVals, fillCols,
			interval, sliding, ts, wEnd,
			astTimeWindow,
		); err != nil {
			return
		}
	}

	// append WINDOW node
	if len(ctx.windows) > 0 {
		if nodeID, err = builder.appendWindowNode(ctx, nodeID, boundHavingList); err != nil {
			return
		}
	}

	// append PROJECT node
	if nodeID, err = builder.appendProjectionNode(ctx, nodeID, notCacheable); err != nil {
		return
	}

	// append DISTINCT node
	if ctx.isDistinct {
		nodeID = builder.appendDistinctNode(ctx, nodeID)
	}

	// append SORT node (include limit, offset)
	if len(boundOrderBys) > 0 {
		nodeID = builder.appendSortNode(ctx, nodeID, boundOrderBys)
	}

	// attach limit/offset to last node
	if boundCountExpr != nil || boundOffsetExpr != nil || rankOption != nil {
		node := builder.qry.Nodes[nodeID]

		node.Limit = boundCountExpr
		node.Offset = boundOffsetExpr
		if rankOption != nil {
			node.RankOption = rankOption
		}
	}

	// append result PROJECT node
	if builder.qry.Nodes[nodeID].NodeType != plan.Node_PROJECT {
		nodeID = builder.appendResultProjectionNode(ctx, nodeID, resultLen)
	} else {
		ctx.results = ctx.projects
	}

	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return
}

func (builder *QueryBuilder) bindSelectClause(
	ctx *BindContext,
	clause *tree.SelectClause,
	astLimit *tree.Limit,
	astTimeWindow *tree.TimeWindow,
	helpFunc *helpFunc,
	isRoot bool,
) (
	nodeID int32,
	selectList tree.SelectExprs,
	lockNode *plan.Node,
	notCacheable bool,
	boundTimeWindowGroupBy *plan.Expr,
	havingBinder *HavingBinder,
	boundHavingList []*plan.Expr,
	err error,
) {
	if ctx.bindingRecurStmt() && clause.Distinct {
		err = moerr.NewParseError(builder.GetContext(), "not support DISTINCT in recursive cte")
		return
	}

	if len(clause.Exprs) == 1 {
		switch clause.Exprs[0].Expr.(type) {
		case tree.UnqualifiedStar:
			if astLimit != nil && astLimit.Count != nil {
				var limitExpr *plan.Expr
				limitBinder := NewLimitBinder(builder, ctx)
				if limitExpr, err = limitBinder.BindExpr(astLimit.Count, 0, true); err != nil {
					return
				}

				if cExpr, ok := limitExpr.Expr.(*plan.Expr_Lit); ok {
					if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
						if c.U64Val == 0 {
							builder.isSkipResolveTableDef = true
						}
					}
				}
			}
		}
	}

	// build FROM clause
	if nodeID, err = builder.buildFrom(clause.From.Tables, ctx, isRoot); err != nil {
		return
	}

	ctx.binder = NewWhereBinder(builder, ctx)
	// unfold stars and generate headings
	if selectList, err = appendSelectList(builder, ctx, selectList, clause.Exprs...); err != nil {
		return
	}
	if len(selectList) == 0 {
		err = moerr.NewParseError(builder.GetContext(), "No tables used")
		return
	}

	if builder.isForUpdate {
		lockTargets := builder.collectLockTargets(nodeID)
		if len(lockTargets) > 0 {
			lockNode = &Node{
				NodeType:    plan.Node_LOCK_OP,
				Children:    []int32{nodeID},
				TableDef:    builder.qry.Nodes[nodeID].GetTableDef(),
				LockTargets: lockTargets,
				BindingTags: []int32{builder.genNewBindTag()},
			}

			if astLimit == nil {
				nodeID = builder.appendNode(lockNode, ctx)
			}
		}
	}

	// rewrite right join to left join
	builder.rewriteRightJoinToLeftJoin(nodeID)
	if clause.Where != nil {
		var boundFilterList []*plan.Expr
		if nodeID, boundFilterList, notCacheable, err = builder.bindWhere(ctx, clause.Where, nodeID); err != nil {
			return
		}

		nodeID = builder.appendWhereNode(ctx, nodeID, boundFilterList, notCacheable)
	}

	// Preprocess aliases
	for i := range selectList {
		if selectList[i].Expr, err = ctx.qualifyColumnNames(selectList[i].Expr, NoAlias); err != nil {
			return
		}

		builder.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = tree.String(selectList[i].Expr, dialect.MYSQL)

		if selectList[i].As != nil && !selectList[i].As.Empty() {
			ctx.aliasMap[selectList[i].As.Compare()] = &aliasItem{
				idx:     int32(i),
				astExpr: selectList[i].Expr,
			}
			ctx.aliasFrequency[selectList[i].As.Compare()]++
		}

		field := SelectField{
			ast: selectList[i].Expr,
			pos: int32(i),
		}

		if selectList[i].As != nil && !selectList[i].As.Empty() {
			field.aliasName = selectList[i].As.Compare()
		}
		ctx.projectByAst = append(ctx.projectByAst, field)
	}

	// bind GROUP BY clause
	if clause.GroupBy != nil || astTimeWindow != nil {
		if boundTimeWindowGroupBy, err = builder.bindGroupBy(ctx, clause.GroupBy, selectList, astTimeWindow, helpFunc); err != nil {
			return
		}
	}

	// bind HAVING clause
	havingBinder = NewHavingBinder(builder, ctx)
	if clause.Having != nil {
		if boundHavingList, err = builder.bindHaving(ctx, clause.Having, havingBinder); err != nil {
			return
		}
	}

	// distinct
	ctx.isDistinct = clause.Distinct
	return
}

func (builder *QueryBuilder) bindWhere(
	ctx *BindContext,
	clause *tree.Where,
	nodeID int32,
) (newNodeID int32, boundFilterList []*plan.Expr, notCacheable bool, err error) {
	convertedExpr := builder.convertAccountToAccountIdIfNeeded(ctx, clause.Expr)
	whereList, err := splitAndBindCondition(convertedExpr, NoAlias, ctx)
	if err != nil {
		return
	}
	var expr *plan.Expr
	for _, cond := range whereList {
		if nodeID, expr, err = builder.flattenSubqueries(nodeID, cond, ctx); err != nil {
			return
		}
		boundFilterList = append(boundFilterList, expr)
	}

	for _, filter := range boundFilterList {
		if detectedExprWhetherTimeRelated(filter) {
			notCacheable = true
		}
		if err = checkGrouping(ctx.binder.GetContext(), filter); err != nil {
			return
		}
	}

	newNodeID = nodeID
	return
}

// isAccountConvertibleTable checks if a binding refers to statement_info or metric table.
// It checks both binding.table (which might be an alias) and the actual table name from node.TableDef.Name.
func isAccountConvertibleTable(binding *Binding, builder *QueryBuilder, dbName, tableName string) bool {
	if binding == nil {
		return false
	}
	if binding.db != dbName {
		return false
	}
	// Check if binding.table matches (no alias case)
	if binding.table == tableName {
		return true
	}
	// Check the actual table name from node.TableDef.Name (alias case)
	if int(binding.nodeId) < len(builder.qry.Nodes) {
		node := builder.qry.Nodes[binding.nodeId]
		if node != nil && node.TableDef != nil && node.TableDef.Name == tableName {
			return true
		}
	}
	return false
}

// buildAccountConvertibleTableAliasMap builds a map of table aliases/names that refer to statement_info or metric table.
// The key is the table alias/name used in the query, the value indicates if it refers to statement_info or metric.
func (builder *QueryBuilder) buildAccountConvertibleTableAliasMap(ctx *BindContext) map[string]bool {
	tableAliasMap := make(map[string]bool)
	for tableName, binding := range ctx.bindingByTable {
		// Check for statement_info table in system database
		if isAccountConvertibleTable(binding, builder, catalog.MO_SYSTEM, catalog.MO_STATEMENT) {
			tableAliasMap[tableName] = true
			tableAliasMap[catalog.MO_STATEMENT] = true
		}
		// Check for metric table in system_metrics database
		if isAccountConvertibleTable(binding, builder, catalog.MO_SYSTEM_METRICS, catalog.MO_METRIC) {
			tableAliasMap[tableName] = true
			tableAliasMap[catalog.MO_METRIC] = true
		}
	}
	return tableAliasMap
}

// hasStatementInfoOrMetricTable checks if the query involves statement_info or metric table
// It checks both binding.table (which might be an alias) and the actual table name from node.TableDef.Name
func hasStatementInfoOrMetricTable(ctx *BindContext, builder *QueryBuilder) bool {
	for _, binding := range ctx.bindingByTable {
		if isAccountConvertibleTable(binding, builder, catalog.MO_SYSTEM, catalog.MO_STATEMENT) {
			return true
		}
		if isAccountConvertibleTable(binding, builder, catalog.MO_SYSTEM_METRICS, catalog.MO_METRIC) {
			return true
		}
	}
	return false
}

// convertAccountToAccountIdIfNeeded converts account to account_id if the query involves statement_info or metric table.
// This conversion is primarily designed for compatibility with mo-cloud's business-level usage of the account field.
// When mo-cloud queries statement_info or metric using account names (e.g., WHERE account = 'sys'), this function automatically
// converts the account filter to account_id filter by looking up the corresponding account_id from mo_catalog.mo_account.
// This provides a more user-friendly interface while maintaining compatibility with the underlying account_id-based storage.
func (builder *QueryBuilder) convertAccountToAccountIdIfNeeded(ctx *BindContext, expr tree.Expr) tree.Expr {
	if !hasStatementInfoOrMetricTable(ctx, builder) {
		return expr
	}

	currentAccountID, err := builder.compCtx.GetAccountId()
	if err != nil {
		return expr
	}

	isSystemAccount := currentAccountID == catalog.System_Account
	currentAccountName := "sys"
	if !isSystemAccount {
		currentAccountName = builder.compCtx.GetAccountName()
	}

	tableAliasMap := builder.buildAccountConvertibleTableAliasMap(ctx)
	// Create account_id resolver function that queries account_id from mo_catalog.mo_account
	accountIdResolver := func(accountName string) (uint32, error) {
		accountIds, err := builder.compCtx.ResolveAccountIds([]string{accountName})
		if err != nil {
			return 0, err
		}
		if len(accountIds) == 0 {
			// Account not found, return 0 to indicate not found
			return 0, nil
		}
		return accountIds[0], nil
	}
	convertedExpr, _ := util.ConvertAccountToAccountIdWithTableCheck(expr, isSystemAccount, currentAccountName, currentAccountID, tableAliasMap, accountIdResolver)
	return convertedExpr
}

func (builder *QueryBuilder) bindGroupBy(
	ctx *BindContext,
	clause *tree.GroupByClause,
	selectList tree.SelectExprs,
	astTimeWindow *tree.TimeWindow,
	helpFunc *helpFunc,
) (boundTimeWindowGroupBy *plan.Expr, err error) {
	if ctx.bindingRecurStmt() {
		err = moerr.NewParseErrorf(builder.GetContext(), "not support group by in recursive cte: '%v'", tree.String(clause, dialect.MYSQL))
		return
	}

	groupBinder := NewGroupBinder(builder, ctx, selectList)

	if clause != nil {
		for _, list := range clause.GroupByExprsList {
			for _, group := range list {
				if group, err = ctx.qualifyColumnNames(group, AliasAfterColumn); err != nil {
					return
				}
				if _, err = groupBinder.BindExpr(group, 0, true); err != nil {
					return
				}
			}
		}

		ctx.groupingFlag = make([]bool, len(ctx.groups))
		if clause.Apart {
			ctx.isGroupingSet = true
			for _, group := range clause.GroupingSet {
				if _, err = groupBinder.BindExpr(group, 0, true); err != nil {
					return
				}
			}
		} else {
			for i := range ctx.groupingFlag {
				ctx.groupingFlag[i] = true
			}
		}
	}

	if astTimeWindow != nil {
		var group tree.Expr
		if group, err = ctx.qualifyColumnNames(helpFunc.truncate, AliasAfterColumn); err != nil {
			return
		}

		if _, err = groupBinder.BindExpr(group, 0, true); err != nil {
			return
		}

		colPos := groupBinder.ctx.groupByAst[tree.String(helpFunc.truncate, dialect.MYSQL)]
		boundTimeWindowGroupBy = &plan.Expr{
			Typ: groupBinder.ctx.groups[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: groupBinder.ctx.groupTag,
					ColPos: colPos,
				},
			},
		}
	}
	return
}

func (builder *QueryBuilder) bindHaving(
	ctx *BindContext,
	clause *tree.Where,
	havingBinder *HavingBinder,
) (boundHavingList []*plan.Expr, err error) {
	if ctx.bindingRecurStmt() {
		err = moerr.NewParseErrorf(builder.GetContext(), "not support having in recursive cte: '%v'", tree.String(clause, dialect.MYSQL))
		return
	}
	ctx.binder = havingBinder
	return splitAndBindCondition(clause.Expr, AliasAfterColumn, ctx)
}

func (builder *QueryBuilder) bindProjection(
	ctx *BindContext,
	projectionBinder *ProjectionBinder,
	selectList tree.SelectExprs,
	notCacheable bool,
) (resultLen int, newNotCacheable bool, err error) {
	if err = bindProjectionList(ctx, projectionBinder, selectList); err != nil {
		return
	}

	resultLen = len(ctx.projects)
	for i, proj := range ctx.projects {
		protoSz := proj.ProtoSize()
		if protoSz < 256 {
			exprBytes := make([]byte, protoSz)
			_, err = proj.MarshalToSizedBuffer(exprBytes)
			if err != nil {
				return
			}

			exprStr := string(exprBytes)
			if _, ok := ctx.projectByExpr[exprStr]; !ok {
				ctx.projectByExpr[exprStr] = int32(i)
			}

		}

		if exprCol, ok := proj.Expr.(*plan.Expr_Col); ok {
			if col := exprCol.Col; col != nil {
				if binding, ok := ctx.bindingByTag[col.RelPos]; ok {
					col.DbName = binding.db
					col.TblName = binding.table
				}
			}
		}

		if !notCacheable {
			if detectedExprWhetherTimeRelated(proj) {
				notCacheable = true
			}
		}
	}

	newNotCacheable = notCacheable
	return
}

func (builder *QueryBuilder) bindTimeWindow(
	ctx *BindContext,
	projectionBinder *ProjectionBinder,
	astTimeWindow *tree.TimeWindow,
	timeWindowGroup *plan.Expr,
	helpFunc *helpFunc,
) (
	fillType plan.Node_FillType,
	fillVals, fillCols []*Expr,
	interval, sliding, ts, wEnd *Expr,
	boundTimeWindowOrderBy *plan.OrderBySpec,
	err error,
) {
	h := projectionBinder.havingBinder
	col, err := ctx.qualifyColumnNames(astTimeWindow.Interval.Col, NoAlias)
	if err != nil {
		return
	}

	h.insideAgg = true
	if ts, err = h.BindExpr(col, 0, true); err != nil {
		return
	}
	h.insideAgg = false

	t := types.Type{Oid: types.T(ts.Typ.Id)}
	if !t.IsTemporal() {
		// If the column type is string (VARCHAR/CHAR/TEXT), try to cast it to DATETIME
		// This allows date_add() results (which return VARCHAR for MySQL compatibility) to be used in time windows
		if t.Oid == types.T_varchar || t.Oid == types.T_char || t.Oid == types.T_text {
			ts, err = appendCastBeforeExpr(builder.GetContext(), ts, plan.Type{
				Id: int32(types.T_datetime),
			})
			if err != nil {
				return
			}
		} else {
			err = moerr.NewNotSupportedf(builder.GetContext(), "the type of %s (%s) must be temporal in time window", tree.String(col, dialect.MYSQL), t.String())
			return
		}
	}

	boundTimeWindowOrderBy = &plan.OrderBySpec{
		Expr: timeWindowGroup,
		Flag: plan.OrderBySpec_INTERNAL | plan.OrderBySpec_ASC | plan.OrderBySpec_NULLS_FIRST,
	}

	if interval, err = projectionBinder.BindExpr(helpFunc.interval, 0, true); err != nil {
		return
	}

	if astTimeWindow.Sliding != nil {
		if sliding, err = projectionBinder.BindExpr(helpFunc.sliding, 0, true); err != nil {
			return
		}
	} else {
		var tmp *plan.Expr
		if tmp, err = projectionBinder.BindExpr(helpFunc.dateAdd, 0, true); err != nil {
			return
		}
		if wEnd, err = appendCastBeforeExpr(builder.GetContext(), tmp, ts.Typ); err != nil {
			return
		}
	}

	if astTimeWindow.Fill != nil && astTimeWindow.Fill.Mode != tree.FillNone && astTimeWindow.Fill.Mode != tree.FillNull {
		switch astTimeWindow.Fill.Mode {
		case tree.FillPrev:
			fillType = plan.Node_PREV
		case tree.FillNext:
			fillType = plan.Node_NEXT
		case tree.FillValue:
			fillType = plan.Node_VALUE
		case tree.FillLinear:
			fillType = plan.Node_LINEAR
		}

		var v, castedExpr *Expr
		if astTimeWindow.Fill.Val != nil {
			if v, err = projectionBinder.BindExpr(astTimeWindow.Fill.Val, 0, true); err != nil {
				return
			}
		}

		for _, t := range ctx.times {
			if e, ok := t.Expr.(*plan.Expr_Col); ok {
				if e.Col.Name == TimeWindowStart || e.Col.Name == TimeWindowEnd {
					continue
				}
			}
			if astTimeWindow.Fill.Val != nil {
				if castedExpr, err = appendCastBeforeExpr(builder.GetContext(), v, t.Typ); err != nil {
					return
				}
				fillVals = append(fillVals, castedExpr)
			}
			fillCols = append(fillCols, t)
		}

		if astTimeWindow.Fill.Mode == tree.FillLinear {
			for i, timeAst := range ctx.timeAsts {
				b := &tree.BinaryExpr{
					Op: tree.DIV,
					Left: &tree.ParenExpr{
						Expr: &tree.BinaryExpr{
							Op:    tree.PLUS,
							Left:  timeAst,
							Right: timeAst,
						},
					},
					Right: tree.NewNumVal(int64(2), "2", false, tree.P_int64),
				}
				if v, err = projectionBinder.BindExpr(b, 0, true); err != nil {
					return
				}
				if castedExpr, err = appendCastBeforeExpr(builder.GetContext(), v, fillCols[i].Typ); err != nil {
					return
				}
				fillVals = append(fillVals, castedExpr)
			}
		}
	}
	return
}

func (builder *QueryBuilder) bindOrderBy(
	ctx *BindContext,
	astOrderBy tree.OrderBy,
	projectionBinder *ProjectionBinder,
	selectList tree.SelectExprs,
) (boundOrderBys []*plan.OrderBySpec, err error) {
	if ctx.bindingRecurStmt() {
		err = moerr.NewParseErrorf(builder.GetContext(), "not support order by in recursive cte: '%v'", tree.String(&astOrderBy, dialect.MYSQL))
		return
	}

	orderBinder := NewOrderBinder(projectionBinder, selectList)
	boundOrderBys = make([]*plan.OrderBySpec, 0, len(astOrderBy))
	for _, order := range astOrderBy {
		var expr *plan.Expr
		if expr, err = orderBinder.BindExpr(order.Expr); err != nil {
			return
		}

		orderBy := &plan.OrderBySpec{
			Expr: expr,
			Flag: plan.OrderBySpec_INTERNAL,
		}

		switch order.Direction {
		case tree.Ascending:
			orderBy.Flag |= plan.OrderBySpec_ASC
		case tree.Descending:
			orderBy.Flag |= plan.OrderBySpec_DESC
		}

		switch order.NullsPosition {
		case tree.NullsFirst:
			orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
		case tree.NullsLast:
			orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
		}

		boundOrderBys = append(boundOrderBys, orderBy)
	}

	return
}

func (builder *QueryBuilder) bindLimit(
	ctx *BindContext,
	astLimit *tree.Limit,
	astRankOption *tree.RankOption,
) (boundOffsetExpr, boundCountExpr *Expr, rankOption *plan.RankOption, err error) {
	if astLimit != nil {
		limitBinder := NewLimitBinder(builder, ctx)
		if astLimit.Offset != nil {
			if boundOffsetExpr, err = limitBinder.BindExpr(astLimit.Offset, 0, true); err != nil {
				return
			}
		}
		if astLimit.Count != nil {
			if boundCountExpr, err = limitBinder.BindExpr(astLimit.Count, 0, true); err != nil {
				return
			}

			if cExpr, ok := boundCountExpr.Expr.(*plan.Expr_Lit); ok {
				if c, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
					ctx.hasSingleRow = c.U64Val == 1
				}
			}
		}
	}

	if astRankOption != nil && astRankOption.Option != nil {
		if rankOption, err = parseRankOption(astRankOption.Option, builder.GetContext()); err != nil {
			return nil, nil, nil, err
		}
	}

	return
}

func (builder *QueryBuilder) bindValues(
	ctx *BindContext,
	valuesClause *tree.ValuesClause,
) (nodeID int32, selectList tree.SelectExprs, err error) {
	rowCount := len(valuesClause.Rows)
	if len(valuesClause.Rows) == 0 {
		err = moerr.NewInternalError(builder.GetContext(), "values statement have not rows")
		return
	}

	colCnt := len(valuesClause.Rows[0])
	for j := 1; j < rowCount; j++ {
		if len(valuesClause.Rows[j]) != colCnt {
			err = moerr.NewInternalError(builder.GetContext(), fmt.Sprintf("have different column count in row '%v'", j))
			return
		}
	}

	ctx.hasSingleRow = rowCount == 1
	rowSetData := &plan.RowsetData{
		Cols: make([]*plan.ColData, colCnt),
	}
	tableDef := &plan.TableDef{
		TblId: 0,
		Name:  "",
		Cols:  make([]*plan.ColDef, colCnt),
	}
	ctx.binder = NewWhereBinder(builder, ctx)
	for i := 0; i < colCnt; i++ {
		rowSetData.Cols[i] = &plan.ColData{}

		colName := fmt.Sprintf("column_%d", i) // like MySQL
		selectList = append(selectList, tree.SelectExpr{
			Expr: tree.NewUnresolvedColName(colName),
			As:   tree.NewCStr(colName, ctx.lower),
		})
		ctx.headings = append(ctx.headings, colName)
		tableDef.Cols[i] = &plan.ColDef{
			ColId: 0,
			Name:  colName,
		}

		for j := 0; j < rowCount; j++ {
			var planExpr *plan.Expr
			if planExpr, err = ctx.binder.BindExpr(valuesClause.Rows[j][i], 0, true); err != nil {
				return
			}

			tableDef.Cols[i].Typ = planExpr.Typ
			rowSetData.Cols[i].Data = append(rowSetData.Cols[i].Data, &plan.RowsetExpr{
				Expr: planExpr,
			})
		}
	}
	nodeUUID, _ := uuid.NewV7()
	nodeID = builder.appendNode(&plan.Node{
		NodeType:     plan.Node_VALUE_SCAN,
		RowsetData:   rowSetData,
		TableDef:     tableDef,
		BindingTags:  []int32{builder.genNewBindTag()},
		Uuid:         nodeUUID[:],
		NotCacheable: true,
	}, ctx)

	err = builder.addBinding(nodeID, tree.AliasClause{Alias: "_valuescan"}, ctx)
	return
}

func (builder *QueryBuilder) appendWhereNode(
	ctx *BindContext,
	nodeID int32,
	boundFilterList []*plan.Expr,
	notCacheable bool,
) int32 {
	return builder.appendNode(&plan.Node{
		NodeType:     plan.Node_FILTER,
		Children:     []int32{nodeID},
		FilterList:   boundFilterList,
		NotCacheable: notCacheable,
	}, ctx)
}

func (builder *QueryBuilder) appendSampleNode(
	ctx *BindContext,
	nodeID int32,
	boundHavingList []*plan.Expr,
) (newNodeID int32, err error) {
	nodeID = builder.appendNode(generateSamplePlanNode(ctx, nodeID), ctx)

	if len(boundHavingList) > 0 {
		var newFilterList []*plan.Expr
		var expr *plan.Expr

		for _, cond := range boundHavingList {
			if nodeID, expr, err = builder.flattenSubqueries(nodeID, cond, ctx); err != nil {
				return
			}

			newFilterList = append(newFilterList, expr)
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{nodeID},
			FilterList: newFilterList,
		}, ctx)
	}

	for name, id := range ctx.groupByAst {
		builder.nameByColRef[[2]int32{ctx.groupTag, id}] = name
	}

	for name, id := range ctx.sampleByAst {
		builder.nameByColRef[[2]int32{ctx.sampleTag, id}] = name
	}

	newNodeID = nodeID
	return
}

func (builder *QueryBuilder) appendAggNode(
	ctx *BindContext,
	nodeID int32,
	boundHavingList []*plan.Expr,
	rollupFilter bool,
) (newNodeID int32, err error) {
	if ctx.bindingRecurStmt() {
		err = moerr.NewInternalError(builder.GetContext(), "not support aggregate function recursive cte")
		return
	}
	if builder.isForUpdate {
		err = moerr.NewInternalError(builder.GetContext(), "not support select aggregate function for update")
		return
	}

	nodeID = builder.appendNode(&plan.Node{
		NodeType:     plan.Node_AGG,
		Children:     []int32{nodeID},
		GroupBy:      ctx.groups,
		GroupingFlag: ctx.groupingFlag,
		AggList:      ctx.aggregates,
		BindingTags:  []int32{ctx.groupTag, ctx.aggregateTag},
		SpillMem:     builder.aggSpillMem,
	}, ctx)

	// Plan-level rewrite: count(not_null_col) -> starcount (ObjName + Obj) so compile uses countStarExec.
	if aggNode := builder.qry.Nodes[nodeID]; len(aggNode.Children) > 0 {
		if childNode := builder.qry.Nodes[aggNode.Children[0]]; childNode.NodeType == plan.Node_TABLE_SCAN && childNode.TableDef != nil {
			RewriteCountNotNullColToStarcount(aggNode, childNode.TableDef)
		}
	}

	if len(boundHavingList) > 0 {
		var newFilterList []*plan.Expr
		var expr *plan.Expr

		for _, cond := range boundHavingList {
			if nodeID, expr, err = builder.flattenSubqueries(nodeID, cond, ctx); err != nil {
				return
			}

			newFilterList = append(newFilterList, expr)
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType:     plan.Node_FILTER,
			Children:     []int32{nodeID},
			FilterList:   newFilterList,
			RollupFilter: rollupFilter,
		}, ctx)
	}

	for name, id := range ctx.groupByAst {
		builder.nameByColRef[[2]int32{ctx.groupTag, id}] = name
	}

	for name, id := range ctx.aggregateByAst {
		builder.nameByColRef[[2]int32{ctx.aggregateTag, id}] = name
	}

	newNodeID = nodeID
	return
}

func (builder *QueryBuilder) appendTimeWindowNode(
	ctx *BindContext,
	nodeID int32,
	boundTimeWindowOrderBy *plan.OrderBySpec,
	boundTimeWindowGroupBy *plan.Expr,
	fillType plan.Node_FillType,
	fillVals, fillCols []*Expr,
	interval, sliding, ts, wEnd *Expr,
	astTimeWindow *tree.TimeWindow,
) (newNodeID int32, err error) {
	if ctx.bindingRecurStmt() {
		err = moerr.NewInternalError(builder.GetContext(), "not support time window in recursive cte")
		return
	}

	nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TIME_WINDOW,
		Children:    []int32{nodeID},
		AggList:     ctx.times,
		BindingTags: []int32{ctx.timeTag, ctx.groupTag},
		OrderBy:     []*plan.OrderBySpec{boundTimeWindowOrderBy},
		Interval:    interval,
		Sliding:     sliding,
		GroupBy:     []*plan.Expr{boundTimeWindowGroupBy},
		Timestamp:   ts,
		WEnd:        wEnd,
	}, ctx)

	for name, id := range ctx.timeByAst {
		builder.nameByColRef[[2]int32{ctx.timeTag, id}] = name
	}

	if astTimeWindow.Fill != nil {
		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_FILL,
			Children:    []int32{nodeID},
			AggList:     fillCols,
			BindingTags: []int32{ctx.timeTag},
			FillType:    fillType,
			FillVal:     fillVals,
		}, ctx)
	}

	newNodeID = nodeID
	return
}

func (builder *QueryBuilder) appendWindowNode(
	ctx *BindContext,
	nodeID int32,
	boundHavingList []*plan.Expr,
) (newNodeID int32, err error) {
	if ctx.bindingRecurStmt() {
		err = moerr.NewInternalError(builder.GetContext(), "not support window function in recursive cte")
		return
	}

	for i, w := range ctx.windows {
		e := w.GetW()
		if len(e.PartitionBy) > 0 {
			partitionBy := make([]*plan.OrderBySpec, 0, len(e.PartitionBy))
			for _, p := range e.PartitionBy {
				partitionBy = append(partitionBy, &plan.OrderBySpec{
					Expr: p,
					Flag: plan.OrderBySpec_INTERNAL,
				})
			}
			nodeID = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_PARTITION,
				Children:    []int32{nodeID},
				OrderBy:     partitionBy,
				BindingTags: []int32{ctx.windowTag},
			}, ctx)
		}
		nodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_WINDOW,
			Children:    []int32{nodeID},
			WinSpecList: []*Expr{w},
			WindowIdx:   int32(i),
			BindingTags: []int32{ctx.windowTag},
		}, ctx)
	}

	for name, id := range ctx.windowByAst {
		builder.nameByColRef[[2]int32{ctx.windowTag, id}] = name
	}

	newNodeID = nodeID
	return
}

func (builder *QueryBuilder) appendProjectionNode(
	ctx *BindContext,
	nodeID int32,
	notCacheable bool,
) (newNodeID int32, err error) {
	for i, proj := range ctx.projects {
		if nodeID, proj, err = builder.flattenSubqueries(nodeID, proj, ctx); err != nil {
			return
		}
		ctx.projects[i] = proj
	}

	nodeID = builder.appendNode(&plan.Node{
		NodeType:     plan.Node_PROJECT,
		ProjectList:  ctx.projects,
		Children:     []int32{nodeID},
		BindingTags:  []int32{ctx.projectTag},
		NotCacheable: notCacheable,
	}, ctx)

	newNodeID = nodeID
	return
}

func (builder *QueryBuilder) appendDistinctNode(ctx *BindContext, nodeID int32) int32 {
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_DISTINCT,
		Children: []int32{nodeID},
	}, ctx)
}

func (builder *QueryBuilder) appendSortNode(ctx *BindContext, nodeID int32, boundOrderBys []*plan.OrderBySpec) int32 {
	return builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{nodeID},
		OrderBy:  boundOrderBys,
	}, ctx)
}

func (builder *QueryBuilder) appendResultProjectionNode(ctx *BindContext, nodeID int32, resultLen int) int32 {
	for i := 0; i < resultLen; i++ {
		ctx.results = append(ctx.results, &plan.Expr{
			Typ: ctx.projects[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.projectTag,
					ColPos: int32(i),
				},
			},
		})
	}

	ctx.resultTag = builder.genNewBindTag()
	return builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.results,
		Children:    []int32{nodeID},
		BindingTags: []int32{ctx.resultTag},
	}, ctx)
}

type helpFunc struct {
	interval *tree.FuncExpr
	sliding  *tree.FuncExpr
	truncate *tree.FuncExpr
	dateAdd  *tree.FuncExpr
}

func makeHelpFuncForTimeWindow(astTimeWindow *tree.TimeWindow) (*helpFunc, error) {
	h := &helpFunc{}

	name := tree.NewUnresolvedColName("interval")
	arg2 := tree.NewNumVal(astTimeWindow.Interval.Unit, astTimeWindow.Interval.Unit, false, tree.P_char)
	h.interval = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{astTimeWindow.Interval.Val, arg2},
	}

	expr := h.interval

	if astTimeWindow.Sliding != nil {
		name = tree.NewUnresolvedColName("interval")
		arg2 = tree.NewNumVal(astTimeWindow.Sliding.Unit, astTimeWindow.Sliding.Unit, false, tree.P_char)
		h.sliding = &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(name),
			Exprs: tree.Exprs{astTimeWindow.Sliding.Val, arg2},
		}

		name = tree.NewUnresolvedColName("mo_win_divisor")
		div := &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(name),
			Exprs: tree.Exprs{h.interval, h.sliding},
		}

		name = tree.NewUnresolvedColName("interval")
		arg2 = tree.NewNumVal("second", "second", false, tree.P_char)
		expr = &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(name),
			Exprs: tree.Exprs{div, arg2},
		}
	}

	name = tree.NewUnresolvedColName("mo_win_truncate")
	h.truncate = &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(name),
		Exprs: tree.Exprs{astTimeWindow.Interval.Col, expr},
	}

	if astTimeWindow.Sliding == nil {
		name = tree.NewUnresolvedColName("date_add")
		h.dateAdd = &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(name),
			Exprs: tree.Exprs{h.truncate, h.interval},
		}
	}
	return h, nil
}

func appendSelectList(
	builder *QueryBuilder,
	ctx *BindContext,
	selectList tree.SelectExprs, exprs ...tree.SelectExpr) (tree.SelectExprs, error) {
	accountId, err := builder.compCtx.GetAccountId()
	if err != nil {
		return nil, err
	}
	for _, selectExpr := range exprs {
		switch expr := selectExpr.Expr.(type) {
		case tree.UnqualifiedStar:
			cols, names, err := ctx.unfoldStar(builder.GetContext(), "", accountId == catalog.System_Account)
			if err != nil {
				return nil, err
			}
			for i, name := range names {
				selectList = append(selectList, cols[i])
				ctx.headings = append(ctx.headings, name)
			}

		case *tree.SampleExpr:
			var err error
			if err = ctx.sampleFunc.GenerateSampleFunc(expr); err != nil {
				return nil, err
			}

			oldLen := len(selectList)
			columns, isStar := expr.GetColumns()
			if isStar {
				if selectList, err = appendSelectList(builder, ctx, selectList, tree.SelectExpr{Expr: tree.UnqualifiedStar{}}); err != nil {
					return nil, err
				}
			} else {
				for _, column := range columns {
					if selectList, err = appendSelectList(builder, ctx, selectList, tree.SelectExpr{Expr: column}); err != nil {
						return nil, err
					}
				}
			}

			// deal with alias.
			sampleCount := len(selectList) - oldLen
			if selectExpr.As != nil && !selectExpr.As.Empty() {
				if sampleCount != 1 {
					return nil, moerr.NewSyntaxError(builder.GetContext(), "sample multi columns cannot have alias")
				}
				ctx.headings[len(ctx.headings)-1] = selectExpr.As.Origin()
				selectList[len(selectList)-1].As = selectExpr.As
			}

			ctx.sampleFunc.SetStartOffset(oldLen, sampleCount)

		case *tree.UnresolvedName:
			if expr.Star {
				cols, names, err := ctx.unfoldStar(builder.GetContext(), expr.ColName(), accountId == catalog.System_Account)
				if err != nil {
					return nil, err
				}
				selectList = append(selectList, cols...)
				ctx.headings = append(ctx.headings, names...)
			} else {
				if selectExpr.As != nil && !selectExpr.As.Empty() {
					ctx.headings = append(ctx.headings, selectExpr.As.Origin())
				} else {
					ctx.headings = append(ctx.headings, expr.ColNameOrigin())
				}

				newExpr, err := ctx.qualifyColumnNames(expr, NoAlias)
				if err != nil {
					return nil, err
				}

				selectList = append(selectList, tree.SelectExpr{
					Expr: newExpr,
					As:   selectExpr.As,
				})
			}
		case *tree.NumVal:
			if expr.ValType == tree.P_null {
				expr.ValType = tree.P_nulltext
			}

			if selectExpr.As != nil && !selectExpr.As.Empty() {
				ctx.headings = append(ctx.headings, selectExpr.As.Origin())
			} else {
				ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: expr,
				As:   selectExpr.As,
			})
		default:
			if selectExpr.As != nil && !selectExpr.As.Empty() {
				ctx.headings = append(ctx.headings, selectExpr.As.Origin())
			} else {
				for {
					if parenExpr, ok := expr.(*tree.ParenExpr); ok {
						expr = parenExpr.Expr
					} else {
						break
					}
				}
				ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
			}

			newExpr, err := ctx.qualifyColumnNames(expr, NoAlias)
			if err != nil {
				return nil, err
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: newExpr,
				As:   selectExpr.As,
			})
		}
	}
	return selectList, nil
}

func bindProjectionList(
	ctx *BindContext,
	projectionBinder *ProjectionBinder,
	selectList tree.SelectExprs) error {
	ctx.binder = projectionBinder

	sampleRangeLeft, sampleRangeRight := ctx.sampleFunc.start, ctx.sampleFunc.start+ctx.sampleFunc.offset
	if ctx.sampleFunc.hasSampleFunc {
		// IF sample function exists, we should bind the sample column first.
		sampleList, err := ctx.sampleFunc.BindSampleColumn(ctx, projectionBinder, selectList[sampleRangeLeft:sampleRangeRight])
		if err != nil {
			return err
		}

		for i := range selectList[:sampleRangeLeft] {
			expr, err := projectionBinder.BindExpr(selectList[i].Expr, 0, true)
			if err != nil {
				return err
			}
			ctx.projects = append(ctx.projects, expr)
		}
		ctx.projects = append(ctx.projects, sampleList...)
	}

	for i := range selectList[sampleRangeRight:] {
		expr, err := projectionBinder.BindExpr(selectList[i].Expr, 0, true)
		if err != nil {
			return err
		}

		ctx.projects = append(ctx.projects, expr)
	}
	return nil
}

func (builder *QueryBuilder) appendStep(nodeID int32) int32 {
	stepPos := len(builder.qry.Steps)
	builder.qry.Steps = append(builder.qry.Steps, nodeID)
	return int32(stepPos)
}

func (builder *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext) int32 {
	nodeID := int32(len(builder.qry.Nodes))
	node.NodeId = nodeID
	builder.qry.Nodes = append(builder.qry.Nodes, node)
	builder.ctxByNode = append(builder.ctxByNode, ctx)
	ReCalcNodeStats(nodeID, builder, false, true, true)
	for _, tag := range node.BindingTags {
		builder.tag2NodeID[tag] = nodeID
	}
	return nodeID
}

func (builder *QueryBuilder) rewriteRightJoinToLeftJoin(nodeID int32) {
	node := builder.qry.Nodes[nodeID]

	for i := range node.Children {
		builder.rewriteRightJoinToLeftJoin(node.Children[i])
	}

	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_RIGHT {
		node.JoinType = plan.Node_LEFT
		node.Children[0], node.Children[1] = node.Children[1], node.Children[0]
	}
}

func (builder *QueryBuilder) buildFrom(stmt tree.TableExprs, ctx *BindContext, isRoot bool) (int32, error) {
	if len(stmt) == 1 {
		return builder.buildTable(stmt[0], ctx, -1, nil)
	}
	return 0, moerr.NewInternalError(ctx.binder.GetContext(), "stmt's length should be zero")
	// for now, stmt'length always be zero. if someday that change in parser, you should uncomment these codes
	// leftCtx := NewBindContext(builder, ctx)
	// leftChildID, err := builder.buildTable(stmt[0], leftCtx)
	// if err != nil {
	// 	return 0, err
	// }

	// for i := 1; i < len(stmt); i++ {
	// 	rightCtx := NewBindContext(builder, ctx)
	// 	rightChildID, err := builder.buildTable(stmt[i], rightCtx)
	// 	if err != nil {
	// 		return 0, err
	// 	}

	// 	leftChildID = builder.appendNode(&plan.Node{
	// 		NodeType: plan.Node_JOIN,
	// 		Children: []int32{leftChildID, rightChildID},
	// 		JoinType: plan.Node_INNER,
	// 	}, nil)

	// 	if i == len(stmt)-1 {
	// 		builder.ctxByNode[leftChildID] = ctx
	// 		err = ctx.mergeContexts(leftCtx, rightCtx)
	// 		if err != nil {
	// 			return 0, err
	// 		}
	// 	} else {
	// 		newCtx := NewBindContext(builder, ctx)
	// 		builder.ctxByNode[leftChildID] = newCtx
	// 		err = newCtx.mergeContexts(leftCtx, rightCtx)
	// 		if err != nil {
	// 			return 0, err
	// 		}
	// 		leftCtx = newCtx
	// 	}
	// }

	// return leftChildID, err
}

func (builder *QueryBuilder) splitRecursiveMember(stmt *tree.SelectStatement, name string, stmts *[]tree.SelectStatement) (*tree.SelectStatement, error) {
	ok, left, err := builder.checkRecursiveCTE(stmt, name, stmts)
	if !ok || err != nil {
		return left, err
	}
	return builder.splitRecursiveMember(left, name, stmts)
}

func (builder *QueryBuilder) checkRecursiveCTE(left *tree.SelectStatement, name string, stmts *[]tree.SelectStatement) (bool, *tree.SelectStatement, error) {
	if u, ok := (*left).(*tree.UnionClause); ok {
		if !u.All {
			return false, left, nil
		}

		rt, ok := u.Right.(*tree.SelectClause)
		if !ok {
			return false, left, nil
		}

		count, err := builder.checkRecursiveTable(rt.From.Tables[0], name)
		if err != nil && count > 0 {
			return false, left, err
		}
		if count == 0 {
			return false, left, nil
		}
		if count > 1 {
			return false, left, moerr.NewParseErrorf(builder.GetContext(), "unsupport multiple recursive table expr in recursive CTE: %T", left)
		}
		*stmts = append(*stmts, u.Right)
		return true, &u.Left, nil
	}
	return false, left, nil
}

func (builder *QueryBuilder) checkRecursiveTable(stmt tree.TableExpr, name string) (int, error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		return 0, moerr.NewParseErrorf(builder.GetContext(), "unsupport SUBQUERY in recursive CTE: %T", stmt)

	case *tree.TableName:
		table := string(tbl.ObjectName)
		if table == name {
			return 1, nil
		}
		return 0, nil

	case *tree.JoinTableExpr:
		var err, err0 error
		if tbl.JoinType == tree.JOIN_TYPE_LEFT || tbl.JoinType == tree.JOIN_TYPE_RIGHT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			err0 = moerr.NewParseErrorf(builder.GetContext(), "unsupport LEFT, RIGHT or OUTER JOIN in recursive CTE: %T", stmt)
		}
		c1, err1 := builder.checkRecursiveTable(tbl.Left, name)
		c2, err2 := builder.checkRecursiveTable(tbl.Right, name)
		if err0 != nil {
			err = err0
		} else if err1 != nil {
			err = err1
		} else {
			err = err2
		}
		c := c1 + c2
		return c, err

	case *tree.TableFunction:
		return 0, nil

	case *tree.ParenTableExpr:
		return builder.checkRecursiveTable(tbl.Expr, name)

	case *tree.AliasedTableExpr:
		return builder.checkRecursiveTable(tbl.Expr, name)

	default:
		return 0, nil
	}
}

func getSelectTree(s *tree.Select) *tree.Select {
	switch stmt := s.Select.(type) {
	case *tree.ParenSelect:
		return getSelectTree(stmt.Select)
	default:
		return s
	}
}

func (builder *QueryBuilder) bindView(
	ctx *BindContext,
	tableDef *TableDef,
	snapshot *Snapshot,
	obj *ObjectRef,
	schema, table string,
) (nodeID int32, err error) {
	viewDefString := tableDef.ViewSql.View
	if viewDefString == "" {
		return 0, nil
	}
	viewCtx := NewBindContext(builder, nil)
	viewCtx.snapshot = snapshot
	viewCtx.lower = ctx.lower

	viewData := ViewData{}
	err = json.Unmarshal([]byte(viewDefString), &viewData)
	if err != nil {
		return 0, err
	}

	originStmts, err := mysql.Parse(builder.GetContext(), viewData.Stmt, ctx.lower)
	defer func() {
		for _, s := range originStmts {
			s.Free()
		}
	}()
	if err != nil {
		return 0, err
	}
	viewStmt, ok := originStmts[0].(*tree.CreateView)

	// No createview stmt, check alterview stmt.
	if !ok {
		alterstmt, ok := originStmts[0].(*tree.AlterView)
		viewStmt = &tree.CreateView{}
		if !ok {
			return 0, moerr.NewParseError(builder.GetContext(), "can not get view statement")
		}
		viewStmt.Name = alterstmt.Name
		viewStmt.ColNames = alterstmt.ColNames
		viewStmt.AsSource = alterstmt.AsSource
	}

	defaultDatabase := viewData.DefaultDatabase
	if obj.PubInfo != nil {
		defaultDatabase = obj.SubscriptionName
	}
	viewCtx.defaultDatabase = defaultDatabase

	if viewCtx.viewInBinding(schema, table, viewStmt) {
		return 0, moerr.NewParseErrorf(builder.GetContext(), "view %s reference itself", table)
	}
	viewCtx.cteName = table

	nodeID, err = builder.bindSelect(viewStmt.AsSource, viewCtx, false)
	if err != nil {
		return
	}
	ctx.recordViews([]string{schema + "#" + table})
	ctx.recordViews(viewCtx.views)
	return
}

func (builder *QueryBuilder) buildTable(stmt tree.TableExpr, ctx *BindContext, preNodeId int32, leftCtx *BindContext) (nodeID int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		if builder.isForUpdate {
			return 0, moerr.NewInternalError(builder.GetContext(), "not support select from derived table for update")
		}
		subCtx := NewBindContext(builder, ctx)
		nodeID, err = builder.bindSelect(tbl, subCtx, false)
		if subCtx.isCorrelated {
			return 0, moerr.NewNYI(builder.GetContext(), "correlated subquery in FROM clause")
		}

		if subCtx.hasSingleRow {
			ctx.hasSingleRow = true
		}

	case *tree.TableName:
		schema := string(tbl.SchemaName)
		table := string(tbl.ObjectName)
		originTableName := table
		if len(table) == 0 || len(schema) == 0 && table == "dual" { //special table name
			//special dual cases.
			//CORNER CASE 1:
			//  create table `dual`(a int);
			//	select * from dual;
			//		mysql responses:  No tables used
			//		mysql treats it as the placeholder
			//		mo treats it also
			//
			//CORNER CASE 2: select * from `dual`;
			//  create table `dual`(a int);
			//	select * from `dual`;
			//  	mysql responses: success.
			//		mysql treats it as the normal table.
			//		mo treats it also the placeholder.
			//
			//CORNER CASE 3:
			//  create table `dual`(a int);
			//	select * from db.dual;
			//		mysql responses: success.
			//  select * from icp.`dual`;
			//		mysql responses: success.
			//Within quote, the mysql treats the 'dual' as the normal table. mo also.
			nodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_VALUE_SCAN,
			}, ctx)

			ctx.hasSingleRow = true

			break
		}

		if len(schema) == 0 && ctx.bindingNonRecurCte() && table == ctx.cteName {
			return 0, moerr.NewParseErrorf(builder.GetContext(), "In recursive query block of Recursive Common Table Expression %s, the recursive table must be referenced only once, and not in any subquery", table)
		} else if len(schema) == 0 {
			cteRef := ctx.findCTE(table)
			if cteRef != nil {
				if ctx.cteInBinding(table) {
					if ctx.bindingNonRecurCte() {
						return 0, moerr.NewParseErrorf(builder.GetContext(), "cte %s reference itself", table)
					}
				}
				if ctx.bindingRecurStmt() {
					nodeID = ctx.cteState.recScanNodeId
					return
				}

				nodeID, err = builder.bindCte(ctx, stmt, cteRef, table, false)
				if err != nil {
					return 0, err
				}

				break
			}
			schema = ctx.defaultDatabase
		}

		if ctx.remapOption != nil {
			// The map key must contain a database to prevent the following situation
			// /*+ { “rewrites” : {
			// “t1”: “select a, b, c from db2.t1”,
			// }} */
			// Select * from t1; // current database is db1
			// /*+ { “rewrites” : {
			// “t1”: “select a, b, c from db2.t1”,
			// }} */
			// Select * from db1.t1; // db2.t1?
			if len(schema) == 0 {
				schema = builder.compCtx.DefaultDatabase()
			}
			key := schema + "." + table
			if rewrite, ok := ctx.remapOption.Rewrites[key]; ok {
				// prevent recursion from occurring
				m := ctx.remapOption
				ctx.remapOption = nil
				nodeID, err = builder.buildTable(rewrite.Stmt, ctx, preNodeId, leftCtx)
				if err != nil {
					return
				}
				// Do not bind here to avoid double-binding. Instead, set the
				// subquery context name so the outer AliasedTableExpr binds once.
				if int(nodeID) < len(builder.ctxByNode) && builder.ctxByNode[nodeID] != nil {
					builder.ctxByNode[nodeID].cteName = key
				}
				ctx.remapOption = m
				return
			}
		}

		if tbl.AtTsExpr != nil {
			old := ctx.snapshot
			defer func() {
				ctx.snapshot = old
			}()

			ctx.snapshot, err = builder.ResolveTsHint(tbl.AtTsExpr)
			if err != nil {
				return 0, err
			}
		}

		var snapshot *Snapshot
		if ctx.snapshot != nil {
			snapshot = ctx.snapshot
		}

		// TODO
		schema, err = databaseIsValid(schema, builder.compCtx, snapshot)
		if err != nil {
			return 0, err
		}

		var subMeta *SubscriptionMeta
		subMeta, err = builder.compCtx.GetSubscriptionMeta(schema, snapshot)
		if err == nil && builder.isSkipResolveTableDef && snapshot == nil && subMeta == nil {
			var tableDef *TableDef
			tableDef, err = builder.compCtx.BuildTableDefByMoColumns(schema, table)
			if err != nil {
				return 0, err
			}

			nodeID = builder.appendNode(&plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				Stats:        nil,
				ObjRef:       &plan.ObjectRef{DbName: schema, SchemaName: table},
				TableDef:     tableDef,
				BindingTags:  []int32{builder.genNewBindTag()},
				ScanSnapshot: snapshot,
			}, ctx)

			return
		}

		// TODO
		obj, tableDef, err := builder.compCtx.Resolve(schema, table, snapshot)
		if err != nil {
			return 0, err
		}
		if tableDef == nil {
			return 0, moerr.NewParseErrorf(builder.GetContext(), "table %q does not exist", table)
		}

		tableDef.Name2ColIndex = map[string]int32{}
		var tbColToDataCol map[string]int32 = make(map[string]int32, 0)
		for i := 0; i < len(tableDef.Cols); i++ {
			tableDef.Name2ColIndex[tableDef.Cols[i].Name] = int32(i)
			if !tableDef.Cols[i].Hidden {
				tbColToDataCol[tableDef.Cols[i].Name] = int32(i)
			}
		}
		nodeType := plan.Node_TABLE_SCAN
		var externScan *plan.ExternScan
		if tableDef.TableType == catalog.SystemExternalRel {
			nodeType = plan.Node_EXTERNAL_SCAN
			externScan = &plan.ExternScan{
				Type:           int32(plan.ExternType_EXTERNAL_TB),
				TbColToDataCol: tbColToDataCol,
			}
			col := &ColDef{
				Name: catalog.ExternalFilePath,
				Typ: plan.Type{
					Id:    int32(types.T_varchar),
					Width: types.MaxVarcharLen,
					Table: table,
				},
			}
			tableDef.Cols = append(tableDef.Cols, col)
		} else if tableDef.TableType == catalog.SystemSourceRel {
			nodeType = plan.Node_SOURCE_SCAN
		} else if tableDef.TableType == catalog.SystemViewRel {
			if yes, dbOfView, nameOfView := builder.compCtx.GetBuildingAlterView(); yes {
				if dbOfView == schema && nameOfView == table {
					return 0, moerr.NewInternalErrorf(builder.GetContext(), "there is a recursive reference to the view %s", nameOfView)
				}
			}

			nodeID, err = builder.bindView(ctx, tableDef, snapshot, obj, schema, table)
			if err != nil {
				return 0, err
			}
			if nodeID != 0 {
				return nodeID, nil
			}
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType:     nodeType,
			Stats:        nil,
			ObjRef:       obj,
			TableDef:     tableDef,
			ExternScan:   externScan,
			BindingTags:  []int32{builder.genNewBindTag()},
			ScanSnapshot: snapshot,
		}, ctx)

		node := builder.qry.Nodes[nodeID]
		if node.TableDef != nil {
			node.TableDef.OriginalName = originTableName
		}

	case *tree.JoinTableExpr:
		if tbl.Right == nil {
			return builder.buildTable(tbl.Left, ctx, preNodeId, leftCtx)
		}
		return builder.buildJoinTable(tbl, ctx)

	case *tree.ApplyTableExpr:
		_, ok := tbl.Right.(*tree.TableFunction)
		if !ok {
			if _, ok = tbl.Right.(*tree.AliasedTableExpr); ok {
				_, ok = tbl.Right.(*tree.AliasedTableExpr).Expr.(*tree.TableFunction)
			}
		}
		if ok {
			return builder.buildApplyTable(tbl, ctx)
		} else {
			return 0, moerr.NewInternalError(builder.GetContext(), "must apply a table function")
		}

	case *tree.TableFunction:
		if tbl.Id() == "result_scan" {
			return builder.buildResultScan(tbl, ctx)
		}
		return builder.buildTableFunction(tbl, ctx, preNodeId, leftCtx)

	case *tree.ParenTableExpr:
		return builder.buildTable(tbl.Expr, ctx, preNodeId, leftCtx)

	case *tree.AliasedTableExpr: //allways AliasedTableExpr first
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, moerr.NewSyntaxErrorf(builder.GetContext(), "subquery in FROM must have an alias: %T", stmt)
			}
		}

		nodeID, err = builder.buildTable(tbl.Expr, ctx, preNodeId, leftCtx)
		if err != nil {
			return
		}

		err = builder.addBinding(nodeID, tbl.As, ctx)

		//tableDef := builder.qry.Nodes[nodeID].GetTableDef()
		midNode := builder.qry.Nodes[nodeID]
		//if it is the non-sys account and reads the cluster table,
		//we add an account_id filter to make sure that the non-sys account
		//can only read its own data.

		if midNode.NodeType == plan.Node_TABLE_SCAN {
			dbName := midNode.ObjRef.SchemaName
			tableName := midNode.TableDef.Name
			currentAccountID, err := builder.compCtx.GetAccountId()
			if err != nil {
				return 0, err
			}
			if sub := builder.compCtx.GetQueryingSubscription(); sub != nil {
				currentAccountID = uint32(sub.AccountId)
				builder.qry.Nodes[nodeID].NotCacheable = true
			}
			if currentAccountID != catalog.System_Account {
				// add account filter for system table scan
				if dbName == catalog.MO_CATALOG && tableName == catalog.MO_DATABASE {
					modatabaseFilter := util.BuildMoDataBaseFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(modatabaseFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_SYSTEM_METRICS && (tableName == catalog.MO_METRIC || tableName == catalog.MO_SQL_STMT_CU) {
					motablesFilter := util.BuildSysMetricFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_SYSTEM && tableName == catalog.MO_STATEMENT {
					motablesFilter := util.BuildSysStatementInfoFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_CATALOG && tableName == catalog.MO_TABLES {
					motablesFilter := util.BuildMoTablesFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(motablesFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if dbName == catalog.MO_CATALOG && tableName == catalog.MO_COLUMNS {
					moColumnsFilter := util.BuildMoColumnsFilter(uint64(currentAccountID))
					ctx.binder = NewWhereBinder(builder, ctx)
					accountFilterExprs, err := splitAndBindCondition(moColumnsFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				} else if util.TableIsClusterTable(midNode.GetTableDef().GetTableType()) {
					ctx.binder = NewWhereBinder(builder, ctx)
					left := tree.NewUnresolvedColName(util.GetClusterTableAttributeName())
					right := tree.NewNumVal(uint64(currentAccountID), strconv.Itoa(int(currentAccountID)), false, tree.P_uint64)
					//account_id = the accountId of the non-sys account
					accountFilter := &tree.ComparisonExpr{
						Op:    tree.EQUAL,
						Left:  left,
						Right: right,
					}
					accountFilterExprs, err := splitAndBindCondition(accountFilter, NoAlias, ctx)
					if err != nil {
						return 0, err
					}
					builder.qry.Nodes[nodeID].FilterList = accountFilterExprs
				}
			}
		}
		return
	case *tree.StatementSource:
		return 0, moerr.NewParseErrorf(builder.GetContext(), "unsupport table expr: %T", stmt)

	default:
		// Values table not support
		return 0, moerr.NewParseErrorf(builder.GetContext(), "unsupport table expr: %T", stmt)
	}

	return
}

func (builder *QueryBuilder) genNewBindTag() int32 {
	builder.nextBindTag++
	return builder.nextBindTag
}

func (builder *QueryBuilder) genNewMsgTag() (ret int32) {
	// start from 1, and 0 means do not handle with message
	builder.nextMsgTag++
	return builder.nextMsgTag
}

func (builder *QueryBuilder) addBinding(nodeID int32, alias tree.AliasClause, ctx *BindContext) error {
	node := builder.qry.Nodes[nodeID]

	var cols []string
	var colIsHidden []bool
	var types []*plan.Type
	var defaultVals []string
	var binding *Binding
	var table string

	scanNodes := []plan.Node_NodeType{
		plan.Node_TABLE_SCAN,
		plan.Node_MATERIAL_SCAN,
		plan.Node_EXTERNAL_SCAN,
		plan.Node_FUNCTION_SCAN,
		plan.Node_VALUE_SCAN,
		plan.Node_SINK_SCAN,
		plan.Node_RECURSIVE_SCAN,
		plan.Node_SOURCE_SCAN,
	}
	lower := builder.compCtx.GetLowerCaseTableNames()
	if slices.Contains(scanNodes, node.NodeType) {
		if (node.NodeType == plan.Node_VALUE_SCAN || node.NodeType == plan.Node_SINK_SCAN || node.NodeType == plan.Node_RECURSIVE_SCAN) && node.TableDef == nil {
			return nil
		}
		if len(alias.Cols) > len(node.TableDef.Cols) {
			return moerr.NewSyntaxErrorf(builder.GetContext(), "table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols))
		}

		table = node.TableDef.Name
		if node.TableDef.OriginalName != "" {
			table = node.TableDef.OriginalName
		}
		if alias.Alias != "" {
			table = string(alias.Alias)
		} else {
			if node.NodeType == plan.Node_FUNCTION_SCAN {
				return moerr.NewSyntaxError(builder.GetContext(), "Every table function must have an alias")
			}
			if node.NodeType == plan.Node_RECURSIVE_SCAN || node.NodeType == plan.Node_SINK_SCAN {
				return nil
			}
			table = tree.NewCStr(table, lower).Compare()
		}

		if _, ok := ctx.bindingByTable[table]; ok {
			return moerr.NewSyntaxErrorf(builder.GetContext(), "table name %q specified more than once", table)
		}

		colLength := len(node.TableDef.Cols)
		cols = make([]string, colLength)
		originCols := make([]string, colLength)
		colIsHidden = make([]bool, colLength)
		types = make([]*plan.Type, colLength)
		defaultVals = make([]string, colLength)

		tag := node.BindingTags[0]

		for i, col := range node.TableDef.Cols {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col.Name
			}
			originCols[i] = cols[i]
			cols[i] = strings.ToLower(cols[i])
			colIsHidden[i] = col.Hidden
			types[i] = &col.Typ
			if col.Default != nil {
				defaultVals[i] = col.Default.OriginString
			}
			name := table + "." + cols[i]
			builder.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBinding(tag, nodeID, node.TableDef.DbName, table, node.TableDef.TblId, cols, colIsHidden, types,
			util.TableIsClusterTable(node.TableDef.TableType), defaultVals)
		binding.originCols = originCols
	} else {
		// Subquery
		subCtx := builder.ctxByNode[nodeID]
		tag := subCtx.rootTag()
		headings := subCtx.headings
		projects := subCtx.projects

		if len(alias.Cols) > len(headings) {
			return moerr.NewSyntaxErrorf(builder.GetContext(), "table %q has %d columns available but %d columns specified", alias.Alias, len(headings), len(alias.Cols))
		}

		table = subCtx.cteName
		if len(alias.Alias) > 0 {
			table = string(alias.Alias)
		}
		if len(table) == 0 {
			table = fmt.Sprintf("mo_table_subquery_alias_%d", tag)
		}
		if _, ok := ctx.bindingByTable[table]; ok {
			return moerr.NewSyntaxErrorf(builder.GetContext(), "table name %q specified more than once", table)
		}

		colLength := len(headings)
		cols = make([]string, colLength)
		originCols := make([]string, colLength)
		colIsHidden = make([]bool, colLength)
		types = make([]*plan.Type, colLength)
		defaultVals = make([]string, colLength)

		for i, col := range headings {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col
			}
			originCols[i] = cols[i]
			cols[i] = strings.ToLower(cols[i])
			types[i] = &projects[i].Typ
			colIsHidden[i] = false
			defaultVals[i] = ""
			name := table + "." + cols[i]
			builder.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBinding(tag, nodeID, "", table, 0, cols, colIsHidden, types, false, defaultVals)
		binding.originCols = originCols
	}

	ctx.bindings = append(ctx.bindings, binding)
	ctx.bindingByTag[binding.tag] = binding
	ctx.bindingByTable[binding.table] = binding

	if node.NodeType != plan.Node_RECURSIVE_SCAN && node.NodeType != plan.Node_SINK_SCAN {
		for _, col := range binding.cols {
			if _, ok := ctx.bindingByCol[col]; ok {
				ctx.bindingByCol[col] = nil
			} else {
				ctx.bindingByCol[col] = binding
			}
		}
	}

	ctx.bindingTree = &BindingTreeNode{
		binding: binding,
	}

	return nil
}

func (builder *QueryBuilder) buildJoinTable(tbl *tree.JoinTableExpr, ctx *BindContext) (int32, error) {
	joinType := plan.Node_INNER

	switch tbl.JoinType {
	case tree.JOIN_TYPE_CROSS, tree.JOIN_TYPE_INNER, tree.JOIN_TYPE_NATURAL:
		joinType = plan.Node_INNER
	case tree.JOIN_TYPE_CENTROIDX:
		joinType = plan.Node_L2
		if len(tbl.Option) == 0 {
			return 0, moerr.NewSyntaxError(builder.GetContext(), "CENTROIDX without optype")
		}
	case tree.JOIN_TYPE_LEFT, tree.JOIN_TYPE_NATURAL_LEFT:
		joinType = plan.Node_LEFT
	case tree.JOIN_TYPE_RIGHT, tree.JOIN_TYPE_NATURAL_RIGHT:
		joinType = plan.Node_RIGHT
	case tree.JOIN_TYPE_FULL:
		joinType = plan.Node_OUTER
	case tree.JOIN_TYPE_DEDUP:
		joinType = plan.Node_DEDUP
	}

	leftCtx := NewBindContext(builder, ctx)
	rightCtx := NewBindContext(builder, ctx)

	leftChildID, err := builder.buildTable(tbl.Left, leftCtx, -1, leftCtx)
	if err != nil {
		return 0, err
	}

	if _, ok := tbl.Right.(*tree.TableFunction); ok {
		return 0, moerr.NewSyntaxError(builder.GetContext(), "Every table function must have an alias")
	}
	rightChildID, err := builder.buildTable(tbl.Right, rightCtx, leftChildID, leftCtx)
	if err != nil {
		return 0, err
	}

	if builder.qry.Nodes[rightChildID].NodeType == plan.Node_FUNCTION_SCAN {
		if joinType != plan.Node_INNER {
			return 0, moerr.NewSyntaxError(builder.GetContext(), "table function can only be used in a inner join")
		}
	}

	err = ctx.mergeContexts(builder.GetContext(), leftCtx, rightCtx)
	if err != nil {
		return 0, err
	}

	node := &plan.Node{
		NodeType:     plan.Node_JOIN,
		Children:     []int32{leftChildID, rightChildID},
		JoinType:     joinType,
		ExtraOptions: tbl.Option,
	}
	nodeID := builder.appendNode(node, ctx)

	ctx.binder = NewTableBinder(builder, ctx)

	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		joinConds, err := splitAndBindCondition(cond.Expr, NoAlias, ctx)
		if err != nil {
			return 0, err
		}
		node.OnList = joinConds

	case *tree.UsingJoinCond:
		if tbl.JoinType == tree.JOIN_TYPE_CENTROIDX {
			for _, col := range cond.Cols {
				expr, err := ctx.addUsingColForCrossL2(string(col), joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}
				node.OnList = append(node.OnList, expr)
			}
		} else {
			for _, col := range cond.Cols {
				expr, err := ctx.addUsingCol(string(col), joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}
				node.OnList = append(node.OnList, expr)
			}
		}
	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			leftCols := make(map[string]bool)
			for _, binding := range leftCtx.bindings {
				for i, col := range binding.cols {
					if binding.colIsHidden[i] {
						continue
					}
					leftCols[col] = true
				}
			}

			var usingCols []string
			for _, binding := range rightCtx.bindings {
				for _, col := range binding.cols {
					if leftCols[col] {
						usingCols = append(usingCols, col)
					}
				}
			}

			for _, col := range usingCols {
				expr, err := ctx.addUsingCol(col, joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}

				node.OnList = append(node.OnList, expr)
			}
		}
	}

	return nodeID, nil
}

func (builder *QueryBuilder) buildApplyTable(tbl *tree.ApplyTableExpr, ctx *BindContext) (int32, error) {
	var applyType plan.Node_ApplyType

	switch tbl.ApplyType {
	case tree.APPLY_TYPE_CROSS:
		applyType = plan.Node_CROSSAPPLY
	case tree.APPLY_TYPE_OUTER:
		applyType = plan.Node_OUTERAPPLY
	}

	leftCtx := NewBindContext(builder, ctx)
	rightCtx := NewBindContext(builder, ctx)

	leftChildID, err := builder.buildTable(tbl.Left, leftCtx, -1, leftCtx)
	if err != nil {
		return 0, err
	}

	rightChildID, err := builder.buildTable(tbl.Right, rightCtx, leftChildID, leftCtx)
	if err != nil {
		return 0, err
	}
	builder.qry.Nodes[rightChildID].Children = nil //ignore the child of table_function in apply

	err = ctx.mergeContexts(builder.GetContext(), leftCtx, rightCtx)
	if err != nil {
		return 0, err
	}
	nodeID := builder.appendNode(&plan.Node{
		NodeType:  plan.Node_APPLY,
		Children:  []int32{leftChildID, rightChildID},
		ApplyType: applyType,
	}, ctx)
	return nodeID, nil
}

func (builder *QueryBuilder) buildTableFunction(tbl *tree.TableFunction, ctx *BindContext, preNodeId int32, leftCtx *BindContext) (int32, error) {
	var (
		childId int32
		err     error
		nodeId  int32
	)

	var children []int32
	if preNodeId == -1 {
		ctx.binder = NewTableBinder(builder, ctx)
	} else {
		ctx.binder = NewTableBinder(builder, leftCtx)
		childId = builder.copyNode(ctx, preNodeId)
		children = []int32{childId}
	}

	exprs := make([]*plan.Expr, 0, len(tbl.Func.Exprs))
	for _, v := range tbl.Func.Exprs {
		curExpr, err := ctx.binder.BindExpr(v, 0, false)
		if err != nil {
			return 0, err
		}
		exprs = append(exprs, curExpr)
	}
	id := tbl.Id()
	switch id {
	case "unnest":
		nodeId, err = builder.buildUnnest(tbl, ctx, exprs, children)
	case "generate_series":
		nodeId = builder.buildGenerateSeries(tbl, ctx, exprs, children)
	case "generate_random_int64":
		nodeId = builder.buildGenerateRandomInt64(tbl, ctx, exprs, children)
	case "generate_random_float64":
		nodeId = builder.buildGenerateRandomFloat64(tbl, ctx, exprs, children)
	case "meta_scan":
		nodeId, err = builder.buildMetaScan(tbl, ctx, exprs, children)
	case "current_account":
		nodeId, err = builder.buildCurrentAccount(tbl, ctx, exprs, children)
	case "metadata_scan":
		nodeId = builder.buildMetadataScan(tbl, ctx, exprs, children)
	case "processlist", "mo_sessions":
		nodeId, err = builder.buildProcesslist(tbl, ctx, exprs, children)
	case "mo_configurations":
		nodeId, err = builder.buildMoConfigurations(tbl, ctx, exprs, children)
	case "mo_locks":
		nodeId, err = builder.buildMoLocks(tbl, ctx, exprs, children)
	case "mo_transactions":
		nodeId, err = builder.buildMoTransactions(tbl, ctx, exprs, children)
	case "mo_cache":
		nodeId, err = builder.buildMoCache(tbl, ctx, exprs, children)
	case "fulltext_index_scan":
		nodeId, err = builder.buildFullTextIndexScan(tbl, ctx, exprs, children)
	case "fulltext_index_tokenize":
		nodeId, err = builder.buildFullTextIndexTokenize(tbl, ctx, exprs, children)
	case "stage_list":
		nodeId, err = builder.buildStageList(tbl, ctx, exprs, children)
	case "moplugin_table":
		nodeId, err = builder.buildPluginExec(tbl, ctx, exprs, children)
	case "hnsw_create":
		nodeId, err = builder.buildHnswCreate(tbl, ctx, exprs, children)
	case "hnsw_search":
		nodeId, err = builder.buildHnswSearch(tbl, ctx, exprs, children)
	case "ivf_create":
		nodeId, err = builder.buildIvfCreate(tbl, ctx, exprs, children)
	case "ivf_search":
		nodeId, err = builder.buildIvfSearch(tbl, ctx, exprs, children)
	case "parse_jsonl_data":
		nodeId, err = builder.buildParseJsonlData(tbl, ctx, exprs, children)
	case "parse_jsonl_file":
		nodeId, err = builder.buildParseJsonlFile(tbl, ctx, exprs, children)
	case "table_stats":
		nodeId = builder.buildTableStats(tbl, ctx, exprs, children)
	case "load_file_chunks":
		nodeId = builder.buildLoadFileChunks(tbl, ctx, exprs, children)
	default:
		err = moerr.NewNotSupportedf(builder.GetContext(), "table function '%s' not supported", id)
	}
	return nodeId, err
}

func (builder *QueryBuilder) GetContext() context.Context {
	if builder == nil {
		return context.TODO()
	}
	return builder.compCtx.GetContext()
}

// parseRankOption parses rank options from a map of option key-value pairs.
// It extracts the "mode" option case-insensitively and validates it.
// Returns a RankOption with the parsed mode if valid, or nil if no mode is specified.
// Returns an error if the mode value is invalid (must be "pre", "post", or "force").
func parseRankOption(options map[string]string, ctx context.Context) (*plan.RankOption, error) {
	if len(options) == 0 {
		return nil, nil
	}

	rankOption := &plan.RankOption{}

	// Helper function to get value from map case-insensitively
	getOptionValue := func(key string) (string, bool) {
		keyLower := strings.ToLower(key)
		for k, v := range options {
			if strings.ToLower(k) == keyLower {
				return v, true
			}
		}
		return "", false
	}

	if mode, ok := getOptionValue("mode"); ok {
		modeLower := strings.ToLower(strings.TrimSpace(mode))
		// Mode options:
		// - "pre": Enable vector index with BloomFilter pushdown
		// - "post": Enable vector index with standard behavior (post-filtering)
		// - "force": Force disable vector index, use full table scan
		// - "auto": Adaptive mode that automatically selects the best strategy
		if modeLower != "pre" && modeLower != "post" && modeLower != "force" && modeLower != "auto" {
			return nil, moerr.NewInvalidInputf(ctx, "mode must be 'pre', 'post', 'force', or 'auto', got '%s'", mode)
		}
		rankOption.Mode = modeLower
	}

	if rankOption.Mode == "" {
		return nil, nil
	}

	return rankOption, nil
}

func (builder *QueryBuilder) checkExprCanPushdown(expr *Expr, node *Node) bool {
	switch node.NodeType {
	case plan.Node_FUNCTION_SCAN:
		if onlyContainsTag(expr, node.BindingTags[0]) {
			return true
		}
		for _, childId := range node.Children {
			if builder.checkExprCanPushdown(expr, builder.qry.Nodes[childId]) {
				return true
			}
		}
		return false
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_SOURCE_SCAN:
		return onlyContainsTag(expr, node.BindingTags[0])
	case plan.Node_JOIN:
		if containsTag(expr, builder.qry.Nodes[node.Children[0]].BindingTags[0]) && containsTag(expr, builder.qry.Nodes[node.Children[1]].BindingTags[0]) {
			return true
		}
		for _, childId := range node.Children {
			if builder.checkExprCanPushdown(expr, builder.qry.Nodes[childId]) {
				return true
			}
		}
		return false

	default:
		for _, childId := range node.Children {
			if builder.checkExprCanPushdown(expr, builder.qry.Nodes[childId]) {
				return true
			}
		}
		return false
	}
}

func (builder *QueryBuilder) ResolveTsHint(tsExpr *tree.AtTimeStamp) (snapshot *Snapshot, err error) {
	if tsExpr == nil {
		return
	}

	conds := splitAstConjunction(tsExpr.Expr)
	if len(conds) != 1 {
		err = moerr.NewParseError(builder.GetContext(), "invalid timestamp hint")
		return
	}

	binder := NewDefaultBinder(builder.GetContext(), nil, nil, plan.Type{}, nil)
	binder.builder = builder
	defExpr, err := binder.BindExpr(conds[0], 0, false)
	if err != nil {
		return
	}
	exprLit, ok := defExpr.Expr.(*plan.Expr_Lit)
	if !ok {
		err = moerr.NewParseError(builder.GetContext(), "invalid timestamp hint")
		return
	}

	var tenant *SnapshotTenant
	if bgSnapshot := builder.compCtx.GetSnapshot(); IsSnapshotValid(bgSnapshot) {
		tenant = &SnapshotTenant{
			TenantName: bgSnapshot.Tenant.TenantName,
			TenantID:   bgSnapshot.Tenant.TenantID,
		}
	}

	switch lit := exprLit.Lit.Value.(type) {
	case *plan.Literal_Sval:
		if tsExpr.Type == tree.ATTIMESTAMPTIME {
			var ts time.Time
			if ts, err = time.Parse("2006-01-02 15:04:05.999999999", lit.Sval); err != nil {
				return
			}

			tsNano := ts.UTC().UnixNano()
			if tsNano <= 0 {
				err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value", lit.Sval)
				return
			}

			if time.Now().UTC().UnixNano()-tsNano <= options.DefaultGCTTL.Nanoseconds() && 0 <= time.Now().UTC().UnixNano()-tsNano {
				snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: tsNano}, Tenant: tenant}
			} else {
				var valid bool
				if valid, err = builder.compCtx.CheckTimeStampValid(tsNano); err != nil {
					return
				}

				if !valid {
					err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value, no corresponding snapshot ", lit.Sval)
					return
				}

				snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: tsNano}, Tenant: tenant}
			}
		} else if tsExpr.Type == tree.ATTIMESTAMPSNAPSHOT {
			return builder.compCtx.ResolveSnapshotWithSnapshotName(lit.Sval)
		} else if tsExpr.Type == tree.ATMOTIMESTAMP {
			var ts timestamp.Timestamp
			if ts, err = timestamp.ParseTimestamp(lit.Sval); err != nil {
				return
			}

			snapshot = &Snapshot{TS: &ts, Tenant: tenant}
		} else if tsExpr.Type == tree.ASOFTIMESTAMP {
			var ts int64
			if ts, err = doResolveTimeStamp(lit.Sval); err != nil {
				return
			}
			tStamp := &timestamp.Timestamp{PhysicalTime: ts}
			snapshot = &Snapshot{TS: tStamp, Tenant: tenant}
		} else {
			err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp hint type", tsExpr.Type.String())
			return
		}
	case *plan.Literal_I64Val:
		if tsExpr.Type == tree.ATTIMESTAMPTIME {
			if lit.I64Val <= 0 {
				err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value", lit.I64Val)
				return
			}
			snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: lit.I64Val}, Tenant: tenant}
		} else if tsExpr.Type == tree.ATMOTIMESTAMP {
			if lit.I64Val <= 0 {
				err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp value", lit.I64Val)
				return
			}
			if bgSnapshot := builder.compCtx.GetSnapshot(); builder.isRestoreByTs {
				tenant = &SnapshotTenant{
					TenantName: bgSnapshot.Tenant.TenantName,
					TenantID:   bgSnapshot.Tenant.TenantID,
				}
			}
			snapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: lit.I64Val}, Tenant: tenant}
		} else {
			err = moerr.NewInvalidArg(builder.GetContext(), "invalid timestamp hint for snapshot hint", lit.I64Val)
			return
		}
	default:
		err = moerr.NewInvalidArg(builder.GetContext(), "invalid input expr ", tsExpr.Expr.String())
	}

	return
}

func IsSnapshotValid(snapshot *Snapshot) bool {
	if snapshot == nil {
		return false
	}

	if snapshot.TS == nil || snapshot.TS.Equal(timestamp.Timestamp{PhysicalTime: 0, LogicalTime: 0}) {
		return false
	}

	return true
}

// getPartitionColNameFromExpr tries to extract the first column name from a partition expression
// like "hash(col)". It recursively searches function arguments until it finds a column reference.
func getPartitionColNameFromExpr(expr *plan.Expr) string {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range e.F.Args {
			switch col := e.F.Args[i].Expr.(type) {
			case *plan.Expr_Col:
				return col.Col.Name
			case *plan.Expr_F:
				if name := getPartitionColNameFromExpr(e.F.Args[i]); name != "" {
					return name
				}
			}
		}
	}
	return ""
}

// collectLockTargets traverses the plan tree rooted at nodeID and collects
// LockTarget entries for all TABLE_SCAN nodes found. This supports SELECT ... FOR UPDATE
// with JOIN tables by locking rows from every table involved in the query.
func (builder *QueryBuilder) collectLockTargets(nodeID int32) []*plan.LockTarget {
	node := builder.qry.Nodes[nodeID]
	var targets []*plan.LockTarget

	if node.NodeType == plan.Node_TABLE_SCAN {
		tableDef := node.GetTableDef()
		if tableDef != nil && tableDef.Pkey != nil {
			pkPos, pkTyp := getPkPos(tableDef, false)
			if pkPos >= 0 {
				lockTarget := &plan.LockTarget{
					TableId:            tableDef.TblId,
					PrimaryColIdxInBat: int32(pkPos),
					PrimaryColRelPos:   node.BindingTags[0],
					PrimaryColTyp:      pkTyp,
					Block:              true,
					RefreshTsIdxInBat:  -1,
				}

				if tableDef.Partition != nil && len(tableDef.Partition.PartitionDefs) > 0 {
					if colName := getPartitionColNameFromExpr(tableDef.Partition.PartitionDefs[0].Def); colName != "" {
						if pos, ok := tableDef.Name2ColIndex[colName]; ok {
							lockTarget.HasPartitionCol = true
							lockTarget.PartitionColIdxInBat = pos
						}
					}
				}

				targets = append(targets, lockTarget)
			}
		}
	}

	for _, childID := range node.Children {
		targets = append(targets, builder.collectLockTargets(childID)...)
	}

	return targets
}
