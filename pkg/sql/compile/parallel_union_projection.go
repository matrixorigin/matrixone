// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// A total, pointwise projection commutes with concatenation. Evaluate it on
// each existing branch (including packed Mcpu workers) before the one-stream
// fan-in, without changing worker count, branch evaluation, or output types.
func (c *Compile) canCompileParallelUnionAllProjection(
	step, nodeID int32, nodes []*plan.Node, outerDemand bool,
) bool {
	if c.IsTpQuery() || outerDemand || nodeID < 0 || int(nodeID) >= len(nodes) {
		return false
	}
	node := nodes[nodeID]
	if node == nil || node.NodeType != plan.Node_PROJECT || len(node.Children) != 1 ||
		len(node.ProjectList) == 0 || !parallelUnionProjectionHasOnlyRowExpressions(node) {
		return false
	}
	childID := node.Children[0]
	if childID < 0 || int(childID) >= len(nodes) || childID == nodeID {
		return false
	}
	child := nodes[childID]
	if !canPreserveParallelUnionAllBranches(child) ||
		!parallelUnionProjectionHasOnlyRowExpressions(child) ||
		orderedScalarUnionAllResult(step, childID, c.pn.GetQuery()) {
		return false
	}
	for _, branchID := range child.Children {
		if branchID < 0 || int(branchID) >= len(nodes) || nodes[branchID] == nil ||
			branchID == childID || branchID == nodeID {
			return false
		}
	}
	for _, expr := range node.ProjectList {
		if !parallelUnionProjectionExprIsTotal(expr, child.ProjectList) {
			return false
		}
	}
	return true
}

// Reject executable owners and dependencies outside ProjectList. Statistics,
// analysis and binding metadata stay on the original node; no plan is rewritten.
func parallelUnionProjectionHasOnlyRowExpressions(node *plan.Node) bool {
	if node.ExtraOptions != "" || node.FilterIsBarrier || node.RankOption != nil ||
		len(node.FilterList) != 0 || len(node.BlockFilterList) != 0 || len(node.OrderBy) != 0 ||
		node.SampleFunc != nil || len(node.GroupingFlag) != 0 ||
		len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 ||
		len(node.LockTargets) != 0 || len(node.SendMsgList) != 0 || len(node.RecvMsgList) != 0 ||
		len(node.SourceStep) != 0 || node.RecursiveSink || node.RecursiveCte ||
		node.RowsetData != nil || node.ExternScan != nil || node.IndexReaderParam != nil ||
		node.IndexSearchScan != nil || node.ClusterTable != nil ||
		node.InsertCtx != nil || node.DeleteCtx != nil || node.PreInsertCtx != nil ||
		node.PreInsertUkCtx != nil || node.PreInsertSkCtx != nil || node.PostDmlCtx != nil ||
		len(node.UpdateCtxList) != 0 || node.DedupJoinCtx != nil || node.Fuzzymessage != nil {
		return false
	}
	remaining := *node
	remaining.ProjectList = nil
	remaining.TableDef = nil // catalog metadata is not evaluated by PROJECT/UNION ALL
	unknown := false
	_ = plan.VisitExpressionsInOwner(&remaining, func(*plan.Expr) error {
		unknown = true
		return nil
	})
	return !unknown
}

func parallelUnionProjectionExprIsTotal(expr *plan.Expr, input []*plan.Expr) bool {
	if expr == nil {
		return false
	}
	if _, err := types.TypeFromPlan(expr.Typ); err != nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.RelPos != 0 || e.Col.ColPos < 0 ||
			int(e.Col.ColPos) >= len(input) || input[e.Col.ColPos] == nil {
			return false
		}
		inputType, err := types.TypeFromPlan(input[e.Col.ColPos].Typ)
		return err == nil && inputType.Eq(plan2.MakeTypeByPlan2Type(expr.Typ))
	case *plan.Expr_Lit:
		return parallelUnionProjectionLiteralIsTotal(expr.Typ, e.Lit)
	case *plan.Expr_F:
		if e.F == nil || e.F.Func == nil {
			return false
		}
		id, _ := function.DecodeOverloadID(e.F.Func.Obj)
		if _, ok := function.GetFunctionByIdWithoutError(e.F.Func.Obj); !ok {
			return false
		}
		switch id {
		case function.GROUPING:
			if len(e.F.Args) == 0 || expr.Typ.Id != int32(types.T_int64) {
				return false
			}
			for _, arg := range e.F.Args {
				if arg.GetCol() == nil || !parallelUnionProjectionExprIsTotal(arg, input) {
					return false
				}
			}
			return true
		case function.IFF:
			if len(e.F.Args) != 3 {
				return false
			}
			for _, arg := range e.F.Args {
				if !parallelUnionProjectionExprIsTotal(arg, input) {
					return false
				}
			}
			if !parallelUnionProjectionTruthyType(types.T(e.F.Args[0].Typ.Id)) ||
				!planTypeEqual(expr.Typ, e.F.Args[1].Typ) || !planTypeEqual(expr.Typ, e.F.Args[2].Typ) {
				return false
			}
			args := []types.Type{
				plan2.MakeTypeByPlan2Type(e.F.Args[0].Typ),
				plan2.MakeTypeByPlan2Type(e.F.Args[1].Typ),
				plan2.MakeTypeByPlan2Type(e.F.Args[2].Typ),
			}
			resolved, ok := function.GetFunctionByNameWithoutError("if", args)
			if !ok || resolved.GetEncodedOverloadID() != e.F.Func.Obj ||
				!resolved.GetReturnType().Eq(plan2.MakeTypeByPlan2Type(expr.Typ)) {
				return false
			}
			return true
		}
	}
	return false
}

func parallelUnionProjectionLiteralIsTotal(typ plan.Type, lit *plan.Literal) bool {
	// Src may own a deferred conversion/diagnostic, unlike a stored value.
	if lit == nil || lit.Src != nil {
		return false
	}
	if _, err := colexec.DecodeLiteralStringSource(lit); err != nil {
		return false
	}
	if lit.Isnull {
		return true
	}
	oid := types.T(typ.Id)
	switch value := lit.Value.(type) {
	case *plan.Literal_Bval:
		return oid == types.T_bool
	case *plan.Literal_I8Val:
		return oid == types.T_int8
	case *plan.Literal_I16Val:
		return oid == types.T_int16
	case *plan.Literal_I32Val:
		return oid == types.T_int32 || oid == types.T_year
	case *plan.Literal_I64Val:
		return oid == types.T_int64
	case *plan.Literal_U8Val:
		return oid == types.T_uint8
	case *plan.Literal_U16Val:
		return oid == types.T_uint16
	case *plan.Literal_U32Val:
		return oid == types.T_uint32
	case *plan.Literal_U64Val:
		return oid == types.T_uint64 || oid == types.T_bit
	case *plan.Literal_Fval:
		return oid == types.T_float32
	case *plan.Literal_Dval:
		return oid == types.T_float64
	case *plan.Literal_Dateval:
		return oid == types.T_date
	case *plan.Literal_Timeval:
		return oid == types.T_time
	case *plan.Literal_Datetimeval:
		return oid == types.T_datetime
	case *plan.Literal_Timestampval:
		return oid == types.T_timestamp && typ.Scale >= 0 && typ.Scale <= 6
	case *plan.Literal_Decimal64Val:
		return oid == types.T_decimal64 && value.Decimal64Val != nil
	case *plan.Literal_Decimal128Val:
		return oid == types.T_decimal128 && value.Decimal128Val != nil
	case *plan.Literal_EnumVal:
		return oid == types.T_enum
	case *plan.Literal_Sval:
		// Array/datalink string carriers require parsing at executor creation.
		return oid.IsMySQLString() || oid == types.T_json || oid == types.T_geometry
	}
	return false
}

// These are exactly IffConditionTruthyAt's non-failing physical domains. Text
// conditions can report conversion errors; arbitrary functions/casts are not
// admitted merely because they are deterministic.
func parallelUnionProjectionTruthyType(oid types.T) bool {
	switch oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return true
	}
	return false
}
