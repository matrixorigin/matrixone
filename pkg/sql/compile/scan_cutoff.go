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

package compile

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

func remoteOrderScanCutoffBound(expr *plan.Expr) (uint64, bool) {
	if expr == nil || types.T(expr.Typ.Id) != types.T_uint64 {
		return 0, false
	}
	lit := expr.GetLit()
	if lit == nil || lit.Isnull || lit.Src != nil {
		return 0, false
	}
	value, ok := lit.Value.(*plan.Literal_U64Val)
	if !ok || value.U64Val <= mergeTopResidentPlanThreshold {
		return 0, false
	}
	return value.U64Val, true
}

// paginationCutoffScan reuses the planner's Top-to-scan lineage and its
// expression-safety proof. The physical table definition must independently
// prove that unread blocks cannot contain NULLs for the leading key.
func (c *Compile) paginationCutoffScan(node *plan.Node) (*plan.Node, int32) {
	if c == nil || c.pn == nil || statementHasSQLCalcFoundRows(c.stmt) || c.foundRowsOwnerNode != nil ||
		// A prepared TP physical pipeline can be reused for new parameter
		// values. AP compiles are statement-owned and are rebound each time.
		(c.pn.IsPrepare && c.IsTpQuery()) ||
		!plan2.PreparedPaginationTopInputSafe(c.pn.GetQuery(), node) ||
		len(node.SendMsgList) != 1 || node.SendMsgList[0].MsgType != int32(message.MsgTopValue) ||
		node.SendMsgList[0].MsgTag <= 0 || !order.SupportsScanCutoffType(node.OrderBy[0].Expr.Typ) {
		return nil, 0
	}
	query := c.pn.GetQuery()
	id := node.Children[0]
	for depth := 0; depth < len(query.Nodes); depth++ {
		input := query.Nodes[id] // PreparedPaginationTopInputSafe checked this path
		if input.NodeType == plan.Node_TABLE_SCAN {
			if len(input.RecvMsgList) != 1 || input.RecvMsgList[0].MsgTag != node.SendMsgList[0].MsgTag ||
				input.RecvMsgList[0].MsgType != node.SendMsgList[0].MsgType ||
				len(input.OrderBy) != 1 || input.OrderBy[0] == nil || input.OrderBy[0].Expr == nil ||
				input.OrderBy[0].Expr.GetCol() == nil || input.TableDef == nil ||
				input.TableDef.Partition != nil || input.TableDef.FeatureFlag != 0 || input.TableDef.IsTemporary || input.ClusterTable != nil ||
				!sameScanCutoffType(node.OrderBy[0].Expr.Typ, input.OrderBy[0].Expr.Typ) ||
				(node.OrderBy[0].Flag&plan.OrderBySpec_DESC) != (input.OrderBy[0].Flag&plan.OrderBySpec_DESC) ||
				!scanCutoffPhysicalColumn(input.TableDef, input.OrderBy[0].Expr) {
				return nil, 0
			}
			return input, node.SendMsgList[0].MsgTag
		}
		id = input.Children[0]
	}
	return nil, 0
}

func sameScanCutoffType(a, b plan.Type) bool {
	return a.Id == b.Id && a.Scale == b.Scale && a.Charset == b.Charset &&
		a.CollationVersion == b.CollationVersion && a.PadSpace == b.PadSpace &&
		order.SupportsScanCutoffType(a) && order.SupportsScanCutoffType(b)
}

func scanCutoffPhysicalColumn(def *plan.TableDef, expr *plan.Expr) bool {
	if def == nil || expr == nil || expr.GetCol() == nil || !order.SupportsScanCutoffType(expr.Typ) {
		return false
	}
	name := strings.ToLower(expr.GetCol().Name)
	if i := strings.LastIndexByte(name, '.'); i >= 0 {
		name = name[i+1:]
	}
	if name == "" {
		return false
	}
	found := false
	for _, col := range def.Cols {
		if col == nil || strings.Contains(col.Name, ".") {
			return false
		}
		// Readers use lower-case names, not Unicode simple-fold equivalence.
		if strings.ToLower(col.Name) == name {
			if found || !sameScanCutoffType(col.Typ, expr.Typ) {
				return false
			}
			found = true
		}
	}
	return found
}

// The hint can only attach to an actual scan scope whose intervening physical
// operators preserve the planner's row lineage without duplication.
func scanCutoffScopeMatches(s *Scope, scan *plan.Node) bool {
	if s == nil || s.Magic != Remote || s.DataSource == nil || s.DataSource.node != scan ||
		s.IsLoad || s.IsTbFunc || len(s.PreScopes) != 0 || s.RootOp == nil {
		return false
	}
	return scanCutoffInputMatches(s.RootOp)
}

func scanCutoffInputMatches(op vm.Operator) bool {
	for op != nil {
		switch op.OpType() {
		case vm.TableScan:
			_, ok := op.(*table_scan.TableScan)
			return ok && op.GetOperatorBase().NumChildren() == 0
		case vm.Filter, vm.Projection:
			if op.GetOperatorBase().NumChildren() != 1 {
				return false
			}
			op = op.GetOperatorBase().GetChildren(0)
		default:
			return false
		}
	}
	return false
}

// initScanCutoff binds one fresh shared summary to this reader generation.
// Return the unshifted tag only when every reader is paired with this Order.
func initScanCutoff(s *Scope) int32 {
	if s != nil {
		clearScanCutoffs(s.RootOp)
	}
	if s == nil || s.DataSource == nil || s.DataSource.node == nil || s.RootOp == nil ||
		len(s.DataSource.RecvMsgList) != 1 || len(s.DataSource.OrderBy) != 1 ||
		s.DataSource.OrderBy[0] == nil || s.DataSource.OrderBy[0].Expr == nil ||
		s.DataSource.IndexReaderParam != nil || s.DataSource.node.NodeType != plan.Node_TABLE_SCAN ||
		len(s.DataSource.RuntimeFilterSpecs) != 0 || s.DataSource.node.TableDef == nil ||
		s.DataSource.node.TableDef.Partition != nil || s.DataSource.node.TableDef.FeatureFlag != 0 || s.DataSource.node.TableDef.IsTemporary ||
		s.DataSource.TableDef == nil || s.DataSource.TableDef.Partition != nil || s.DataSource.TableDef.FeatureFlag != 0 || s.DataSource.TableDef.IsTemporary ||
		!scanCutoffPhysicalColumn(s.DataSource.TableDef, s.DataSource.OrderBy[0].Expr) {
		return 0
	}
	header := s.DataSource.RecvMsgList[0]
	if header.MsgType != int32(message.MsgTopValue) || header.MsgTag <= 0 {
		return 0
	}
	for op := s.RootOp; op != nil; {
		if o, ok := op.(*order.Order); ok {
			if o.ScanCutoffLimit == 0 || o.ScanCutoffTag != header.MsgTag ||
				len(o.OrderBySpec) == 0 || o.OrderBySpec[0] == nil || o.OrderBySpec[0].Expr == nil ||
				o.OrderBySpec[0].Expr.GetCol() == nil || o.OrderBySpec[0].Expr.GetCol().RelPos != 0 ||
				o.OrderBySpec[0].Expr.GetCol().ColPos < 0 ||
				!sameScanCutoffType(o.OrderBySpec[0].Expr.Typ, s.DataSource.OrderBy[0].Expr.Typ) ||
				(o.OrderBySpec[0].Flag&plan.OrderBySpec_DESC) != (s.DataSource.OrderBy[0].Flag&plan.OrderBySpec_DESC) ||
				o.NumChildren() != 1 || !scanCutoffInputMatches(o.GetChildren(0)) {
				return 0
			}
			o.ScanCutoff = order.NewScanCutoff(o.ScanCutoffLimit,
				types.MustTypeFromPlan(o.OrderBySpec[0].Expr.Typ),
				o.OrderBySpec[0].Flag&plan.OrderBySpec_DESC != 0)
			return header.MsgTag
		}
		switch op.OpType() {
		case vm.Connector, vm.MergeOrder, vm.Offset, vm.Limit, vm.Projection, vm.Output:
		default:
			return 0
		}
		if op.GetOperatorBase().NumChildren() != 1 {
			return 0
		}
		op = op.GetOperatorBase().GetChildren(0)
	}
	return 0
}

func clearScanCutoffs(op vm.Operator) {
	if op == nil {
		return
	}
	if candidate, ok := op.(*order.Order); ok {
		candidate.ScanCutoff = nil
	}
	for i := 0; i < op.GetOperatorBase().NumChildren(); i++ {
		clearScanCutoffs(op.GetOperatorBase().GetChildren(i))
	}
}
