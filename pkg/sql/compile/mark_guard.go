// Copyright 2026 Matrix Origin
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

package compile

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// A guard is optional: the original MARK and filter remain authoritative. The
// count, byte, and worker bounds also cap the per-reader IN executor maps.
const (
	markGuardKeyLimit     = 128
	markGuardPayloadBytes = 128 << 10
	markGuardMaxWidth     = 128
	markGuardMaxWorkers   = 8
	markGuardMinRows      = 8192
)

type markScanGuard struct {
	probe       *plan.RuntimeFilterSpec
	alternative *plan.Expr
	producer    *Scope
}

// markGuardCandidate is a complete, private proof for one local guard. The
// finder only reads the physical plan and copies expressions; installation
// commits the consumer and producer together after the codec is validated.
type markGuardCandidate struct {
	root        *Scope
	producer    *Scope
	build       *hashbuild.HashBuild
	probe       *plan.Expr
	alternative *plan.Expr
	tag         int32
}

// installMarkScanGuard only attaches local execution state, after physical
// scheduling. It must never add a probe to a serialized plan or remote scope:
// the OR arm is an essential part of the guard, not an independent PK filter.
func (c *Compile) installMarkScanGuard(qry *plan.Query, roots []*Scope) {
	candidate := c.findMarkGuardCandidate(qry, roots)
	if candidate == nil {
		return
	}
	// HashBuild collects equality keys by key slot, independently of the
	// original build input's column position. There is exactly one key here.
	buildKey := plan2.GetColExpr(candidate.build.Conditions[0].Typ, 0, 0)
	consumer, producer, ok := plan2.MakeExactRuntimeFilterPair(c.proc.Ctx, c.proc.GetService(), candidate.tag, false, markGuardKeyLimit, candidate.probe, buildKey, true)
	if !ok {
		return
	}
	candidate.root.DataSource.markGuard = &markScanGuard{probe: consumer, alternative: candidate.alternative, producer: candidate.producer}
	candidate.build.RuntimeFilterSpec = producer
	// At most 128 keys of 512 bytes bound live key bytes to 64 KiB. Vector
	// capacity, metadata and serialization add bounded overhead.
	candidate.build.OptionalRuntimeFilterMaxKeyBytes = 4 * markGuardMaxWidth
}

// findMarkGuardCandidate examines eligibility and column lineage without
// attaching a producer, consumer or changing a shared plan.
func (c *Compile) findMarkGuardCandidate(qry *plan.Query, roots []*Scope) *markGuardCandidate {
	if qry == nil || c.isPrepare || len(roots) != 1 || qry.StmtType != plan.Query_SELECT {
		return nil
	}
	root := roots[0]
	if root == nil || root.DataSource == nil || root.DataSource.node == nil || root.RootOp == nil || c.proc == nil || !markGuardLocalScopes(root, c.addr, c.proc.GetMessageBoard()) {
		return nil
	}
	if _, ok := vm.GetLeafOp(root.RootOp).(*table_scan.TableScan); !ok {
		return nil
	}
	var mark *hashjoin.HashJoin
	var parentFilter *filter.Filter
	marks := 0
	_ = vm.HandleAllOp(root.RootOp, func(parent vm.Operator, op vm.Operator) error {
		if j, ok := op.(*hashjoin.HashJoin); ok && j.JoinType == plan.Node_MARK {
			mark = j
			parentFilter, _ = parent.(*filter.Filter)
			marks++
		}
		return nil
	})
	if marks != 1 || parentFilter == nil || len(parentFilter.FilterExprs) != 1 || mark.IsShuffle || mark.CanSkipProbe || mark.NonEqCond != nil || len(mark.EqConds) != 2 || len(mark.EqConds[0]) != 1 || len(mark.EqConds[1]) != 1 {
		return nil
	}
	var build *hashbuild.HashBuild
	builds := 0
	var buildScope *Scope
	for _, s := range root.PreScopes {
		if s.RootOp != nil {
			_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
				if b, ok := op.(*hashbuild.HashBuild); ok && b.JoinMapTag == mark.JoinMapTag {
					build = b
					buildScope = s
					builds++
				}
				return nil
			})
		}
	}
	if builds != 1 {
		return nil
	}
	// The optional producer must be an independent table scan, never a
	// channel/merge fed by the probe which would introduce a waiting cycle.
	if _, ok := vm.GetLeafOp(buildScope.RootOp).(*table_scan.TableScan); !ok {
		return nil
	}
	// A build scan with its own message dependency could wait on the probe.
	// Do not add a reverse dependency unless the producer is independent.
	if !markGuardIndependentProducer(buildScope) {
		return nil
	}
	if !build.NeedHashMap || !build.TrackNullKeys || build.IsShuffle || build.RuntimeFilterSpec != nil || len(build.Conditions) != 1 || build.Conditions[0].GetCol() == nil {
		return nil
	}
	rightKey := mark.EqConds[1][0].GetCol()
	if rightKey == nil || rightKey.RelPos != 1 || rightKey.ColPos != build.Conditions[0].GetCol().ColPos || !guardVarcharPair(mark.EqConds[1][0].Typ, build.Conditions[0].Typ) {
		return nil
	}

	// Only the physical filter directly above this MARK is authoritative.
	// Optimizer node arrays can retain unreachable historical FILTER nodes.
	filterNode := queryNode(qry, int32(parentFilter.GetOperatorBase().GetIdx()))
	if filterNode == nil || filterNode.NodeType != plan.Node_FILTER || len(filterNode.Children) != 1 {
		return nil
	}
	selected := queryNode(qry, filterNode.Children[0])
	if selected == nil || selected.NodeType != plan.Node_JOIN || selected.JoinType != plan.Node_MARK || int(selected.NodeId) != mark.GetOperatorBase().GetIdx() || len(selected.Children) != 2 || len(selected.OnList) != 1 {
		return nil
	}
	expr := parentFilter.FilterExprs[0].GetF()
	if expr == nil || expr.Func == nil || expr.Func.ObjName != "or" || len(expr.Args) != 2 {
		return nil
	}
	fid, _ := function.DecodeOverloadID(expr.Func.Obj)
	if fid != function.OR {
		return nil
	}
	var alternative *plan.Expr
	for i := 0; i < 2; i++ {
		ref := expr.Args[i].GetCol()
		if ref == nil || ref.RelPos != 0 || ref.ColPos < 0 || int(ref.ColPos) >= len(selected.ProjectList) {
			continue
		}
		marker := selected.ProjectList[ref.ColPos].GetCol()
		if marker == nil || marker.RelPos != -1 || marker.ColPos != 0 {
			continue
		}
		alt := plan2.DeepCopyExpr(expr.Args[1-i])
		if _, ok := guardStringEquality(alt); !ok || alternative != nil {
			return nil
		}
		alternative = alt
	}
	if alternative == nil {
		return nil
	}
	left := queryNode(qry, selected.Children[0])
	if left == nil {
		return nil
	}
	var index *plan.Node
	if left.NodeType == plan.Node_JOIN && left.JoinType == plan.Node_INDEX {
		if len(left.Children) != 2 || left.Limit != nil || left.Offset != nil || len(left.OrderBy) != 0 || !guardDirectProjections(left, false) {
			return nil
		}
		index = left
		left = queryNode(qry, left.Children[0])
		if left == nil {
			return nil
		}
	}
	scan := root.DataSource.node
	if left.NodeType != plan.Node_TABLE_SCAN || left.NodeId != scan.NodeId || left.Limit != nil || left.Offset != nil || len(left.OrderBy) != 0 || len(left.AggList) != 0 || !guardDirectProjections(left, false) || selected.Limit != nil || selected.Offset != nil || !guardDirectProjections(selected, true) || scan.Stats == nil || plan2.IsDefaultStats(scan.Stats) || (math.IsNaN(scan.Stats.TableCnt) || math.IsInf(scan.Stats.TableCnt, 0) || scan.Stats.TableCnt < markGuardMinRows) {
		return nil
	}
	// The guard is evaluated before ordinary row filters, so only admit a scan
	// whose existing filters are safe to evaluate on any input row.
	for _, f := range scan.FilterList {
		if _, ok := guardStringEquality(f); !ok {
			return nil
		}
	}
	altCol, _ := guardStringEquality(alternative)
	altType := alternative.GetF().Args[0].Typ
	if alternative.GetF().Args[0].GetCol() == nil {
		altType = alternative.GetF().Args[1].Typ
	}
	pos, ok := mapMarkGuardColumn(selected, index, scan, altCol.ColPos, true, altType)
	if !ok {
		return nil
	}
	altCol.ColPos = pos
	altCol.RelPos = 0
	probe := plan2.DeepCopyExpr(mark.EqConds[0][0])
	key := probe.GetCol()
	if key == nil || key.RelPos != 0 {
		return nil
	}
	pos, ok = mapMarkGuardColumn(selected, index, scan, key.ColPos, false, probe.Typ)
	if !ok {
		return nil
	}
	key.ColPos = pos
	key.RelPos = 0
	if !guardVarcharPair(probe.Typ, build.Conditions[0].Typ) || !guardHasWideProjection(scan, key.ColPos, altCol.ColPos) {
		return nil
	}
	tag, ok := markGuardMessageTag(qry, root)
	if !ok {
		return nil
	}
	return &markGuardCandidate{
		root: root, producer: buildScope, build: build,
		probe: probe, alternative: alternative, tag: tag,
	}
}

// No guard dependency may cross a remote boundary or be cloned into multiple
// build producers. Unknown addresses are not sufficient proof of locality.
func markGuardLocalScopes(root *Scope, addr string, board *message.MessageBoard) bool {
	if root == nil || board == nil || root.Proc == nil || root.Proc.GetMessageBoard() != board || addr == "" || root.NodeInfo.Addr == "" || root.IsRemote || !root.ipAddrMatch(addr) || len(root.RemoteReceivRegInfos) != 0 || root.NodeInfo.Mcpu <= 0 || root.NodeInfo.Mcpu > markGuardMaxWorkers || len(root.PreScopes) == 0 || len(root.PreScopes) > 4 || (root.Magic != Merge && root.Magic != Remote) || root.LazyPreScopes || len(root.lazyRemoteFragmentCounts) != 0 {
		return false
	}
	for _, s := range root.PreScopes {
		if s == nil || s.Proc == nil || s.Proc.GetMessageBoard() != board || s.NodeInfo.Addr == "" || s.IsRemote || !s.ipAddrMatch(addr) || len(s.PreScopes) != 0 || len(s.RemoteReceivRegInfos) != 0 || s.NodeInfo.Mcpu != 1 || (s.Magic != Normal && s.Magic != Merge && s.Magic != Remote) || s.LazyPreScopes || len(s.lazyRemoteFragmentCounts) != 0 {
			return false
		}
	}
	return true
}

// DataSource dependency fields are filled only during initialization. Check
// the plan at installation and both representations again before waiting.
func markGuardIndependentProducer(scope *Scope) bool {
	if scope == nil || scope.DataSource == nil || scope.DataSource.node == nil {
		return false
	}
	source := scope.DataSource
	return len(source.node.RuntimeFilterProbeList) == 0 &&
		len(source.node.RecvMsgList) == 0 &&
		len(source.RuntimeFilterSpecs) == 0 &&
		len(source.RecvMsgList) == 0
}

func queryNode(q *plan.Query, id int32) *plan.Node {
	if id < 0 || int(id) >= len(q.Nodes) {
		return nil
	}
	return q.Nodes[id]
}

// mapMarkGuardColumn crosses each actual projection, ending in the raw batch
// consumed by the scan filter (which can differ from scan output positions).
func mapMarkGuardColumn(mark, index, scan *plan.Node, pos int32, fromMark bool, want plan.Type) (int32, bool) {
	nodes := []*plan.Node{}
	if fromMark {
		nodes = append(nodes, mark)
	}
	if index != nil {
		nodes = append(nodes, index)
	}
	nodes = append(nodes, scan)
	for _, n := range nodes {
		if n == nil || pos < 0 || int(pos) >= len(n.ProjectList) {
			return 0, false
		}
		project := n.ProjectList[pos]
		col := project.GetCol()
		if col == nil || col.RelPos != 0 || col.ColPos < 0 || !guardVarcharPair(want, project.Typ) {
			return 0, false
		}
		pos = col.ColPos
	}
	if scan.TableDef == nil || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil || !guardVarcharPair(want, scan.TableDef.Cols[pos].Typ) {
		return 0, false
	}
	return pos, true
}

// Row filtering helps when it avoids materializing wide values. On narrow
// scans it adds per-row work even when the other OR arm matches most rows.
// Use the actual scan projection rather than the table's unprojected schema.
func guardHasWideProjection(scan *plan.Node, probePos, alternativePos int32) bool {
	if scan.TableDef == nil || probePos < 0 || alternativePos < 0 ||
		int(probePos) >= len(scan.TableDef.Cols) || int(alternativePos) >= len(scan.TableDef.Cols) {
		return false
	}
	filterColumns := make([]bool, len(scan.TableDef.Cols))
	filterColumns[probePos] = true
	filterColumns[alternativePos] = true
	for _, expr := range scan.FilterList {
		if !table_scan.CollectFilterColumnPositions(expr, filterColumns) {
			return false
		}
	}
	for _, spec := range scan.RuntimeFilterProbeList {
		if spec == nil || spec.Expr == nil || !table_scan.CollectFilterColumnPositions(spec.Expr, filterColumns) {
			return false
		}
	}
	for _, expr := range scan.ProjectList {
		if expr == nil {
			continue
		}
		col := expr.GetCol()
		if col != nil && col.ColPos >= 0 && int(col.ColPos) < len(filterColumns) &&
			!filterColumns[col.ColPos] && scan.TableDef.Cols[col.ColPos] != nil &&
			table_scan.IsLateMaterializationCandidate(scan.TableDef.Cols[col.ColPos].Typ) {
			return true
		}
	}
	return false
}

// Projection expressions before the original WHERE may have observable
// effects or errors. Only move a guard across simple column forwarding.
func guardDirectProjections(node *plan.Node, allowMarker bool) bool {
	for _, expr := range node.ProjectList {
		col := expr.GetCol()
		if col == nil || (col.RelPos != 0 && !(allowMarker && col.RelPos == -1)) {
			return false
		}
	}
	return true
}

func guardVarcharPair(left, right plan.Type) bool {
	return left.Id == int32(types.T_varchar) && right.Id == left.Id && left.Width > 0 && left.Width <= markGuardMaxWidth && left.Width == right.Width && left.Scale == right.Scale &&
		left.Charset == right.Charset && left.Charset <= uint32(types.CharsetUTF8) && left.CollationVersion == right.CollationVersion && !left.CollationMergeConflict && !right.CollationMergeConflict
}

// guardStringEquality deliberately accepts only a column and an already-folded
// literal of the same bounded VARCHAR domain. It is safe on rows that the
// original scan's other filters may later reject.
func guardStringEquality(expr *plan.Expr) (*plan.ColRef, bool) {
	if expr == nil {
		return nil, false
	}
	f := expr.GetF()
	if f == nil || f.Func == nil || f.Func.ObjName != "=" || len(f.Args) != 2 {
		return nil, false
	}
	id, _ := function.DecodeOverloadID(f.Func.Obj)
	if id != function.EQUAL {
		return nil, false
	}
	for i := 0; i < 2; i++ {
		col := f.Args[i].GetCol()
		lit := f.Args[1-i].GetLit()
		if col == nil || col.RelPos != 0 || col.ColPos < 0 || lit == nil || lit.Isnull || !guardVarcharPair(f.Args[i].Typ, f.Args[1-i].Typ) {
			continue
		}
		value, ok := lit.Value.(*plan.Literal_Sval)
		if !ok || len(value.Sval) > 4*markGuardMaxWidth {
			continue
		}
		return col, true
	}
	return nil, false
}

// No top-value tags may be present: their physical parallel copies derive new
// tags after compilation. All other declared query tags and join-map tags are
// included before allocating this execution-local tag.
func markGuardMessageTag(q *plan.Query, root *Scope) (int32, bool) {
	if uint64(len(q.Nodes)) >= math.MaxInt32 {
		return 0, false
	}
	// Some compiler-created join-map tags are derived from plan node IDs.
	maxTag := int32(len(q.Nodes))
	visit := func(t int32) {
		if t > maxTag {
			maxTag = t
		}
	}
	for _, n := range q.Nodes {
		if n == nil {
			continue
		}
		for _, h := range n.SendMsgList {
			if h.MsgType == int32(message.MsgTopValue) {
				return 0, false
			}
			visit(h.MsgTag)
		}
		for _, h := range n.RecvMsgList {
			if h.MsgType == int32(message.MsgTopValue) {
				return 0, false
			}
			visit(h.MsgTag)
		}
		for _, r := range n.RuntimeFilterBuildList {
			if r != nil {
				visit(r.Tag)
			}
		}
		for _, r := range n.RuntimeFilterProbeList {
			if r != nil {
				visit(r.Tag)
			}
		}
	}
	for _, s := range append([]*Scope{root}, root.PreScopes...) {
		if s.DataSource != nil {
			for _, h := range s.DataSource.RecvMsgList {
				if h.MsgType == int32(message.MsgTopValue) {
					return 0, false
				}
				visit(h.MsgTag)
			}
			for _, r := range s.DataSource.RuntimeFilterSpecs {
				if r != nil {
					visit(r.Tag)
				}
			}
		}
		if s.RootOp != nil {
			_ = vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
				switch x := op.(type) {
				case *hashbuild.HashBuild:
					visit(x.JoinMapTag)
					if x.RuntimeFilterSpec != nil {
						visit(x.RuntimeFilterSpec.Tag)
					}
				case *hashjoin.HashJoin:
					visit(x.JoinMapTag)
					for _, r := range x.RuntimeFilterSpecs {
						if r != nil {
							visit(r.Tag)
						}
					}
				}
				return nil
			})
		}
	}
	if maxTag == math.MaxInt32 {
		return 0, false
	}
	return maxTag + 1, true
}

// receiveMarkScanGuard turns the optional key message into a row predicate.
// A missing/unsupported filter must leave the original MARK plan untouched.
func (s *Scope) receiveMarkScanGuard(c *Compile, guard *markScanGuard) *plan.Expr {
	receiver := message.NewMessageReceiver([]int32{guard.probe.Tag}, message.AddrBroadCastOnCurrentCN(), c.proc.GetMessageBoard())
	msgs, canceled, err := receiver.ReceiveMessage(true, s.Proc.Ctx)
	if err != nil || canceled {
		// The original MARK owns errors and cancellation. A failed optional
		// guard must not create another reader-cleanup or error path.
		return nil
	}
	// One producer publishes one complete terminal message per generation.
	// Conflicting messages already visible together cannot safely narrow rows.
	// A later duplicate is not detectable here; producer ownership still matters.
	if len(msgs) != 1 {
		return nil
	}
	msg, ok := msgs[0].(message.RuntimeFilterMessage)
	if !ok {
		return nil
	}
	switch msg.Typ {
	case message.RuntimeFilter_DROP:
		// An empty IN branch does not eliminate rows satisfying the other arm.
		return plan2.DeepCopyExpr(guard.alternative)
	case message.RuntimeFilter_IN:
		if msg.Card <= 0 || msg.Card > markGuardKeyLimit || len(msg.Data) > markGuardPayloadBytes {
			return nil
		}
		in := plan2.MakeInExpr(c.proc.Ctx, plan2.DeepCopyExpr(guard.probe.Expr), msg.Card, msg.Data, false)
		expr, bindErr := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "or", []*plan.Expr{in, plan2.DeepCopyExpr(guard.alternative)})
		if bindErr != nil {
			return nil
		}
		return expr
	default:
		// PASS and unrecognized optional payloads cannot narrow the scan.
		return nil
	}
}

// applyMarkScanGuard runs after readers are built but before scan Prepare.
// The guard is row-only, so reader/range setup can overlap the MARK build.
func (s *Scope) applyMarkScanGuard(c *Compile) {
	guard := s.DataSource.markGuard
	if guard == nil || !markGuardIndependentProducer(guard.producer) || (c.proc == nil || !markGuardLocalScopes(s, c.addr, c.proc.GetMessageBoard())) {
		return
	}
	expr := s.receiveMarkScanGuard(c, guard)
	if expr == nil {
		return
	}
	if scan, ok := vm.GetLeafOp(s.RootOp).(*table_scan.TableScan); ok {
		scan.RuntimeFilterExprs = append(scan.RuntimeFilterExprs, expr)
	}
}
