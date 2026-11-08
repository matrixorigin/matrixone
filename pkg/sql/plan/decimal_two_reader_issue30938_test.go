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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// The materialized two-reader witnesses of design cell (xxi) (b) (decision
// log 74 (ii)) and the (xxii) channel family (decision logs 77 (i)/78 (i)).
// A value-unstable shared CTE exports COALESCE(a*b, <76-digit literal>) whose
// head (76,0) equals its merged envelope (76,0) — no head divergence — while
// the product arm's parent domain (65,0) diverges from its published (76,0):
// an AVG/SUM reader must still admit the product seam (F-HEAD-GATE removal).
// The pinned base binds the 76-digit literal through the pre-existing
// decimal-source channel (makePlan2DecimalExprWithType ->
// makePlan2DecimalSourceExpr, make.go): the literal arm is a binder-produced
// cast from a varchar(76) source literal to decimal256(76,0), a conversion
// outside the decimal-to-decimal warning-free family. The approved
// replay-safety/diagnostic-freedom predicate (design section 5, decision
// logs 77 (i)/78 (i)) therefore classifies this producing expression
// UNPROVEN, and its admission rides the single original evaluation in place:
// the (65,0) envelope cast is spliced at the product seam inside the one
// exported COALESCE, no shadow replica exists, and every reader shares that
// single evaluation — each external effect fires exactly once per produced
// row and the statement's client-visible warning stream equals the parent's
// exactly (the in-place half of the two-reader contract). The uuid() export
// makes the CTE value-unstable so it materializes unconditionally
// (cte_identity.go).
//
// The shadow channel's PROVEN half is pinned by the bare-product witness
// below: WITH c AS (SELECT a*b AS v, uuid() AS r ...) carries only a decimal
// arithmetic operator, decimal-to-decimal alignment casts and column leaves,
// so its admission rides one statement-local shadow replica appended at the
// producing projection, the AVG/SUM reader rebinding to it while every
// value-selecting sibling keeps the original export carrier-exact (the
// design's revision-28/29 shadow-shape oracle executed on the shape whose
// every cast stays inside the decimal-to-decimal family).
//
// The coalesce statements reference only v, so the final SINK/SINK_SCAN
// column pruning drops the unreferenced uuid export and leaves bare column
// mirrors between the SINK and the producing projection (query_builder.go's
// removeUnnecessaryProjections runs before the reconcile pass at the
// createQuery exit). The export resolution follows those mirrors to the
// producing expression (design section 5, F-SEAM-RESOLUTION); the helpers
// here are therefore written against the export wiring, not fixed node
// positions.

const twoReader76Nines = "9999999999999999999999999999999999999999999999999999999999999999999999999999"

func buildTwoReaderBoundaryPlan(t *testing.T, selectList string) *plan.Query {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock, "create table m30938r(a decimal(38,0), b decimal(38,0))")
	built, err := runOneStmt(mock, t,
		"with c as (select coalesce(a*b, "+twoReader76Nines+") as v, uuid() as r from m30938r) "+
			selectList)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	return query
}

// buildBareProductTwoReaderPlan builds the PROVEN-channel twin: the shared
// producer exports the bare widened product, whose producing expression
// carries only a decimal arithmetic operator, decimal-to-decimal alignment
// casts and column leaves.
func buildBareProductTwoReaderPlan(t *testing.T, selectList string) *plan.Query {
	t.Helper()
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock, "create table m30938r(a decimal(38,0), b decimal(38,0))")
	built, err := runOneStmt(mock, t,
		"with c as (select a*b as v, uuid() as r from m30938r) "+
			selectList)
	require.NoError(t, err)
	query := built.GetQuery()
	require.NotNil(t, query)
	return query
}

func twoReaderNodeIndex(query *plan.Query) map[int32]*plan.Node {
	byID := make(map[int32]*plan.Node, len(query.Nodes))
	for _, n := range query.Nodes {
		if n != nil {
			byID[n.NodeId] = n
		}
	}
	return byID
}

// twoReaderSink locates the shared producer's SINK, its child (the producer
// root of the post-prune layout) and every SINK_SCAN reading it.
func twoReaderSink(t *testing.T, query *plan.Query) (sink, root *plan.Node, scans []*plan.Node) {
	t.Helper()
	byID := twoReaderNodeIndex(query)
	for _, n := range query.Nodes {
		if n != nil && n.NodeType == plan.Node_SINK {
			require.Nil(t, sink, "the statement carries exactly one shared producer SINK")
			sink = n
		}
	}
	require.NotNil(t, sink, "the value-unstable CTE materializes as a shared producer")
	require.Len(t, sink.Children, 1)
	root = byID[sink.Children[0]]
	require.NotNil(t, root)
	for _, n := range query.Nodes {
		if n != nil && n.NodeType == plan.Node_SINK_SCAN && len(n.SourceStep) == 1 {
			scans = append(scans, n)
		}
	}
	require.NotEmpty(t, scans, "the shared producer has at least one reader scan")
	return sink, root, scans
}

// twoReaderExport names one projection entry in the statement plan.
type twoReaderExport struct {
	node *plan.Node
	slot int
	expr *plan.Expr
}

// twoReaderRootedExports collects every projection entry whose root function
// carries the given name, in plan order.
func twoReaderRootedExports(query *plan.Query, name string) []twoReaderExport {
	var out []twoReaderExport
	for _, n := range query.Nodes {
		if n == nil {
			continue
		}
		for i, e := range n.ProjectList {
			fn := e.GetF()
			if fn == nil || fn.Func == nil || fn.Func.ObjName != name {
				continue
			}
			out = append(out, twoReaderExport{node: n, slot: i, expr: e})
		}
	}
	return out
}

// twoReaderAdmittedExports collects every projection entry that carries the
// product seam's (65,0) envelope-admission cast over a multiplication
// anywhere in its subtree — the single in-place export of an unproven
// producer, or the appended shadow replica of a proven one.
func twoReaderAdmittedExports(query *plan.Query) []twoReaderExport {
	var out []twoReaderExport
	for _, n := range query.Nodes {
		if n == nil {
			continue
		}
		for i, e := range n.ProjectList {
			if twoReaderHasAdmissionCast(e) {
				out = append(out, twoReaderExport{node: n, slot: i, expr: e})
			}
		}
	}
	return out
}

// twoReaderHasAdmissionCast reports whether expr contains the (65,0)
// envelope-admission cast wrapped directly over the multiplication.
func twoReaderHasAdmissionCast(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil {
		if fn.Func.ObjName == "cast" &&
			expr.Typ.Id == int32(types.T_decimal256) && expr.Typ.Width == 65 && expr.Typ.Scale == 0 &&
			len(fn.Args) >= 1 && fn.Args[0].GetF() != nil &&
			fn.Args[0].GetF().Func != nil && fn.Args[0].GetF().Func.ObjName == "*" {
			return true
		}
		for _, arg := range fn.Args {
			if twoReaderHasAdmissionCast(arg) {
				return true
			}
		}
	}
	return false
}

// twoReaderResolveScanSlot follows a consumer column reference through the
// projection mirror chain down to a SINK_SCAN export slot (post-remap
// node/slot identities: RelPos names the owner's child ordinal, ColPos the
// child's local projection slot).
func twoReaderResolveScanSlot(t *testing.T, query *plan.Query, owner *plan.Node, col *plan.ColRef) (scan *plan.Node, scanPos int32, ok bool) {
	t.Helper()
	byID := twoReaderNodeIndex(query)
	cur, c := owner, col
	for depth := 0; depth < 32; depth++ {
		if c.RelPos < 0 || int(c.RelPos) >= len(cur.Children) {
			return nil, 0, false
		}
		child := byID[cur.Children[c.RelPos]]
		if child == nil {
			return nil, 0, false
		}
		if child.NodeType == plan.Node_SINK_SCAN {
			return child, c.ColPos, true
		}
		if c.ColPos < 0 || int(c.ColPos) >= len(child.ProjectList) {
			return nil, 0, false
		}
		entry := child.ProjectList[c.ColPos].GetCol()
		if entry == nil {
			return nil, 0, false
		}
		cur, c = child, entry
	}
	return nil, 0, false
}

// twoReaderFollowExport walks one scan export slot through the SINK and the
// producer-root mirror chain down to the slot holding the producing
// expression, returning the producing node and its local slot.
func twoReaderFollowExport(t *testing.T, query *plan.Query, sink, scan *plan.Node, scanPos int32) (*plan.Node, int32) {
	t.Helper()
	byID := twoReaderNodeIndex(query)
	require.True(t, scanPos >= 0 && int(scanPos) < len(scan.ProjectList), "the scan export slot exists")
	scanCol := scan.ProjectList[scanPos].GetCol()
	require.NotNil(t, scanCol, "the scan export is a SINK column reference")
	sinkPos := scanCol.ColPos
	require.True(t, sinkPos >= 0 && int(sinkPos) < len(sink.ProjectList), "the SINK export slot exists")
	sinkCol := sink.ProjectList[sinkPos].GetCol()
	require.NotNil(t, sinkCol, "the SINK export is a producer column reference")
	require.Len(t, sink.Children, 1)
	cur := byID[sink.Children[0]]
	require.NotNil(t, cur)
	pos := sinkCol.ColPos
	for depth := 0; depth < 32; depth++ {
		require.True(t, pos >= 0 && int(pos) < len(cur.ProjectList), "the mirror chain slot exists")
		entry := cur.ProjectList[pos]
		if entry.GetF() != nil {
			return cur, pos
		}
		c := entry.GetCol()
		require.NotNil(t, c, "the mirror chain holds column references or the producing expression")
		require.True(t, c.RelPos >= 0 && int(c.RelPos) < len(cur.Children), "the mirror child ordinal exists")
		next := byID[cur.Children[c.RelPos]]
		require.NotNil(t, next)
		cur, pos = next, c.ColPos
	}
	t.Fatal("the mirror chain resolves to the producing expression")
	return nil, 0
}

// twoReaderAggArgs collects the argument column of every avg/max/min call in
// the statement together with its owning AGG node, keyed by function name;
// the owners slices stay parallel to the argument slices. The binder can wrap
// an aggregate argument in a normalization cast (the bare-product witness's
// AVG reads the shadow slot through a (65,0)->(65,0) cast), so the walk
// descends through cast layers - whose second argument is the metadata type
// descriptor - until it reaches the produced column.
func twoReaderAggArgs(t *testing.T, query *plan.Query) (map[string][]*plan.ColRef, map[string][]*plan.Node) {
	t.Helper()
	args := make(map[string][]*plan.ColRef)
	owners := make(map[string][]*plan.Node)
	for _, n := range query.Nodes {
		if n == nil || n.NodeType != plan.Node_AGG {
			continue
		}
		for _, agg := range n.AggList {
			fn := agg.GetF()
			if fn == nil || fn.Func == nil {
				continue
			}
			name := fn.Func.ObjName
			if name != "avg" && name != "max" && name != "min" {
				continue
			}
			require.Len(t, fn.Args, 1)
			arg := fn.Args[0]
			for {
				if col := arg.GetCol(); col != nil {
					args[name] = append(args[name], col)
					owners[name] = append(owners[name], n)
					break
				}
				inner := arg.GetF()
				require.NotNil(t, inner, "the aggregate argument resolves to a column through cast layers")
				require.Equal(t, "cast", inner.Func.ObjName,
					"only normalization casts may wrap the aggregate argument column")
				require.NotEmpty(t, inner.Args)
				arg = inner.Args[0]
			}
		}
	}
	return args, owners
}

// twoReaderAvgMaxPaths resolves the AVG and MAX readers of the shared
// column down to their producing slots.
func twoReaderAvgMaxPaths(t *testing.T, query *plan.Query, sink *plan.Node) (producerNode *plan.Node, avgPos, maxPos int32) {
	t.Helper()
	args, owners := twoReaderAggArgs(t, query)
	require.Len(t, args["avg"], 1)
	require.Len(t, args["max"], 1)
	avgScan, avgScanPos, ok := twoReaderResolveScanSlot(t, query, owners["avg"][0], args["avg"][0])
	require.True(t, ok, "the AVG argument resolves to a reader scan slot")
	maxScan, maxScanPos, ok := twoReaderResolveScanSlot(t, query, owners["max"][0], args["max"][0])
	require.True(t, ok, "the MAX argument resolves to a reader scan slot")
	avgProducer, avgPos := twoReaderFollowExport(t, query, sink, avgScan, avgScanPos)
	maxProducer, maxPos := twoReaderFollowExport(t, query, sink, maxScan, maxScanPos)
	require.Equal(t, avgProducer.NodeId, maxProducer.NodeId,
		"both readers resolve to the same shared producer projection")
	return avgProducer, avgPos, maxPos
}

// TestDecimalSharedProducerTwoReaderInPlaceAdmissionIssue30938 pins the
// fitting two-reference regression on the in-place channel: the producing
// expression's literal-arm cast leaves the decimal-to-decimal family, so no
// shadow replica exists; the (65,0) envelope-admission cast is spliced at
// the product seam inside the one exported COALESCE, the export keeps
// publishing the widened (76,0) head, and both the AVG and the MAX reader
// share the single original evaluation.
func TestDecimalSharedProducerTwoReaderInPlaceAdmissionIssue30938(t *testing.T) {
	query := buildTwoReaderBoundaryPlan(t,
		"select (select avg(v) from c) av, (select max(v) from c) mx")
	sink, _, _ := twoReaderSink(t, query)
	coalesce := twoReaderRootedExports(query, "coalesce")
	require.Len(t, coalesce, 1,
		"the unproven producer keeps one single-evaluation export: no shadow replica is emitted")
	export := coalesce[0]
	require.True(t, twoReaderHasAdmissionCast(export.expr.GetF().Args[0]),
		"the (65,0) envelope-admission cast is spliced in place at the product seam inside the original export")
	require.Equal(t, int32(types.T_decimal256), export.expr.Typ.Id)
	require.Equal(t, int32(76), export.expr.Typ.Width,
		"the export keeps publishing the widened (76,0) head")
	require.Equal(t, int32(0), export.expr.Typ.Scale)
	admitted := twoReaderAdmittedExports(query)
	require.Len(t, admitted, 1,
		"exactly one admitted export exists: the single original evaluation itself")
	require.Equal(t, export.node.NodeId, admitted[0].node.NodeId)
	require.Equal(t, export.slot, admitted[0].slot)

	// The AVG and MAX readers both read the original export slot through
	// the post-prune mirror chain: one evaluation, shared by every reader.
	pathProducer, avgPos, maxPos := twoReaderAvgMaxPaths(t, query, sink)
	require.Equal(t, export.node.NodeId, pathProducer.NodeId)
	require.Equal(t, int32(export.slot), avgPos,
		"the AVG reader shares the single original evaluation")
	require.Equal(t, int32(export.slot), maxPos,
		"the MAX reader shares the single original evaluation")
}

// TestDecimalSharedProducerCancellationCastIssue30938 pins the cancellation
// rejection oracle: the original export carries the product seam's (65,0)
// envelope-admission cast directly over the multiplication, so a +/-10^65
// product value of (38,0)x(38,0) operands is rejected by the pre-change
// engine's checked invalid-input family at the seam — never silently
// averaged to 0 — while the genuine 76-digit literal arm stays admitted.
func TestDecimalSharedProducerCancellationCastIssue30938(t *testing.T) {
	query := buildTwoReaderBoundaryPlan(t,
		"select (select avg(v) from c) av, (select max(v) from c) mx")
	coalesce := twoReaderRootedExports(query, "coalesce")
	require.Len(t, coalesce, 1)
	export := coalesce[0]

	// The product arm unwraps through any alignment layer(s) down to the
	// (65,0) admission cast over the multiplication.
	admission := export.expr.GetF().Args[0]
	for {
		castFn := admission.GetF()
		if castFn == nil || castFn.Func == nil || castFn.Func.ObjName != "cast" || len(castFn.Args) < 1 {
			break
		}
		if admission.Typ.Id == int32(types.T_decimal256) &&
			admission.Typ.Width == 65 && admission.Typ.Scale == 0 {
			break
		}
		admission = castFn.Args[0]
	}
	require.NotNil(t, admission.GetF(), "the product arm unwraps to the admission cast")
	require.NotEmpty(t, admission.GetF().Args)
	require.Equal(t, int32(types.T_decimal256), admission.Typ.Id)
	require.Equal(t, int32(65), admission.Typ.Width,
		"the product seam's admission cast targets the parent's (65,0) domain")
	require.Equal(t, int32(0), admission.Typ.Scale)
	inner := admission.GetF().Args[0].GetF()
	require.NotNil(t, inner)
	require.Equal(t, "*", inner.Func.ObjName,
		"the admission cast wraps the widened multiplication directly")

	// The genuine 76-digit literal arm stays admitted-free.
	require.False(t, twoReaderHasAdmissionCast(export.expr.GetF().Args[1]),
		"the genuine wide literal arm carries no admission cast")
}

// TestDecimalSharedProducerShortCircuitDemandIssue30938 pins short-circuit
// demand: the admission cast sits INSIDE the COALESCE product arm, so it
// evaluates only on rows where the pre-change engine selects that arm; the
// merged head type is unchanged and no statement gains or loses an error
// relative to the parent's evaluation demand.
func TestDecimalSharedProducerShortCircuitDemandIssue30938(t *testing.T) {
	query := buildTwoReaderBoundaryPlan(t,
		"select (select avg(v) from c) av, (select max(v) from c) mx")
	coalesce := twoReaderRootedExports(query, "coalesce")
	require.Len(t, coalesce, 1)

	head := coalesce[0].expr
	require.Equal(t, "coalesce", head.GetF().Func.ObjName,
		"the admission is demand-guarded under the merge, never above it")
	require.Equal(t, int32(types.T_decimal256), head.Typ.Id)
	require.Equal(t, int32(76), head.Typ.Width,
		"the merged head type is unchanged: no cast above the merge")
	require.Equal(t, int32(0), head.Typ.Scale)
	require.True(t, twoReaderHasAdmissionCast(head.GetF().Args[0]),
		"the admission cast sits inside the product arm: it evaluates only where the parent selects that arm")
	require.False(t, twoReaderHasAdmissionCast(head.GetF().Args[1]),
		"the genuine wide literal arm evaluates exactly as before")
}

// TestDecimalSharedProducerValueReadersUnaffectedIssue30938 pins the
// unaffected MAX-only reader control: a shared producer read only by
// value-selecting consumers gains no admission cast and no replica.
func TestDecimalSharedProducerValueReadersUnaffectedIssue30938(t *testing.T) {
	query := buildTwoReaderBoundaryPlan(t,
		"select (select max(v) from c) mx, (select min(v) from c) mn")
	sink, root, scans := twoReaderSink(t, query)
	coalesce := twoReaderRootedExports(query, "coalesce")
	require.Len(t, coalesce, 1, "exactly one authored COALESCE export exists")
	require.False(t, twoReaderHasAdmissionCast(coalesce[0].expr),
		"value-selecting readers alone splice no admission cast into the export")
	require.Empty(t, twoReaderAdmittedExports(query),
		"no admitted replica exists without an AVG/SUM reader")
	require.Equal(t, len(root.ProjectList), len(sink.ProjectList),
		"the SINK exports exactly the producer root's referenced columns")
	for _, scan := range scans {
		require.Equal(t, len(sink.ProjectList), len(scan.ProjectList),
			"every reader scan exports exactly the SINK's columns")
	}
	for _, n := range query.Nodes {
		if n == nil {
			continue
		}
		for _, e := range n.ProjectList {
			require.False(t, twoReaderHasAdmissionCast(e),
				"no (65,0) admission cast appears anywhere in the statement")
		}
		for _, agg := range n.AggList {
			require.False(t, twoReaderHasAdmissionCast(agg),
				"no (65,0) admission cast appears in any aggregate")
		}
	}
	// Both readers resolve to the original export slot.
	args, owners := twoReaderAggArgs(t, query)
	require.Len(t, args["max"], 1)
	require.Len(t, args["min"], 1)
	for _, name := range []string{"max", "min"} {
		scan, scanPos, ok := twoReaderResolveScanSlot(t, query, owners[name][0], args[name][0])
		require.True(t, ok)
		producer, pos := twoReaderFollowExport(t, query, sink, scan, scanPos)
		require.NotNil(t, producer)
		require.Equal(t, coalesce[0].node.NodeId, producer.NodeId)
		require.Equal(t, int32(coalesce[0].slot), pos,
			"the "+name+" reader keeps the original export")
	}
}

// TestDecimalSharedProducerAllColumnsReferencedIssue30938 pins the unpruned
// shape: when every authored export is referenced, no narrowing mirror is
// inserted, and the unproven producer still admits in place — exactly the
// authored exports, the admission cast inside the single COALESCE, and all
// three readers sharing the single evaluations.
func TestDecimalSharedProducerAllColumnsReferencedIssue30938(t *testing.T) {
	query := buildTwoReaderBoundaryPlan(t,
		"select (select avg(v) from c) av, (select max(v) from c) mx, (select max(r) from c) mr")
	sink, root, scans := twoReaderSink(t, query)
	coalesce := twoReaderRootedExports(query, "coalesce")
	require.Len(t, coalesce, 1,
		"no shadow replica in the unpruned shape either: the unproven producer admits in place")
	export := coalesce[0]
	require.True(t, twoReaderHasAdmissionCast(export.expr.GetF().Args[0]),
		"the (65,0) admission cast rides the single original export")
	require.Equal(t, export.node.NodeId, root.NodeId,
		"with every authored export referenced the sink sits on the producing projection")
	require.Len(t, export.node.ProjectList, 2,
		"exactly the authored v and r exports: no appended replica")
	require.Len(t, sink.ProjectList, 2)
	require.Len(t, scans, 3, "avg(v), max(v) and max(r) read through their own scans")
	// Each scalar-subquery scan exports exactly its one referenced authored
	// column.
	readAuthored := make(map[int32]bool)
	for _, scan := range scans {
		require.Len(t, scan.ProjectList, 1,
			"each single-column reader scan exports exactly its referenced authored column")
		authored := scan.ProjectList[0].GetCol()
		require.NotNil(t, authored)
		readAuthored[authored.ColPos] = true
	}
	require.Len(t, readAuthored, 2, "both authored columns keep their own readers")
	args, owners := twoReaderAggArgs(t, query)
	require.Len(t, args["avg"], 1)
	require.Len(t, args["max"], 2, "max(v) and max(r) are separate readers")
	avgScan, avgScanPos, ok := twoReaderResolveScanSlot(t, query, owners["avg"][0], args["avg"][0])
	require.True(t, ok)
	avgProducer, avgPos := twoReaderFollowExport(t, query, sink, avgScan, avgScanPos)
	require.Equal(t, export.node.NodeId, avgProducer.NodeId)
	require.Equal(t, int32(export.slot), avgPos,
		"the AVG reader shares the single original evaluation")
	uuidExports := twoReaderRootedExports(query, "uuid")
	require.Len(t, uuidExports, 1, "exactly one authored uuid export exists")
	maxSlots := make(map[int32]int)
	for i, col := range args["max"] {
		scan, scanPos, ok := twoReaderResolveScanSlot(t, query, owners["max"][i], col)
		require.True(t, ok)
		maxProducer, pos := twoReaderFollowExport(t, query, sink, scan, scanPos)
		require.Equal(t, export.node.NodeId, maxProducer.NodeId)
		maxSlots[pos]++
	}
	require.Equal(t, 1, maxSlots[int32(export.slot)], "max(v) shares the single original evaluation")
	require.Equal(t, 1, maxSlots[int32(uuidExports[0].slot)], "max(r) keeps the authored r slot")
}

// TestDecimalSharedProducerBareProductShadowIssue30938 pins the shadow
// channel's PROVEN half: the bare-product producing expression carries only
// a decimal arithmetic operator, decimal-to-decimal alignment casts and
// column leaves, so the AVG/SUM admission rides exactly one statement-local
// shadow replica appended after the authored exports at the producing
// projection; the AVG reader rebinds to the shadow slot while the MAX
// reader keeps the original export carrier-exact.
func TestDecimalSharedProducerBareProductShadowIssue30938(t *testing.T) {
	query := buildBareProductTwoReaderPlan(t,
		"select (select avg(v) from c) av, (select max(v) from c) mx")
	sink, _, _ := twoReaderSink(t, query)

	originals := twoReaderRootedExports(query, "*")
	require.Len(t, originals, 1, "exactly one authored product export exists")
	original := originals[0]
	require.False(t, twoReaderHasAdmissionCast(original.expr),
		"the original export stays carrier-exact: no admission cast at its root")
	require.Equal(t, int32(types.T_decimal256), original.expr.Typ.Id)
	require.Equal(t, int32(76), original.expr.Typ.Width,
		"the original export publishes the widened (76,0)")
	require.Equal(t, int32(0), original.expr.Typ.Scale)

	admitted := twoReaderAdmittedExports(query)
	require.Len(t, admitted, 1,
		"the proven producer emits exactly one envelope-shadow replica")
	shadow := admitted[0]
	require.Equal(t, original.node.NodeId, shadow.node.NodeId,
		"the shadow replica appends at the original export's own projection")
	require.Greater(t, shadow.slot, original.slot,
		"the shadow appends after the authored exports")
	head := shadow.expr
	require.Equal(t, "cast", head.GetF().Func.ObjName)
	require.Equal(t, int32(types.T_decimal256), head.Typ.Id)
	require.Equal(t, int32(65), head.Typ.Width,
		"the shadow carries the product seam's parent (65,0) admission at its head")
	require.Equal(t, int32(0), head.Typ.Scale)
	inner := head.GetF().Args[0].GetF()
	require.NotNil(t, inner)
	require.Equal(t, "*", inner.Func.ObjName,
		"the shadow's admission cast wraps the widened multiplication directly")

	pathProducer, avgPos, maxPos := twoReaderAvgMaxPaths(t, query, sink)
	require.Equal(t, original.node.NodeId, pathProducer.NodeId)
	require.Equal(t, int32(shadow.slot), avgPos,
		"the AVG reader rebinds to the shadow slot")
	require.Equal(t, int32(original.slot), maxPos,
		"the MAX reader keeps the original export carrier-exact")
}
