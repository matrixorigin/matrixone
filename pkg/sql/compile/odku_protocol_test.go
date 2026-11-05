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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightdedupjoin"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestOrderedODKUProtocolRoundTripAndGate(t *testing.T) {
	sid := "" // A process without a lock service resolves the default runtime.
	var rt moruntime.Runtime
	moruntime.RunTest(sid, func(r moruntime.Runtime) { rt = r })
	oldVersion, existed := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if existed {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
		}
	})
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, sid, mp)
	t.Cleanup(func() { proc.Free(); require.Zero(t, mp.CurrNB()) })
	col := func(rel, pos int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
	}
	pre := preinsertunique.NewArgument()
	pre.PreInsertCtx = odkuProtocolLayout()
	c := pre.PreInsertCtx.OdkuActions
	for _, op := range []vm.Operator{pre} {
		s := newScope(Normal)
		s.Proc = proc
		s.NodeInfo.Mcpu = 1
		s.RootOp = op
		data, e := encodeScope(s)
		require.NoError(t, e)
		received, e := decodeScope(data, proc, false, nil)
		require.NoError(t, e)
		require.Equal(t, pre.PreInsertCtx, received.RootOp.(*preinsertunique.PreInsertUnique).PreInsertCtx)
		received.release()
		for _, version := range []any{int64(defines.MORPCVersion11), nil} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			_, e = encodeScope(s)
			require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported), "%v", e)
			_, e = decodeScope(data, proc, false, nil)
			require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported), "%v", e)
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
		require.True(t, rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12)))
		_, e = encodeScope(s)
		require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported))
		_, e = decodeScope(data, proc, false, nil)
		require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported))
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
		s.release()
	}
	// Ordinary unique-index insertion remains available at the old gate.
	ordinary := preinsertunique.NewArgument()
	ordinary.PreInsertCtx = &plan.PreInsertUkCtx{Columns: []int32{0}, PkColumn: 1}
	s := newScope(Normal)
	s.Proc = proc
	s.NodeInfo.Mcpu = 1
	s.RootOp = ordinary
	defer s.release()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion11))
	data, e := encodeScope(s)
	require.NoError(t, e)
	r, e := decodeScope(data, proc, false, nil)
	require.NoError(t, e)
	require.False(t, r.RootOp.(*preinsertunique.PreInsertUnique).PreInsertCtx.OrderedOdku)
	r.release()

	// Reject removed UPDATE and inconsistent current metadata before either side starts.
	oldUpdate := dedupjoin.NewArgument()
	oldUpdate.OnDuplicateAction = plan.Node_UPDATE
	oldUpdate.Conditions = [][]*plan.Expr{{col(0, 0)}, {col(0, 0)}}
	oldUpdate.LeftTypes = []types.Type{types.T_int32.ToType()}
	oldUpdate.RightTypes = []types.Type{types.T_int32.ToType()}
	oldBuild := hashbuild.NewArgument()
	oldBuild.IsDedup = true
	oldBuild.OnDuplicateAction = plan.Node_UPDATE
	oldBuild.Conditions = []*plan.Expr{col(0, 0)}
	oldRight := rightdedupjoin.NewArgument()
	oldRight.OnDuplicateAction = plan.Node_UPDATE
	oldRight.Conditions = [][]*plan.Expr{{col(0, 0)}, {col(0, 0)}}
	badRight := rightdedupjoin.NewArgument()
	badRight.OnDuplicateAction = plan.Node_OnDuplicateAction(123)
	badRight.Conditions = [][]*plan.Expr{{col(0, 0)}, {col(0, 0)}}
	oldTarget := preinsertunique.NewArgument()
	oldTarget.PreInsertCtx = &plan.PreInsertUkCtx{OrderedOdku: true}
	missingMarker := preinsertunique.NewArgument()
	missingMarker.PreInsertCtx = &plan.PreInsertUkCtx{OdkuActions: c}
	for _, tc := range []struct {
		op   vm.Operator
		code uint16
	}{
		{oldUpdate, moerr.ErrNotSupported}, {oldBuild, moerr.ErrNotSupported},
		{oldRight, moerr.ErrNotSupported}, {badRight, moerr.ErrInternal},
		{oldTarget, moerr.ErrNotSupported}, {missingMarker, moerr.ErrNotSupported},
	} {
		func() {
			s := newScope(Normal)
			s.Proc = proc
			s.NodeInfo.Mcpu = 1
			s.RootOp = tc.op
			defer s.release()
			_, e := encodeScope(s)
			require.True(t, moerr.IsMoErrCode(e, tc.code), "%v", e)
			// Bypass the new sender check to represent bytes from an old CN.
			p, e := fillPipeline(s)
			require.NoError(t, e)
			data, e := p.Marshal()
			require.NoError(t, e)
			r, e := decodeScope(data, proc, false, nil)
			require.True(t, moerr.IsMoErrCode(e, tc.code), "%v", e)
			require.Nil(t, r)
			e = validateODKUPipelineProtocol(&pipeline.Pipeline{Children: []*pipeline.Pipeline{p}}, proc)
			require.True(t, moerr.IsMoErrCode(e, tc.code), "%v", e)
		}()
	}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
	p := &pipeline.Pipeline{Qry: &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{
		{NodeType: plan.Node_FILTER, FilterIsBarrier: true},
		{NodeType: plan.Node_JOIN, JoinType: plan.Node_DEDUP, OnDuplicateAction: plan.Node_UPDATE},
	}}}}}
	e = validateODKUPipelineProtocol(p, proc)
	require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported), "%v", e)
	require.ErrorContains(t, e, "ODKU requires the ordered action operator")
}

func TestRightDedupPreflightBeforeProducers(t *testing.T) {
	c, registry := newRunLifecycleCompile(t)
	t.Cleanup(c.Release)
	var calls atomic.Uint32
	root := newScope(Merge)
	root.Proc = c.proc.NewNoContextChildProc(0)
	for range 2 {
		producer := newScope(Normal)
		producer.Proc = c.proc.NewNoContextChildProc(0)
		producer.RootOp = colexec.NewMockOperator().WithBatchs([]*batch.Batch{batch.EmptyForConstFoldBatch}).WithBatchCallback(func(int) { calls.Add(1) })
		root.PreScopes = append(root.PreScopes, producer)
	}
	right := rightdedupjoin.NewArgument()
	right.OnDuplicateAction = plan.Node_UPDATE
	root.RootOp = right
	c.scopes = []*Scope{root}
	c.lockMeta = nil
	var err error
	require.NotPanics(t, func() { _, err = c.Run(0) })
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), "%v", err)
	require.Zero(t, calls.Load())
	require.Zero(t, registry.LiveAllocationMetadata())
}

func odkuProtocolLayout() *plan.PreInsertUkCtx {
	col := func(rel, pos int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: pos}}}
	}
	truth := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}
	ctx := &plan.PreInsertUkCtx{OrderedOdku: true, OdkuActions: &plan.OdkuActionCtx{
		SourceColumns: []*plan.Expr{col(0, 0), col(0, 1), col(0, 2), col(0, 3), col(0, 4)},
		OldColumns:    []*plan.Expr{col(0, 5), col(0, 6), col(0, 7)}, InputColumns: []int32{0, 1, -1},
		PkColumn: 0, RowIdColumn: 2, UpdateColumns: []int32{1}, UpdateExprs: []*plan.Expr{col(1, 1)},
		InsertChecks: []*plan.Expr{truth}, InsertCheckConstraintCount: 1, UpdateChecks: []*plan.Expr{truth},
		ActionFinalColumn: 3, SourceOrdinal: col(0, 2),
		ForeignKeyChecks: []*plan.OdkuForeignKeyCheck{{Columns: []plan.ColRef{{RelPos: 0, ColPos: 1}}, Eligibility: col(1, 4), CheckUpdates: true}},
		Constraints:      []*plan.OdkuConstraint{{Name: "pk", Types: []plan.Type{{Id: int32(types.T_int32)}}, TargetEligible: true, InputKey: col(0, 0), PriorTarget: col(0, 5), CurrentKey: col(0, 0)}},
	}}
	c := ctx.OdkuActions
	c.OldColumns[2].Typ.Id = int32(types.T_Rowid)
	c.SourceOrdinal.Typ.Id = int32(types.T_int64)
	c.SourceColumns[2].Typ.Id = int32(types.T_int64)
	c.SourceColumns[3].Typ.Id = int32(types.T_bool)
	c.SourceColumns[4].Typ.Id = int32(types.T_bool)
	c.ForeignKeyChecks[0].Eligibility.Typ.Id = int32(types.T_bool)
	return ctx
}

func TestOrderedODKUPlanOutputLayout(t *testing.T) {
	for _, shape := range []string{"current", "missing old column", "extra original PK"} {
		t.Run(shape, func(t *testing.T) {
			ctx := odkuProtocolLayout()
			actions := ctx.OdkuActions
			project := append([]*plan.Expr{}, actions.SourceColumns...)
			project = append(project, actions.OldColumns...)
			switch shape {
			case "missing old column":
				project = project[:len(project)-1]
			case "extra original PK":
				project = append([]*plan.Expr{}, actions.SourceColumns...)
				project = append(project, actions.OldColumns[actions.PkColumn])
				project = append(project, actions.OldColumns...)
			}
			node := &plan.Node{NodeType: plan.Node_PRE_INSERT_UK, PreInsertUkCtx: ctx, ProjectList: project}
			data, err := node.Marshal()
			require.NoError(t, err)
			decoded := &plan.Node{}
			require.NoError(t, decoded.Unmarshal(data))
			err = validateODKUPlanNode(decoded)
			if shape == "current" {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
				require.ErrorContains(t, err, "invalid ODKU output layout")
			}
		})
	}
}

func TestOrderedODKULocalPreflightBeforeProducers(t *testing.T) {
	for _, location := range []string{"plan", "scope", "decoded plan"} {
		for _, kind := range []string{"missing ordered payload", "missing image", "prior nil image", "ordinal relation", "assignment relation", "old output width"} {
			if location == "scope" && kind == "old output width" {
				continue // An instruction-only scope carries no output layout.
			}
			t.Run(location+"/"+kind, func(t *testing.T) {
				c, registry := newRunLifecycleCompile(t)
				t.Cleanup(c.Release)
				ctx := odkuProtocolLayout()
				want := moerr.ErrInternal
				switch kind {
				case "missing ordered payload":
					ctx = &plan.PreInsertUkCtx{OrderedOdku: true}
					want = moerr.ErrNotSupported
				case "missing image":
					ctx.OdkuActions.OldColumns = nil
				case "prior nil image":
					cols := make([]*plan.ColDef, len(ctx.OdkuActions.OldColumns))
					for i, e := range ctx.OdkuActions.OldColumns {
						cols[i] = &plan.ColDef{Typ: e.Typ}
					}
					ctx.OdkuActions.PriorData = &plan.OdkuPriorData{Ref: &plan.ObjectRef{Obj: 42}, TableDef: &plan.TableDef{Cols: cols}}
					ctx.OdkuActions.OldColumns[0] = nil
					if location == "decoded plan" {
						// A protobuf message encodes an empty entry; a nil repeated
						// message is invalid input to the generated marshaler.
						ctx.OdkuActions.OldColumns[0] = &plan.Expr{}
					}
				case "ordinal relation":
					ctx.OdkuActions.SourceOrdinal.GetCol().RelPos = 1
				case "assignment relation":
					ctx.OdkuActions.UpdateExprs[0].GetCol().RelPos = 2
				}
				var calls atomic.Uint32
				root := newScope(Merge)
				root.Proc = c.proc.NewNoContextChildProc(0)
				for range 2 {
					producer := newScope(Normal)
					producer.Proc = c.proc.NewNoContextChildProc(0)
					producer.RootOp = colexec.NewMockOperator().WithBatchs([]*batch.Batch{batch.EmptyForConstFoldBatch}).WithBatchCallback(func(int) { calls.Add(1) })
					root.PreScopes = append(root.PreScopes, producer)
				}
				if location != "scope" {
					node := &plan.Node{NodeType: plan.Node_PRE_INSERT_UK, PreInsertUkCtx: ctx}
					if kind == "old output width" {
						actions := ctx.OdkuActions
						node.ProjectList = append(node.ProjectList, actions.SourceColumns...)
						node.ProjectList = append(node.ProjectList, actions.OldColumns[actions.PkColumn])
						node.ProjectList = append(node.ProjectList, actions.OldColumns...)
					}
					c.pn.GetQuery().Nodes = []*plan.Node{node}
					if location == "decoded plan" {
						encoded, e := c.pn.Marshal()
						require.NoError(t, e)
						c.pn = &plan.Plan{}
						require.NoError(t, c.pn.Unmarshal(encoded))
					}
					root.RootOp = colexec.NewMockOperator()
				} else {
					op := preinsertunique.NewArgument()
					op.PreInsertCtx = ctx
					op.AppendChild(colexec.NewMockOperator())
					root.RootOp = op
				}
				c.scopes = []*Scope{root}
				// Also poison the following initialization stage, so the test
				// cannot pass by racing a fast Prepare failure against producers.
				c.lockMeta = nil
				var err error
				require.NotPanics(t, func() { _, err = c.Run(0) })
				require.True(t, moerr.IsMoErrCode(err, want), "%v", err)
				require.Zero(t, calls.Load(), "no separately scheduled producer may start")
				require.Zero(t, registry.LiveAllocationMetadata())
			})
		}
	}
}

// Check the shared field assignments without asserting mixed-branch execution.
func TestOrderedODKUWireFieldAssignments(t *testing.T) {
	var node plan.Node
	// Main uses packed field 78 for group keys and field 79 for the barrier.
	require.NoError(t, node.Unmarshal([]byte{0xf2, 0x04, 0x01, 0x00, 0xf8, 0x04, 0x01}))
	require.True(t, node.FilterIsBarrier)
	var unrelated plan.PreInsertUkCtx
	// Main's INSERT IGNORE and target-column fields have different meanings.
	require.NoError(t, unrelated.Unmarshal([]byte{0x28, 0x01, 0x52, 0x01, 0x02}))
	require.False(t, unrelated.OrderedOdku)
	require.Nil(t, unrelated.OdkuActions)
	original := odkuProtocolLayout()
	wire, err := original.Marshal()
	require.NoError(t, err)
	var decoded plan.PreInsertUkCtx
	require.NoError(t, decoded.Unmarshal(wire))
	require.Equal(t, original, &decoded)
}
