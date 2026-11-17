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
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func markGuardTestCol(typ plan.Type, pos int32) *plan.Expr {
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
}
func markGuardTestEquality(typ plan.Type, pos int32) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "="},
		Args: []*plan.Expr{markGuardTestCol(typ, pos), {Typ: typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "case1"}}}}},
	}}}
}

func TestMarkGuardColumnLineage(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
	marker := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: -1}}}
	mark := &plan.Node{ProjectList: []*plan.Expr{markGuardTestCol(typ, 0), markGuardTestCol(typ, 1), marker}}
	index := &plan.Node{ProjectList: []*plan.Expr{markGuardTestCol(typ, 0), markGuardTestCol(typ, 1)}}
	scan := &plan.Node{ProjectList: []*plan.Expr{markGuardTestCol(typ, 0), markGuardTestCol(typ, 2)}, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Typ: typ}, {Typ: typ}, {Typ: typ}}}}
	// The join outputs slot 1, but the scan's raw input slot 1 is workspace;
	// the alternative predicate must read slot 2 instead.
	pos, ok := mapMarkGuardColumn(mark, index, scan, 1, true, typ)
	require.True(t, ok)
	require.Equal(t, int32(2), pos)
	pos, ok = mapMarkGuardColumn(mark, index, scan, 0, false, typ)
	require.True(t, ok)
	require.Equal(t, int32(0), pos)
	_, ok = mapMarkGuardColumn(mark, index, scan, 2, true, typ)
	require.False(t, ok)
	index.ProjectList[1].GetCol().RelPos = 1
	_, ok = mapMarkGuardColumn(mark, index, scan, 1, true, typ)
	require.False(t, ok)
	index.ProjectList[1].GetCol().RelPos = 0
	scan.TableDef.Cols[2].Typ.Width = 32
	_, ok = mapMarkGuardColumn(mark, index, scan, 1, true, typ)
	require.False(t, ok)
}

func TestMarkGuardRejectsUnsafeAlternatives(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
	_, ok := guardStringEquality(markGuardTestEquality(typ, 1))
	require.True(t, ok)
	for _, tc := range []struct {
		name   string
		mutate func(*plan.Expr)
	}{
		{"not_equal", func(e *plan.Expr) { e.GetF().Func.ObjName = "<>" }},
		{"null", func(e *plan.Expr) { e.GetF().Args[1].GetLit().Isnull = true }},
		{"different_width", func(e *plan.Expr) { e.GetF().Args[1].Typ.Width = 32 }},
		{"oversized_column", func(e *plan.Expr) { e.GetF().Args[0].Typ.Width = 129; e.GetF().Args[1].Typ.Width = 129 }},
		{"char_padding", func(e *plan.Expr) {
			e.GetF().Args[0].Typ.Id = int32(types.T_char)
			e.GetF().Args[1].Typ.Id = int32(types.T_char)
		}},
		{"uca_collation", func(e *plan.Expr) {
			e.GetF().Args[0].Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
			e.GetF().Args[1].Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		}},
		{"different_collation", func(e *plan.Expr) { e.GetF().Args[1].Typ.Charset = uint32(types.CharsetUTF8) }},
		{"different_collation_version", func(e *plan.Expr) { e.GetF().Args[1].Typ.CollationVersion = 1 }},
		{"right_column", func(e *plan.Expr) { e.GetF().Args[0].GetCol().RelPos = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := markGuardTestEquality(typ, 1)
			tc.mutate(e)
			_, ok := guardStringEquality(e)
			require.False(t, ok)
		})
	}
}

func TestMarkGuardTagAvoidsPlanMessages(t *testing.T) {
	root := &Scope{}
	q := &plan.Query{Nodes: []*plan.Node{{SendMsgList: []plan.MsgHeader{{MsgType: int32(message.MsgJoinMap), MsgTag: 42}}}}}
	tag, ok := markGuardMessageTag(q, root)
	require.True(t, ok)
	require.Equal(t, int32(43), tag)
	q.Nodes[0].SendMsgList[0].MsgTag = math.MaxInt32
	_, ok = markGuardMessageTag(q, root)
	require.False(t, ok)
	q.Nodes[0].SendMsgList[0] = plan.MsgHeader{MsgType: int32(message.MsgTopValue), MsgTag: 1}
	_, ok = markGuardMessageTag(q, root)
	require.False(t, ok)
}

func TestMarkGuardMessagesAreRowOnly(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
	for _, tc := range []struct {
		name  string
		kind  int32
		card  int32
		bytes int
		want  string
	}{
		{"empty_build_keeps_other_arm", message.RuntimeFilter_DROP, 0, 0, "="},
		{"available_keys_or_other_arm", message.RuntimeFilter_IN, 1, 0, "or"},
		{"over_cardinality_falls_back", message.RuntimeFilter_IN, markGuardKeyLimit + 1, 0, ""},
		{"over_byte_budget_falls_back", message.RuntimeFilter_IN, 1, markGuardPayloadBytes + 1, ""},
		{"producer_pass_falls_back", message.RuntimeFilter_PASS, 0, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			board := message.NewMessageBoard()
			defer board.Reset()
			proc.SetMessageBoard(board)
			scan := table_scan.NewArgument()
			defer scan.Release()
			spec := plan2.MakeRuntimeFilter(101, false, 0, markGuardTestCol(typ, 0), true)
			s := &Scope{Proc: proc, RootOp: scan, DataSource: &Source{markGuard: &markScanGuard{probe: spec, alternative: markGuardTestEquality(typ, 1)}}}
			c := &Compile{proc: proc, addr: "127.0.0.1:6001"}
			s.NodeInfo.Addr, s.NodeInfo.Mcpu = c.addr, 1
			localProducer := &Scope{Proc: proc, DataSource: &Source{node: &plan.Node{}}}
			localProducer.NodeInfo.Addr, localProducer.NodeInfo.Mcpu = c.addr, 1
			s.PreScopes = []*Scope{localProducer}
			s.DataSource.markGuard.producer = localProducer
			message.SendMessage(message.RuntimeFilterMessage{Tag: 101, Typ: tc.kind, Card: tc.card, Data: make([]byte, tc.bytes)}, board)
			s.applyMarkScanGuard(c)
			require.Nil(t, s.DataSource.FilterExpr)
			if tc.want == "" {
				require.Empty(t, scan.RuntimeFilterExprs)
				return
			}
			require.Len(t, scan.RuntimeFilterExprs, 1)
			require.Equal(t, tc.want, scan.RuntimeFilterExprs[0].GetF().Func.ObjName)
		})
	}
}

func TestMarkGuardRejectsConflictingMessages(t *testing.T) {
	for _, order := range [][2]int32{
		{message.RuntimeFilter_DROP, message.RuntimeFilter_IN},
		{message.RuntimeFilter_IN, message.RuntimeFilter_DROP},
	} {
		t.Run(fmt.Sprintf("%d_then_%d", order[0], order[1]), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			board := message.NewMessageBoard()
			defer board.Reset()
			proc.SetMessageBoard(board)
			typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
			guard := &markScanGuard{
				probe:       plan2.MakeRuntimeFilter(101, false, 0, markGuardTestCol(typ, 0), true),
				alternative: markGuardTestEquality(typ, 1),
			}
			for _, kind := range order {
				message.SendMessage(message.RuntimeFilterMessage{Tag: 101, Typ: kind, Card: 1}, board)
			}
			s := &Scope{Proc: proc}
			require.Nil(t, s.receiveMarkScanGuard(&Compile{proc: proc}, guard))
		})
	}
}

func TestMarkGuardClearsPriorReaderGeneration(t *testing.T) {
	scan := table_scan.NewArgument()
	defer scan.Release()
	scan.RuntimeFilterExprs = []*plan.Expr{markGuardTestEquality(plan.Type{Id: int32(types.T_varchar), Width: 64}, 1)}
	s := &Scope{RootOp: scan, DataSource: &Source{markGuard: &markScanGuard{}}, StarCountOnly: true}
	s.NodeInfo.Mcpu = 1
	readers, _, err := s.buildReaders(&Compile{})
	require.NoError(t, err)
	require.Len(t, readers, 1)
	require.Empty(t, scan.RuntimeFilterExprs)
}

// An unreachable old plan FILTER must never authorize filtering live rows.
// The producer's input column and equality-key slot are deliberately different.
func TestMarkGuardRequiresLivePhysicalFilter(t *testing.T) {
	typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
	boolType := plan.Type{Id: int32(types.T_bool)}
	marker := &plan.Expr{Typ: boolType, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: -1}}}
	wide := plan.Type{Id: int32(types.T_text)}
	scanNode := &plan.Node{NodeId: 0, NodeType: plan.Node_TABLE_SCAN, Stats: &plan.Stats{TableCnt: 10000}, ProjectList: []*plan.Expr{markGuardTestCol(typ, 0), markGuardTestCol(typ, 2), markGuardTestCol(wide, 3)}, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Typ: typ}, {Typ: typ}, {Typ: typ}, {Typ: wide}}}}
	markNode := &plan.Node{NodeId: 2, NodeType: plan.Node_JOIN, JoinType: plan.Node_MARK, Children: []int32{0, 1}, OnList: []*plan.Expr{{}}, ProjectList: []*plan.Expr{markGuardTestCol(typ, 0), markGuardTestCol(typ, 1), marker}}
	currentFilter := &plan.Node{NodeId: 3, NodeType: plan.Node_FILTER, Children: []int32{2}}
	staleFilter := &plan.Node{NodeId: 4, NodeType: plan.Node_FILTER, Children: []int32{2}}
	q := &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{scanNode, {}, markNode, currentFilter, staleFilter}}
	orExpr := &plan.Expr{Typ: boolType, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.OR) << 32, ObjName: "or"}, Args: []*plan.Expr{markGuardTestCol(boolType, 2), markGuardTestEquality(typ, 1)}}}}
	staleFilter.FilterList = []*plan.Expr{orExpr}
	scan := table_scan.NewArgument()
	defer scan.Release()
	join := hashjoin.NewArgument()
	defer join.Release()
	join.JoinType = plan.Node_MARK
	join.JoinMapTag = 11
	join.EqConds = [][]*plan.Expr{{markGuardTestCol(typ, 0)}, {markGuardTestCol(typ, 5)}}
	join.EqConds[1][0].GetCol().RelPos = 1
	join.SetIdx(2)
	join.AppendChild(scan)
	physical := filter.NewArgument()
	defer physical.Release()
	physical.SetIdx(3)
	physical.AppendChild(join)
	physical.FilterExprs = []*plan.Expr{markGuardTestEquality(typ, 1)}
	build := hashbuild.NewArgument()
	defer build.Release()
	build.NeedHashMap = true
	build.TrackNullKeys = true
	build.JoinMapTag = 11
	build.Conditions = []*plan.Expr{markGuardTestCol(typ, 5)}
	c := NewMockCompile(t)
	c.addr = "127.0.0.1:6001"
	board := message.NewMessageBoard()
	defer board.Reset()
	c.proc.SetMessageBoard(board)
	root := &Scope{Proc: c.proc, RootOp: physical, DataSource: &Source{node: scanNode}}
	root.NodeInfo.Addr = c.addr
	root.NodeInfo.Mcpu = 1
	buildScan := table_scan.NewArgument()
	defer buildScan.Release()
	build.AppendChild(buildScan)
	producer := &Scope{Proc: c.proc, RootOp: build, DataSource: &Source{node: &plan.Node{}}}
	producer.NodeInfo.Addr = c.addr
	producer.NodeInfo.Mcpu = 1
	root.PreScopes = []*Scope{producer}
	c.installMarkScanGuard(q, []*Scope{root})
	require.Nil(t, root.DataSource.markGuard)
	require.Nil(t, build.RuntimeFilterSpec)
	// A live physical OR authorizes the optimization; the old node is irrelevant.
	physical.FilterExprs = []*plan.Expr{orExpr}
	candidate := c.findMarkGuardCandidate(q, []*Scope{root})
	require.NotNil(t, candidate)
	require.Nil(t, root.DataSource.markGuard)
	require.Nil(t, build.RuntimeFilterSpec)
	require.Zero(t, build.OptionalRuntimeFilterMaxKeyBytes)
	require.Equal(t, int32(2), candidate.alternative.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(1), orExpr.GetF().Args[1].GetF().Args[0].GetCol().ColPos)
	require.Zero(t, join.EqConds[0][0].GetCol().ColPos)
	c.installMarkScanGuard(q, []*Scope{root})
	require.NotNil(t, root.DataSource.markGuard)
	require.NotNil(t, build.RuntimeFilterSpec)
	require.Equal(t, int32(2), root.DataSource.markGuard.alternative.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(0), build.RuntimeFilterSpec.BuildExpr.GetCol().ColPos)
	require.Equal(t, 4*markGuardMaxWidth, build.OptionalRuntimeFilterMaxKeyBytes)
	clone := dupOperator(build, 0, 1).(*hashbuild.HashBuild)
	defer clone.Release()
	require.Equal(t, build.OptionalRuntimeFilterMaxKeyBytes, clone.OptionalRuntimeFilterMaxKeyBytes)
	// Unknown, remote, or cloned producers must fall back before attachment.
	for _, tc := range []struct {
		name   string
		modify func()
	}{
		{"unknown_address", func() { producer.NodeInfo.Addr = "" }},
		{"remote_producer", func() { producer.NodeInfo.Addr = "127.0.0.1:7001" }},
		{"parallel_producer", func() { producer.NodeInfo.Mcpu = 2 }},
		{"narrow_projection", func() { scanNode.ProjectList = scanNode.ProjectList[:2] }},
		{"wide_column_is_filtered", func() { scanNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Expr: markGuardTestCol(wide, 3)}} }},
		{"planned_dependency", func() { producer.DataSource.node.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 12}} }},
		{"initialized_dependency", func() { producer.DataSource.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{Tag: 12}} }},
		{"planned_receiver", func() { producer.DataSource.node.RecvMsgList = []plan.MsgHeader{{MsgTag: 12}} }},
		{"lazy_root", func() { root.LazyPreScopes = true }},
		{"different_board", func() {
			producer.Proc = testutil.NewProcess(t)
			producer.Proc.SetMessageBoard(message.NewMessageBoard())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root.DataSource.markGuard = nil
			build.RuntimeFilterSpec = nil
			build.OptionalRuntimeFilterMaxKeyBytes = 0
			scanNode.ProjectList = []*plan.Expr{markGuardTestCol(typ, 0), markGuardTestCol(typ, 2), markGuardTestCol(wide, 3)}
			scanNode.RuntimeFilterProbeList = nil
			producer.NodeInfo.Addr = c.addr
			producer.NodeInfo.Mcpu = 1
			producer.Proc = c.proc
			producer.DataSource.RuntimeFilterSpecs = nil
			producer.DataSource.node.RuntimeFilterProbeList = nil
			producer.DataSource.node.RecvMsgList = nil
			root.LazyPreScopes = false
			tc.modify()
			require.Nil(t, c.findMarkGuardCandidate(q, []*Scope{root}))
			require.Nil(t, root.DataSource.markGuard)
			require.Nil(t, build.RuntimeFilterSpec)
			require.Zero(t, build.OptionalRuntimeFilterMaxKeyBytes)
			c.installMarkScanGuard(q, []*Scope{root})
			require.Nil(t, root.DataSource.markGuard)
			require.Nil(t, build.RuntimeFilterSpec)
			require.Zero(t, build.OptionalRuntimeFilterMaxKeyBytes)
		})
	}
}

// Done is evaluated by MessageReceiver after it registers its waiter. This
// lets failure tests signal only after the guard has reached its blocking path.
type markGuardWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *markGuardWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

// The optional waiter must leave failure and reader ownership to the original
// MARK pipeline, including when no probe row has ever been read.
func TestMarkGuardFailureKeepsOriginalOwner(t *testing.T) {
	for _, scenario := range []string{"canceled", "internal_stop", "closed_board", "build_error", "blocked_cancel", "blocked_build_error"} {
		t.Run(scenario, func(t *testing.T) {
			run := func(enable bool) (error, error, int) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				proc := testutil.NewProcess(t)
				proc.BuildPipelineContext(ctx)
				board := message.NewMessageBoard()
				defer board.CloseAndDrain()
				proc.SetMessageBoard(board)
				typ := plan.Type{Id: int32(types.T_varchar), Width: 64}
				spec := plan2.MakeRuntimeFilter(101, false, 0, markGuardTestCol(typ, 0), true)
				scan := table_scan.NewArgument()
				defer scan.Release()
				join := hashjoin.NewArgument()
				defer join.Release()
				registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
				require.NoError(t, err)
				account, err := registry.Open(1 << 40)
				require.NoError(t, err)
				require.NoError(t, join.SetAllocationAccount(account))
				defer func() { require.NoError(t, join.ClearAllocationAccount(account)) }()
				join.JoinType = plan.Node_MARK
				join.JoinMapTag = 202
				join.LeftTypes = []types.Type{types.T_varchar.ToType()}
				join.RightTypes = []types.Type{types.T_varchar.ToType()}
				join.EqConds = [][]*plan.Expr{{markGuardTestCol(typ, 0)}, {markGuardTestCol(typ, 0)}}
				join.ResultCols = []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)}
				join.AppendChild(scan)
				reader := &mockReaderForParallelOrderBy{}
				s := &Scope{Proc: proc, RootOp: join, DataSource: &Source{R: reader, Rel: &mockRelationForMembershipFilter{}, markGuard: &markScanGuard{probe: spec, alternative: markGuardTestEquality(typ, 1)}}}
				c := &Compile{proc: proc, addr: "127.0.0.1:6001"}
				s.NodeInfo.Addr, s.NodeInfo.Mcpu = c.addr, 1
				producer := &Scope{Proc: proc, DataSource: &Source{node: &plan.Node{}}}
				producer.NodeInfo.Addr, producer.NodeInfo.Mcpu = c.addr, 1
				s.PreScopes = []*Scope{producer}
				s.DataSource.markGuard.producer = producer
				blocked := scenario == "blocked_cancel" || scenario == "blocked_build_error"
				var waiterDone chan struct{}
				if enable && blocked {
					waitCtx := &markGuardWaitContext{Context: proc.Ctx, entered: make(chan struct{})}
					proc.Ctx = waitCtx
					waiterDone = make(chan struct{})
					go func() {
						s.applyMarkScanGuard(c)
						close(waiterDone)
					}()
					select {
					case <-waitCtx.entered:
					case <-time.After(5 * time.Second):
						cancel()
						t.Fatal("guard did not enter its registered wait")
					}
				}
				switch scenario {
				case "canceled", "blocked_cancel":
					cancel()
				case "internal_stop":
					proc.Cancel(context.Canceled)
					require.NoError(t, ctx.Err())
				case "closed_board":
					board.Close()
				case "build_error", "blocked_build_error":
					message.SendMessage(message.RuntimeFilterMessage{Tag: 101, Typ: message.RuntimeFilter_PASS}, board)
					message.SendJoinMapResult(message.NewJoinMapBuildErrorResult(moerr.NewOOM(proc.Ctx)), 202, false, 0, board)
				}
				if enable {
					if blocked {
						select {
						case <-waiterDone:
						case <-time.After(5 * time.Second):
							t.Fatal("guard did not leave its wait")
						}
					} else {
						s.applyMarkScanGuard(c)
					}
				}
				require.Empty(t, scan.RuntimeFilterExprs)
				err = s.Run(c)
				require.Nil(t, s.DataSource.R)
				return err, proc.Ctx.Err(), reader.closeCalls
			}
			baseline, baselineCtx, baselineClosed := run(false)
			candidate, candidateCtx, candidateClosed := run(true)
			require.Equal(t, baselineClosed, candidateClosed)
			require.Equal(t, 1, candidateClosed)
			require.Equal(t, baselineCtx, candidateCtx)
			if baseline == nil {
				require.NoError(t, candidate)
			} else {
				require.EqualError(t, candidate, baseline.Error())
			}
			if scenario == "canceled" || scenario == "internal_stop" || scenario == "blocked_cancel" {
				require.ErrorIs(t, candidateCtx, context.Canceled)
			}
			if scenario == "build_error" || scenario == "blocked_build_error" {
				require.True(t, moerr.IsMoErrCode(candidate, moerr.ErrOOM), candidate)
			}
			if scenario == "closed_board" {
				require.ErrorContains(t, candidate, "closed")
			}
		})
	}
}
