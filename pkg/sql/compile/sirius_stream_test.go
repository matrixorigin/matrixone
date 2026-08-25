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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func siriusStreamTestPlan() *planpb.Plan {
	return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Headings: []string{"a"},
		Nodes: []*planpb.Node{{
			NodeId:   0,
			NodeType: planpb.Node_TABLE_SCAN,
			Stats:    &planpb.Stats{},
			ObjRef:   &planpb.ObjectRef{Obj: 42, ObjName: "t"},
			TableDef: &planpb.TableDef{
				DbId: 7, TblId: 42, Version: 3, Name: "t", TableType: "r",
				Cols: []*planpb.ColDef{{
					Name: "a", ColId: 11, Seqnum: 5,
					Typ: planpb.Type{Id: int32(types.T_int64)},
				}},
			},
		}},
	}}}
}

func TestCompileSiriusStreamReadBindsSnapshotAndNativeInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().Readonly().Return(true)
	workspace.EXPECT().WriteOffset().Return(uint64(0))
	workspace.EXPECT().GetSnapshotWriteOffset().Return(0)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 42, LogicalTime: 3})

	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = txnOp
	queryPlan := siriusStreamTestPlan()
	queryID := []byte("0123456789abcdef")
	started := time.Now()

	readPlan, err := (&Compile{proc: proc}).CompileSiriusStreamRead(
		context.Background(), queryPlan, 7, queryID, time.Minute,
	)
	require.NoError(t, err)
	require.Len(t, readPlan.StreamInputs, 1)
	require.Len(t, readPlan.StreamInputs[0].StreamRef, 32)
	require.Equal(t, int32(0), readPlan.StreamInputs[0].NodeID)
	require.Equal(t, queryPlan.GetQuery().Headings, readPlan.Headings)
	require.Len(t, readPlan.OutputTypes, 1)
	require.WithinDuration(t, started.Add(time.Minute), readPlan.LeaseExpiresAt, time.Second)

	var exported spb.Plan
	require.NoError(t, proto.Unmarshal(readPlan.Plan, &exported))
	require.Equal(t, []string{substrait.StreamReadTypeURL}, exported.ExpectedTypeUrls)
	read := exported.Relations[0].GetRoot().Input.GetRead()
	require.NotNil(t, read)
	require.Equal(t, substrait.StreamReadTypeURL, read.GetExtensionTable().Detail.TypeUrl)
	require.NotEmpty(t, read.GetExtensionTable().Detail.Value)
}

func TestCompileSiriusStreamReadRejectsActualUint32NativeBatchBeforePrepare(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)

	native := batch.NewWithSize(1)
	native.Vecs[0] = vector.NewVec(types.T_uint32.ToType())
	require.NoError(t, vector.AppendFixed(native.Vecs[0], uint32(42), false, proc.Mp()))
	native.SetRowCount(1)
	defer native.Clean(proc.Mp())
	payload, err := native.MarshalBinary()
	require.NoError(t, err)

	decoded := batch.NewOffHeapEmpty()
	require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(payload, proc.Mp()))
	defer decoded.Clean(proc.Mp())
	require.Equal(t, types.T_uint32, decoded.Vecs[0].GetType().Oid,
		"the MO-native codec must not silently widen uint32 to int64")

	queryPlan := siriusStreamTestPlan()
	queryPlan.GetQuery().Nodes[0].TableDef.Cols[0].Typ = planpb.Type{
		Id: int32(decoded.Vecs[0].GetType().Oid),
	}
	_, err = (&Compile{proc: proc}).CompileSiriusStreamRead(
		context.Background(), queryPlan, 7, []byte("0123456789abcdef"), time.Minute,
	)
	require.True(t, substrait.IsNotEligible(err))
	require.ErrorContains(t, err, "unsupported native input type INT UNSIGNED")
}

func TestCompileSiriusStreamReadRejectsBeforeOpeningTransport(t *testing.T) {
	valid := siriusStreamTestPlan()
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)

	for _, tc := range []struct {
		name    string
		compile *Compile
		plan    *planpb.Plan
		queryID []byte
		ttl     time.Duration
		want    string
	}{
		{name: "nil compile", plan: valid, queryID: make([]byte, 16), ttl: time.Minute, want: "no SELECT plan"},
		{name: "missing process", compile: &Compile{}, plan: valid, queryID: make([]byte, 16), ttl: time.Minute, want: "no SELECT plan"},
		{name: "unsupported plan", compile: &Compile{proc: proc}, plan: &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{StmtType: planpb.Query_SELECT}}}, queryID: make([]byte, 16), ttl: time.Minute, want: "SELECT query root"},
		{name: "bad query identity", compile: &Compile{proc: proc}, plan: valid, queryID: make([]byte, 15), ttl: time.Minute, want: "identity is unsupported"},
		{name: "bad lease ttl", compile: &Compile{proc: proc}, plan: valid, queryID: make([]byte, 16), want: "identity is unsupported"},
		{name: "missing transaction", compile: &Compile{proc: proc}, plan: valid, queryID: make([]byte, 16), ttl: time.Minute, want: "read-only snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.compile.CompileSiriusStreamRead(context.Background(), tc.plan, 0, tc.queryID, tc.ttl)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestTryCompileSiriusStreamReadCleansUpPreVisibilityPrepareFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := mock_frontend.NewMockWorkspace(ctrl)
	workspace.EXPECT().Readonly().Return(true)
	workspace.EXPECT().WriteOffset().Return(uint64(0))
	workspace.EXPECT().GetSnapshotWriteOffset().Return(0)
	workspace.EXPECT().GetHaveDDL().Return(false)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 42})

	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = txnOp
	c := &Compile{proc: proc}
	runtime := &SiriusRuntime{
		Flight: new(sidecarflight.Runtime), LeaseTTL: time.Minute, CleanupTimeout: time.Second,
	}
	offloaded, err := c.tryCompileSiriusStreamRead(
		defines.AttachAccountId(context.Background(), 7), siriusStreamTestPlan(), runtime,
	)
	require.False(t, offloaded)
	require.ErrorContains(t, err, "lease-safe execution deadline has expired")
	require.NotNil(t, c.anal)
	c.anal.release()
}

func TestSiriusStreamRuntimeRejectsIncompleteLocalExecution(t *testing.T) {
	execution := new(sidecarflight.Execution)
	runtime := &SiriusRuntime{CleanupTimeout: time.Second}
	directOwner := newSiriusReadOwner(execution, runtime)
	require.NoError(t, directOwner.finish(context.Background(), true))
	require.NoError(t, directOwner.finish(context.Background(), false))

	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	c := &Compile{
		proc: proc,
		fill: func(*batch.Batch, *perfcounter.CounterSet) error {
			return nil
		},
		siriusRead: directOwner,
	}
	require.ErrorContains(t, c.runSiriusRead(context.Background(), nil), "invalid execution")

	c.siriusRead = newSiriusStreamOwner(execution, runtime, []*sidecarflight.NativeInput{nil})
	require.ErrorContains(t, c.runSiriusRead(context.Background(), nil), "has no message board")

	board := message.NewMessageBoard()
	t.Cleanup(func() { board.CloseAndDrain() })
	c.MessageBoard = board
	c.addr = "local"
	c.scopes = []*Scope{{Magic: Remote, NodeInfo: engine.Node{Addr: "remote"}}}
	require.ErrorContains(t, c.runSiriusRead(context.Background(), nil), "is not local-CN")

	startErr := context.DeadlineExceeded
	input := new(sidecarflight.NativeInput)
	input.Abort(startErr)
	c.scopes = nil
	streamOwner := newSiriusStreamOwner(execution, runtime, []*sidecarflight.NativeInput{input})
	require.ErrorIs(t, c.runSiriusStreamRead(context.Background(), streamOwner, nil), startErr)

	emptyOwner := newSiriusStreamOwner(execution, runtime, nil)
	require.ErrorContains(t, c.runSiriusStreamRead(context.Background(), emptyOwner, nil), "invalid execution")
}

func TestSiriusStreamCompileRejectsMissingScanAndRuntime(t *testing.T) {
	c := new(Compile)
	_, _, err := c.compileSiriusStreamScopes(
		&planpb.Query{}, []SiriusStreamInput{{NodeID: 0, StreamRef: make([]byte, 32)}}, nil,
	)
	require.ErrorContains(t, err, "streamed scan node is missing")

	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	offloaded, err := (&Compile{proc: proc, stmt: &tree.Select{}}).tryCompileSiriusRead(
		WithSiriusStreamOffload(context.Background()), siriusStreamTestPlan(),
	)
	require.False(t, offloaded)
	require.ErrorContains(t, err, "runtime is not configured")

	streamRuntime := &SiriusRuntime{LeaseTTL: time.Minute}
	offloaded, err = (&Compile{proc: proc}).tryCompileSiriusStreamRead(
		context.Background(), siriusStreamTestPlan(), streamRuntime,
	)
	require.False(t, offloaded)
	require.Error(t, err)
	offloaded, err = (&Compile{proc: proc}).tryCompileSiriusStreamRead(
		defines.AttachAccountId(context.Background(), 7), siriusStreamTestPlan(), streamRuntime,
	)
	require.False(t, offloaded)
	require.True(t, substrait.IsNotEligible(err))
}

func TestCompileSiriusStreamScopesReleasesTreeWhenInputRegistrationFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage := mock_frontend.NewMockEngine(ctrl)
	database := mock_frontend.NewMockDatabase(ctrl)
	relation := mock_frontend.NewMockRelation(ctrl)
	storage.EXPECT().Database(gomock.Any(), "db", gomock.Any()).Return(database, nil)
	database.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).Return(relation, nil)

	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	queryPlan := siriusStreamTestPlan()
	node := queryPlan.GetQuery().Nodes[0]
	node.ObjRef.SchemaName = "db"
	node.Stats = &planpb.Stats{Dop: 2}
	node.TableDef.Cols = append([]*planpb.ColDef{
		{Name: "hidden", Hidden: true, Typ: planpb.Type{Id: int32(types.T_int64)}},
	}, node.TableDef.Cols...)
	node.Offset = plan2.MakePlan2Uint64ConstExprWithType(1)
	node.Limit = plan2.MakePlan2Uint64ConstExprWithType(2)
	node.AggList = []*planpb.Expr{{}}

	c := &Compile{proc: proc, e: storage, addr: "local", ncpu: 4}
	c.initSiriusStreamCompile(queryPlan)
	t.Cleanup(c.anal.release)
	before := proc.Mp().CurrNB()
	inputs, scopes, err := c.compileSiriusStreamScopes(
		queryPlan.GetQuery(),
		[]SiriusStreamInput{{NodeID: 0, StreamRef: make([]byte, 32)}},
		new(sidecarflight.Execution),
	)
	require.ErrorContains(t, err, "invalid native input identity")
	require.Nil(t, inputs)
	require.Nil(t, scopes)
	require.Equal(t, before, proc.Mp().CurrNB())
}
