//go:build mo_m0

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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type m0BoundDispatchTxn struct{ *m0DispatchStampedTxn }

func (op *m0BoundDispatchTxn) Txn() txn.TxnMeta {
	meta := op.TxnOperator.Txn()
	meta.SnapshotTS = op.snapshot
	return meta
}

func (op *m0BoundDispatchTxn) SnapshotTS() timestamp.Timestamp { return op.snapshot }

// Exercises the existing stream dispatch, scope codec and receiver reader
// construction in one process. The engine and stream are deliberately mocks;
// this is not a deployed multi-CN qualification.
func TestM0SingleRemoteRebuildDispatchAndReceive(t *testing.T) {
	ctrl := gomock.NewController(t)
	workspace := &m0MutableSchedulingWorkspace{Ws: &Ws{}, readOnly: true}
	_, inner := newTestTxnClientAndOp(ctrl, workspace)
	ts := timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 1}
	op := &m0BoundDispatchTxn{&m0DispatchStampedTxn{TxnOperator: inner, workspace: workspace, snapshot: ts}}
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(context.Background(), 42), 30*time.Second)
	t.Cleanup(cancel)
	c.proc.Ctx = ctx
	c.proc.ReplaceTopCtx(ctx)
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	c.proc.Base.TxnOperator = op
	c.proc.Base.TxnClient = &m0WatermarkClient{result: ts}
	c.addr = "ingress:6001"
	def := &plan.TableDef{TblId: 42, Name: "t", Cols: []*plan.ColDef{{ColId: 1, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}}}}
	node := &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: "t", Obj: 42}, TableDef: def, Stats: &plan.Stats{}}
	rel := &m0RecordingTombstoneRelation{stubRelation: newStubRelation("t"), result: readutil.NewEmptyTombstoneData()}
	worker := &engine.Node{Addr: "remote:6001", CNCNT: 1, Mcpu: 1}
	require.NoError(t, (&remoteScanTombstoneAttacher{c: c, node: node, rel: rel}).attach(worker))
	root := connector.NewArgument()
	root.Reg = &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
	senderScope := &Scope{Magic: Remote, Proc: c.proc.NewNoContextChildProc(0), RootOp: root, ScopeAnalyzer: &ScopeAnalyzer{isStoped: true},
		NodeInfo: *worker, DataSource: &Source{node: node, SchemaName: "db", RelationName: "t", TableDef: def, Timestamp: ts, Attributes: []string{"a"}}, TxnOffset: c.TxnOffset}
	senderScope.Proc.Ctx = ctx
	t.Cleanup(senderScope.release)
	certificate := senderScope.NodeInfo.Data
	senderScope.NodeInfo.Data = readutil.BuildEmptyRelData()
	uncertified, err := senderScope.remoteRun(c)
	require.Nil(t, uncertified, "an uncertified scan must fail before opening a stream")
	require.ErrorContains(t, err, "single remote scan lacks workspace certificate")
	senderScope.NodeInfo.Data = certificate

	remoteEngine := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	remoteRel := mock_frontend.NewMockRelation(ctrl)
	remoteEngine.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).Return(remoteRel, nil).AnyTimes()
	remoteRel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
	combined := &disttae.CombinedRelData{}
	remoteRel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ engine.RangesParam) (engine.RelData, error) {
		require.True(t, m0read.NeedsRemoteMemberValidation(ctx), "remote range selection must request member validation")
		return combined, nil
	})
	remoteRel.EXPECT().BuildReaders(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ any, _ *plan.Expr, data engine.RelData, _, _ int, _ bool, policy engine.TombstoneApplyPolicy, _ engine.FilterHint) ([]engine.Reader, error) {
			require.Same(t, combined, data)
			require.Equal(t, engine.TombstoneApplyPolicy(engine.Policy_CheckAll), policy)
			return []engine.Reader{new(readutil.EmptyReader)}, nil
		})
	c.e = remoteEngine
	responses := make(chan morpc.Message, 1)
	stream := mock_morpc.NewMockStream(ctrl)
	stream.EXPECT().ID().Return(uint64(17)).AnyTimes()
	stream.EXPECT().Receive().Return(responses, nil)
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, msg morpc.Message) error {
		message := msg.(*pipeline.Message)
		require.Equal(t, pipeline.Method_PipelineMessage, message.GetCmd())
		// Model a later write of the same statement after its scan was sent.
		// It must not change the already encoded zero-prefix scan.
		workspace.readOnly = false
		workspace.revision++
		received, err := decodeScope(message.GetData(), c.proc, true, nil)
		require.NoError(t, err)
		defer received.release()
		require.Zero(t, received.TxnOffset)
		received.Proc.Ctx = c.proc.Ctx
		readers, _, err := received.buildReaders(c)
		require.NoError(t, err)
		require.Len(t, readers, 1)
		require.NoError(t, readers[0].Close())
		responses <- &pipeline.Message{Id: 17, Cmd: pipeline.Method_PipelineMessage, Sid: pipeline.Status_MessageEnd}
		return nil
	}).Times(1)
	stream.EXPECT().Close(true).Return(nil)
	installIncompleteTemporalTestStream(t, c, stream)
	sender, err := senderScope.remoteRun(c)
	if sender != nil {
		t.Cleanup(func() { sender.close(context.Background()) })
	}
	require.NoError(t, err)
}

// Establish the production sender admission before testing changes after the
// stream opens. This helper does not manufacture a certificate or NULL proof.
