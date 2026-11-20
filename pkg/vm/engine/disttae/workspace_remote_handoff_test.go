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

package disttae

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type handoffEngineQueryClient struct {
	queryclient.QueryClient
	engine *Engine
}

func (c handoffEngineQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}
func (c handoffEngineQueryClient) SendMessage(_ context.Context, _ string, req *query.Request) (*query.Response, error) {
	data, err := ioutil.ExecuteRemoteWorkspaceHandoff(req.GetProtocolVersion.WorkspaceHandoffRequest, "remote-cn", c.engine)
	if err != nil {
		return nil, err
	}
	return &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{WorkspaceHandoffVersion: 1, WorkspaceHandoffResponse: data}}, nil
}
func (handoffEngineQueryClient) Release(*query.Response) {}

type handoffEngineCluster struct{ clusterservice.MOCluster }

func (handoffEngineCluster) GetCNServiceWithoutWorkingState(_ clusterservice.Selector, fn func(metadata.CNService) bool) {
	fn(metadata.CNService{PipelineServiceAddress: "remote-cn", QueryAddress: "remote-query"})
}

type handoffEngineLock struct {
	lockservice.LockService
	sid string
}

func (l handoffEngineLock) GetConfig() lockservice.Config {
	return lockservice.Config{ServiceID: l.sid}
}

type handoffDecisionProvider struct{ engine *Engine }

func (p handoffDecisionProvider) WorkspaceHandoffDecision(id ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error) {
	return p.engine.QueryCoordinatorWorkspaceHandoff(id)
}

func newRemoteWorkspaceHandoffForTest(t *testing.T) (*Transaction, ioutil.RemoteWorkspaceHandoffID, *workspaceCleanupFS) {
	t.Helper()
	txn := newTransactionWithActivePKTableForTest(t, "v")
	fs := &workspaceCleanupFS{}
	txn.engine.fs = fs
	id := ioutil.RemoteWorkspaceHandoffID{
		Version: ioutil.RemoteWorkspaceHandoffVersion1, TxnID: string(txn.op.Txn().ID),
		ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 1, ObjectName: "remote-object",
	}
	require.NoError(t, txn.engine.reserveUnpublishedPendingObject(id.ObjectName))
	txn.noteUnpublishedDumpReservation(id.ObjectName)
	require.NoError(t, txn.bindRemoteWorkspaceHandoff(id))
	return txn, id, fs
}

func TestRemoteWorkspaceHandoffAdoptionWaitsForProducer(t *testing.T) {
	txn, id, fs := newRemoteWorkspaceHandoffForTest(t)
	require.NoError(t, txn.bindRemoteWorkspaceHandoff(id))
	for range 2 {
		matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
		require.NoError(t, err)
		require.True(t, matched)
	}
	require.EqualValues(t, 1, txn.engine.gcProducers, "an ACK does not stop an active writer")
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	require.Zero(t, txn.engine.gcProducers)
	queued, live, inFlight := txn.engine.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.Zero(t, txn.engine.unpublishedPending.remotePending)
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched, "a repeated decision returns its settled receipt")
	matched, err = txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffRejected)
	require.False(t, matched)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.Empty(t, fs.deleted)
}

func TestRemoteWorkspaceHandoffUnknownIsBoundedAndReported(t *testing.T) {
	txn, id, fs := newRemoteWorkspaceHandoffForTest(t)
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	require.Zero(t, txn.engine.gcProducers)
	require.EqualValues(t, 1, txn.engine.unpublishedPending.remotePending)
	require.Empty(t, txn.unpublishedDumpReservations)
	require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(context.Background()))
	require.Empty(t, fs.deleted, "an unknown result must never be deleted by the drain")
	err := txn.engine.closeWorkspaceGC(time.Now().Add(time.Second))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
	require.Zero(t, txn.engine.unpublishedPending.remotePending)
}

func TestRemoteWorkspaceHandoffUnsentVersusExposed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		expose bool
	}{
		{"never_emitted", false},
		{"output_exposed", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "v")
			fs := &workspaceCleanupFS{}
			txn.engine.fs = fs
			scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn:1"}
			require.NoError(t, txn.ConfigureRemoteWorkspaceHandoff(scope))
			name := "object"
			require.NoError(t, txn.engine.reserveUnpublishedPendingObject(name))
			txn.noteUnpublishedDumpReservation(name)
			require.NoError(t, txn.bindRemoteWorkspaceHandoff(scope.Object(name)))
			if tc.expose {
				require.NoError(t, txn.MarkRemoteWorkspaceOutputReady([]string{name}))
			}
			txn.RetainUnrecordedS3Objects([]string{name}, "writer exited")
			require.Zero(t, txn.engine.gcProducers)
			if tc.expose {
				require.EqualValues(t, 1, txn.engine.unpublishedPending.remotePending)
				require.Empty(t, txn.engine.unpublishedPending.names)
				require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(context.Background()))
				require.Empty(t, fs.deleted, "an exposed result may already have been adopted")
			} else {
				require.Zero(t, txn.engine.unpublishedPending.remotePending)
				require.Equal(t, []string{name}, txn.engine.unpublishedPending.names)
				require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(context.Background()))
				require.Equal(t, []string{name}, fs.deleted)
			}
		})
	}
}

func TestRemoteWorkspaceHandoffPartialOutputKeepsOtherObjectUnsent(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "v")
	txn.engine.fs = &workspaceCleanupFS{}
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn:1"}
	require.NoError(t, txn.ConfigureRemoteWorkspaceHandoff(scope))
	for _, name := range []string{"sent", "unsent"} {
		require.NoError(t, txn.engine.reserveUnpublishedPendingObject(name))
		txn.noteUnpublishedDumpReservation(name)
		require.NoError(t, txn.bindRemoteWorkspaceHandoff(scope.Object(name)))
	}
	require.NoError(t, txn.MarkRemoteWorkspaceOutputReady([]string{"sent"}))
	txn.RetainUnrecordedS3Objects([]string{"sent", "unsent"}, "partial output")
	require.Zero(t, txn.engine.gcProducers)
	require.EqualValues(t, 1, txn.engine.unpublishedPending.remotePending)
	require.Equal(t, []string{"unsent"}, txn.engine.unpublishedPending.names)
	require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(context.Background()))
	require.Equal(t, []string{"unsent"}, txn.engine.fs.(*workspaceCleanupFS).deleted)
}

func TestRemoteWorkspaceHandoffRecordRefusalKeepsProducerOwnership(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "v")
	oldLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = oldLimit })
	require.NoError(t, txn.engine.reserveUnpublishedPendingObject("already-admitted"))
	t.Cleanup(func() { txn.engine.abandonUnpublishedPendingReservations([]string{"already-admitted"}) })
	objectName := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	stats := objectio.NewObjectStatsWithObjectID(objectName.ObjectId(), false, false, true)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_binary.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_binary.ToType())
	t.Cleanup(func() { bat.Clean(txn.proc.Mp()) })
	var block objectio.BlockInfo
	block.ConstructBlockID(objectName, 0)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], objectio.EncodeBlockInfo(&block), false, txn.proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], stats[:], false, txn.proc.Mp()))
	bat.SetRowCount(1)
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn:1"}
	var err error
	bat.ExtraBuf, err = ioutil.MarshalRemoteWorkspaceHandoffScope(scope)
	require.NoError(t, err)
	badScope := scope
	badScope.TxnID = "another-transaction"
	badMetadata, err := ioutil.MarshalRemoteWorkspaceHandoffScope(badScope)
	require.NoError(t, err)
	validMetadata := bat.ExtraBuf
	bat.ExtraBuf = badMetadata
	require.Error(t, txn.registerUnpublishedObjectRecordLocked(INSERT, bat))
	require.Empty(t, txn.pendingObjectCleanup)
	require.Empty(t, txn.engine.unpublishedPending.ownedNames)
	bat.ExtraBuf = validMetadata
	err = txn.registerUnpublishedObjectRecordLocked(INSERT, bat)
	require.Error(t, err)
	require.Empty(t, txn.pendingObjectCleanup, "coordinator must not queue a refused remote object")
	require.Empty(t, txn.engine.unpublishedPending.names)
	require.Empty(t, txn.engine.unpublishedPending.ownedNames)
	budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
	defer txn.closeWorkspaceAllocation()
	defer func() { require.NoError(t, txn.releaseRemoteCoordinatorRecordsLocked()) }()
	require.Error(t, txn.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, 42, "db", "tbl", "file", bat, DNStore{}, 0, false))
	require.Equal(t, 1, txn.engine.unpublishedPending.remoteCoordinatorReceipts, "definite rejection retains one bounded receipt")
	state, err := txn.engine.QueryCoordinatorWorkspaceHandoff(scope.Object(objectName.String()))
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffRejected, state)
	require.Error(t, txn.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, 42, "db", "tbl", "file", bat, DNStore{}, 0, false), "a rejected identity must remain fenced")
	require.Empty(t, txn.writes)
	require.NoError(t, txn.releaseRemoteCoordinatorRecordsLocked())
	require.Error(t, txn.engine.admitUnpublishedPendingRecordedNames(txn, []string{objectName.String()}), "a late different attempt must not reacquire a name queued for remote cleanup")
}

func TestRemoteWorkspaceHandoffMixedRedeliveryPreservesObjectBlocks(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "v")
	bat := colexec.AllocCNS3ResultBat(false)
	defer bat.Clean(txn.proc.Mp())
	first := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	second := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	for _, spec := range []struct {
		name objectio.ObjectName
		seq  uint16
	}{{first, 0}, {second, 0}, {second, 1}} {
		var block objectio.BlockInfo
		block.ConstructBlockID(spec.name, spec.seq)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], objectio.EncodeBlockInfo(&block), false, txn.proc.Mp()))
	}
	for _, name := range []objectio.ObjectName{first, second} {
		stats := objectio.NewObjectStatsWithObjectID(name.ObjectId(), false, false, true)
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], stats[:], false, txn.proc.Mp()))
	}
	bat.SetRowCount(3)
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn:1"}
	var err error
	bat.ExtraBuf, err = ioutil.MarshalRemoteWorkspaceHandoffScope(scope)
	require.NoError(t, err)
	target := remoteWorkspaceRecordTarget{typ: INSERT, accountID: 1, databaseID: 7, tableID: 42}
	txn.remoteWorkspaceRecords = map[ioutil.RemoteWorkspaceHandoffID]remoteWorkspaceRecordReceipt{scope.Object(first.String()): {target: target, decision: ioutil.RemoteWorkspaceHandoffAdopted}}
	var foreign objectio.BlockInfo
	foreign.ConstructBlockID(objectio.BuildObjectName(objectio.NewSegmentid(), 0), 0)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], objectio.EncodeBlockInfo(&foreign), false, txn.proc.Mp()))
	bat.SetRowCount(4)
	_, _, _, err = txn.selectRemoteWorkspaceRecordsLocked(INSERT, bat, target)
	require.Error(t, err, "an unrelated block must not be silently discarded")
	bat.Vecs[0].Shrink([]int64{0, 1, 2}, false)
	bat.SetRowCount(3)
	keep, ids, present, err := txn.selectRemoteWorkspaceRecordsLocked(INSERT, bat, target)
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, []int64{1}, keep)
	require.Equal(t, []ioutil.RemoteWorkspaceHandoffID{scope.Object(second.String())}, ids)
	require.NoError(t, shrinkRemoteWorkspaceMetadata(INSERT, bat, keep))
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, 2, bat.Vecs[0].Length())
	require.Equal(t, 1, bat.Vecs[1].Length())
	for i := 0; i < bat.RowCount(); i++ {
		block := objectio.DecodeBlockInfo(bat.Vecs[0].GetBytesAt(i))
		require.Equal(t, *second.ObjectId(), *block.BlockID.Object())
	}
	stats := objectio.ObjectStats(bat.Vecs[1].GetBytesAt(0))
	require.Equal(t, second, stats.ObjectName())
}

func TestRemoteWorkspaceHandoffPartlyRepeatedRecordIsPublishedOnce(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		txn.closeWorkspaceAllocation()
		require.NoError(t, txn.releaseRemoteCoordinatorRecordsLocked())
	}()
	first := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	second := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ProducerCN: "cn:1"}
	metadata := func(names ...objectio.ObjectName) *batch.Batch {
		bat := colexec.AllocCNS3ResultBat(false)
		for _, name := range names {
			blocks := 1
			if *name.ObjectId() == *second.ObjectId() {
				blocks = 2
			}
			for i := 0; i < blocks; i++ {
				var block objectio.BlockInfo
				block.ConstructBlockID(name, uint16(i))
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], objectio.EncodeBlockInfo(&block), false, txn.proc.Mp()))
			}
			stats := objectio.NewObjectStatsWithObjectID(name.ObjectId(), false, false, true)
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], stats[:], false, txn.proc.Mp()))
		}
		bat.SetRowCount(bat.Vecs[0].Length())
		bat.ExtraBuf, err = ioutil.MarshalRemoteWorkspaceHandoffScope(scope)
		require.NoError(t, err)
		return bat
	}
	one := metadata(first)
	defer one.Clean(txn.proc.Mp())
	mixed := metadata(first, second)
	defer mixed.Clean(txn.proc.Mp())
	write := func(b *batch.Batch, tableID uint64) error {
		return txn.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, tableID, "db", "tbl", "file", b, DNStore{}, 0, false)
	}
	require.NoError(t, write(one, 42))
	state, err := txn.engine.QueryCoordinatorWorkspaceHandoff(scope.Object(first.String()))
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffAdopted, state)
	state, err = txn.engine.QueryCoordinatorWorkspaceHandoff(scope.Object(second.String()))
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffUnknown, state)
	require.NoError(t, write(mixed, 42))
	require.Len(t, txn.writes, 2)
	require.Equal(t, 2, txn.engine.unpublishedPending.remoteCoordinatorReceipts)
	require.Equal(t, 2, txn.writes[1].bat.RowCount())
	require.Equal(t, 1, txn.writes[1].bat.Vecs[1].Length())
	require.NoError(t, write(mixed, 42))
	require.Len(t, txn.writes, 2)
	require.Equal(t, 2, txn.engine.unpublishedPending.remoteCoordinatorReceipts)
	require.Error(t, write(mixed, 43), "the same identity cannot be installed for another table")
	require.Len(t, txn.writes, 2)
	require.NoError(t, txn.releaseRemoteCoordinatorRecordsLocked())
	require.Zero(t, txn.engine.unpublishedPending.remoteCoordinatorReceipts)
	state, err = txn.engine.QueryCoordinatorWorkspaceHandoff(scope.Object(first.String()))
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffUnknown, state, "a released receipt is unknown, never a rejection")
}

func TestRemoteWorkspaceHandoffCoordinatorReceiptsAreCNBounded(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "v")
	oldLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 2
	defer func() { unpublishedPendingAdmissionLimit = oldLimit }()
	require.NoError(t, txn.reserveRemoteCoordinatorReceipts(2))
	other := newTransactionWithActivePKTableForTest(t, "v")
	other.engine = txn.engine
	require.Error(t, other.reserveRemoteCoordinatorReceipts(1), "transactions share the CN receipt bound")
	require.Equal(t, 2, txn.engine.unpublishedPending.remoteCoordinatorReceipts)
	require.Error(t, txn.releaseRemoteCoordinatorReceipts(3), "accounting invariant failure must not panic or refund")
	require.Equal(t, 2, txn.engine.unpublishedPending.remoteCoordinatorReceipts)
	require.NoError(t, txn.releaseRemoteCoordinatorReceipts(2))
	require.NoError(t, other.reserveRemoteCoordinatorReceipts(1))
	require.NoError(t, other.releaseRemoteCoordinatorReceipts(1))
	require.Zero(t, txn.engine.unpublishedPending.remoteCoordinatorReceipts)
}

func TestRemoteWorkspaceHandoffSharedSourceRefusalStaysUnknown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*Transaction, string)
	}{
		{"ccpr", func(txn *Transaction, _ string) { txn.isCCPRTxn = true }},
		{"clone", func(txn *Transaction, _ string) { txn.isCloneTxn = true }},
		{"existing-owner", func(txn *Transaction, name string) {
			txn.engine.unpublishedPending.ownedNames = map[string]map[*Transaction]struct{}{name: {txn: {}}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "v")
			id := ioutil.RemoteWorkspaceHandoffID{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ObjectName: "shared-object"}
			tc.setup(txn, id.ObjectName)
			require.NoError(t, txn.reserveRemoteCoordinatorReceipts(1))
			require.Zero(t, txn.retainRejectedRemoteRecordsLocked([]ioutil.RemoteWorkspaceHandoffID{id}, remoteWorkspaceRecordTarget{typ: INSERT}))
			require.NoError(t, txn.releaseRemoteCoordinatorReceipts(1))
			state, err := txn.engine.QueryCoordinatorWorkspaceHandoff(id)
			require.NoError(t, err)
			require.Equal(t, ioutil.RemoteWorkspaceHandoffUnknown, state)
			require.False(t, txn.engine.unpublishedPending.mightHaveRemoteRejectedNameLocked(id.ObjectName))
		})
	}
}

func TestRemoteWorkspaceHandoffDecisionPassesThroughRelationHandle(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "v")
	id := ioutil.RemoteWorkspaceHandoffID{Version: 1, TxnID: string(txn.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, ObjectName: "object"}
	txn.engine.unpublishedPending.remoteCoordinatorDecisions = map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision{id: ioutil.RemoteWorkspaceHandoffRejected}
	table := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	delegate := &txnTableDelegate{origin: table}
	state, err := delegate.WorkspaceHandoffDecision(id)
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffRejected, state)
	id.TxnID = "different-transaction"
	_, err = delegate.WorkspaceHandoffDecision(id)
	require.Error(t, err)
}

func TestRemoteWorkspaceHandoffRejectionKeepsCleanupOnFailure(t *testing.T) {
	txn, id, fs := newRemoteWorkspaceHandoffForTest(t)
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffRejected)
	require.NoError(t, err)
	require.True(t, matched)
	require.Empty(t, txn.engine.unpublishedPending.names, "do not delete while the producer is writing")
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	require.Equal(t, []string{id.ObjectName}, txn.engine.unpublishedPending.names)
	require.Zero(t, txn.engine.gcProducers)
	fs.err = errors.New("injected delete error")
	require.Error(t, txn.engine.deleteQueuedUnpublishedPendingOnce(context.Background()))
	require.Equal(t, []string{id.ObjectName}, txn.engine.unpublishedPending.names)
	fs.err = nil
	require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(context.Background()))
	require.Equal(t, []string{id.ObjectName}, fs.deleted)
	require.Empty(t, txn.engine.unpublishedPending.names)
}

func TestRemoteWorkspaceHandoffOldAttemptCannotSettleNew(t *testing.T) {
	txn, old, _ := newRemoteWorkspaceHandoffForTest(t)
	require.NoError(t, txn.stopRemoteWorkspaceProducer(old, false))
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(old, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
	newID := old
	newID.Attempt++
	require.NoError(t, txn.engine.reserveUnpublishedPendingObject(newID.ObjectName))
	txn.noteUnpublishedDumpReservation(newID.ObjectName)
	require.NoError(t, txn.bindRemoteWorkspaceHandoff(newID))
	state, err := txn.engine.QueryRemoteWorkspaceHandoff(old)
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffAdopted, state)
	state, err = txn.engine.QueryRemoteWorkspaceHandoff(newID)
	require.NoError(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffUnknown, state)
	matched, err = txn.engine.decideRemoteWorkspaceHandoff(old, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched, "a late retry may observe the old receipt but cannot settle the new attempt")
	require.EqualValues(t, 1, txn.engine.gcProducers)
	require.NoError(t, txn.stopRemoteWorkspaceProducer(newID, false))
	require.EqualValues(t, 1, txn.engine.unpublishedPending.remotePending)
	matched, err = txn.engine.decideRemoteWorkspaceHandoff(newID, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
}

func TestRemoteWorkspaceHandoffRejectionCannotDeleteEarlierAdoption(t *testing.T) {
	txn, old, fs := newRemoteWorkspaceHandoffForTest(t)
	require.NoError(t, txn.stopRemoteWorkspaceProducer(old, false))
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(old, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
	newID := old
	newID.Attempt++
	require.NoError(t, txn.engine.reserveUnpublishedPendingObject(newID.ObjectName))
	txn.noteUnpublishedDumpReservation(newID.ObjectName)
	require.NoError(t, txn.bindRemoteWorkspaceHandoff(newID))
	matched, err = txn.engine.decideRemoteWorkspaceHandoff(newID, ioutil.RemoteWorkspaceHandoffRejected)
	require.NoError(t, err)
	require.False(t, matched, "a prior adoption makes deletion unsafe")
	require.NoError(t, txn.stopRemoteWorkspaceProducer(newID, true))
	require.EqualValues(t, 1, txn.engine.unpublishedPending.remotePending)
	require.Empty(t, txn.engine.unpublishedPending.names)
	require.Empty(t, fs.deleted)
}

func TestRemoteWorkspaceHandoffConcurrentStopAndAdoption(t *testing.T) {
	for range 100 {
		txn, id, _ := newRemoteWorkspaceHandoffForTest(t)
		var wg sync.WaitGroup
		var stopErr, ackErr error
		var matched bool
		wg.Add(2)
		go func() { defer wg.Done(); stopErr = txn.stopRemoteWorkspaceProducer(id, false) }()
		go func() {
			defer wg.Done()
			matched, ackErr = txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
		}()
		wg.Wait()
		require.NoError(t, stopErr)
		require.NoError(t, ackErr)
		require.True(t, matched)
		require.Zero(t, txn.engine.gcProducers)
		require.Zero(t, txn.engine.unpublishedPending.inFlight)
		require.Zero(t, txn.engine.unpublishedPending.remotePending)
		require.Empty(t, txn.engine.unpublishedPending.names)
	}
}

func TestRemoteWorkspaceHandoffPendingConsumesAdmission(t *testing.T) {
	txn, id, _ := newRemoteWorkspaceHandoffForTest(t)
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	oldLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = oldLimit })
	require.Error(t, txn.engine.reserveUnpublishedPendingObject("different-object"))
	require.Error(t, txn.engine.admitUnpublishedPendingRecordedNames(txn, []string{id.ObjectName}), "an unresolved object cannot be republished locally")
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
	require.NoError(t, txn.engine.reserveUnpublishedPendingObject("different-object"))
	txn.engine.abandonUnpublishedPendingReservations([]string{"different-object"})
}

func TestRemoteWorkspaceHandoffRejectsConflictingDecision(t *testing.T) {
	txn, id, _ := newRemoteWorkspaceHandoffForTest(t)
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
	matched, err = txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffRejected)
	require.False(t, matched)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidState))
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	require.Empty(t, txn.engine.unpublishedPending.names)
}

func TestRemoteWorkspaceHandoffReceiptsAreBounded(t *testing.T) {
	txn, id, _ := newRemoteWorkspaceHandoffForTest(t)
	require.NoError(t, txn.stopRemoteWorkspaceProducer(id, false))
	matched, err := txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.True(t, matched)
	q := &txn.engine.unpublishedPending
	q.mu.Lock()
	for n := 0; n < remoteWorkspaceHandoffReceiptLimit; n++ {
		next := id
		next.ObjectName = "receipt-" + strconv.Itoa(n)
		q.remotePending++
		txn.engine.settleRemoteWorkspaceHandoffLocked(next, remoteWorkspaceHandoffRecord{decision: ioutil.RemoteWorkspaceHandoffAdopted})
	}
	count := len(q.remoteReceipts)
	q.mu.Unlock()
	require.Equal(t, remoteWorkspaceHandoffReceiptLimit, count)
	matched, err = txn.engine.decideRemoteWorkspaceHandoff(id, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	require.False(t, matched, "an evicted receipt is unknown, not a negative acknowledgement")
	require.Zero(t, q.remotePending)
}

func TestRemoteWorkspaceHandoffRealWriterAndCoordinatorRecord(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	remote := newTransactionWithActivePKTableForTest(t, "pk")
	coordinator := newTransactionWithActivePKTableForTest(t, "pk")
	budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, coordinator.proc.SetHashBuildBudgetForTesting(parent))
	fs, err := colexec.GetSharedFSFromProc(remote.proc)
	require.NoError(t, err)
	remote.engine.fs, coordinator.engine.fs = fs, fs
	coordinator.cnObjsSummary = make(map[types.Objectid]Summary)
	coordinator.batchSelectList = make(map[*batch.Batch][]int64)
	scope := ioutil.RemoteWorkspaceHandoffScope{
		Version: ioutil.RemoteWorkspaceHandoffVersion1, TxnID: string(remote.op.Txn().ID),
		ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 1, ProducerCN: "remote-cn",
	}
	require.NoError(t, remote.ConfigureRemoteWorkspaceHandoff(scope))
	remote.proc.Base.TxnOperator = remote.op
	def := &pbplan.TableDef{Cols: []*pbplan.ColDef{{Name: "pk", Seqnum: 0, Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true}}, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"}}
	w := colexec.NewCNS3DataWriter(remote.proc.Mp(), fs, def, -1, false, remote.SinkerCapacityOption())
	defer w.Close()
	b := batch.NewWithSize(1)
	b.Attrs = []string{"pk"}
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer b.Clean(remote.proc.Mp())
	require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(1), false, remote.proc.Mp()))
	b.SetRowCount(1)
	require.NoError(t, w.Write(ctx, b))
	// The pressure-flush path transfers its result slice out of the sinker;
	// those objects must still be reported when the writer stops.
	meta, err := w.SyncAndFillBlockInfoBat(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, meta.Vecs[1].Length())
	stats := objectio.ObjectStats(meta.Vecs[1].GetBytesAt(0))
	name := stats.ObjectName().String()
	require.NoError(t, colexec.AttachWorkspaceHandoffScope(remote.proc, meta, name))
	require.NoError(t, coordinator.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, 42, "db", "tbl", name, meta, DNStore{}, 0, false))
	defer func() {
		for i := range coordinator.writes {
			coordinator.releaseWorkspaceEntryBatchLocked(i)
		}
		coordinator.closeWorkspaceAllocation()
		require.NoError(t, coordinator.releaseRemoteCoordinatorRecordsLocked())
	}()
	writesAfterFirst := len(coordinator.writes)
	usedAfterFirst := coordinator.workspaceAccount.Snapshot().Used
	require.NoError(t, coordinator.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, 42, "db", "tbl", name, meta, DNStore{}, 0, false))
	require.Len(t, coordinator.writes, writesAfterFirst, "redelivery must not publish the same object twice")
	require.Equal(t, usedAfterFirst, coordinator.workspaceAccount.Snapshot().Used, "redelivery must not reserve another batch")
	remote.RetainUnrecordedS3Objects(w.AbandonAndRetainableNames(), "remote writer exited")
	remote.proc.Free()
	require.Zero(t, remote.engine.gcProducers)
	require.EqualValues(t, 1, remote.engine.unpublishedPending.remotePending)
	require.Empty(t, remote.unpublishedDumpReservations)
	require.True(t, objectExistsInFS(ctx, fs, name))
	sid := t.Name()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.ClusterService, handoffEngineCluster{})
	rpcProc := &process.Process{Base: &process.BaseProcess{LockService: handoffEngineLock{sid: sid}, QueryClient: handoffEngineQueryClient{engine: remote.engine}}}
	require.NoError(t, colexec.ConfirmRemoteWorkspaceHandoffs(ctx, rpcProc, scope, []string{name}, ioutil.RemoteWorkspaceHandoffAdopted))
	require.Zero(t, remote.engine.unpublishedPending.remotePending)
	require.True(t, objectExistsInFS(ctx, fs, name), "the adopted object belongs to the coordinator")
}

func TestRemoteWorkspaceHandoffRealWriterRejectedRecordIsCleaned(t *testing.T) {
	ctx := context.Background()
	remote := newTransactionWithActivePKTableForTest(t, "pk")
	coordinator := newTransactionWithActivePKTableForTest(t, "pk")
	budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, coordinator.proc.SetHashBuildBudgetForTesting(parent))
	defer coordinator.closeWorkspaceAllocation()
	defer func() { require.NoError(t, coordinator.releaseRemoteCoordinatorRecordsLocked()) }()
	fs, err := colexec.GetSharedFSFromProc(remote.proc)
	require.NoError(t, err)
	remote.engine.fs, coordinator.engine.fs = fs, fs
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: string(remote.op.Txn().ID), ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 1, ProducerCN: "remote-cn"}
	require.NoError(t, remote.ConfigureRemoteWorkspaceHandoff(scope))
	remote.proc.Base.TxnOperator = remote.op
	def := &pbplan.TableDef{Cols: []*pbplan.ColDef{{Name: "pk", Seqnum: 0, Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true}}, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"}}
	w := colexec.NewCNS3DataWriter(remote.proc.Mp(), fs, def, -1, false, remote.SinkerCapacityOption())
	defer w.Close()
	b := batch.NewWithSize(1)
	b.Attrs = []string{"pk"}
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer b.Clean(remote.proc.Mp())
	require.NoError(t, vector.AppendFixed(b.Vecs[0], int64(1), false, remote.proc.Mp()))
	b.SetRowCount(1)
	require.NoError(t, w.Write(ctx, b))
	meta, err := w.SyncAndFillBlockInfoBat(ctx)
	require.NoError(t, err)
	stats := objectio.ObjectStats(meta.Vecs[1].GetBytesAt(0))
	name := stats.ObjectName().String()
	require.NoError(t, colexec.AttachWorkspaceHandoffScope(remote.proc, meta, name))
	oldLimit := unpublishedPendingAdmissionLimit
	unpublishedPendingAdmissionLimit = 1
	defer func() { unpublishedPendingAdmissionLimit = oldLimit }()
	require.NoError(t, coordinator.engine.reserveUnpublishedPendingObject("blocker"))
	defer coordinator.engine.abandonUnpublishedPendingReservations([]string{"blocker"})
	require.Error(t, coordinator.writeFileLockedWithAutoIncrEpochKnown(INSERT, 1, 7, 42, "db", "tbl", name, meta, DNStore{}, 0, false))
	require.True(t, objectExistsInFS(ctx, fs, name), "the producer must retain the object until explicit rejection")
	sid := t.Name()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.ClusterService, handoffEngineCluster{})
	rpcProc := &process.Process{Ctx: ctx, Base: &process.BaseProcess{LockService: handoffEngineLock{sid: sid}, QueryClient: handoffEngineQueryClient{engine: remote.engine}}}
	require.NoError(t, colexec.SettleFailedWorkspaceHandoff(rpcProc, handoffDecisionProvider{engine: coordinator.engine}, scope, meta, 1))
	require.True(t, objectExistsInFS(ctx, fs, name), "producer exit must precede deletion")
	remote.RetainUnrecordedS3Objects(w.AbandonAndRetainableNames(), "remote writer exited after rejection")
	require.Zero(t, remote.engine.gcProducers)
	require.Equal(t, []string{name}, remote.engine.unpublishedPending.names)
	require.NoError(t, remote.engine.deleteQueuedUnpublishedPendingOnce(ctx))
	require.False(t, objectExistsInFS(ctx, fs, name))
}
