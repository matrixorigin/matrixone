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

// Dispatcher-level transport proof ladder for issue #25590 design r11
// (D1/D2/D4, round-29 pre-PR finding 2): the disttae-side r23
// regressions call the classifiers and Adopt/Finalize in place, which
// the round-28 review found bypasses the compile dispatcher - no test
// invoked AdoptRemoteRunIntakeMetadata, the three receive loops, or the
// settle's transfer/keep classification with real scopes. These legs run
// in the compile package, where every layer is reachable:
//
//   - the intake mode matrix drives the REAL dispatcher export with
//     production-format metadata batches under ordinary and metadata
//     modes, with the coordnator's adoption observable through the
//     engine's admission ledger (PinUnpublishedPendingAdmissionLimitForTest:
//     a successful reserve consumes one bound slot permanently in this
//     fixture, so whether the intake REALLY adopted a name is decided by
//     whether the next probe adoption is refused at the bound);
//   - fail-closed metadata: a malformed tail errors ONLY through a
//     metadata-mode gate; the very same batch is byte-silent through an
//     ordinary-mode stream (the round-24 F2 harm reversed);
//   - the three receive loops run against a stub morpc stream: adoption
//     precedes the ACK, an intake error fails the loop BEFORE the ACK
//     and BEFORE forwarding, a stopped receiver still ACKs after
//     adoption, and an ordinary-mode stream ACKs a malformed tail
//     without error;
//   - the settle legs run through settleRemoteFragmentWorkspaceAfterDrain
//     with real runCompile/receiver fixtures: the capable-peer clean
//     drain TRANSFERS the retained names (the fragment engine queue
//     stays empty and the witness file is physically retained), while
//     the below-advertisement request, the cancelled run, the
//     receiver-stopped flow and the drain failure each KEEP the local
//     owner (the fragment engine queue ACQUIRES the retained names, and
//     the running engine's drain - proven physically by the in-package
//     disttae composed regressions r23/r21 - retires them).
//
// The physical deletion of a KEEPed fragment's queue is the in-package
// disttae machinery (the queue drain is unexported); its end-to-end
// delete-with-witness proof lives in
// pkg/vm/engine/disttae/txn_issue25590_test.go and stays green -
// TestIssue25590RemoteIntakeAdoptsBeforeRelease and the failed-run
// settlement leg delete retained names through the real engine drain.
// These compile legs pin where the deletion ticket is filed by each
// terminal: transferred releases, every keep queues.

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// r29IntakeBound pins the admission ledger bound small enough that a
	// single adoption fills a slot and the next distinct-name probe
	// decides the outcome.
	r29IntakeBound = 2
)

// r29IntakeFixture builds a scope whose process owns a real disttae
// workspace on a fixture engine, with the admission bound pinned so
// adoption is observable. The returned restore unpins the bound.
func r29IntakeFixture(t *testing.T) (*Scope, *disttae.Transaction, func()) {
	t.Helper()
	restore := disttae.PinUnpublishedPendingAdmissionLimitForTest(r29IntakeBound)
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	t.Cleanup(closeFn)
	// testutil.NewProcess AND NewTestTxnOperator both REPLACE the
	// default service runtime, so the fixture globals must be
	// registered after the last replacement (same order the r28
	// admission tests pinned).
	colexec.NewServer("")
	moruntime.ServiceRuntime("").SetGlobalVariables(
		moruntime.ClusterService, r11MockMOCluster{})
	eng := &disttae.Engine{}
	ws := disttae.NewTxnWorkSpace(eng, proc)
	txnOp.AddWorkspace(ws)
	ws.BindTxnOp(txnOp)
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx
	scope := &Scope{Proc: proc}
	return scope, ws, restore
}

// r29ProbeAdoption adopts one more distinct name and reports whether the
// bound still had room: after a fixture pre-fills every slot but one, a
// METADATA-mode intake that truly adopted consumes the last slot and this
// probe is refused; an ordinary-mode intake that adopted nothing leaves
// the slot and the probe succeeds.
func r29ProbeAdoption(t *testing.T, ws *disttae.Transaction, probe string) error {
	t.Helper()
	return ws.AdoptRemoteTransferredObjectNames([]string{probe})
}

func r29PrepareBeacon(t *testing.T, ws *disttae.Transaction, filler string) {
	t.Helper()
	require.NoError(t, ws.AdoptRemoteTransferredObjectNames([]string{filler}),
		"the beacon filler consumes the one remaining slot below the pinned bound")
}

// r29Stats validates and names one production-shaped ObjectStats.
func r29NewObjectStats(t *testing.T) (objectio.ObjectStats, string) {
	t.Helper()
	rowid := types.RandomRowid()
	oid := rowid.BorrowObjectID()
	stats := objectio.NewObjectStatsWithObjectID(oid, false, false, true)
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	return *stats, stats.ObjectName().String()
}

// r29StatsTailBatch marshals the writer's own result batch shape
// ([BlockMeta_BlockInfo, ObjectMeta_ObjectStats], one serialized
// ObjectStats row per persisted object) exactly as the recording side
// serializes an embedded metadata tail.
func r29StatsTailBytes(t *testing.T, mp *mpool.MPool, stats []objectio.ObjectStats) []byte {
	t.Helper()
	inner := colexec.AllocCNS3ResultBat(false)
	defer inner.Clean(mp)
	for i := range stats {
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			mp, false, inner, true, stats[i]))
	}
	inner.SetRowCount(inner.Vecs[1].Length())
	var buf bytes.Buffer
	data, err := inner.MarshalBinaryWithBuffer(&buf, true)
	require.NoError(t, err)
	return data
}

// r29RelayBatch mirrors the multi_update fragment writer's relay
// construction ([action, tableID, rowCount, name, serializedOrigin]).
func r29RelayBatch(
	t *testing.T, mp *mpool.MPool, objectCount int, malformed bool,
) (*batch.Batch, []string) {
	t.Helper()
	out := batch.NewWithSize(5)
	out.SetAttributes([]string{"action", "table_id", "row_count", "name", "data"})
	out.Vecs[0] = vector.NewVec(types.T_uint8.ToType())
	out.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
	out.Vecs[2] = vector.NewVec(types.T_uint64.ToType())
	out.Vecs[3] = vector.NewVec(types.T_varchar.ToType())
	out.Vecs[4] = vector.NewVec(types.T_text.ToType())
	names := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		stats, name := r29NewObjectStats(t)
		names = append(names, name)
		var tail []byte
		if malformed {
			tail = []byte("definitely not a marshaled metadata batch")
		} else {
			tail = r29StatsTailBytes(t, mp, []objectio.ObjectStats{stats})
		}
		require.NoError(t, vector.AppendFixed(out.Vecs[0], uint8(0), false, mp))
		require.NoError(t, vector.AppendFixed(out.Vecs[1], uint64(7), false, mp))
		require.NoError(t, vector.AppendFixed(out.Vecs[2], uint64(1), false, mp))
		require.NoError(t, vector.AppendBytes(out.Vecs[3], []byte("relay"), false, mp))
		require.NoError(t, vector.AppendBytes(out.Vecs[4], tail, false, mp))
	}
	out.SetRowCount(out.Vecs[0].Length())
	return out, names
}

// r29InsertBatch builds the standalone insert writer's own result batch;
// with malformed=true one row's stats value is too short to carry an
// ObjectStats (the fail-closed metadata case).
func r29InsertBatch(
	t *testing.T, mp *mpool.MPool, objectCount int, malformed bool,
) (*batch.Batch, []string) {
	t.Helper()
	bat := colexec.AllocCNS3ResultBat(false)
	names := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		if malformed {
			// One row in the writer's own layout whose stats value is
			// too short to carry an ObjectStats: both columns carry a
			// value so the recognizer's shape gates pass and the stats
			// row itself fails the proof.
			require.NoError(t, vector.AppendBytes(
				bat.Vecs[0], []byte("block-info"), false, mp))
			require.NoError(t, vector.AppendBytes(
				bat.Vecs[1], []byte("short"), false, mp))
			continue
		}
		stats, name := r29NewObjectStats(t)
		names = append(names, name)
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			mp, false, bat, true, stats))
	}
	bat.SetRowCount(bat.Vecs[1].Length())
	return bat, names
}

// r29DeleteBatch mirrors the deletion operator's remote batch
// ([BlockMeta_Delete_ID, BlockMeta_DeltaLoc, BlockMeta_Type,
// BlockMeta_Partition, BlockMeta_Deletes_Length]) with one
// FlushDeltaLoc row per flushed tombstone object.
func r29DeleteBatch(
	t *testing.T, mp *mpool.MPool, objectCount int, malformed bool,
) (*batch.Batch, []string) {
	t.Helper()
	out := batch.NewWithSize(5)
	out.SetAttributes([]string{
		catalog.BlockMeta_Delete_ID,
		catalog.BlockMeta_DeltaLoc,
		catalog.BlockMeta_Type,
		catalog.BlockMeta_Partition,
		catalog.BlockMeta_Deletes_Length,
	})
	out.Vecs[0] = vector.NewVec(types.T_text.ToType())
	out.Vecs[1] = vector.NewVec(types.T_text.ToType())
	out.Vecs[2] = vector.NewVec(types.T_int8.ToType())
	out.Vecs[3] = vector.NewVec(types.T_int32.ToType())
	out.Vecs[4] = vector.NewVec(types.T_int32.ToType())
	names := make([]string, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		stats, name := r29NewObjectStats(t)
		names = append(names, name)
		require.NoError(t, vector.AppendBytes(
			out.Vecs[0], []byte("segment"), false, mp))
		tail := r29StatsTailBytes(t, mp, []objectio.ObjectStats{stats})
		if malformed {
			tail = []byte("definitely not a marshaled stats batch")
		}
		require.NoError(t, vector.AppendBytes(out.Vecs[1], tail, false, mp))
		require.NoError(t, vector.AppendFixed(
			out.Vecs[2], int8(deletion.FlushDeltaLoc), false, mp))
		require.NoError(t, vector.AppendFixed(out.Vecs[3], int32(0), false, mp))
		require.NoError(t, vector.AppendFixed(out.Vecs[4], int32(0), false, mp))
	}
	out.SetRowCount(out.Vecs[0].Length())
	return out, names
}

// r29ProducerScope builds a scope whose RootOp is the producer operator
// itself (raw or wrapper) - the operator tree the coordinator sent.
func r29ProducerScope(s *Scope, root vm.Operator) *Scope {
	s.RootOp = root
	return s
}

// TestIssue25590DispatcherIntakeModeMatrixR29 drives the REAL dispatcher
// export (AdoptRemoteRunIntakeMetadata) with production-format metadata
// batches: a metadata-mode producer's rows adopt their witness names
// onto the coordinator workspace, and the very same families in ordinary
// modes - or their partition wrappers in ordinary modes, or any of them
// nested in PreScopes without a metadata edge - adopt nothing while
// passing the same rows through untouched.
func TestIssue25590DispatcherIntakeModeMatrixR29(t *testing.T) {
	// One fresh fixture per leg: every leg's probe consumes one of the
	// pinned bound's slots when the intake did NOT adopt (the probe
	// succeeds), so reuse across legs would exhaust the bound and turn
	// later ordinary legs red for the wrong reason.
	type leg struct {
		probe string
		why   string
		root  func() vm.Operator
		batch func(t *testing.T, mp *mpool.MPool) *batch.Batch
		adopt bool
		pre   func() []*Scope
	}
	run := func(l leg) {
		t.Helper()
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		mp := s.Proc.Mp()
		bat := l.batch(t, mp)
		defer bat.Clean(mp)
		r29PrepareBeacon(t, ws, "filler-"+l.probe)
		s.RootOp = l.root()
		if l.pre != nil {
			s.PreScopes = append(s.PreScopes, l.pre()...)
		}
		require.NoError(t, AdoptRemoteRunIntakeMetadata(s, bat),
			l.why+": the intake must not error on this stream")
		err := r29ProbeAdoption(t, ws, "probe-"+l.probe)
		if l.adopt {
			require.Error(t, err,
				l.why+": the intake adopted - the probe admission must be refused at the bound")
		} else {
			require.NoError(t, err,
				l.why+": nothing adopted - the probe admission still fits the pinned bound")
		}
	}
	relayOf := func(malformed bool) func(t *testing.T, mp *mpool.MPool) *batch.Batch {
		return func(t *testing.T, mp *mpool.MPool) *batch.Batch {
			bat, _ := r29RelayBatch(t, mp, 1, malformed)
			return bat
		}
	}
	insertOf := func(malformed bool) func(t *testing.T, mp *mpool.MPool) *batch.Batch {
		return func(t *testing.T, mp *mpool.MPool) *batch.Batch {
			bat, _ := r29InsertBatch(t, mp, 1, malformed)
			return bat
		}
	}
	deleteOf := func(malformed bool) func(t *testing.T, mp *mpool.MPool) *batch.Batch {
		return func(t *testing.T, mp *mpool.MPool) *batch.Batch {
			bat, _ := r29DeleteBatch(t, mp, 1, malformed)
			return bat
		}
	}

	legs := []leg{
		{
			probe: "mu-s3", adopt: true,
			why: "an UpdateWriteS3 writer's relay rows adopt",
			root: func() vm.Operator {
				mu := multi_update.NewArgument()
				mu.Action = multi_update.UpdateWriteS3
				return mu
			},
			batch: relayOf(false),
		},
		{
			probe: "mu-table", adopt: false,
			why:   "an UpdateWriteTable stream's rows are ordinary data",
			root:  func() vm.Operator { return multi_update.NewArgument() },
			batch: relayOf(false),
		},
		{
			probe: "mu-wrap-s3", adopt: true,
			why: "a partition wrapper around an UpdateWriteS3 writer adopts",
			root: func() vm.Operator {
				raw := multi_update.NewArgument()
				raw.Action = multi_update.UpdateWriteS3
				return multi_update.NewPartitionMultiUpdate(raw)
			},
			batch: relayOf(false),
		},
		{
			probe: "mu-wrap-table", adopt: false,
			why: "a partition wrapper around an UpdateWriteTable writer adopts nothing",
			root: func() vm.Operator {
				return multi_update.NewPartitionMultiUpdate(multi_update.NewArgument())
			},
			batch: relayOf(false),
		},
		{
			probe: "ins-s3", adopt: true,
			why: "an S3 insert's result rows adopt",
			root: func() vm.Operator {
				ins := insert.NewArgument()
				ins.ToWriteS3 = true
				return ins
			},
			batch: insertOf(false),
		},
		{
			probe: "ins-ordinary", adopt: false,
			why:   "a workspace insert's result-shaped rows are ordinary data",
			root:  func() vm.Operator { return insert.NewArgument() },
			batch: insertOf(false),
		},
		{
			probe: "ins-ext", adopt: false,
			why: "an external-table writer's rows are ordinary data",
			root: func() vm.Operator {
				ins := insert.NewArgument()
				ins.ToWriteS3 = true
				ins.ToExternal = true
				return ins
			},
			batch: insertOf(false),
		},
		{
			probe: "ins-wrap", adopt: true,
			why: "a partition wrapper around an S3 insert adopts",
			root: func() vm.Operator {
				raw := insert.NewArgument()
				raw.ToWriteS3 = true
				return insert.NewPartitionInsert(raw, 7)
			},
			batch: insertOf(false),
		},
		{
			probe: "del-remote", adopt: true,
			why: "a remote delete's tombstone rows adopt",
			root: func() vm.Operator {
				del := deletion.NewArgument()
				del.RemoteDelete = true
				return del
			},
			batch: deleteOf(false),
		},
		{
			probe: "del-local", adopt: false,
			why:   "a local delete's remote-shaped rows are ordinary data",
			root:  func() vm.Operator { return deletion.NewArgument() },
			batch: deleteOf(false),
		},
		{
			probe: "del-wrap", adopt: true,
			why: "a partition wrapper around a remote delete adopts",
			root: func() vm.Operator {
				raw := deletion.NewArgument()
				raw.RemoteDelete = true
				return deletion.NewPartitionDelete(raw, 7)
			},
			batch: deleteOf(false),
		},
		{
			probe: "nested", adopt: true,
			why: "a metadata-mode producer nested in PreScopes keeps the fragment gated",
			root: func() vm.Operator {
				outer := connector.NewArgument()
				outer.GetOperatorBase().AppendChild(value_scan.NewArgument())
				return outer
			},
			pre: func() []*Scope {
				mu := multi_update.NewArgument()
				mu.Action = multi_update.UpdateWriteS3
				return []*Scope{{RootOp: mu}}
			},
			batch: relayOf(false),
		},
		{
			probe: "ordinary-tree", adopt: false,
			why: "the same three families in ordinary modes, nested at that, still never adopt",
			root: func() vm.Operator {
				outer := connector.NewArgument()
				outer.GetOperatorBase().AppendChild(value_scan.NewArgument())
				return outer
			},
			pre: func() []*Scope {
				return []*Scope{
					{RootOp: multi_update.NewArgument()},
					{RootOp: insert.NewArgument()},
					{RootOp: deletion.NewArgument()},
				}
			},
			batch: relayOf(false),
		},
	}

	for _, l := range legs {
		t.Run(l.probe+" - "+l.why, func(t *testing.T) {
			run(l)
		})
	}
}

// TestIssue25590DispatcherFailClosedMetadataModesR29 pins the r7 D3
// fail-closed rule THROUGH the dispatcher: a malformed metadata tail
// errors a metadata-mode stream, while the very same malformed batch is
// byte-silent through every ordinary mode - the exact harm the
// round-24 F2 finding documented (invalid tails rejecting legitimate
// data).
func TestIssue25590DispatcherFailClosedMetadataModesR29(t *testing.T) {
	metadataModes := map[string]vm.Operator{
		"multi_update.UpdateWriteS3": func() vm.Operator {
			mu := multi_update.NewArgument()
			mu.Action = multi_update.UpdateWriteS3
			return mu
		}(),
		"insert.ToWriteS3": func() vm.Operator {
			ins := insert.NewArgument()
			ins.ToWriteS3 = true
			return ins
		}(),
		"deletion.RemoteDelete": func() vm.Operator {
			del := deletion.NewArgument()
			del.RemoteDelete = true
			return del
		}(),
	}
	// Through a metadata-mode gate each family's OWN malformed metadata
	// must error: a relay tail that cannot decode (multi_update), an
	// ObjectStats value too short to be one (insert), and a tombstone
	// DeltaLoc that cannot decode (deletion).
	malformedOfFamily := map[string]func(t *testing.T, mp *mpool.MPool) *batch.Batch{
		"multi_update.UpdateWriteS3": func(t *testing.T, mp *mpool.MPool) *batch.Batch {
			bat, _ := r29RelayBatch(t, mp, 1, true)
			return bat
		},
		"insert.ToWriteS3": func(t *testing.T, mp *mpool.MPool) *batch.Batch {
			bat, _ := r29InsertBatch(t, mp, 1, true)
			return bat
		},
		"deletion.RemoteDelete": func(t *testing.T, mp *mpool.MPool) *batch.Batch {
			bat, _ := r29DeleteBatch(t, mp, 1, true)
			return bat
		},
	}
	// The ordinary grounds: the same families' OWN malformed batch
	// through their ordinary-mode operators never reaches a recognizer -
	// no error, no interpretation.
	ordinaryOfFamily := map[string]struct {
		root  vm.Operator
		batch func(t *testing.T, mp *mpool.MPool) *batch.Batch
	}{
		"multi_update.UpdateWriteTable": {
			root: multi_update.NewArgument(),
			batch: func(t *testing.T, mp *mpool.MPool) *batch.Batch {
				bat, _ := r29RelayBatch(t, mp, 1, true)
				return bat
			},
		},
		"insert workspace": {
			root: insert.NewArgument(),
			batch: func(t *testing.T, mp *mpool.MPool) *batch.Batch {
				bat, _ := r29InsertBatch(t, mp, 1, true)
				return bat
			},
		},
		"insert external": {
			root: func() vm.Operator {
				ins := insert.NewArgument()
				ins.ToWriteS3 = true
				ins.ToExternal = true
				return ins
			}(),
			batch: func(t *testing.T, mp *mpool.MPool) *batch.Batch {
				bat, _ := r29InsertBatch(t, mp, 1, true)
				return bat
			},
		},
		"deletion local": {
			root: deletion.NewArgument(),
			batch: func(t *testing.T, mp *mpool.MPool) *batch.Batch {
				bat, _ := r29DeleteBatch(t, mp, 1, true)
				return bat
			},
		},
	}

	for name, root := range metadataModes {
		malformed := malformedOfFamily[name]
		t.Run(name+"/malformed-errors", func(t *testing.T) {
			s, _, restore := r29IntakeFixture(t)
			defer restore()
			bat := malformed(t, s.Proc.Mp())
			defer bat.Clean(s.Proc.Mp())
			err := AdoptRemoteRunIntakeMetadata(r29ProducerScope(s, root), bat)
			require.Error(t, err,
				"a metadata-mode stream that cannot prove its tail rejects fail-closed")
		})
	}
	for name, leg := range ordinaryOfFamily {
		root, malformed := leg.root, leg.batch
		t.Run(name+"/malformed-silent", func(t *testing.T) {
			s, _, restore := r29IntakeFixture(t)
			defer restore()
			bat := malformed(t, s.Proc.Mp())
			defer bat.Clean(s.Proc.Mp())
			require.NoError(t, AdoptRemoteRunIntakeMetadata(r29ProducerScope(s, root), bat),
				"an ordinary-mode stream's rows are ordinary data: no recognizer may reject them")
		})
	}
}

// r29AckRecorder records the ACK sequences the stub stream sent.
type r29AckRecorder struct {
	mu   sync.Mutex
	acks []uint64
}

func (r *r29AckRecorder) record(seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks = append(r.acks, seq)
}

func (r *r29AckRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.acks)
}

// r29StubSender assembles an in-package client-side sender whose wire
// transport is a mock morpc stream (ACKs are recorded) and whose
// receive channel is preloaded with the given messages - the same
// receive loops the production client runs. An optional onAck callback
// runs inside the ACK path itself, at the exact boundary, so a test can
// observe the adoption state the sender sees when its ACK is sent.
func r29StubSender(
	t *testing.T,
	proc *process.Process,
	messages []morpc.Message,
	onAck ...func(seq uint64),
) (*messageSenderOnClient, *r29AckRecorder) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	stream := mock_morpc.NewMockStream(ctrl)
	recorder := &r29AckRecorder{}
	var ackHook func(uint64)
	if len(onAck) > 0 {
		ackHook = onAck[0]
	}
	stream.EXPECT().ID().Return(uint64(31)).AnyTimes()
	stream.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request morpc.Message) error {
			if m, ok := request.(*pipeline.Message); ok {
				recorder.record(m.GetBatchAckSequence())
				if ackHook != nil {
					ackHook(m.GetBatchAckSequence())
				}
			}
			return nil
		}).AnyTimes()
	recvCh := make(chan morpc.Message, len(messages)+1)
	for _, m := range messages {
		recvCh <- m
	}
	close(recvCh)
	return &messageSenderOnClient{
		ctx:          context.Background(),
		mp:           proc.Mp(),
		streamSender: stream,
		receiveCh:    recvCh,
		safeToClose:  true,
	}, recorder
}

// r29WireBatch encodes a batch exactly as the wire does.
func r29WireBatch(t *testing.T, bat *batch.Batch) *pipeline.Message {
	t.Helper()
	var buf bytes.Buffer
	data, err := bat.MarshalBinaryForPipeline(&buf, true, true)
	require.NoError(t, err)
	return &pipeline.Message{Data: data, BatchSequence: 1}
}

func r29EndMessage() *pipeline.Message {
	return &pipeline.Message{Sid: pipeline.Status_MessageEnd}
}

// TestIssue25590ReceiveLoopsAdoptBeforeForwardAndAckR29 runs the three
// production receive loops against a stub wire:
//
//   - ONLY-RUN loop: a metadata-mode stream adopts its relayed names and
//     then ACKs; an intake error (a saturated ledger refusing the
//     adoption) fails the loop BEFORE the ACK; an ordinary-mode stream
//     ACKs the same malformed tail without error;
//   - CONNECTOR loop: adoption precedes forwarding (the edge receives
//     the batch after the intake adopted), an intake error leaves the
//     edge empty and sends no ACK, and a stopped receiver still ACKs
//     AFTER adoption;
//   - DISPATCH loop: adoption precedes the re-emission into local regs.
func TestIssue25590ReceiveLoopsAdoptBeforeForwardAndAckR29(t *testing.T) {
	t.Run("only-run loop", func(t *testing.T) {
		// Metadata mode, valid tail: adopted then ACKed.
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		relay, _ := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		r29PrepareBeacon(t, ws, "filler-run-loop")
		muS3 := multi_update.NewArgument()
		muS3.Action = multi_update.UpdateWriteS3
		s.RootOp = muS3
		sender, acks := r29StubSender(t, s.Proc, []morpc.Message{
			r29WireBatch(t, relay), r29EndMessage(),
		})
		require.NoError(t, receiveMessageFromCnServerIfOnlyRun(s, sender))
		require.Equal(t, 1, acks.count(),
			"the loop ACKs the relay batch after adopting it")
		require.Error(t, r29ProbeAdoption(t, ws, "probe-run-loop"),
			"adoption really happened on the loop's own workspace")

		// Metadata mode, but the coordinator ledger is saturated: the
		// intake refuses BEFORE the ACK.
		s2, ws2, restore2 := r29IntakeFixture(t)
		defer restore2()
		r29PrepareBeacon(t, ws2, "filler-saturated-one")
		require.NoError(t, ws2.AdoptRemoteTransferredObjectNames([]string{"filler-saturated-two"}),
			"the second filler consumes the last slot under the pinned bound")
		relaySat, _ := r29RelayBatch(t, s2.Proc.Mp(), 1, false)
		muS3sat := multi_update.NewArgument()
		muS3sat.Action = multi_update.UpdateWriteS3
		s2.RootOp = muS3sat
		sender2, acks2 := r29StubSender(t, s2.Proc, []morpc.Message{
			r29WireBatch(t, relaySat), r29EndMessage(),
		})
		err := receiveMessageFromCnServerIfOnlyRun(s2, sender2)
		require.Error(t, err,
			"a refused adoption fails the receive loop")
		require.Zero(t, acks2.count(),
			"no ACK is sent for a batch the intake never adopted")

		// Ordinary mode, malformed tail: byte-silent through the loop.
		s3, _, restore3 := r29IntakeFixture(t)
		defer restore3()
		relayBad, _ := r29RelayBatch(t, s3.Proc.Mp(), 1, true)
		s3.RootOp = multi_update.NewArgument()
		sender3, acks3 := r29StubSender(t, s3.Proc, []morpc.Message{
			r29WireBatch(t, relayBad), r29EndMessage(),
		})
		require.NoError(t, receiveMessageFromCnServerIfOnlyRun(s3, sender3),
			"an ordinary-mode stream's rows pass the only-run loop untouched")
		require.Equal(t, 1, acks3.count(),
			"the ordinary stream's malformed-shaped batch is ACKed without interpretation")
	})

	t.Run("connector loop", func(t *testing.T) {
		// Loop input shape: RootOp is the CONNECTOR the loop asserts,
		// and the metadata-mode producer sits in the scope's PreScopes
		// - the nested placement the registry's walk covers.
		newConnectorScope := func(s *Scope, edge *process.PipelineEdge, metadataMode bool) {
			mu := multi_update.NewArgument()
			if metadataMode {
				mu.Action = multi_update.UpdateWriteS3
			}
			cn := connector.NewArgument()
			cn.Reg = edge
			s.RootOp = cn
			s.PreScopes = append(s.PreScopes, &Scope{RootOp: mu})
		}

		// The intake error precedes forwarding AND the ACK: a malformed
		// tail through a metadata-mode scope fails the run before any
		// local consumer sees the batch.
		s, _, restore := r29IntakeFixture(t)
		defer restore()
		relayBad, _ := r29RelayBatch(t, s.Proc.Mp(), 1, true)
		edge := process.NewPipelineEdge(2, 1)
		newConnectorScope(s, edge, true)
		sender, acks := r29StubSender(t, s.Proc, []morpc.Message{
			r29WireBatch(t, relayBad), r29EndMessage(),
		})
		err := receiveMessageFromCnServerIfConnector(s, sender)
		require.Error(t, err, "the malformed tail fails the connector loop")
		require.Zero(t, acks.count(), "no ACK on a refused intake")
		select {
		case <-edge.Ch2:
			t.Fatal("no batch may be forwarded when the intake refused it")
		default:
		}

		// A metadata-mode stream saturated at the admission ledger:
		// the very same ordering without any decode involvement.
		sSat, wsSat, restoreSat := r29IntakeFixture(t)
		defer restoreSat()
		r29PrepareBeacon(t, wsSat, "filler-conn-sat-one")
		require.NoError(t, wsSat.AdoptRemoteTransferredObjectNames([]string{"filler-conn-sat-two"}))
		relaySat, _ := r29RelayBatch(t, sSat.Proc.Mp(), 1, false)
		edgeSat := process.NewPipelineEdge(2, 1)
		newConnectorScope(sSat, edgeSat, true)
		senderSat, acksSat := r29StubSender(t, sSat.Proc, []morpc.Message{
			r29WireBatch(t, relaySat), r29EndMessage(),
		})
		require.Error(t, receiveMessageFromCnServerIfConnector(sSat, senderSat),
			"a saturated adoption fails the connector loop before the ACK")
		require.Zero(t, acksSat.count(),
			"a refused admission is not ACKed")
		select {
		case <-edgeSat.Ch2:
			t.Fatal("a refused admission must not forward its batch")
		default:
		}

		// Valid tail: adoption happens, the batch is FORWARDed, then ACKed.
		s2, ws2, restore2 := r29IntakeFixture(t)
		defer restore2()
		relay, _ := r29RelayBatch(t, s2.Proc.Mp(), 1, false)
		r29PrepareBeacon(t, ws2, "filler-conn")
		edge2 := process.NewPipelineEdge(2, 1)
		newConnectorScope(s2, edge2, true)
		sender2, acks2 := r29StubSender(t, s2.Proc, []morpc.Message{
			r29WireBatch(t, relay), r29EndMessage(),
		})
		require.NoError(t, receiveMessageFromCnServerIfConnector(s2, sender2))
		require.Equal(t, 1, acks2.count(), "the adopted batch is ACKed")
		select {
		case sig, ok := <-edge2.Ch2:
			require.True(t, ok, "the forwarded signal must be delivered")
			forwarded, actionErr := sig.Action()
			require.NoError(t, actionErr)
			require.NotNil(t, forwarded, "the adopted batch is forwarded, not swallowed")
			require.Equal(t, relay.Vecs[0].Length(), forwarded.Vecs[0].Length(),
				"the forwarded stream carries the relay batch's rows unchanged")
		default:
			t.Fatal("the adopted batch must be forwarded after adoption")
		}
		require.Error(t, r29ProbeAdoption(t, ws2, "probe-connector"),
			"adoption preceded the forward and the ACK")

		// A STOPPED receiver still ACKs after adoption: the names the
		// fragment persisted must own a reachable cleanup owner on the
		// recording side even when the local consumer is gone.
		s3, ws3, restore3 := r29IntakeFixture(t)
		defer restore3()
		relayStopped, _ := r29RelayBatch(t, s3.Proc.Mp(), 1, false)
		r29PrepareBeacon(t, ws3, "filler-stopped")
		edge3 := process.NewPipelineEdge(2, 1)
		edge3.Abort(moerr.NewInternalErrorNoCtx("receiver stopped"))
		newConnectorScope(s3, edge3, true)
		sender3, acks3 := r29StubSender(t, s3.Proc, []morpc.Message{
			r29WireBatch(t, relayStopped), r29EndMessage(),
		})
		require.NoError(t, receiveMessageFromCnServerIfConnector(s3, sender3),
			"a stopped receiver ends the loop cleanly")
		require.Equal(t, 1, acks3.count(),
			"a stopped receiver still ACKs so the sender's credit is released")
		require.Error(t, r29ProbeAdoption(t, ws3, "probe-stopped"),
			"adoption happened even though the local receiver had stopped")

		// An ordinary-mode connector stream ACKs a malformed-shaped tail
		// without interpretation - the rows were never metadata.
		s4, _, restore4 := r29IntakeFixture(t)
		defer restore4()
		relayOrd, _ := r29RelayBatch(t, s4.Proc.Mp(), 1, true)
		edge4 := process.NewPipelineEdge(2, 1)
		newConnectorScope(s4, edge4, false)
		sender4, acks4 := r29StubSender(t, s4.Proc, []morpc.Message{
			r29WireBatch(t, relayOrd), r29EndMessage(),
		})
		require.NoError(t, receiveMessageFromCnServerIfConnector(s4, sender4),
			"an ordinary-mode stream's rows pass the connector loop untouched")
		require.Equal(t, 1, acks4.count(),
			"the ordinary stream's malformed-shaped batch is ACKed without interpretation")
	})

	t.Run("dispatch loop", func(t *testing.T) {
		// r31 review finding 2: the dispatch legs pin adoption to BOTH
		// released boundaries. The ACK stub's in-path callback probes
		// the intake's consumed state INSIDE the ACK path (a credit-
		// releasing ACK may only follow a consumed intake), and a
		// watcher goroutine holding the forwarded signal probes the
		// same state in the forwarding window. The ordering's
		// deterministic discriminator is the malformed-tail leg
		// below: under the review's exact reordering (adoption moved
		// after vm.Exec but before bat.Clean) the malformed tail is
		// already re-emitted into the edge when the intake refuses
		// the run, and that leg fails at its no-re-emission assertion
		// - the recorded red run of the reordering fails exactly there.
		newDispatchScope := func(s *Scope, edge *process.PipelineEdge, metadataMode bool) {
			mu := multi_update.NewArgument()
			if metadataMode {
				mu.Action = multi_update.UpdateWriteS3
			}
			arg := dispatch.NewArgument()
			arg.LocalRegs = []*process.WaitRegister{edge}
			// The iota block in dispatch/types.go assigns
			// SendToAllLocalFunc = 1 (maxMessageSizeToMoRpc holds iota 0),
			// and the zero FuncId falls to the default case - set the
			// local fan-out explicitly.
			arg.FuncId = dispatch.SendToAllLocalFunc
			s.RootOp = arg
			s.PreScopes = append(s.PreScopes, &Scope{RootOp: mu})
		}

		// Success leg: adoption precedes the forward and the ACK. The
		// intake fixture pins the admission bound at 2 and the beacon
		// filler consumes one slot, so a probe adoption REFUSES exactly
		// while the intake has adopted - the boundary observers decide
		// consumed-vs-not from inside the boundary callbacks.
		s, ws, restore := r29IntakeFixture(t)
		defer restore()
		r29PrepareBeacon(t, ws, "filler-dispatch")
		relay, _ := r29RelayBatch(t, s.Proc.Mp(), 1, false)
		edge := process.NewPipelineEdge(2, 1)
		newDispatchScope(s, edge, true)

		forwardAdopted := make(chan bool, 1)
		go func() {
			_, ok := <-edge.Ch2
			// The forwarded signal is the forwarding boundary: adoption
			// must already be consumed here for the ordering to hold.
			forwardAdopted <- !ok || (r29ProbeAdoption(t, ws, "probe-dispatch-forward") != nil)
		}()
		ackAdopted := false
		sender, acks := r29StubSender(t, s.Proc, []morpc.Message{
			r29WireBatch(t, relay), r29EndMessage(),
		}, func(seq uint64) {
			// Inside the ACK path: the adoption must be consumed
			// before this credit-releasing ACK is sent.
			ackAdopted = r29ProbeAdoption(t, ws, "probe-dispatch-ack") != nil
		})
		require.NoError(t, receiveMessageFromCnServerIfDispatch(s, sender))
		require.Equal(t, 1, acks.count(),
			"the dispatch loop ACKs the batch it adopted before re-emitting it")
		require.True(t, <-forwardAdopted,
			"adoption was already consumed at the FORWARDING boundary - the re-emission happens after the intake")
		require.True(t, ackAdopted,
			"adoption was already consumed at the ACK boundary - the credit-release ACK follows the intake")
		require.Error(t, r29ProbeAdoption(t, ws, "probe-dispatch-final"),
			"the loop's own workspace really adopted the relay batch")

		// Malformed-tail leg: the intake fails the run BEFORE any
		// re-emission and BEFORE any ACK.
		s2, _, restore2 := r29IntakeFixture(t)
		defer restore2()
		relayBad, _ := r29RelayBatch(t, s2.Proc.Mp(), 1, true)
		edge2 := process.NewPipelineEdge(2, 1)
		newDispatchScope(s2, edge2, true)
		sender2, acks2 := r29StubSender(t, s2.Proc, []morpc.Message{
			r29WireBatch(t, relayBad), r29EndMessage(),
		})
		require.Error(t, receiveMessageFromCnServerIfDispatch(s2, sender2),
			"a malformed tail fails the dispatch loop at the intake")
		require.Zero(t, acks2.count(), "no ACK for a refused intake")
		select {
		case <-edge2.Ch2:
			t.Fatal("no batch may be re-emitted when the intake refused it")
		default:
		}

		// Saturated-ledger leg: the same ordering through the admission
		// refusal, decode-independent.
		s3, ws3, restore3 := r29IntakeFixture(t)
		defer restore3()
		r29PrepareBeacon(t, ws3, "filler-dispatch-sat-one")
		require.NoError(t, ws3.AdoptRemoteTransferredObjectNames([]string{"filler-dispatch-sat-two"}))
		relaySat, _ := r29RelayBatch(t, s3.Proc.Mp(), 1, false)
		edge3 := process.NewPipelineEdge(2, 1)
		newDispatchScope(s3, edge3, true)
		sender3, acks3 := r29StubSender(t, s3.Proc, []morpc.Message{
			r29WireBatch(t, relaySat), r29EndMessage(),
		})
		require.Error(t, receiveMessageFromCnServerIfDispatch(s3, sender3),
			"a saturated ledger refuses the dispatch intake before the re-emission")
		require.Zero(t, acks3.count(), "a refused admission is not ACKed")
		select {
		case <-edge3.Ch2:
			t.Fatal("no batch may be re-emitted when the adoption was refused")
		default:
		}

		// Stopped-receiver leg: the local consumer already stopped, but
		// the adoption still happened BEFORE the dispatch re-emission,
		// and the loop still ACKs so the fragment's credit releases.
		s4, ws4, restore4 := r29IntakeFixture(t)
		defer restore4()
		r29PrepareBeacon(t, ws4, "filler-dispatch-stopped")
		relayStopped, _ := r29RelayBatch(t, s4.Proc.Mp(), 1, false)
		edge4 := process.NewPipelineEdge(2, 1)
		edge4.Abort(moerr.NewInternalErrorNoCtx("receiver stopped"))
		newDispatchScope(s4, edge4, true)
		sender4, acks4 := r29StubSender(t, s4.Proc, []morpc.Message{
			r29WireBatch(t, relayStopped), r29EndMessage(),
		})
		require.NoError(t, receiveMessageFromCnServerIfDispatch(s4, sender4),
			"a stopped receiver ends the dispatch loop cleanly")
		require.Equal(t, 1, acks4.count(),
			"a stopped receiver still ACKs so the sender's credit is released")
		require.Error(t, r29ProbeAdoption(t, ws4, "probe-dispatch-stopped"),
			"adoption happened even though the local receiver had stopped")
	})
}

// r29SettleFixture builds the server-side settle inputs: a runCompile
// whose process owns a real workspace on an ENGINE FIXTURE ATTACHED TO
// THE WITNESS FILE SERVICE (disttae.NewEngineForTest, the round-31
// review's finding-3 requirement), a receiver with the given request
// advertisement, and a batch flow. The fixture returns the attached
// engine so the settle legs can assert the exact queue handoff and the
// physical retirement through the engine's own bounded drain.
func r29SettleFixture(
	t *testing.T,
	fs fileservice.FileService,
	witness string,
) (*messageReceiverOnServer, *Compile, *disttae.Transaction, *disttae.Engine, *pipelineBatchFlow) {
	t.Helper()
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	txnOp, closeFn := client.NewTestTxnOperator(ctx)
	t.Cleanup(closeFn)
	// Fixture globals register after the LAST runtime replacement (the
	// r28 order: NewProcess and NewTestTxnOperator both replace the
	// default runtime).
	colexec.NewServer("")
	moruntime.ServiceRuntime("").SetGlobalVariables(
		moruntime.ClusterService, r11MockMOCluster{})
	eng := disttae.NewEngineForTest(fs)
	fragmentWs := disttae.NewTxnWorkSpace(eng, proc)
	txnOp.AddWorkspace(fragmentWs)
	fragmentWs.BindTxnOp(txnOp)
	proc.Base.TxnOperator = txnOp
	proc.Ctx = ctx

	// The fragment retained one persisted witness name.
	fragmentWs.RetainUnrecordedS3Objects([]string{witness},
		"r31 settle fixture: the fragment's persisted object")

	gotPending, gotReserved := r29UnsettledNames(t, fragmentWs)
	require.Equal(t, []string{witness}, gotPending,
		"pre-settle state: the fragment transaction owns the witness name")
	require.Empty(t, gotReserved, "pre-settle state: no outstanding reservations")
	queued, live, inFlight := eng.InspectUnpublishedPendingForTest()
	require.Empty(t, queued, "pre-settle state: the engine cleanup queue starts empty")
	require.Equal(t, 1, live, "pre-settle state: the retain holds the sole owned membership")
	require.Zero(t, inFlight, "pre-settle state: no in-flight reservation")

	mu := multi_update.NewArgument()
	mu.Action = multi_update.UpdateWriteS3
	write := &Scope{}
	write.RootOp = mu
	runCompile := &Compile{proc: proc}
	runCompile.scopes = []*Scope{write}

	flow := newPipelineBatchFlow(pipelineBatchCreditCount, pipelineBatchCreditBytes)
	receiver := &messageReceiverOnServer{
		coordinatorProtocolVersion: defines.MORPCVersion132,
		messageCtx:                 ctx,
		connectionCtx:              ctx,
		streamLifecycle:            &pipelineStreamLifecycle{batchFlow: flow},
	}
	return receiver, runCompile, fragmentWs, eng, flow
}

func r29UnsettledNames(t *testing.T, txn *disttae.Transaction) (pending []string, reserved []string) {
	t.Helper()
	return txn.UnsettledCleanupOwnershipForTest()
}

// r29RequireWitnessKept asserts the witness file is still physically
// present on the fixture's file service.
func r29RequireWitnessKept(t *testing.T, fs fileservice.FileService, ctx context.Context, witness, why string) {
	t.Helper()
	_, statErr := fs.StatFile(ctx, witness)
	require.NoError(t, statErr, why)
}

// r29RequireWitnessDeleted asserts the witness file is physically gone.
func r29RequireWitnessDeleted(t *testing.T, fs fileservice.FileService, ctx context.Context, witness, why string) {
	t.Helper()
	_, statErr := fs.StatFile(ctx, witness)
	require.Error(t, statErr, why)
}

// TestIssue25590SettlementTransferAndKeepOwnershipR31 runs
// settleRemoteFragmentWorkspaceAfterDrain through real fixtures for
// every terminal class (design r11 D3/D5, round-31 review finding 3):
// the capable-peer clean drain TRANSFERS, and every keep class
// (below-advertisement, cancelled run, receiver-stopped flow, drain
// failure) hands the retained name to the fragment engine's cleanup
// queue. Each leg asserts the EXACT ownership trajectory - the
// transaction's ownership clears, the correct name reaches engine
// cleanup (the queue holds exactly it, and ONLY for the keep classes),
// the bounded drain physically deletes the kept witness from the
// fixture's real file service with balanced accounting, and the
// transferred witness is physically retained with an empty queue.
func TestIssue25590SettlementTransferAndKeepOwnershipR31(t *testing.T) {
	ctx := context.Background()

	witness := "settle-witness-0001"
	fsFor := func(t *testing.T) fileservice.FileService {
		t.Helper()
		fs, err := fileservice.NewMemoryFS("issue25590-r31", fileservice.DisabledCacheConfig, nil)
		require.NoError(t, err)
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{
			FilePath: witness,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("w")}},
		}))
		return fs
	}

	// Transfer leg: capable peer, clean drain - transferred=true.
	t.Run("capable peer transfers", func(t *testing.T) {
		fs := fsFor(t)
		receiver, runCompile, fragmentWs, eng, _ := r29SettleFixture(t, fs, witness)
		runErr := error(nil)
		require.NoError(t, settleRemoteFragmentWorkspaceAfterDrain(receiver, runCompile, &runErr))
		require.NoError(t, runErr, "the clean, drained run settled without error")

		gotPending, gotReserved := r29UnsettledNames(t, fragmentWs)
		require.Empty(t, gotPending,
			"the transferred terminal clears the transaction's cleanup ownership")
		require.Empty(t, gotReserved,
			"the transferred terminal leaves no outstanding reservation")
		queued, live, inFlight := eng.InspectUnpublishedPendingForTest()
		require.Empty(t, queued,
			"the transferred name never enters the FRAGMENT engine's cleanup queue - the coordinator adopted it")
		require.Zero(t, live, "the released membership retired exactly")
		require.Zero(t, inFlight, "no reservation was left in flight")
		r29RequireWitnessKept(t, fs, ctx, witness,
			"the transferred witness file is physically retained after the settle - its cleanup owner is the recording side")
	})

	// All four keep classes share the same exact ownership trajectory.
	keepLeg := func(t *testing.T, name string, mutate func(*messageReceiverOnServer, *pipelineBatchFlow) func(*error)) {
		t.Helper()
		fs := fsFor(t)
		receiver, runCompile, fragmentWs, eng, flow := r29SettleFixture(t, fs, witness)
		setRunErr := mutate(receiver, flow)
		runErr := error(nil)
		setRunErr(&runErr)

		require.NoError(t, settleRemoteFragmentWorkspaceAfterDrain(receiver, runCompile, &runErr))

		gotPending, gotReserved := r29UnsettledNames(t, fragmentWs)
		require.Empty(t, gotPending,
			name+": the keep terminal hands the name out of the transaction - the ownership clears, it is not stranded")
		require.Empty(t, gotReserved,
			name+": the keep terminal leaves no outstanding reservation on the transaction")
		queued, live, inFlight := eng.InspectUnpublishedPendingForTest()
		require.Equal(t, []string{witness}, queued,
			name+": exactly the retained witness name reached engine cleanup")
		require.Zero(t, live, name+": the owned-ledger membership retired at the handoff")
		require.Zero(t, inFlight, name+": no capacity stayed held in flight")

		// The bounded drain retire the kept witness physically, with
		// balanced accounting and an exactly-empty queue.
		r29RequireWitnessKept(t, fs, ctx, witness,
			name+": the file is still present before the engine drain - the queue ticket, not the queue itself, owns it")
		require.NoError(t, eng.DrainUnpublishedPendingForTest(ctx),
			name+": the bounded drain deletes the queued witness")
		r29RequireWitnessDeleted(t, fs, ctx, witness,
			name+": after the drain the kept witness file is physically deleted")
		queued, live, inFlight = eng.InspectUnpublishedPendingForTest()
		require.Empty(t, queued, name+": the drain retired the queue exactly")
		require.Zero(t, live, name+": the ledger is balanced after the drain")
		require.Zero(t, inFlight, name+": the drain moved no capacity")
	}

	t.Run("below-advertisement keeps", func(t *testing.T) {
		keepLeg(t, "below-advertisement", func(receiver *messageReceiverOnServer, flow *pipelineBatchFlow) func(*error) {
			receiver.coordinatorProtocolVersion = defines.MORPCVersion113
			return func(*error) {}
		})
	})

	t.Run("cancelled run keeps", func(t *testing.T) {
		keepLeg(t, "cancelled run", func(receiver *messageReceiverOnServer, flow *pipelineBatchFlow) func(*error) {
			return func(runErr *error) {
				*runErr = moerr.NewInternalErrorNoCtx("the fragment run failed")
			}
		})
	})

	t.Run("receiver-stopped flow keeps", func(t *testing.T) {
		keepLeg(t, "receiver-stopped flow", func(receiver *messageReceiverOnServer, flow *pipelineBatchFlow) func(*error) {
			flow.stop(moerr.NewInternalErrorNoCtx("receiver-side stop"))
			return func(*error) {}
		})
	})

	t.Run("drain failure keeps", func(t *testing.T) {
		keepLeg(t, "drain failure", func(receiver *messageReceiverOnServer, flow *pipelineBatchFlow) func(*error) {
			// One outstanding reserved batch whose request context is
			// already cancelled: the drain can only fail.
			reserveCtx, reserveCancel := context.WithCancel(ctx)
			_, err := flow.reserve(reserveCtx, context.Background(), 1)
			require.NoError(t, err)
			reserveCancel()
			messageCtx, messageCancel := context.WithCancel(ctx)
			receiver.messageCtx = messageCtx
			messageCancel()
			return func(*error) {}
		})
	})
}
