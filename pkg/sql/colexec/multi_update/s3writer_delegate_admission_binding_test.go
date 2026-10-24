// Copyright 2025 Matrix Origin
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

package multi_update

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// issue25590WorkspaceStub is a client.Workspace (through the embedded
// generated mock) that also carries the structural surfaces the
// round-20 producer binding of issue #25590 asserts on: the
// SinkerOwner option binds the per-object capacity admission into every
// writer the delegate constructs, and the
// UnrecordedSinkerObjectsRetainer records every unwind transfer the
// delegate's Reset/Free performs.
type issue25590WorkspaceStub struct {
	*mock_frontend.MockWorkspace

	mu         sync.Mutex
	refuse     func(string) error
	hookCalls  int
	names      []string
	hasRetains bool
}

func (s *issue25590WorkspaceStub) SinkerCapacityOption() ioutil.SinkerOption {
	return ioutil.WithUncommittedCapacityHook(
		ioutil.UncommittedCapacityHook(func(name string) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.hookCalls++
			if s.refuse != nil {
				return s.refuse(name)
			}
			return nil
		}))
}

func (s *issue25590WorkspaceStub) RetainUnrecordedS3Objects(
	persisted []string, reason string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names = append(s.names, persisted...)
	s.hasRetains = len(s.names) > 0
}

func (s *issue25590WorkspaceStub) retained() ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.names...), s.hasRetains
}

func issue25590StubbedDelegate(t *testing.T, refuse func(string) error,
) (*s3WriterDelegate, *process.Process, *issue25590WorkspaceStub) {
	t.Helper()
	_, ctrl, proc := prepareTestCtx(t, true)
	stub := &issue25590WorkspaceStub{
		MockWorkspace: mock_frontend.NewMockWorkspace(ctrl),
		refuse:        refuse,
	}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().Return(stub).AnyTimes()
	proc.Base.TxnOperator = txnOp

	_, tableDef := getTestMainTable()
	tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	updateCtx := &MultiUpdateCtx{
		TableDef:   tableDef,
		InsertCols: []int{0, 1, 2, 3},
		DeleteCols: []int{0, 1},
	}
	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{updateCtx}}
	update.resetMultiUpdateCtxs()
	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)
	return writer, proc, stub
}

// TestS3WriterDelegateBindsWorkspaceAdmissionOnProducers pins the
// round-20 review finding 2 at the producer's seam: EVERY S3 writer the
// multi_update delegate constructs binds the workspace's per-object
// capacity option - the persistent insert sinkers, the temporary table
// writers of sortAndSyncOneTable, and their tombstone shapes alike -
// because all of them persist BEFORE multi_update.go's source.Write
// records the objects, and the refusal must land on the writer, not
// orphan the file behind a refused record. The temp writers' Close drops
// the tracked references, so their persisted names are collected onto
// the delegate and handed to the workspace retainer at Reset/Free - the
// only teardown running AFTER the caller's record attempt.
func TestS3WriterDelegateBindsWorkspaceAdmissionOnProducers(t *testing.T) {
	writer, proc, stub := issue25590StubbedDelegate(t, nil)
	defer func() { require.NoError(t, writer.free(proc)) }()

	require.NoError(t, writer.ensureInsertSinkers(proc))
	require.True(t, writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"the persistent insert sinker binds the workspace's per-object capacity hook")
	// The temporary TABLE writer persists through sortAndSyncOneTable and
	// its Close drops the name references: the delegate collected them.
	tableDef := writer.updateCtxs[0].TableDef
	bats, _ := prepareTestInsertBatchs(proc.Mp(), 2, colexec.DefaultBatchSize, false, false)
	require.NoError(t, writer.sortAndSyncOneTable(
		proc, tableDef,
		process.NewAnalyzer(0, false, false, "multi-update-data-t"),
		0, false, bats, true))

	// The interruption shape: data objects persisted, metadata records
	// never landed (a refused source.Write upstream). Free hands the
	// collected names to the retainer before closing the writers.
	require.NoError(t, writer.free(proc))
	retained, hasRetains := stub.retained()
	require.True(t, hasRetains,
		"the delegate handed the temp writers' persisted names - collected before their Close - to the workspace retainer")
	require.NotEmpty(t, retained,
		"the data writer's persisted objects left over unrecorded")

	// Idempotence: a second free hands nothing new (the accumulators and
	// the sinkers were cleared by the first one).
	require.NoError(t, writer.free(proc))
	retainedAgain, _ := stub.retained()
	require.Equal(t, retained, retainedAgain)
	require.Nil(t, writer.insertSinkers)
}

// TestS3WriterDelegateTombstoneWriterBindsAndRetains is the tombstone
// half of the producer coverage (round-20 review, finding 2): the
// delete/tombstone writer of sortAndSyncOneTable constructs with the
// workspace's capacity option as well, and its teardown hands the
// persisted tombstone object's name to the retainer.
func TestS3WriterDelegateTombstoneWriterBindsAndRetains(t *testing.T) {
	writer, proc, stub := issue25590StubbedDelegate(t, nil)
	defer func() { require.NoError(t, writer.free(proc)) }()

	tableDef := writer.updateCtxs[0].TableDef
	tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	segid := objectio.NewSegmentid()
	blkid := *objectio.NewBlockid(segid, 0, 0)
	rows := []types.Rowid{
		types.NewRowid(&blkid, 0),
		types.NewRowid(&blkid, 1),
	}
	delBat := batch.NewWithSize(2)
	delBat.SetAttributes([]string{"a", "pk"})
	delBat.Vecs[0] = testutil.MakeRowIdVector(rows, nil, proc.Mp())
	delBat.Vecs[1] = testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp())
	delBat.SetRowCount(2)
	bats := []*batch.Batch{delBat}
	require.NoError(t, writer.sortAndSyncOneTable(
		proc, tableDef,
		process.NewAnalyzer(0, false, false, "multi-update-tombstone-t"),
		0, true, bats, true))

	require.NoError(t, writer.free(proc))
	retained, hasRetains := stub.retained()
	require.True(t, hasRetains)
	require.NotEmpty(t, retained,
		"the tombstone writer's persisted object name reached the workspace retainer")
}

// TestS3WriterDelegateSaturationRefusesBeforePersist drives the real
// delegate against a saturated workspace hook: the writer's Sync consults
// the capacity hook before the pipelined job persists, so a refusal fails
// the call with the hook's error and NOTHING persists - repeated refused
// rounds keep the retainer empty (the pre-fix writers had no hook at all:
// a saturated ledger could refuse only the metadata record, and the
// persisted files fell out of every ownership enumeration).
func TestS3WriterDelegateSaturationRefusesBeforePersist(t *testing.T) {
	for round := 0; round < 3; round++ {
		writer, proc, stub := issue25590StubbedDelegate(
			t, func(string) error {
				return moerr.NewInternalErrorNoCtx("stub capacity saturated")
			})
		require.NoError(t, writer.ensureInsertSinkers(proc))
		require.True(t, writer.insertSinkers[0].HasUncommittedCapacityHook())

		bats, _ := prepareTestInsertBatchs(
			proc.Mp(), 2, colexec.DefaultBatchSize, false, false)
		err := writer.sortAndSyncOneTable(
			proc, writer.updateCtxs[0].TableDef,
			process.NewAnalyzer(0, false, false, "multi-update-sat-t"),
			0, false, bats, true)
		require.ErrorContains(t, err, "stub capacity saturated",
			"round %d: the saturated hook's refusal propagates out of the writer's Sync", round)
		require.NoError(t, writer.free(proc))
		retained, _ := stub.retained()
		require.Empty(t, retained,
			"round %d: the refused writer persisted nothing, so the retainer stayed empty", round)
	}
}

// TestS3WriterDelegateWithoutWorkspaceKeepsCompatBehavior pins the
// compatibility guard: a workspace without the structural owner surface -
// a mock, a non-disttae workspace - keeps the delegate's writers unhooked
// and its teardown retake-free, exactly the pre-fix behavior.
func TestS3WriterDelegateWithoutWorkspaceKeepsCompatBehavior(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	defer proc.Free()
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().
		Return(mock_frontend.NewMockWorkspace(ctrl)).AnyTimes()
	proc.Base.TxnOperator = txnOp

	_, tableDef := getTestMainTable()
	tableDef.Cols[0].Typ = pbplan.Type{Id: int32(types.T_int64)}
	updateCtx := &MultiUpdateCtx{
		TableDef:   tableDef,
		InsertCols: []int{0, 1, 2, 3},
	}
	update := &MultiUpdate{MultiUpdateCtx: []*MultiUpdateCtx{updateCtx}}
	update.resetMultiUpdateCtxs()
	writer, err := newS3Writer(proc.GetService(), update)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.free(proc)) }()

	require.NoError(t, writer.ensureInsertSinkers(proc))
	require.False(t, writer.insertSinkers[0].HasUncommittedCapacityHook(),
		"no workspace owner surface means no capacity hook - the pre-fix compat shape")

	bats, _ := prepareTestInsertBatchs(proc.Mp(), 2, colexec.DefaultBatchSize, false, false)
	require.NoError(t, writer.sortAndSyncOneTable(
		proc, tableDef,
		process.NewAnalyzer(0, false, false, "multi-update-compat-t"),
		0, false, bats, true),
		"the unhooked writer keeps the pre-fix success behavior")
	require.NoError(t, writer.free(proc))
	require.Empty(t, writer.unrecordedSinkNames,
		"without a retainer surface the teardown hands nothing and drops the collected names silently")
	require.Nil(t, writer.insertSinkers)
}
