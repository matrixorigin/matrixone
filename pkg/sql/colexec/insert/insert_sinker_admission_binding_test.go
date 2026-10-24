// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package insert

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// issue25590WorkspaceStub is a client.Workspace (through the embedded
// generated mock) that also carries the round-19 structural surfaces the
// direct-S3 insert operator asserts on: the SinkerOwner option binds the
// per-object admission into the writer Prepare builds, and the
// UnrecordedSinkerObjectsRetainer records every unwind transfer the
// operator's Free/Reset performs.
type issue25590WorkspaceStub struct {
	*mock_frontend.MockWorkspace

	mu      sync.Mutex
	names   []string
	reasons []string
}

func (s *issue25590WorkspaceStub) SinkerCapacityOption() ioutil.SinkerOption {
	return ioutil.WithUncommittedCapacityHook(func(string) error {
		return nil
	})
}

func (s *issue25590WorkspaceStub) RetainUnrecordedS3Objects(
	persisted []string, reason string,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names = append(s.names, persisted...)
	s.reasons = append(s.reasons, reason)
}

func (s *issue25590WorkspaceStub) retained() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.names...), append([]string(nil), s.reasons...)
}

// TestInsertToWriteS3BindsWorkspaceAdmissionAndRetainsOnFree pins the
// round-19 review finding 2 at the operator's seam: Prepare binds the
// workspace's per-object capacity option into the direct-S3 writer through
// the structural SinkerOwner interface, and a pipeline teardown hands the
// writer's persisted/possibly-persisted names to the workspace's
// UnrecordedSinkerObjectsRetainer BEFORE the close drops the tracked
// references. The pre-fix writer had NO capacity hook, and its Free only
// closed it - a stream that died between persistence and mergeBlock's
// record dropped the only surviving reference to physical objects, so
// repeated refused statements each leaked one more object.
func TestInsertToWriteS3BindsWorkspaceAdmissionAndRetainsOnFree(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().GetWorkspace().
		Return(&issue25590WorkspaceStub{
			MockWorkspace: mock_frontend.NewMockWorkspace(ctrl)}).
		AnyTimes()

	proc := testutil.NewProc(t)
	defer proc.Free()
	proc.Base.TxnOperator = txnOp

	insert := &Insert{
		ToWriteS3: true,
		InsertCtx: &InsertCtx{TableDef: testInsertS3TableDef()},
	}
	require.NoError(t, insert.Prepare(proc))
	require.True(t, insert.ctr.s3Writer.HasUncommittedCapacityHook(),
		"Prepare binds the workspace's per-object capacity hook into the direct-S3 writer")

	// The writer persists one object - the exact production shape of a
	// spill whose downstream record never ran when the pipeline fails.
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{"a", "b"}
	bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp())
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())
	require.NoError(t, insert.ctr.s3Writer.Write(proc.Ctx, bat))
	_, syncErr := insert.ctr.s3Writer.Sync(proc.Ctx)
	require.NoError(t, syncErr)
	names := insert.ctr.s3Writer.PersistedNames()
	require.NotEmpty(t, names,
		"the spilled writer must have persisted an object for the unwind leg")

	stub, ok := proc.GetTxnOperator().GetWorkspace().(*issue25590WorkspaceStub)
	require.True(t, ok)

	// The failed pipeline's Free transfers the writer's names; a second
	// Free (the teardown choreography may call it twice) hands nothing
	// more.
	insert.Free(proc, true, moerr.NewInternalErrorNoCtx("pipeline failed"))
	retained, reasons := stub.retained()
	require.Equal(t, names, retained,
		"Free hands the writer's unrecorded persisted names to the workspace's retainer before the close drops them")
	require.Len(t, reasons, 1)
	insert.Free(proc, true, nil)
	retained, _ = stub.retained()
	require.Equal(t, names, retained,
		"the second Free hands nothing: the first one already closed and nil'd the writer")
}

// TestInsertToWriteS3WithoutWorkspaceKeepsCompatBehavior pins the
// compatibility guard: a workspace without the structural owner surface -
// a mock, a non-disttae workspace, a test double - keeps an unhooked
// writer and a retake-free teardown, exactly the pre-fix behavior; no
// assertion error or panic may leave the compat path.
func TestInsertToWriteS3WithoutWorkspaceKeepsCompatBehavior(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	insert := &Insert{
		ToWriteS3: true,
		InsertCtx: &InsertCtx{TableDef: testInsertS3TableDef()},
	}
	require.NoError(t, insert.Prepare(proc))
	require.False(t, insert.ctr.s3Writer.HasUncommittedCapacityHook(),
		"no workspace owner means no capacity hook - the pre-fix compat shape")
	insert.Free(proc, true, nil)
	require.Nil(t, insert.ctr.s3Writer)
}
