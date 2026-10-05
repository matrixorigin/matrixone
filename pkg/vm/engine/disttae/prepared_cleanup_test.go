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

package disttae

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type preparedCleanupFS struct {
	fileservice.FileService
	offline atomic.Bool
	names   []string
}

func (fs *preparedCleanupFS) Write(ctx context.Context, v fileservice.IOVector) error {
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	fs.names = append(fs.names, v.FilePath)
	return nil
}

func (fs *preparedCleanupFS) Delete(ctx context.Context, names ...string) error {
	if fs.offline.Load() {
		return errors.New("injected storage outage")
	}
	return fs.FileService.Delete(ctx, names...)
}

type failingPreparedInput struct{ *colexec.MockOperator }

func (op *failingPreparedInput) Call(proc *process.Process) (vm.CallResult, error) {
	r, err := op.MockOperator.Call(proc)
	if err == nil && r.Batch == nil {
		err = errors.New("input failed before ownership handoff")
	}
	return r, err
}

func TestPreparedInsertRollbackRetriesCleanupWithoutAnotherExecute(t *testing.T) {
	server := colexec.NewServer("")
	t.Cleanup(func() { require.NoError(t, server.CloseUnpublishedS3Cleanup(context.Background())) })
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	proc := txn.proc
	defer proc.Free()
	proc.Base.TxnOperator = txn.op
	proc.BuildPipelineContext(proc.Ctx)
	baseFS, err := colexec.GetSharedFSFromProc(proc)
	require.NoError(t, err)
	fs := &preparedCleanupFS{FileService: baseFS}
	fs.offline.Store(true)
	t.Cleanup(func() { fs.offline.Store(false) })
	fss, err := fileservice.NewFileServices(baseFS.Name(), fs)
	require.NoError(t, err)
	proc.SetFileService(fss)

	// Exercise the real insert Call path, using the existing small-object fault
	// to spill before the next input fails (no output/ownership handoff).
	fault.Enable()
	defer fault.Disable()
	require.NoError(t, fault.AddFaultPoint(context.Background(), objectio.FJ_CNFlushSmallObjs, ":::", "echo", 0, "prepared_cleanup", false))
	defer func() { _, _ = fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNFlushSmallObjs) }()
	arg := insert.NewArgument()
	arg.ToWriteS3 = true
	arg.InsertCtx = &insert.InsertCtx{Attrs: []string{"a"}, TableDef: &plan.TableDef{
		DbName: "prepared_cleanup", Name: "t",
		Pkey: &plan.PrimaryKeyDef{},
		Cols: []*plan.ColDef{
			{Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_varchar)}},
			{Name: catalog.Row_ID, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
	}}
	bat := &batch.Batch{Attrs: []string{"a"}, Vecs: []*vector.Vector{
		testutil.MakeVarcharVector([]string{strings.Repeat("x", 2*colexec.FaultInjectedS3Threshold)}, nil, proc.Mp()),
	}}
	bat.SetRowCount(1)
	input := &failingPreparedInput{colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})}
	arg.AppendChild(input)
	require.NoError(t, arg.Prepare(proc))
	_, err = arg.Call(proc)
	require.ErrorContains(t, err, "input failed")
	require.NotEmpty(t, fs.names, "the insert must actually spill before failing")
	require.Empty(t, txn.unpublishedS3Cleanup)
	pipeline.New(0, nil, arg).Cleanup(proc, true, true, err)
	require.Len(t, txn.unpublishedS3Cleanup, 1, "Reset alone must transfer the failed writer")
	require.NoError(t, txn.Rollback(context.Background()))
	require.True(t, txn.removed)
	require.Empty(t, txn.unpublishedS3Cleanup, "CN owns retry after rollback")
	for _, name := range fs.names {
		_, err := baseFS.StatFile(context.Background(), name)
		require.NoError(t, err)
	}
	fs.offline.Store(false)
	require.Eventually(t, func() bool {
		for _, name := range fs.names {
			_, err := baseFS.StatFile(context.Background(), name)
			if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	// No further EXECUTE, Reset, Free or manual cleanup ran before recovery.
	arg.Free(proc, true, nil)
	input.Free(proc, true, nil)
	arg.Release()
}
