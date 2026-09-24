// Copyright 2021 Matrix Origin
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

package colexec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type testUnpublishedS3CleanupWorkspace struct {
	client.Workspace
	cleanups []func(context.Context) error
}

func TestUnpublishedS3CleanupContextSharesOuterDeadline(t *testing.T) {
	outerDeadline := time.Now().Add(time.Minute)
	outer, cancelOuter := context.WithDeadline(context.Background(), outerDeadline)
	cancelOuter()
	for range 4 {
		inner, cancel := UnpublishedS3CleanupContext(outer)
		deadline, ok := inner.Deadline()
		require.True(t, ok)
		require.Equal(t, outerDeadline, deadline)
		require.NoError(t, inner.Err(), "request cancellation must not abort cleanup")
		cancel()
	}
}

func TestTerminalUnpublishedS3CleanupContextSurvivesCanceledRequest(t *testing.T) {
	request, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	cleanup, cancelCleanup := TerminalUnpublishedS3CleanupContext(request)
	defer cancelCleanup()
	require.NoError(t, cleanup.Err())
	deadline, ok := cleanup.Deadline()
	require.True(t, ok)
	require.Greater(t, time.Until(deadline), 0*time.Second)
	require.LessOrEqual(t, time.Until(deadline), 10*time.Minute)
}

func (w *testUnpublishedS3CleanupWorkspace) RetainUnpublishedS3Cleanup(
	cleanup func(context.Context) error,
) {
	w.cleanups = append(w.cleanups, cleanup)
}

func TestRetainUnpublishedS3CleanupTransfersToWorkspace(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	workspace := &testUnpublishedS3CleanupWorkspace{}
	txnOp.EXPECT().GetWorkspace().Return(workspace).AnyTimes()
	proc.Base.TxnOperator = txnOp

	called := false
	require.True(t, RetainUnpublishedS3Cleanup(proc, func(context.Context) error {
		called = true
		return nil
	}))
	require.Len(t, workspace.cleanups, 1)
	require.NoError(t, workspace.cleanups[0](proc.Ctx))
	require.True(t, called)
}

func TestCNS3DataWriterMemoryThresholdAndSyncAndFillBlockInfoBat(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	fs, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.SharedFileServiceName)
	require.NoError(t, err)

	tableDef := testCNS3WriterTableDef()

	t.Run("custom threshold", func(t *testing.T) {
		writer := NewCNS3DataWriter(proc.Mp(), fs, tableDef, 1, false)
		defer writer.Close()

		require.Equal(t, 1, writer.MemorySizeThreshold())

		bat := &batch.Batch{
			Attrs: []string{"a", "b"},
			Vecs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
				testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
			},
		}
		bat.SetRowCount(1)
		defer bat.Clean(proc.Mp())

		err = writer.Write(proc.Ctx, bat)
		require.NoError(t, err)

		blockInfoBat, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
		require.NoError(t, err)
		require.NotNil(t, blockInfoBat)
		require.Greater(t, blockInfoBat.RowCount(), 0)

		blockInfoBat, err = writer.SyncAndFillBlockInfoBat(proc.Ctx)
		require.NoError(t, err)
		require.NotNil(t, blockInfoBat)
		require.Equal(t, 0, blockInfoBat.RowCount())
	})

	t.Run("flush on sync", func(t *testing.T) {
		writer := NewCNS3DataWriter(proc.Mp(), fs, tableDef, -1, true)
		defer writer.Close()
		require.Equal(t, math.MaxInt, writer.MemorySizeThreshold())
	})
}

func TestCNS3WriterRetainsDetachedObjectOwnershipUntilHandoff(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()

	baseFS, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	t.Run("failed cleanup can be retried", func(t *testing.T) {
		fs := &failOnceDeleteFileService{FileService: baseFS}
		writer, objectName := writeDetachedTestObject(t, proc, fs)
		fs.failNextDelete = true

		err := writer.CloseWithCleanup(proc.Ctx, true)
		require.ErrorIs(t, err, errInjectedDelete)
		require.Len(t, writer.ownedPersistedNames, 1)
		require.Nil(t, writer.sinker, "retry ownership must not retain sinker buffers")
		require.Nil(t, writer.blockInfoBat, "retry ownership must not retain an mpool batch")
		_, err = baseFS.StatFile(proc.Ctx, objectName)
		require.NoError(t, err)

		// A later lifecycle callback may report success; pending abort ownership
		// still requires deletion and must not be mistaken for a handoff.
		require.NoError(t, writer.CloseWithCleanup(proc.Ctx, false))
		require.Empty(t, writer.ownedPersistedNames)
		_, err = baseFS.StatFile(proc.Ctx, objectName)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "object should be deleted, got %v", err)
	})

	t.Run("successful handoff preserves object", func(t *testing.T) {
		writer, objectName := writeDetachedTestObject(t, proc, baseFS)
		require.NoError(t, writer.CloseWithCleanup(proc.Ctx, false))
		_, err := baseFS.StatFile(proc.Ctx, objectName)
		require.NoError(t, err)
	})
}

func TestCNS3WriterCleanupCompletesDespitePipelineDrainError(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	baseFS, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName,
	)
	require.NoError(t, err)
	writeErr := errors.New("injected pipeline write failure")
	fs := &failWriteFileService{FileService: baseFS, writeErr: writeErr}
	writer := NewCNS3DataWriter(
		proc.Mp(), fs, testCNS3WriterTableDef(), 1, false,
		ioutil.WithPipelineFlush(),
	)
	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp()),
			testutil.MakeVarcharVector([]string{"x"}, nil, proc.Mp()),
		},
	}
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())
	require.NoError(t, writer.Write(proc.Ctx, bat))
	_, err = writer.Sync(proc.Ctx)
	require.ErrorIs(t, err, writeErr)

	// The failed pipeline is already reported by Sync. Cleanup must still
	// release a fully drained sinker rather than retaining a closed owner.
	require.NoError(t, writer.CloseWithCleanup(proc.Ctx, true))
	require.Nil(t, writer.sinker)
	require.False(t, writer.cleanupPending)
}

var errInjectedDelete = errors.New("injected delete failure")

type failOnceDeleteFileService struct {
	fileservice.FileService
	failNextDelete bool
}

type failWriteFileService struct {
	fileservice.FileService
	writeErr error
}

func (fs *failWriteFileService) Write(context.Context, fileservice.IOVector) error {
	return fs.writeErr
}

func (fs *failOnceDeleteFileService) Delete(ctx context.Context, paths ...string) error {
	if fs.failNextDelete {
		fs.failNextDelete = false
		return errInjectedDelete
	}
	return fs.FileService.Delete(ctx, paths...)
}

func writeDetachedTestObject(
	t *testing.T,
	proc *process.Process,
	fs fileservice.FileService,
) (*CNS3Writer, string) {
	t.Helper()
	writer := NewCNS3DataWriter(proc.GetMPool(), fs, testCNS3WriterTableDef(), 1, false)
	t.Cleanup(func() {
		if writer.sinker != nil {
			_ = writer.CloseWithCleanup(proc.Ctx, true)
		}
	})
	bat := &batch.Batch{
		Attrs: []string{"a", "b"},
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1}, nil, proc.GetMPool()),
			testutil.MakeVarcharVector([]string{"x"}, nil, proc.GetMPool()),
		},
	}
	bat.SetRowCount(1)
	defer bat.Clean(proc.GetMPool())
	require.NoError(t, writer.Write(proc.Ctx, bat))
	_, err := writer.SyncAndFillBlockInfoBat(proc.Ctx)
	require.NoError(t, err)
	require.Len(t, writer.ownedPersistedNames, 1)
	return writer, writer.ownedPersistedNames[0]
}

func TestCNS3DataWriterChunkedColumnProtocolGateIsLive(t *testing.T) {
	previousObjectSizeLimit := objectio.ObjectSizeLimit
	objectio.SetObjectSizeLimit(3 * mpool.GB)
	t.Cleanup(func() { objectio.SetObjectSizeLimit(previousObjectSizeLimit) })

	proc := testutil.NewProc(t)
	defer proc.Free()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	serviceID := "chunked-column-" + t.Name()
	rt := moruntime.DefaultRuntime()
	originalVersion, hadOriginalVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginalVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, originalVersion)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	NewServer(serviceID)

	tableDef := &plan.TableDef{
		Name: "wide_column",
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "payload", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_text)}},
			{ColId: 1, Name: catalog.Row_ID, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{},
	}
	bat := batch.NewWithSize(1)
	bat.Attrs = []string{"payload"}
	bat.SetVector(0, vector.NewVec(types.T_text.ToType()))
	payload := make([]byte, 20<<10)
	for i := range 512 {
		payload[0] = byte(i)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], payload, false, proc.Mp()))
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	defer bat.Clean(proc.Mp())

	writeAndColumnAlgorithm := func(writer *CNS3Writer) uint8 {
		t.Helper()
		defer writer.Close()
		require.NoError(t, writer.Write(proc.Ctx, bat))
		stats, syncErr := writer.Sync(proc.Ctx)
		require.NoError(t, syncErr)
		require.Len(t, stats, 1)
		location := stats[0].ObjectLocation()
		meta, loadErr := objectio.FastLoadObjectMeta(proc.Ctx, &location, false, fs)
		require.NoError(t, loadErr)
		dataMeta, ok := meta.DataMeta()
		require.True(t, ok)
		return dataMeta.GetBlockMeta(0).MustGetColumn(0).Location().Alg()
	}

	// The latest pre-feature protocol must remain on the legacy extent format.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion28)
	enabledAfterConstruction := NewCNS3DataWriterForService(
		serviceID, proc.Mp(), fs, tableDef, -1, true,
	)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion29)
	require.Equal(t, uint8(compress.Lz4Chunked), writeAndColumnAlgorithm(enabledAfterConstruction))

	disabledAfterConstruction := NewCNS3DataWriterForService(
		serviceID, proc.Mp(), fs, tableDef, -1, true,
	)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion28)
	require.Equal(t, uint8(compress.Lz4), writeAndColumnAlgorithm(disabledAfterConstruction))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion16)
	withoutServiceOwner := NewCNS3DataWriter(proc.Mp(), fs, tableDef, -1, true)
	require.Equal(t, uint8(compress.Lz4), writeAndColumnAlgorithm(withoutServiceOwner))
}

func TestChunkedColumnPolicyProtocolThreshold(t *testing.T) {
	require.Nil(t, chunkedColumnPolicyForService(""))

	serviceID := fmt.Sprintf("chunked-policy-%s", t.Name())
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	policy := chunkedColumnPolicyForService(serviceID)
	require.NotNil(t, policy)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion28)
	require.False(t, policy())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion29)
	require.True(t, policy())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, int32(defines.MORPCVersion29))
	require.False(t, policy())

	missingPolicy := chunkedColumnPolicyForService("missing-" + serviceID)
	require.NotNil(t, missingPolicy)
	require.False(t, missingPolicy())
}

func TestCNS3TombstoneWriterProtocolGate(t *testing.T) {
	proc := testutil.NewProc(t)
	defer proc.Free()
	fs, err := fileservice.Get[fileservice.FileService](
		proc.Base.FileService, defines.SharedFileServiceName,
	)
	require.NoError(t, err)

	serviceID := fmt.Sprintf("chunked-tombstone-%s", t.Name())
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	NewServer(serviceID)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion28)
	legacy := NewCNS3TombstoneWriterForService(
		serviceID, proc.Mp(), fs, types.T_int32.ToType(), -1,
	)
	require.NotNil(t, legacy)
	require.NoError(t, legacy.Close())

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion29)
	chunked := NewCNS3TombstoneWriterForService(
		serviceID, proc.Mp(), fs, types.T_int32.ToType(), -1,
	)
	require.NotNil(t, chunked)
	require.NoError(t, chunked.Close())

	withoutOwner := NewCNS3TombstoneWriter(
		proc.Mp(), fs, types.T_int32.ToType(), -1,
	)
	require.NotNil(t, withoutOwner)
	require.NoError(t, withoutOwner.Close())
}

func testCNS3WriterTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name: "t1",
		Cols: []*plan.ColDef{
			{ColId: 0, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}, NotNull: true, Primary: true, Default: &plan.Default{NullAbility: false}},
			{ColId: 1, Name: "b", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 8192}, NotNull: true},
			{ColId: 2, Name: catalog.Row_ID, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        []uint64{0},
			PkeyColId:   0,
			PkeyColName: "a",
			Names:       []string{"a"},
		},
		Name2ColIndex: map[string]int32{
			"a":            0,
			"b":            1,
			catalog.Row_ID: 2,
		},
	}
}
