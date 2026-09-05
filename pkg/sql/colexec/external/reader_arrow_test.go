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

package external

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/arrowbridge"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestExternalArrowLoadFileAndStream(t *testing.T) {
	for _, container := range []string{tree.ARROW_CONTAINER_FILE, tree.ARROW_CONTAINER_STREAM} {
		t.Run(container, func(t *testing.T) {
			objectsBefore := promtestutil.ToFloat64(metric.ArrowLoadObjectCounter.WithLabelValues("success"))
			recordsBefore := promtestutil.ToFloat64(metric.ArrowLoadRecordCounter)
			batchesBefore := promtestutil.ToFloat64(metric.ArrowLoadBatchCounter)
			rowsBefore := promtestutil.ToFloat64(metric.ArrowLoadRowCounter)
			eligibleBefore := promtestutil.ToFloat64(metric.ArrowLoadPayloadBytesCounter.WithLabelValues("eligible"))
			borrowedBefore := promtestutil.ToFloat64(metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"))
			copyBefore := promtestutil.ToFloat64(metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"))
			pinnedBefore := promtestutil.ToFloat64(metric.ArrowLoadPinnedBytesGauge)
			fileBytes := makeExternalArrowIPC(t, container)
			fs, err := fileservice.NewMemoryFS("etl", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			path := "etl:load-" + container + ".arrow"
			require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
				FilePath: path,
				Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(fileBytes)), Data: fileBytes}},
			}))

			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(64 << 20)
			require.NoError(t, err)
			proc := newArrowLoadTestProc(t)
			proc.Base.SessionInfo.TimeZone = nil // reader must use its UTC fallback

			arg := NewArgument().WithEs(externalArrowParam(fs, path, int64(len(fileBytes)), container))
			require.NoError(t, arg.SetAllocationAccount(account))
			require.True(t, arg.ActivatesAllocationAccountLifecycle())
			require.NoError(t, arg.Prepare(proc))

			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecNext, result.Status)
			require.Equal(t, 2, result.Batch.RowCount())
			require.Equal(t, []int64{1, 2}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
			require.Equal(t, "a payload longer than twenty three bytes", result.Batch.Vecs[1].GetStringAt(0))
			require.True(t, result.Batch.Vecs[0].HasBorrowedBacking())
			require.True(t, result.Batch.Vecs[1].HasBorrowedBacking())
			require.Greater(t, account.Snapshot().Used, uint64(0))

			result, err = arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, 2, result.Batch.RowCount())
			require.Equal(t, []int64{3, 4}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))

			result, err = arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, result.Status)
			arg.Free(proc, false, nil)
			require.Zero(t, account.Snapshot().Used)
			require.NoError(t, arg.ClearAllocationAccount(account))
			arg.Release()
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			require.Equal(t, objectsBefore+1, promtestutil.ToFloat64(metric.ArrowLoadObjectCounter.WithLabelValues("success")))
			require.Equal(t, recordsBefore+2, promtestutil.ToFloat64(metric.ArrowLoadRecordCounter))
			require.Equal(t, batchesBefore+2, promtestutil.ToFloat64(metric.ArrowLoadBatchCounter))
			require.Equal(t, rowsBefore+4, promtestutil.ToFloat64(metric.ArrowLoadRowCounter))
			require.Greater(t, promtestutil.ToFloat64(metric.ArrowLoadPayloadBytesCounter.WithLabelValues("eligible")), eligibleBefore)
			require.Greater(t, promtestutil.ToFloat64(metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed")), borrowedBefore)
			require.Greater(t, promtestutil.ToFloat64(metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo")), copyBefore)
			require.Equal(t, pinnedBefore, promtestutil.ToFloat64(metric.ArrowLoadPinnedBytesGauge))
		})
	}
}

func TestExternalArrowForceMaterialize(t *testing.T) {
	borrowedBefore := promtestutil.ToFloat64(
		metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"),
	)
	copyBefore := promtestutil.ToFloat64(
		metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"),
	)
	fileBytes := makeExternalArrowIPC(t, tree.ARROW_CONTAINER_FILE)
	fs, err := fileservice.NewMemoryFS("etl", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	path := "etl:force-materialize.arrow"
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(fileBytes)), Data: fileBytes}},
	}))

	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	proc := newArrowLoadTestProc(t)
	param := externalArrowParam(fs, path, int64(len(fileBytes)), tree.ARROW_CONTAINER_FILE)
	param.ArrowForceMaterialize = true
	arg := NewArgument().WithEs(param)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.False(t, result.Batch.Vecs[0].HasBorrowedBacking())
	require.False(t, result.Batch.Vecs[1].HasBorrowedBacking())
	require.Equal(t, borrowedBefore, promtestutil.ToFloat64(
		metric.ArrowLoadPayloadBytesCounter.WithLabelValues("borrowed"),
	))
	require.Greater(t, promtestutil.ToFloat64(
		metric.ArrowLoadCopyBytesCounter.WithLabelValues("arrow_to_mo"),
	), copyBefore)

	arg.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Release()
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestArrowLoadErrorCategory(t *testing.T) {
	tests := []struct {
		err      error
		category string
	}{
		{err: context.Canceled, category: "canceled"},
		{err: context.DeadlineExceeded, category: "deadline_exceeded"},
		{err: fmt.Errorf("wrapped: %w", fileservice.ErrObjectChanged), category: "object_changed"},
		{err: mpool.ErrAllocationAccountCapacity, category: "resource_exhausted"},
		{err: moerr.NewNotSupportedNoCtx("type"), category: "not_supported"},
		{err: moerr.NewConstraintViolationNoCtx("value"), category: "constraint_violation"},
		{err: moerr.NewInvalidInputNoCtx("input"), category: "invalid_input"},
		{err: fmt.Errorf("wrapped: %w", moerr.NewInvalidInputNoCtx("input")), category: "invalid_input"},
		{err: moerr.NewInternalErrorNoCtx("state"), category: "internal"},
		{err: errors.New("backend"), category: "io"},
	}
	for _, test := range tests {
		require.Equal(t, test.category, arrowLoadErrorCategory(test.err))
	}
}

func TestArrowReaderOpenRejectsMissingFileParam(t *testing.T) {
	proc := newArrowLoadTestProc(t)
	reader := new(ArrowReader)
	_, err := reader.Open(&ExternalParam{
		ExParamConst: ExParamConst{Extern: &tree.ExternParam{}},
	}, proc)
	require.ErrorContains(t, err, "file parameter")
}

type countingArrowCapacityLease struct {
	releases atomic.Int64
}

func (l *countingArrowCapacityLease) Release() {
	l.releases.Add(1)
}

func TestMeteredArrowCapacityLeaseConcurrentRelease(t *testing.T) {
	const capacity = int64(64)
	arrowPinnedMetricState.Lock()
	start := arrowPinnedMetricState.current
	arrowPinnedMetricState.Unlock()

	inner := new(countingArrowCapacityLease)
	lease := &meteredArrowCapacityLease{inner: inner, capacity: capacity}
	adjustArrowPinnedBytes(capacity)

	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			lease.Release()
		}()
	}
	wait.Wait()

	require.Equal(t, int64(1), inner.releases.Load())
	arrowPinnedMetricState.Lock()
	require.Equal(t, start, arrowPinnedMetricState.current)
	arrowPinnedMetricState.Unlock()
	require.Equal(t, float64(start), promtestutil.ToFloat64(metric.ArrowLoadPinnedBytesGauge))
}

func TestExternalArrowLoadFromLocalMinIOAndRejectsObjectChange(t *testing.T) {
	minioServer := startLocalArrowMinIO(t)
	ctx := context.Background()
	payload := makeExternalArrowIPC(t, tree.ARROW_CONTAINER_FILE)
	upload := func(key string, data []byte) {
		t.Helper()
		_, err := minioServer.client.PutObject(
			ctx, minioServer.bucket, key, bytes.NewReader(data), int64(len(data)),
			minio.PutObjectOptions{ContentType: "application/vnd.apache.arrow.file"},
		)
		require.NoError(t, err)
	}
	upload("load.arrow", payload)
	upload("identity.arrow", payload)

	fs, err := fileservice.NewS3FS(
		ctx,
		fileservice.ObjectStorageArguments{
			Name: "etl", Endpoint: "http://" + minioServer.endpoint,
			Region: "us-east-1", Bucket: minioServer.bucket,
			KeyID: minioServer.user, KeySecret: minioServer.password, IsMinio: true,
		},
		fileservice.DisabledCacheConfig, nil, true, true,
	)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })

	// Exercise the normal LOAD operator all the way through the S3-compatible
	// FileService. A File container must use bounded conditional range GETs;
	// it must not stage the entire object locally.
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	proc := newArrowLoadTestProc(t)
	proc.Ctx.Value(config.ParameterUnitKey).(*config.ParameterUnit).SV.ArrowLoad.S3Enabled = true
	path := "etl:load.arrow"
	arg := NewArgument().WithEs(externalArrowParam(fs, path, int64(len(payload)), tree.ARROW_CONTAINER_FILE))
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))
	for _, expected := range [][]int64{{1, 2}, {3, 4}} {
		result, err := arg.Call(proc)
		require.NoError(t, err)
		require.Equal(t, vm.ExecNext, result.Status)
		require.Equal(t, expected, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	}
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	arg.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Release()
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)

	// Fix the ETag during Open, replace the object, then force the next record
	// range read. An unversioned MinIO bucket must fail rather than combine
	// blocks from two object generations.
	identityPath := "etl:identity.arrow"
	identity, err := fs.StatFileIdentity(ctx, identityPath)
	require.NoError(t, err)
	require.Empty(t, identity.VersionID)
	require.NotEmpty(t, identity.ETag)
	registry, err = mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err = registry.Open(64 << 20)
	require.NoError(t, err)
	proc = newArrowLoadTestProc(t)
	param := externalArrowParam(fs, identityPath, int64(len(payload)), tree.ARROW_CONTAINER_FILE)
	param.Fileparam.FileIndex = 1
	param.Fileparam.Filepath = identityPath
	reader, err := NewArrowReader(param, proc, account)
	require.NoError(t, err)
	fileEmpty, err := reader.Open(param, proc)
	require.NoError(t, err)
	require.False(t, fileEmpty)

	replacement := append([]byte(nil), payload...)
	replacement[len(replacement)/2] ^= 0xff
	upload("identity.arrow", replacement)
	_, err = reader.ReadBatch(ctx, batch.NewWithSize(2), proc, nil)
	require.ErrorIs(t, err, fileservice.ErrObjectChanged)
	require.NoError(t, reader.Close())
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

type localArrowMinIO struct {
	endpoint string
	bucket   string
	user     string
	password string
	client   *minio.Client
}

func startLocalArrowMinIO(t *testing.T) localArrowMinIO {
	t.Helper()
	executable, err := exec.LookPath("minio")
	if errors.Is(err, exec.ErrNotFound) {
		t.Skip("local MinIO binary is not installed")
	}
	require.NoError(t, err)

	reserveAddress := func() string {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := listener.Addr().String()
		require.NoError(t, listener.Close())
		return address
	}
	endpoint, consoleEndpoint := reserveAddress(), reserveAddress()
	dataDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "minio.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)

	const user = "arrowtest"
	const password = "arrowtest-secret"
	command := exec.Command(
		executable, "server", dataDir,
		"--address", endpoint, "--console-address", consoleEndpoint,
	)
	command.Env = append(os.Environ(),
		"MINIO_ROOT_USER="+user,
		"MINIO_ROOT_PASSWORD="+password,
	)
	command.Stdout = logFile
	command.Stderr = logFile
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_, _ = command.Process.Wait()
		_ = logFile.Close()
	})

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(user, password, ""),
		Secure: false,
		Region: "us-east-1",
	})
	require.NoError(t, err)
	bucket := "matrixone-arrow-test"
	deadline := time.Now().Add(15 * time.Second)
	for {
		err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "us-east-1"})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_, _ = command.Process.Wait()
			_ = logFile.Close()
			logBytes, _ := os.ReadFile(logPath)
			t.Fatalf("start local MinIO: %v\n%s", err, logBytes)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return localArrowMinIO{
		endpoint: endpoint, bucket: bucket, user: user, password: password, client: client,
	}
}

func TestExternalArrowSkipsZeroRowRecordBatches(t *testing.T) {
	for _, container := range []string{tree.ARROW_CONTAINER_FILE, tree.ARROW_CONTAINER_STREAM} {
		t.Run(container, func(t *testing.T) {
			payload := makeExternalArrowIPCWithRows(t, container, [][]int64{
				{}, {1, 2}, {}, {3}, {},
			})
			fs, err := fileservice.NewMemoryFS("etl", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			path := "etl:empty-records-" + container + ".arrow"
			require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
				FilePath: path,
				Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(payload)), Data: payload}},
			}))

			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(64 << 20)
			require.NoError(t, err)
			proc := newArrowLoadTestProc(t)
			arg := NewArgument().WithEs(externalArrowParam(fs, path, int64(len(payload)), container))
			require.NoError(t, arg.SetAllocationAccount(account))
			require.NoError(t, arg.Prepare(proc))

			for _, expected := range [][]int64{{1, 2}, {3}} {
				result, err := arg.Call(proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecNext, result.Status)
				require.Equal(t, expected, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
			}
			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, result.Status)

			arg.Free(proc, false, nil)
			require.Zero(t, account.Snapshot().Used)
			require.NoError(t, arg.ClearAllocationAccount(account))
			arg.Release()
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
		})
	}
}

func TestExternalArrowPrepareFailsClosedBeforeIO(t *testing.T) {
	proc := newArrowLoadTestProc(t)
	for _, test := range []struct {
		name       string
		externType int32
		scope      pipeline.ArrowExecutionScope
	}{
		{"missing-scope", int32(plan.ExternType_LOAD), pipeline.ArrowExecutionScope_UnknownArrowExecutionScope},
		{"external-table", int32(plan.ExternType_EXTERNAL_TB), pipeline.ArrowExecutionScope_ArrowLoadData},
	} {
		t.Run(test.name, func(t *testing.T) {
			param := externalArrowParam(nil, "does-not-exist.arrow", 1, tree.ARROW_CONTAINER_FILE)
			param.Extern.ExternType = test.externType
			param.ArrowExecutionScope = test.scope
			arg := NewArgument().WithEs(param)
			err := arg.Prepare(proc)
			require.Error(t, err)
			require.Contains(t, err.Error(), "supported only by LOAD DATA")
			require.Nil(t, arg.reader)
			arg.Free(proc, true, err)
			arg.Release()
		})
	}
}

func TestExternalArrowPrepareEnforcesWorkerRolloutGate(t *testing.T) {
	proc := newArrowLoadTestProc(t)
	defer proc.Free()
	settings := proc.Ctx.Value(config.ParameterUnitKey).(*config.ParameterUnit).SV

	param := externalArrowParam(nil, "worker-gate.arrow", 1, tree.ARROW_CONTAINER_FILE)
	param.Extern.Parallel = true
	arg := NewArgument().WithEs(param)
	err := arg.Prepare(proc)
	require.ErrorContains(t, err, "distributed Arrow LOAD is disabled")
	require.Nil(t, arg.reader)
	arg.Release()

	settings.ArrowLoad.DistributedEnabled = true
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	arg = NewArgument().WithEs(param)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))
	arg.Free(proc, false, nil)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Release()
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)

	settings.ArrowLoad.DistributedEnabled = false
	param = externalArrowParam(nil, "s3-gate.arrow", 1, tree.ARROW_CONTAINER_FILE)
	param.Extern.ScanType = tree.S3
	arg = NewArgument().WithEs(param)
	err = arg.Prepare(proc)
	require.ErrorContains(t, err, "S3 or stage is disabled")
	require.Nil(t, arg.reader)
	arg.Release()
}

func TestExternalArrowAllocationAccountContract(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 2)
	require.NoError(t, err)
	first, err := registry.Open(1)
	require.NoError(t, err)
	second, err := registry.Open(1)
	require.NoError(t, err)
	arg := NewArgument().WithEs(&ExternalParam{ExParamConst: ExParamConst{
		ArrowExecutionScope: pipeline.ArrowExecutionScope_ArrowLoadData,
	}})
	require.ErrorIs(t, arg.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, arg.SetAllocationAccount(first))
	require.NoError(t, arg.SetAllocationAccount(first))
	require.ErrorIs(t, arg.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, arg.ClearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.NoError(t, arg.ClearAllocationAccount(first))
	arg.Release()
	first.Seal()
	second.Seal()
	_, err = registry.Finalize(first)
	require.NoError(t, err)
	_, err = registry.Finalize(second)
	require.NoError(t, err)
}

func TestBuildArrowTargetsUsesPhysicalColumnTypeAcrossGeneratedGap(t *testing.T) {
	attrs := []plan.ExternAttr{
		{ColName: "payload", ColIndex: 0, ColFieldIndex: 0},
		{ColName: "id", ColIndex: 2, ColFieldIndex: 1},
	}
	cols := []*plan.ColDef{
		{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar), Width: 128}},
		{Name: "generated", Typ: plan.Type{Id: int32(types.T_float64)}, GeneratedCol: &plan.GeneratedCol{}},
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
	}
	targets, err := BuildArrowTargets(context.Background(), attrs, cols)
	require.NoError(t, err)
	require.Len(t, targets, 2)
	require.Equal(t, "payload", targets[0].Name)
	require.Equal(t, types.T_varchar, targets[0].Type.Oid)
	require.Equal(t, int32(128), targets[0].Type.Width)
	require.Equal(t, 0, targets[0].MOIndex)
	require.Equal(t, "payload", targets[0].AttrName)
	require.Equal(t, "id", targets[1].Name)
	require.Equal(t, types.T_int64, targets[1].Type.Oid)
	require.True(t, targets[1].NotNull)
	require.Equal(t, 1, targets[1].MOIndex)
	require.Equal(t, "id", targets[1].AttrName)

	_, err = BuildArrowTargets(context.Background(), []plan.ExternAttr{{ColIndex: 3}}, cols)
	require.ErrorContains(t, err, "index 3")
}

func TestBuildArrowTargetsPreservesExplicitExternalFieldOrder(t *testing.T) {
	attrs := []plan.ExternAttr{
		{ColName: "a", ColIndex: 0, ColFieldIndex: 1},
		{ColName: "b", ColIndex: 1, ColFieldIndex: 0},
	}
	cols := []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: "b", Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	targets, err := BuildArrowTargets(context.Background(), attrs, cols)
	require.NoError(t, err)
	require.Equal(t, "b", targets[0].Name)
	require.Equal(t, 1, targets[0].MOIndex)
	require.Equal(t, "a", targets[1].Name)
	require.Equal(t, 0, targets[1].MOIndex)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "source_first", Type: arrow.PrimitiveTypes.Int64},
		{Name: "source_second", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	conversion, err := arrowbridge.BindLoad(
		context.Background(), schema, targets, arrowbridge.MatchByPosition,
	)
	require.NoError(t, err)
	allocator := memory.NewGoAllocator()
	firstBuilder := array.NewInt64Builder(allocator)
	firstBuilder.Append(11)
	first := firstBuilder.NewArray()
	firstBuilder.Release()
	defer first.Release()
	secondBuilder := array.NewInt64Builder(allocator)
	secondBuilder.Append(22)
	second := secondBuilder.NewArray()
	secondBuilder.Release()
	defer second.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{first, second}, 1)
	defer record.Release()
	mp := mpool.MustNewZero()
	converted, _, err := conversion.Convert(context.Background(), record, mp, arrowbridge.ConvertOptions{})
	require.NoError(t, err)
	require.Equal(t, []int64{22}, vector.MustFixedColNoTypeCheck[int64](converted.Vecs[0]))
	require.Equal(t, []int64{11}, vector.MustFixedColNoTypeCheck[int64](converted.Vecs[1]))
	converted.Clean(mp)
	require.Zero(t, mp.CurrNB())

	_, err = BuildArrowTargets(context.Background(), []plan.ExternAttr{
		{ColIndex: 0, ColFieldIndex: 1},
		{ColIndex: 1, ColFieldIndex: 1},
	}, cols)
	require.ErrorContains(t, err, "source field index 1 is duplicated")
	_, err = BuildArrowTargets(context.Background(), []plan.ExternAttr{
		{ColIndex: 0, ColFieldIndex: 2},
		{ColIndex: 1, ColFieldIndex: 0},
	}, cols)
	require.ErrorContains(t, err, "source field index 2 is invalid")
}

func TestExternalArrowFileRecordBatchShard(t *testing.T) {
	payload := makeExternalArrowIPC(t, tree.ARROW_CONTAINER_FILE)
	fs, err := fileservice.NewMemoryFS("etl", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	path := "etl:shard.arrow"
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(payload)), Data: payload}},
	}))
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	proc := newArrowLoadTestProc(t)
	param := externalArrowParam(fs, path, int64(len(payload)), tree.ARROW_CONTAINER_FILE)
	param.ArrowRecordBatchShards = []*pipeline.ArrowRecordBatchShard{{
		FileIndex: 0, RecordBatchStart: 1, RecordBatchEnd: 2,
		EstimatedRows: 2, EstimatedWireBytes: int64(len(payload)),
	}}
	arg := NewArgument().WithEs(param)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0]))
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	arg.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Release()
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestExternalArrowConversionPlanVersionAndFingerprintFailClosed(t *testing.T) {
	proc := newArrowLoadTestProc(t)
	for _, test := range []struct {
		name        string
		version     uint32
		fingerprint []byte
	}{
		{name: "unknown-version", version: arrowbridge.ConversionPlanVersion + 1},
		{name: "malformed-fingerprint", version: arrowbridge.ConversionPlanVersion, fingerprint: []byte{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			param := externalArrowParam(nil, "unused.arrow", 1, tree.ARROW_CONTAINER_FILE)
			param.ArrowConversionPlanVersion = test.version
			param.ArrowSchemaFingerprint = test.fingerprint
			registry, err := mpool.NewAllocationAccountRegistry(1, 8)
			require.NoError(t, err)
			account, err := registry.Open(1 << 20)
			require.NoError(t, err)
			arg := NewArgument().WithEs(param)
			require.NoError(t, arg.SetAllocationAccount(account))
			err = arg.Prepare(proc)
			require.Error(t, err)
			arg.Free(proc, true, err)
			require.NoError(t, arg.ClearAllocationAccount(account))
			arg.Release()
			account.Seal()
			_, finalizeErr := registry.Finalize(account)
			require.NoError(t, finalizeErr)
		})
	}
}

func TestExternalArrowRejectsPlannedFingerprintMismatchBeforeRecordRead(t *testing.T) {
	payload := makeExternalArrowIPC(t, tree.ARROW_CONTAINER_FILE)
	fs, err := fileservice.NewMemoryFS("etl", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	path := "etl:fingerprint.arrow"
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: path,
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: int64(len(payload)), Data: payload}},
	}))
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.Open(64 << 20)
	require.NoError(t, err)
	proc := newArrowLoadTestProc(t)
	param := externalArrowParam(fs, path, int64(len(payload)), tree.ARROW_CONTAINER_FILE)
	param.ArrowSchemaFingerprint = make([]byte, 32)
	arg := NewArgument().WithEs(param)
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.Prepare(proc))
	_, err = arg.Call(proc)
	require.ErrorContains(t, err, "does not match the planned contract")
	arg.Free(proc, true, err)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Release()
	account.Seal()
	_, finalizeErr := registry.Finalize(account)
	require.NoError(t, finalizeErr)
}

func TestExternalArrowFailurePathsReleaseAllocationAccount(t *testing.T) {
	payload := makeExternalArrowIPC(t, tree.ARROW_CONTAINER_FILE)
	for _, test := range []struct {
		name       string
		limit      uint64
		cancelRead bool
	}{
		{name: "capacity-during-open", limit: 32},
		{name: "cancellation-after-borrowed-batch", limit: 64 << 20, cancelRead: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, err := fileservice.NewMemoryFS("etl", fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			path := "etl:failure-" + test.name + ".arrow"
			require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
				FilePath: path,
				Entries: []fileservice.IOEntry{{
					Offset: 0, Size: int64(len(payload)), Data: payload,
				}},
			}))
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.Open(test.limit)
			require.NoError(t, err)
			proc := newArrowLoadTestProc(t)
			arg := NewArgument().WithEs(externalArrowParam(
				fs, path, int64(len(payload)), tree.ARROW_CONTAINER_FILE,
			))
			require.NoError(t, arg.SetAllocationAccount(account))
			require.NoError(t, arg.Prepare(proc))

			if test.cancelRead {
				_, err = arg.Call(proc)
				require.NoError(t, err)
				require.Greater(t, account.Snapshot().Used, uint64(0))
				ctx, cancel := context.WithCancel(proc.Ctx)
				proc.Ctx = ctx
				cancel()
				_, err = arg.Call(proc)
				require.ErrorIs(t, err, context.Canceled)
			} else {
				_, err = arg.Call(proc)
				require.Error(t, err)
				require.True(t,
					errors.Is(err, mpool.ErrAllocationAccountCapacity) ||
						mpool.IsMPoolCapacityFailure(err), err)
			}

			arg.Free(proc, true, err)
			require.Zero(t, account.Snapshot().Used)
			require.NoError(t, arg.ClearAllocationAccount(account))
			arg.Release()
			account.Seal()
			_, finalizeErr := registry.Finalize(account)
			require.NoError(t, finalizeErr)
		})
	}
}

func TestFitArrowBatchUsesActualCanonicalWireSize(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewOffHeap([]string{"v"})
	bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	for _, value := range []string{
		"first payload longer than twenty three bytes",
		"second payload longer than twenty three bytes",
		"third payload longer than twenty three bytes",
	} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, mp))
	}
	bat.SetRowCount(3)
	twoRows, err := bat.Window(0, 2)
	require.NoError(t, err)
	twoSize, err := twoRows.MarshalBinarySize()
	require.NoError(t, err)
	twoRows.Clean(mp)
	oneRow, err := bat.Window(0, 1)
	require.NoError(t, err)
	oneSize, err := oneRow.MarshalBinarySize()
	require.NoError(t, err)
	oneRow.Clean(mp)
	require.Greater(t, twoSize, oneSize)

	fitted, rows, err := fitArrowBatchToWireBudget(
		context.Background(), bat, uint64(twoSize-1), mp,
	)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	actual, err := fitted.MarshalBinarySize()
	require.NoError(t, err)
	require.LessOrEqual(t, uint64(actual), uint64(twoSize-1))
	require.Equal(t, "first payload longer than twenty three bytes", fitted.Vecs[0].GetStringAt(0))
	fitted.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestFitArrowBatchRetainsEligibleBorrowedPrefix(t *testing.T) {
	mp := mpool.MustNewZero()
	data := types.EncodeSlice([]int64{11, 22, 33})
	lease, err := vector.NewRefCountedBufferLease(data, int64(cap(data)), nil)
	require.NoError(t, err)
	vec, err := vector.NewBorrowedFixedVector(types.T_int64.ToType(), 3, data, lease)
	require.NoError(t, err)
	lease.Release()
	bat := batch.NewOffHeap([]string{"v"})
	bat.Vecs[0] = vec
	bat.SetRowCount(3)

	twoRows, err := bat.Window(0, 2)
	require.NoError(t, err)
	twoSize, err := twoRows.MarshalBinarySize()
	require.NoError(t, err)
	twoRows.Clean(mp)
	oneRow, err := bat.Window(0, 1)
	require.NoError(t, err)
	oneSize, err := oneRow.MarshalBinarySize()
	require.NoError(t, err)
	oneRow.Clean(mp)
	require.Greater(t, twoSize, oneSize)

	sourcePointer := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	fitted, rows, err := fitArrowBatchToWireBudget(
		context.Background(), bat, uint64(twoSize-1), mp,
	)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	require.True(t, fitted.Vecs[0].HasBorrowedBacking())
	require.Equal(t, sourcePointer,
		uintptr(unsafe.Pointer(unsafe.SliceData(fitted.Vecs[0].GetData()))))
	require.Equal(t, []int64{11}, vector.MustFixedColNoTypeCheck[int64](fitted.Vecs[0]))
	require.NotNil(t, lease.Bytes())

	fitted.Clean(mp)
	require.Nil(t, lease.Bytes())
	require.Zero(t, mp.CurrNB())
}

func TestFitArrowBatchRejectsOversizedFirstRow(t *testing.T) {
	for _, rows := range []int{1, 2} {
		t.Run(fmt.Sprintf("rows-%d", rows), func(t *testing.T) {
			mp := mpool.MustNewZero()
			bat := batch.NewOffHeap([]string{"v"})
			bat.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
			for row := 0; row < rows; row++ {
				value := bytes.Repeat([]byte{'x'}, 256)
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], value, false, mp))
			}
			bat.SetRowCount(rows)
			_, fittedRows, err := fitArrowBatchToWireBudget(
				context.Background(), bat, 64, mp,
			)
			require.ErrorContains(t, err, "exceeds batch limit")
			require.Zero(t, fittedRows)
			bat.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func externalArrowParam(fs fileservice.FileService, path string, size int64, container string) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			ArrowExecutionScope:        pipeline.ArrowExecutionScope_ArrowLoadData,
			ArrowConversionPlanVersion: arrowbridge.ConversionPlanVersion,
			Attrs: []plan.ExternAttr{
				{ColName: "id", ColIndex: 0, ColFieldIndex: 0},
				{ColName: "name", ColIndex: 1, ColFieldIndex: 1},
			},
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar), Width: 100}},
			},
			FileList: []string{path},
			FileSize: []int64{size},
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{
					ScanType: tree.INFILE, Filepath: path, Format: tree.ARROW,
					ArrowContainer: container, Tail: &tree.TailParameter{},
				},
				ExParam: tree.ExParam{ExternType: int32(plan.ExternType_LOAD), FileService: fs},
			},
			Ctx: context.Background(),
		},
		ExParam: ExParam{Fileparam: &ExFileparam{}, Filter: &FilterParam{}},
	}
}

func newArrowLoadTestProc(t *testing.T) *process.Process {
	t.Helper()
	proc := testutil.NewProc(t)
	frontend := &config.FrontendParameters{}
	frontend.SetDefaultValues()
	proc.Ctx = context.WithValue(proc.Ctx, config.ParameterUnitKey,
		config.NewParameterUnit(frontend, nil, nil, nil))
	return proc
}

func makeExternalArrowIPC(t *testing.T, container string) []byte {
	t.Helper()
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	var output bytes.Buffer
	writeRecords := func(write func(arrow.RecordBatch) error) {
		for offset := int64(0); offset < 4; offset += 2 {
			builder := array.NewRecordBuilder(allocator, schema)
			builder.Field(0).(*array.Int64Builder).AppendValues([]int64{offset + 1, offset + 2}, nil)
			builder.Field(1).(*array.StringBuilder).AppendValues(
				[]string{"a payload longer than twenty three bytes", "short"}, []bool{true, offset == 0},
			)
			record := builder.NewRecordBatch()
			require.NoError(t, write(record))
			record.Release()
			builder.Release()
		}
	}
	if container == tree.ARROW_CONTAINER_FILE {
		writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
		require.NoError(t, err)
		writeRecords(writer.Write)
		require.NoError(t, writer.Close())
	} else {
		writer := ipc.NewWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
		writeRecords(writer.Write)
		require.NoError(t, writer.Close())
	}
	return append([]byte(nil), output.Bytes()...)
}

func makeExternalArrowIPCWithRows(t *testing.T, container string, batches [][]int64) []byte {
	t.Helper()
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	var output bytes.Buffer
	writeRecords := func(write func(arrow.RecordBatch) error) {
		for _, ids := range batches {
			builder := array.NewRecordBuilder(allocator, schema)
			builder.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
			names := make([]string, len(ids))
			valid := make([]bool, len(ids))
			for row, id := range ids {
				names[row] = fmt.Sprintf("row-%d payload longer than twenty three bytes", id)
				valid[row] = true
			}
			builder.Field(1).(*array.StringBuilder).AppendValues(names, valid)
			record := builder.NewRecordBatch()
			require.NoError(t, write(record))
			record.Release()
			builder.Release()
		}
	}
	if container == tree.ARROW_CONTAINER_FILE {
		writer, err := ipc.NewFileWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
		require.NoError(t, err)
		writeRecords(writer.Write)
		require.NoError(t, writer.Close())
	} else {
		writer := ipc.NewWriter(&output, ipc.WithSchema(schema), ipc.WithAllocator(allocator))
		writeRecords(writer.Write)
		require.NoError(t, writer.Close())
	}
	return append([]byte(nil), output.Bytes()...)
}
