// Copyright 2021 - 2024 Matrix Origin
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

package hashjoin

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// useDiskV2LocalFS swaps proc's LOCAL fileservice for a checksum-free DISK-V2
// LocalFS, so spilling (which is rooted at LOCAL) runs against the raw backend.
func useDiskV2LocalFS(t *testing.T, proc *process.Process) {
	ctx := context.Background()
	local2, err := fileservice.NewLocalFS2(
		ctx, defines.LocalFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	shared, err := fileservice.NewLocalFS2(
		ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil,
	)
	require.NoError(t, err)
	etl, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, t.TempDir())
	require.NoError(t, err)
	fs, err := fileservice.NewFileServices("", local2, shared, etl)
	require.NoError(t, err)
	proc.Base.FileService = fs
}

// TestHashJoinSpillDiskV2 runs the join build-bucket spill + rebuild round-trip
// with a DISK-V2 (raw, checksum-free) LOCAL fileservice, confirming spill works
// end-to-end when the cluster is configured for DISK-V2.
func TestHashJoinSpillDiskV2(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	useDiskV2LocalFS(t, proc)

	// the LOCAL backing the spill fs is indeed the raw DISK-V2 format
	local, err := fileservice.Get[fileservice.FileService](proc.Base.FileService, defines.LocalFileServiceName)
	require.NoError(t, err)
	require.Equal(t, "DISK-V2", fileservice.LocalBackendOf(local))

	spillfs, err := proc.GetSpillFileService()
	require.NoError(t, err)

	// 100 int32 build rows
	buildVals := make([]int32, 100)
	for i := range buildVals {
		buildVals[i] = int32(i)
	}
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(buildVals, nil, proc.Mp())
	buildBat.SetRowCount(100)

	// spill the build bucket via FlushBucketBatch
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "diskv2_build")
	require.NoError(t, err)
	var buf bytes.Buffer
	bw := spillutil.BucketWriter{Name: "diskv2_build", Fd: buildFile}
	err = spillutil.FlushBucketBatch(proc, buildBat, &bw, &buf, nil)
	require.NoError(t, err)
	buildFd := bw.HandOffFd()
	require.NotNil(t, buildFd)

	// rebuild via SpillEngine
	engine := spillutil.NewSpillEngine(spillutil.SpillEngineConfig{
		BuildKeyExprs: makeKeyExpr(),
		NeedBatches:   true,
	})
	engine.InitFromSpilledMap([]*os.File{buildFd})

	analyzer := process.NewAnalyzer(0, false, false, "test")
	jm, res, err := engine.RebuildHashmap(proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, spillutil.BucketReady, res)
	require.NotNil(t, jm)
	require.Equal(t, int64(100), jm.GetRowCount())

	totalRows := 0
	for _, bat := range jm.GetBatches() {
		totalRows += bat.RowCount()
	}
	require.Equal(t, 100, totalRows)
	jm.Free()
	engine.Cleanup(proc)
}
