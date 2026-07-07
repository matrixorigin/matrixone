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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

	analyzer := process.NewAnalyzer(0, false, false, "test")

	// 100 int32 build rows
	buildBat := batch.NewWithSize(1)
	buildBat.Vecs[0] = testutil.MakeInt32Vector(makeSequence(100), nil, proc.Mp())
	buildBat.SetRowCount(100)

	// spill the build bucket
	buildFile, err := spillfs.CreateAndRemoveFile(context.Background(), "diskv2_build")
	require.NoError(t, err)
	ctr := &container{}
	bw := spillBucketWriter{file: buildFile}
	_, err = ctr.flushBucketBuffer(proc, buildBat, &bw, analyzer)
	require.NoError(t, err)
	buildFd := bw.handOffFd()
	require.NotNil(t, buildFd)

	hashJoin := &HashJoin{
		EqConds: [][]*plan.Expr{
			{}, // probe side (unused in rebuild)
			{
				&plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_int32), Width: 32},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
				},
			},
		},
		HashOnPK: false,
	}
	bucket := spillBucket{buildFd: buildFd, probeFd: nil, depth: 1}

	// rebuild the hashmap from the spilled bucket and verify all rows survive
	jm, _, err := hashJoin.rebuildHashmapForBucket(proc, bucket, analyzer)
	require.NoError(t, err)
	require.NotNil(t, jm)
	require.Equal(t, int64(100), jm.GetRowCount())

	totalRows := 0
	for _, bat := range jm.GetBatches() {
		totalRows += bat.RowCount()
	}
	require.Equal(t, 100, totalRows)
	jm.Free()
}
