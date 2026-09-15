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

//go:build linux

package fulltext2

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func linuxProcMaps(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func linuxSingleInt64Batch(mp *mpool.MPool, value int64) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](b.Vecs[0], value, false, mp)
	b.SetRowCount(1)
	return b
}

func linuxSingleIDBatch(mp *mpool.MPool, id string) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendBytes(b.Vecs[0], []byte(id), false, mp)
	b.SetRowCount(1)
	return b
}

// TestFulltext2SearchLoadTailFailureReleasesOwnedMmap is Linux-only because
// it observes the actual file-backed mmap in /proc/self/maps. A valid base is
// mapped by LoadAllBases, then the real tail SELECT fails; the load error must
// unmap exactly that base before returning. The pre-error map observation makes
// this stronger than checking a nil returned slice or an RSS trend.
func TestFulltext2SearchLoadTailFailureReleasesOwnedMmap(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()
	// A SqlCtx with no Proc deliberately selects createLocalTempFile's OS-file
	// fallback, leaving a named path visible in /proc/self/maps for observation.
	sp := sqlexec.NewSqlProcessWithContext(
		sqlexec.NewSqlContext(context.Background(), "", nil, 0, nil))
	cfg := testStorageCfg()

	b := NewBuilder("s0", int32(types.T_int64))
	feed(t, b, int64(7), "mmap release witness")
	seg, err := b.Finish()
	require.NoError(t, err)
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	baseline := linuxProcMaps(t)
	var mapsAtTailFailure []string
	tailErr := errors.New("synthetic tail SELECT failure after base mmap")

	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "SUM(nrow)"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 1, filesize)}}, nil
		case strings.Contains(sql, "SELECT index_id FROM"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{linuxSingleIDBatch(mp, "s0")}}, nil
		case strings.Contains(sql, "SELECT checksum"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 3)}}, nil
		case strings.Contains(sql, "SUM(filesize"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
		case strings.Contains(sql, "COUNT(*)"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{linuxSingleInt64Batch(mp, 0)}}, nil
		default:
			// This is the non-empty tag=1 tail SELECT. Capture the mapping while
			// LoadAllBases' result is still owned by Fulltext2Search.Load.
			current := linuxProcMaps(t)
			for _, line := range current {
				if strings.Contains(line, "ft2idx") {
					mapsAtTailFailure = append(mapsAtTailFailure, line)
				}
			}
			return executor.Result{}, tailErr
		}
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, stream chan executor.Result, _ chan error) (executor.Result, error) {
		stream <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	search := NewFulltext2Search(cfg)
	err = search.Load(sp)
	require.ErrorIs(t, err, tailErr)
	require.NotEmpty(t, mapsAtTailFailure, "the failing tail query must observe the acquired base mmap")
	// The mapping must have been introduced by this load, rather than merely
	// being an unrelated ft2 test mapping left in the process.
	baselineSet := make(map[string]struct{}, len(baseline))
	for _, line := range baseline {
		baselineSet[line] = struct{}{}
	}
	for _, line := range mapsAtTailFailure {
		_, existed := baselineSet[line]
		require.False(t, existed, "observed mapping already existed before this load: %s", line)
	}

	require.Eventually(t, func() bool {
		current := linuxProcMaps(t)
		for _, owned := range mapsAtTailFailure {
			for _, line := range current {
				if line == owned {
					return false
				}
			}
		}
		return true
	}, time.Second, 10*time.Millisecond, "base mmap remained after tail-load failure")
}
