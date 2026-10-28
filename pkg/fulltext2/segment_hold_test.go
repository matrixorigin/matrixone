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

package fulltext2

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// A loaded base segment holds its index file until Free; a tail frame, copied into memory,
// holds nothing once loaded. A tail frame that fits in one chunk is read from its row, with
// the others in one scan, and no index file is opened for it.
func TestLoadedSegmentsHoldTheirFiles(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	seg, err := b.Finish()
	require.NoError(t, err)
	buf, err := seg.Serialize()
	require.NoError(t, err)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, vectorindex.CheckSumFromBuffer(buf), int64(len(buf)), 5)}}, nil
	})
	files := swapIndexFiles(t)
	files.put("seg0", 0, 0, buf)

	loaded, err := LoadFromStorage(sp, cfg, "seg0")
	require.NoError(t, err)
	require.Equal(t, 1, files.holds("seg0", 0, 0), "a mapped base holds its file")
	loaded.Free()
	loaded.Free()
	require.Equal(t, 0, files.holds("seg0", 0, 0))

	small, err := FrameDeletes(int32(types.T_int64), []DeleteRecord{{Pk: int64(7)}})
	require.NoError(t, err)
	many := make([]DeleteRecord, 10000)
	for i := range many {
		many[i] = DeleteRecord{Pk: int64(100 + i)}
	}
	large, err := FrameDeletes(int32(types.T_int64), many)
	require.NoError(t, err)
	require.False(t, smallTailFrame(int64(len(large))))
	largeStart := int64(4)
	frames := map[int64][]byte{3: small, largeStart: large, 6: small}
	rows := []frameRow{{start: 3, frame: small}, {start: largeStart, frame: large}, {start: 6, frame: small}}
	opens := files.opens
	var scans []string
	chunks := files.storeFrames(frames)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "SUM("):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, int64(2*len(small)+len(large)), 4)}}, nil
		case strings.Contains(sql, "COUNT("):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 4, 0)}}, nil
		case strings.Contains(sql, "ORDER BY"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{frameRowsBatch(t, mp, rows)}}, nil
		default:
			scans = append(scans, sql)
			return executor.Result{Mp: mp, Batches: []*batch.Batch{tailChunkBatch(mp, chunks)}}, nil
		}
	})
	_, deletes, err := LoadTailSegments(sp, cfg)
	require.NoError(t, err)
	require.Len(t, deletes, 1+len(many))
	require.Equal(t, int64(6), deletes[int64(7)], "the later frame wins")
	require.Equal(t, largeStart, deletes[int64(100)])
	require.Len(t, scans, 1, "the one-chunk frames are read in one scan")
	require.Contains(t, scans[0], "IN (3, 6)")
	require.Equal(t, opens+1, files.opens, "only the larger frame is opened as an index file")
	require.Equal(t, 0, files.holds(vectorindex.CdcTailId, int64(vectorindex.Tag_CdcEvents), largeStart))
}
