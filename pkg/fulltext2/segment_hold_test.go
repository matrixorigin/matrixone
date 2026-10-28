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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// A loaded base segment holds its index file until Free; a tail frame, copied into memory,
// holds nothing once loaded.
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

	tb := NewBuilder("tail", int32(types.T_int64))
	feed(t, tb, int64(2), "hello", "tail")
	tseg, err := tb.Finish()
	require.NoError(t, err)
	framed, err := FrameSegment(tseg)
	require.NoError(t, err)
	files.putFrame(3, framed)
	swapTailSql(t, mp, int64(len(framed)), 1, 1, []frameRow{{start: 3, frame: framed}}, nil)
	segs, _, err := LoadTailSegments(sp, cfg)
	require.NoError(t, err)
	defer freeSegs(segs)
	require.Len(t, segs, 1)
	require.Equal(t, 0, files.holds(vectorindex.CdcTailId, int64(vectorindex.Tag_CdcEvents), 3))
}
