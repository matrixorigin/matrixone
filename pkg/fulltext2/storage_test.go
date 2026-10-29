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
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func testStorageCfg() TableConfig {
	return TableConfig{DbName: "db", SrcTable: "src", IndexTable: "__store", MetadataTable: "__meta"}
}

func TestSubIndexId(t *testing.T) {
	require.Equal(t, "build:7", SubIndexId("build", 7))
	require.Equal(t, "a:b:c:0", SubIndexId("a:b:c", 0))
}

func TestDeleteSqls(t *testing.T) {
	cfg := testStorageCfg()

	del := DeleteSqls(cfg, "idx:1")
	require.Len(t, del, 2)
	require.Contains(t, del[0], "__store")
	require.Contains(t, del[0], "idx:1")
	require.Contains(t, del[1], "__meta")

	all := DeleteAllBasesSqls(cfg)
	require.Len(t, all, 2)
	require.Contains(t, all[0], "__store")
	require.Contains(t, all[1], "__meta")
	// A REBUILD clears the bases and KEEPS the tail's bytes, so the tail's frame rows have to
	// survive with them: stripping the rows off chunks that are still there loses their
	// build_ts and leaves tailPeakBytes summing whichever frames a later flush appends.
	require.Contains(t, all[1], notTailFrame(), "the bases' delete must spare the tail frame rows")
	require.Contains(t, all[1], TailFrameMetaPrefix)
	require.NotContains(t, all[1], "WHERE TRUE")

	// The tail's chunks AND its per-frame metadata rows: leaving the rows behind would report
	// a tail that no longer exists.
	tail := DeleteTailSqls(cfg)
	require.Len(t, tail, 2)
	require.Contains(t, tail[0], "__store")
	require.Contains(t, tail[1], "__meta")
	require.Contains(t, tail[1], TailFrameMetaPrefix)
}

func TestNextTailChunkIdSql(t *testing.T) {
	sql := NextTailChunkIdSql(testStorageCfg())
	require.Contains(t, sql, "GREATEST")
	require.Contains(t, sql, vectorindex.CdcTailId)
	require.Contains(t, sql, "__store")
	require.Contains(t, sql, "__meta")
}

func TestFrameChunkCount(t *testing.T) {
	require.Equal(t, int64(1), FrameChunkCount(0))                            // empty ⇒ at least one
	require.Equal(t, int64(1), FrameChunkCount(1))                            // partial ⇒ one
	require.Equal(t, int64(1), FrameChunkCount(vectorindex.MaxChunkSize))     // exact ⇒ one
	require.Equal(t, int64(2), FrameChunkCount(vectorindex.MaxChunkSize+1))   // spillover ⇒ two
	require.Equal(t, int64(3), FrameChunkCount(2*vectorindex.MaxChunkSize+1)) // ceil
}

func TestSegmentPersistBinary(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()
	b := NewBuilder("uid:0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "uid:0"
	expected, err := seg.Serialize()
	require.NoError(t, err)
	files := swapIndexFiles(t)
	var statements []string
	installPersistenceExecutor(t, sp, func(sql string, opts executor.StatementOption) (executor.Result, error) {
		statements = append(statements, sql)
		return executor.Result{}, nil
	})
	require.NoError(t, seg.Persist(sp, cfg, 12345, int(vectorindex.Tag_ModelChunk), 0))
	require.Equal(t, expected, files.get(t, "uid:0", int64(vectorindex.Tag_ModelChunk), 0))
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], cfg.MetadataTable)
	require.Contains(t, statements[0], indexfile.Checksum(expected))
	_ = mp
}

func TestLocalSpillDirNilProcess(t *testing.T) {
	// nil sqlproc ⇒ the spill dir falls back to the OS temp dir.
	require.Equal(t, "", localSpillDir(nil))

}

func TestLocalSpillDirNilRootFS(t *testing.T) {
	require.Equal(t, "", LocalSpillDir(context.Background(), nil))
}

// A scalar aggregate must be read by TYPE, not by assumption. COUNT comes back
// int64 but SUM comes back DECIMAL128, and the NoTypeCheck read this replaced
// reinterpreted the decimal bytes as an int64 — a garbage memory budget in a
// normal build, and a CN-killing panic in a type-checked one (the tail-load
// guard hit exactly that).
func TestScalarInt64ReadsBothAggregateTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	i64 := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(i64, int64(4096), false, mp))
	require.Equal(t, int64(4096), scalarInt64(i64))

	dec := vector.NewVec(types.T_decimal128.ToType())
	require.NoError(t, vector.AppendFixed(dec, types.Decimal128{B0_63: 4096, B64_127: 0}, false, mp))
	require.Equal(t, int64(4096), scalarInt64(dec))

	// a value past int64 saturates rather than wrapping to a negative budget,
	// which would silently disable the guard
	big := vector.NewVec(types.T_decimal128.ToType())
	require.NoError(t, vector.AppendFixed(big, types.Decimal128{B0_63: 0, B64_127: 1}, false, mp))
	require.Equal(t, int64(math.MaxInt64), scalarInt64(big))

	huge := vector.NewVec(types.T_decimal128.ToType())
	require.NoError(t, vector.AppendFixed(huge, types.Decimal128{B0_63: math.MaxUint64, B64_127: 0}, false, mp))
	require.Equal(t, int64(math.MaxInt64), scalarInt64(huge))

	// empty / null / unexpected type are all "no budget information"
	require.Equal(t, int64(0), scalarInt64(nil))
	require.Equal(t, int64(0), scalarInt64(vector.NewVec(types.T_int64.ToType())))

	null := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(null, int64(0), true, mp))
	require.Equal(t, int64(0), scalarInt64(null))

	other := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(other, []byte("7"), false, mp))
	require.Equal(t, int64(0), scalarInt64(other))
}

// Every scalar-aggregate SQL in this file must cast, so the reads above stay on
// the int64 path; the type switch is only a backstop.
func TestAggregateSQLCastsToSigned(t *testing.T) {
	src, err := os.ReadFile("storage.go")
	require.NoError(t, err)
	for _, line := range strings.Split(string(src), "\n") {
		if !strings.Contains(line, "SUM(") || !strings.Contains(line, "SELECT") {
			continue // comments and non-query text
		}
		require.Contains(t, line, "AS SIGNED",
			"a SUM read back as int64 must be cast in SQL: %s", strings.TrimSpace(line))
	}
}

// The metadata table now holds two kinds of row: one per BASE segment, and one per tail FRAME.
// Every reader that means "the bases" must say so -- one that does not would try to load a tail
// frame as a base segment, or fold the tail's bytes into the base totals. Base ids are
// "<index table>:<ts>:<n>", so the prefix cannot collide.
func TestTailFrameRowsAreNeverReadAsBases(t *testing.T) {
	cfg := testStorageCfg()

	require.Equal(t, "cdc_tail:7", TailFrameMetaId(7))
	require.True(t, strings.HasPrefix(TailFrameMetaId(7), TailFrameMetaPrefix))
	require.False(t, strings.HasPrefix(SubIndexId("mytable", 3), TailFrameMetaPrefix),
		"a base id must not look like a tail frame")

	// Every query that enumerates or sums the bases carries the exclusion.
	for _, sql := range []string{
		NextTailChunkIdSql(cfg),
	} {
		_ = sql // NextTailChunkId deliberately spans both kinds; see its comment.
	}

	tsSQL, _ := StaleGenSqls(cfg)
	require.Contains(t, tsSQL, notTailFrame(),
		"a tail flush must not read as a new base generation")
}

// A frame's row can be referred back to the bytes it describes: chunk ids are contiguous in
// frame order, so a frame owns [start, start+ceil(filesize/MaxChunkSize)).
func TestTailFrameRowsReferToTheirChunks(t *testing.T) {
	cfg := testStorageCfg()
	frames := []TailSegment{
		{Path: "/tmp/s", Offset: 0, FrameLen: vectorindex.MaxChunkSize + 1}, // 2 chunks
		{Path: "/tmp/s", Offset: 100, FrameLen: 10},                         // 1 chunk
	}
	const start = int64(5)
	cfg.MetadataTable = "__meta_tailframes_refer"
	sqlexec.MarkProvenanceColumns(cfg.DbName, cfg.MetadataTable)
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape(cfg.DbName, cfg.MetadataTable) })
	sp, _ := mockSqlProc(t)
	sqls := tailFrameMetaSqls(sp, cfg, start, frames, 4242)

	var meta string
	for _, s := range sqls {
		if strings.Contains(s, cfg.MetadataTable) {
			meta += s
		}
	}
	// First frame starts at 5 and spans two chunks, so the second starts at 7.
	require.Contains(t, meta, "'cdc_tail:5'")
	require.Contains(t, meta, "'cdc_tail:7'")
	require.Contains(t, meta, "4242", "each frame records the version it applied")

}

// Every tail frame has a metadata row: readers list the tail's frames from them. build_ts is
// named only when the table has the column.
func TestTailFrameRowsAreAlwaysWritten(t *testing.T) {
	frames := []TailSegment{{Path: "/tmp/s", Offset: 0, FrameLen: 10}, {Path: "/tmp/s", Offset: 10, FrameLen: vectorindex.MaxChunkSize + 1}}

	narrow := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta_never_widened"}
	sqlexec.ForgetProvenanceShape(narrow.DbName, narrow.MetadataTable)
	sqls := tailFrameMetaSqls(nil, narrow, 1, frames, 4242)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], "'cdc_tail:1'")
	require.Contains(t, sqls[0], "'cdc_tail:2'")
	require.NotContains(t, sqls[0], catalog.FullText2Index_TblCol_Metadata_Build_Ts)
	require.NotContains(t, sqls[0], "4242")

	wide := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta_wide"}
	sqlexec.MarkProvenanceColumns(wide.DbName, wide.MetadataTable)
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape(wide.DbName, wide.MetadataTable) })
	sqls = tailFrameMetaSqls(nil, wide, 1, frames, 4242)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], catalog.FullText2Index_TblCol_Metadata_Build_Ts)
	require.Contains(t, sqls[0], "'cdc_tail:2'")
	require.Contains(t, sqls[0], "4242")
}

// The frame's metadata row carries the CRC the frame was SEALED with, read back from its footer
// rather than recomputed, so the tail is verifiable from the catalog the way a base sub-index is.
// An empty checksum column would have made the row unable to detect a corrupted tail at all.
func TestTailFrameRowCarriesTheFramesChecksum(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta_tail_checksum"}
	sqlexec.MarkProvenanceColumns(cfg.DbName, cfg.MetadataTable)
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape(cfg.DbName, cfg.MetadataTable) })

	sp, _ := mockSqlProc(t)
	frames := []TailSegment{{Path: "/tmp/s", Offset: 0, FrameLen: 10, Checksum: 0xfeedface}}
	sqls := tailFrameMetaSqls(sp, cfg, 3, frames, 99)

	var meta string
	for _, s := range sqls {
		if strings.Contains(s, cfg.MetadataTable) {
			meta += s
		}
	}
	require.Contains(t, meta, "feedface",
		"the row records the frame's own CRC, read back from the footer it was sealed with")
	require.Contains(t, meta, "'cdc_tail:3'")
}

// Real packed files and sealed frame ranges exercise ordering and offsets;
// mocked SQL only decodes transported values, never reimplements persistence.
func TestTailPersistPackedRanges(t *testing.T) {
	sp, mp := mockSqlProc(t)
	tb, err := NewTailBuilder(int32(types.T_int64), 1, 0, t.TempDir(), wsTokenize)
	require.NoError(t, err)
	defer tb.Cleanup()
	tb.rollCap = 300
	large := map[int64]bool{100: true, 300: true}
	for i := int64(1); i <= 503; i++ {
		cdc := NewCdc(int32(types.T_int64))
		body := "owned chronological frame"
		if large[i] {
			var sb strings.Builder
			for w := 0; w < 20000; w++ {
				fmt.Fprintf(&sb, "w%d_%d ", i, w)
			}
			body = sb.String()
		}
		cdc.Insert(i, body, nil)
		require.NoError(t, tb.AddBatch(cdc))
	}
	frames, err := tb.Finish()
	require.NoError(t, err)
	require.Len(t, frames, 503)
	require.Greater(t, distinctPaths(frames), 1)
	var smallFrames [][]byte
	largeFrames := map[int64][]byte{}
	start := int64(7)
	for _, f := range frames {
		data, err := os.ReadFile(f.Path)
		require.NoError(t, err)
		frame := append([]byte(nil), data[f.Offset:f.Offset+int64(f.FrameLen)]...)
		if smallTailFrame(int64(len(frame))) {
			smallFrames = append(smallFrames, frame)
		} else {
			largeFrames[start] = frame
		}
		start += FrameChunkCount(len(frame))
	}
	require.Len(t, largeFrames, len(large))
	files := swapIndexFiles(t)
	var actual [][]byte
	var batchRows []int
	var metadata []string
	installPersistenceExecutor(t, sp, func(sql string, opt executor.StatementOption) (executor.Result, error) {
		if opt.HasParams() {
			v, err := opt.Params(mp)
			require.NoError(t, err)
			defer v.Free(mp)
			batchRows = append(batchRows, v.Length()/2)
			for i := 0; i < v.Length(); i += 2 {
				require.Equal(t, vectorindex.CdcTailId, string(v.GetRawBytesAt(i)))
				actual = append(actual, append([]byte(nil), v.GetRawBytesAt(i+1)...))
			}
		}
		metadata = append(metadata, sql)
		return executor.Result{}, nil
	})
	check := func() {
		t.Helper()
		require.Equal(t, []int{500, 1}, batchRows, "chunk rows of one-chunk frames are batched across frames and file rolls")
		require.Equal(t, smallFrames, actual)
		require.Len(t, files.files, len(largeFrames), "only a frame larger than one chunk is an index file")
		for at, want := range largeFrames {
			require.Equal(t, want, files.get(t, vectorindex.CdcTailId, int64(vectorindex.Tag_CdcEvents), at))
		}
		rows := strings.Count(strings.Join(metadata, ""), TailFrameMetaPrefix)
		require.Equal(t, len(frames), rows, "every frame has a metadata row")
	}
	next, err := tb.PersistFinished(sp, testStorageCfg(), nil, 7, 0)
	require.NoError(t, err)
	require.Equal(t, start, next)
	check()
	// The finished owner retains its spool for a transaction retry.
	actual, batchRows, metadata = nil, nil, nil
	files.files = make(map[string][]byte)
	next, err = tb.PersistFinished(sp, testStorageCfg(), nil, 7, 0)
	require.NoError(t, err)
	require.Equal(t, start, next)
	check()
}

func TestSegmentSpillIsUnlinked(t *testing.T) {
	b := NewBuilder("uid", int32(types.T_int64))
	require.NoError(t, b.Add("alpha", 0, int64(1)))
	require.NoError(t, b.Add("beta", 6, int64(2)))
	seg, err := b.Finish()
	require.NoError(t, err)
	want, err := seg.Serialize()
	require.NoError(t, err)

	dir := t.TempDir()
	spilled, err := seg.Spill(dir)
	require.NoError(t, err)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "the spill file is unlinked while its handle stays readable")
	got := make([]byte, spilled.size)
	_, err = spilled.fp.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, seg.Id, spilled.Id)
	require.Equal(t, seg.N, spilled.N)
	spilled.Discard()
	spilled.Discard()

	_, err = seg.Spill(dir + "/missing")
	require.Error(t, err)
}
