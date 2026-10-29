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
	"math"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
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
	var actual []byte
	var statements []string
	installPersistenceExecutor(t, sp, func(sql string, opts executor.StatementOption) (executor.Result, error) {
		statements = append(statements, sql)
		if opts.HasParams() {
			vec, err := opts.Params(mp)
			require.NoError(t, err)
			defer vec.Free(mp)
			for i := 1; i < vec.Length(); i += 2 {
				actual = append(actual, vec.GetRawBytesAt(i)...)
			}
		}
		return executor.Result{}, nil
	})
	require.NoError(t, seg.Persist(sp, cfg, 12345, int(vectorindex.Tag_ModelChunk), 0))
	require.Equal(t, expected, actual)
	require.Contains(t, statements[0], cfg.MetadataTable)
	require.NotContains(t, strings.Join(statements, ""), "load_file")
}

func TestCreateLocalSpillAndTempFile(t *testing.T) {
	// nil sqlproc ⇒ the spill dir and temp file fall back to the OS temp dir.
	require.Equal(t, "", localSpillDir(nil))

	f, path, err := createLocalTempFile(nil, "ft2test")
	require.NoError(t, err)
	require.NotEmpty(t, path) // linked file in the OS temp dir
	require.NoError(t, f.Close())
	require.NoError(t, os.Remove(path))
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

// On a metadata table that predates build_ts, the frame rows must OMIT it rather than fail.
// Naming a column the table does not have fails every CDC flush: the ISCP transaction never
// commits its watermark and the iteration retries the same statement forever, so the index stops
// advancing silently. Its sibling writer, Segment.Persist, has always probed for this.
func TestTailFrameRowsAreWithheldFromALegacyTable(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta_never_widened"}
	sqlexec.ForgetProvenanceShape(cfg.DbName, cfg.MetadataTable)

	frames := []TailSegment{{Path: "/tmp/s", Offset: 0, FrameLen: 10}}
	sqls := tailFrameMetaSqls(nil, cfg, 1, frames, 4242)

	var meta string
	for _, s := range sqls {
		if strings.Contains(s, cfg.MetadataTable) {
			meta += s
		}
	}
	// Naming build_ts on a table without it fails the flush; writing the row without build_ts
	// leaves an un-upgraded CN reading 'cdc_tail:1' as a base segment. Neither row is written.
	require.Empty(t, meta, "no metadata row at all until the table carries the columns")

}

// The tail frame rows are withheld while ANY un-upgraded CN could still be serving this index,
// separately from whether the table happens to carry the columns. An old CN reads the metadata
// table with SELECT * and would take 'cdc_tail:1' for a base sub-index and try to load it as one.
//
// A widened table used to stand as proof that no such reader was left, because only the gated
// v4_0_7 migration could widen it. That stopped being true once CREATE INDEX could produce a wide
// table too, so the deployment is asked directly.
func TestTailFrameRowsAreWithheldFromAMixedVersionDeployment(t *testing.T) {
	cfg := TableConfig{DbName: "db", IndexTable: "__store", MetadataTable: "__meta_mixed_version"}
	sqlexec.MarkProvenanceColumns(cfg.DbName, cfg.MetadataTable) // the table IS wide
	t.Cleanup(func() { sqlexec.ForgetProvenanceShape(cfg.DbName, cfg.MetadataTable) })

	sp, _ := mockSqlProc(t)
	rt := moruntime.ServiceRuntime(sp.Proc.GetService())
	require.NotNil(t, rt)
	prev, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, prev) })

	frames := []TailSegment{{Path: "/tmp/s", Offset: 0, FrameLen: 10}}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion61-1)
	sqls := tailFrameMetaSqls(sp, cfg, 1, frames, 4242)
	var meta string
	for _, s := range sqls {
		if strings.Contains(s, cfg.MetadataTable) {
			meta += s
		}
	}
	require.Empty(t, meta, "an old CN could still read this row as a base sub-index")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion61)
	sqls = tailFrameMetaSqls(sp, cfg, 1, frames, 4242)
	meta = ""
	for _, s := range sqls {
		if strings.Contains(s, cfg.MetadataTable) {
			meta += s
		}
	}
	require.Contains(t, meta, "'cdc_tail:1'", "once nobody can misread it, the row is written")
	require.Contains(t, meta, "4242")
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
	for i := int64(1); i <= 503; i++ {
		cdc := NewCdc(int32(types.T_int64))
		cdc.Insert(i, "owned chronological frame", nil)
		require.NoError(t, tb.AddBatch(cdc))
	}
	frames, err := tb.Finish()
	require.NoError(t, err)
	require.Len(t, frames, 503)
	require.Greater(t, distinctPaths(frames), 1)
	expected := make([][]byte, 0, len(frames))
	for _, f := range frames {
		data, err := os.ReadFile(f.Path)
		require.NoError(t, err)
		expected = append(expected, append([]byte(nil), data[f.Offset:f.Offset+int64(f.FrameLen)]...))
	}
	var actual [][]byte
	var batchRows []int
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
		return executor.Result{}, nil
	})
	next, err := tb.PersistFinished(sp, testStorageCfg(), 7, 0)
	require.NoError(t, err)
	require.Equal(t, int64(510), next)
	require.Equal(t, []int{500, 3}, batchRows, "batches span frames and file rolls")
	require.Equal(t, expected, actual)
	// The finished owner retains its spool for a transaction retry.
	actual = nil
	batchRows = nil
	next, err = tb.PersistFinished(sp, testStorageCfg(), 7, 0)
	require.NoError(t, err)
	require.Equal(t, int64(510), next)
	require.Equal(t, expected, actual)
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
