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

package hnsw

import (
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func quantizeTestVectors(count, dim int) [][]float32 {
	vectors := make([][]float32, count)
	state := uint32(3)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := range vectors[i] {
			state = state*1664525 + 1013904223
			vectors[i][d] = float32(state>>8)/float32(1<<24)*4 - 2
		}
	}
	return vectors
}

// An int8 build gives the range it is set with to every model and persists it in the metadata
// rows; without one it adds no row.
func TestQuantizedBuildUsesBounds(t *testing.T) {
	const count, dim = 900, 16
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(dim)}
	idxcfg.Usearch.Metric = usearch.L2sq
	idxcfg.Usearch.Quantization = usearch.I8
	idxcfg.IndexCapacity = 300
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "meta", IndexTable: "idx", ThreadsBuild: 2}
	vectors := quantizeTestVectors(count, dim)

	unset, err := NewHnswBuild[float32](sqlexec.NewSqlProcess(proc), "u:1:0", 1, idxcfg, tblcfg)
	require.NoError(t, err)
	defer unset.Destroy()
	require.Error(t, unset.Add(0, vectors[0]))

	build, err := NewHnswBuild[float32](sqlexec.NewSqlProcess(proc), "u:1:0", 1, idxcfg, tblcfg)
	require.NoError(t, err)
	defer build.Destroy()
	bounds := TrainQuantizeBounds(vectors)
	build.SetQuantizeBounds(bounds)
	for i, v := range vectors {
		require.NoError(t, build.Add(int64(i), v))
	}
	sqls, err := build.PreparePersistence(time.Now().UnixMicro(), 0, true)
	require.NoError(t, err)

	indexes := build.GetIndexes()
	require.Len(t, indexes, 3)
	var rows int64
	for _, idx := range indexes {
		require.Equal(t, bounds, idx.Bounds)
		rows += idx.Len.Load()
	}
	require.EqualValues(t, count, rows)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], "`"+catalog.Hnsw_TblCol_Metadata_Quantize_Min+"`, `"+catalog.Hnsw_TblCol_Metadata_Quantize_Max+"`) VALUES")
}

// The range is trained from a random sample of the source column, sized in vector elements.
func TestSampleQuantizeBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	old := runSql
	t.Cleanup(func() { runSql = old })
	vectors := quantizeTestVectors(50, 4)
	var seen string
	result := func(typ types.T, rows [][]float32) executor.Result {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(typ, 4, 0))
		for _, v := range rows {
			var b []byte
			if typ == types.T_array_float16 {
				b = types.ArrayToBytes(types.Float32ToFloat16Slice(v))
			} else {
				b = types.ArrayToBytes(v)
			}
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], b, false, mp))
		}
		bat.SetRowCount(len(rows))
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	}
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", KeyPart: "v", DataSize: 5_000_000}

	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		seen = sql
		return result(types.T_array_float32, vectors), nil
	}
	got, err := SampleQuantizeBounds(nil, tblcfg, 4)
	require.NoError(t, err)
	require.Equal(t, TrainQuantizeBounds(vectors), got)
	require.Equal(t, "SELECT SAMPLE(`v`, 10.000000 PERCENT) FROM `db`.`src` WHERE `v` IS NOT NULL LIMIT 500000", seen)

	// a table smaller than the sample is read whole
	tblcfg.DataSize = 1000
	_, err = SampleQuantizeBounds(nil, tblcfg, 4)
	require.NoError(t, err)
	require.Equal(t, "SELECT SAMPLE(`v`, 100.000000 PERCENT) FROM `db`.`src` WHERE `v` IS NOT NULL LIMIT 500000", seen)
	tblcfg.DataSize = 5_000_000

	// a vecf16 column is widened
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return result(types.T_array_float16, vectors), nil
	}
	got, err = SampleQuantizeBounds(nil, tblcfg, 4)
	require.NoError(t, err)
	require.True(t, got.Set)
	require.InDelta(t, TrainQuantizeBounds(vectors).Max, got.Max, 0.01)

	// an empty column has no range
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return result(types.T_array_float32, nil), nil
	}
	got, err = SampleQuantizeBounds(nil, tblcfg, 4)
	require.NoError(t, err)
	require.False(t, got.Set)
}

func TestMetadataSqlCarriesBounds(t *testing.T) {
	plain := metadataRow(true, "m0", "chk", 1, 2, 3, 4, QuantizeBounds{})
	require.Equal(t, catalog.IndexMetadataRow(true, "m0", "chk", 1, 2, 3, 4), plain)
	require.Equal(t, catalog.IndexMetadataInsertSql("db", "meta", true, []string{plain}),
		metadataInsertSql("db", "meta", true, false, []string{plain}))

	row := metadataRow(true, "m0", "chk", 1, 2, 3, 4, QuantizeBounds{Min: -1.5, Max: 2.25, Set: true})
	require.True(t, strings.HasSuffix(row, ", 4, -1.5, 2.25)"), row)
	sql := metadataInsertSql("db", "meta", true, true, []string{row})
	require.Contains(t, sql, "`build_ts`, `quantize_min`, `quantize_max`) VALUES "+row)
}

// LoadMetadata reads the bounds an int8/uint8 model persisted, and none from a row without them.
func TestLoadMetadataReadsBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	withBounds := func(set bool) *batch.Batch {
		bat := metaBatch(t, mp, true, 10, 20)
		wide := batch.NewWithSize(8)
		copy(wide.Vecs, bat.Vecs)
		for i, v := range []float64{-0.75, 1.5} {
			wide.Vecs[6+i] = vector.NewVec(types.T_float64.ToType())
			require.NoError(t, vector.AppendFixed(wide.Vecs[6+i], v, !set, mp))
		}
		wide.SetRowCount(1)
		return wide
	}
	idxs := loadMetaFrom(t, withBounds(true), mp)
	require.Equal(t, QuantizeBounds{Min: -0.75, Max: 1.5, Set: true}, idxs[0].Bounds)
	idxs = loadMetaFrom(t, withBounds(false), mp)
	require.False(t, idxs[0].Bounds.Set, "NULL bounds: not an int8/uint8 model")
	idxs = loadMetaFrom(t, metaBatch(t, mp, true, 10, 20), mp)
	require.False(t, idxs[0].Bounds.Set, "a six-column table has no bounds")
}

// A sync takes the bounds of an existing model, or trains them from the rows it syncs into an
// index built empty, and gives them to the models it creates.
func TestSyncQuantizeBounds(t *testing.T) {
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(4)}
	cfg.Usearch.Quantization = usearch.I8
	cdc := vectorindex.NewVectorIndexCdc[float32](4)
	for i, v := range quantizeTestVectors(3, 4) {
		cdc.Insert(int64(i), v, nil)
	}

	existing := QuantizeBounds{Min: -3, Max: 3, Set: true}
	s := &HnswSync[float32]{idxcfg: cfg, indexes: []*HnswModel[float32]{{Bounds: existing}}, tmpDir: t.TempDir()}
	s.setupBounds(cdc)
	require.Equal(t, existing, s.bounds)

	s = &HnswSync[float32]{idxcfg: cfg, tmpDir: t.TempDir()}
	s.setupBounds(cdc)
	require.True(t, s.bounds.Set)
	m, err := s.newModel(16)
	require.NoError(t, err)
	defer m.Destroy()
	require.Equal(t, s.bounds, m.Bounds)

	cfg.Usearch.Quantization = usearch.F16
	s = &HnswSync[float32]{idxcfg: cfg}
	s.setupBounds(cdc)
	require.False(t, s.bounds.Set, "a float index has no bounds")
}
