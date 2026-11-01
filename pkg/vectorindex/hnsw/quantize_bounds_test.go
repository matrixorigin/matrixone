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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// An int8 build gives the range it is set with to every model; without one it adds no row.
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
	require.NotContains(t, sqls[0], catalog.Hnsw_Dict_QuantizeMin)
}

// A build told its row count sizes its last model to the rows left.
func TestBuildModelCapacity(t *testing.T) {
	const count, dim = 700, 4
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(dim)}
	idxcfg.Usearch.Metric = usearch.L2sq
	idxcfg.IndexCapacity = 300
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "meta", IndexTable: "idx", ThreadsBuild: 1}

	build, err := NewHnswBuild[float32](sqlexec.NewSqlProcess(proc), "u:1:0", 1, idxcfg, tblcfg)
	require.NoError(t, err)
	defer build.Destroy()
	build.SetRowCount(count)
	for i, v := range quantizeTestVectors(count, dim) {
		require.NoError(t, build.Add(int64(i), v))
	}
	indexes := build.GetIndexes()
	caps := make([]uint, 0, len(indexes))
	for _, idx := range indexes {
		caps = append(caps, idx.MaxCapacity)
	}
	require.Equal(t, []uint{300, 300, 100}, caps)

	// a row beyond the count opens a model of the full capacity
	require.Equal(t, uint(300), build.modelCapacity(3))
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

// The range of an int8/uint8 index round-trips through its dict table.
func TestDictBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	old := runSql
	t.Cleanup(func() { runSql = old })

	bounds := QuantizeBounds{Min: -1.0 / 3, Max: 2.25, Set: true}
	sql := DictBoundsSql("db", "dict", bounds)
	require.Equal(t, "INSERT INTO `db`.`dict` (`__mo_index_key`, `__mo_index_val`) VALUES ('quantize_min', '-0.3333333333333333'), "+
		"('quantize_max', '2.25') ON DUPLICATE KEY UPDATE `__mo_index_val` = VALUES(`__mo_index_val`)", sql)

	rows := func(keys []string, vals []float64) executor.Result {
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
		for i, k := range keys {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(k), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], vals[i], false, mp))
		}
		bat.SetRowCount(len(keys))
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	}
	var seen string
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		seen = sql
		return rows([]string{catalog.Hnsw_Dict_QuantizeMax, catalog.Hnsw_Dict_QuantizeMin}, []float64{bounds.Max, bounds.Min}), nil
	}
	got, err := LoadQuantizeBounds(nil, "db", "dict")
	require.NoError(t, err)
	require.Equal(t, bounds, got)
	require.Contains(t, seen, "FROM `db`.`dict` WHERE `__mo_index_key` IN ('quantize_min', 'quantize_max')")

	// a dict table holding one end has no range
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return rows([]string{catalog.Hnsw_Dict_QuantizeMin}, []float64{-1}), nil
	}
	got, err = LoadQuantizeBounds(nil, "db", "dict")
	require.NoError(t, err)
	require.False(t, got.Set)

	// an index with no dict table runs no SQL
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		t.Fatal("no dict table to read")
		return executor.Result{}, nil
	}
	got, err = LoadQuantizeBounds(nil, "db", "")
	require.NoError(t, err)
	require.False(t, got.Set)
}

// Rescaling keeps distances nonzero for a range whose scale squared exceeds float32.
func TestRescaleDistancesTinyRange(t *testing.T) {
	d := []float32{1e5}
	RescaleDistances(d, usearch.I8, QuantizeBounds{Min: 0, Max: 1e-18, Set: true})
	require.Greater(t, d[0], float32(0))
}

// LoadModels gives every model the range in the index's dict table.
func TestLoadModelsAppliesDictBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	old := runSql
	t.Cleanup(func() { runSql = old })
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "`dict`") {
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
			for k, v := range map[string]float64{catalog.Hnsw_Dict_QuantizeMin: -0.75, catalog.Hnsw_Dict_QuantizeMax: 1.5} {
				require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(k), false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], v, false, mp))
			}
			bat.SetRowCount(2)
			return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}, nil
		}
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(t, mp, true, 10, 20)}}, nil
	}
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "meta", DictTable: "dict"}
	idxs, _, err := LoadModels[float32](nil, tblcfg)
	require.NoError(t, err)
	require.Len(t, idxs, 1)
	require.Equal(t, QuantizeBounds{Min: -0.75, Max: 1.5, Set: true}, idxs[0].Bounds)

	tblcfg.DictTable = ""
	idxs, _, err = LoadModels[float32](nil, tblcfg)
	require.NoError(t, err)
	require.False(t, idxs[0].Bounds.Set, "an index with no dict table has no range")

	// a vecint8 index with no dict table maps vectors with the range of its column type
	tblcfg.KeyPartType = int32(types.T_array_int8)
	idxs, _, err = LoadModels[float32](nil, tblcfg)
	require.NoError(t, err)
	require.Equal(t, BaseBounds(int32(types.T_array_int8)), idxs[0].Bounds)
}

// A sync keeps the range it loaded from the dict table, or trains one from the rows it syncs
// into an index whose dict table holds none, gives it to the models it creates and marks it for
// the dict table.
func TestSyncQuantizeBounds(t *testing.T) {
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(4)}
	cfg.Usearch.Quantization = usearch.I8
	tblcfg := vectorindex.IndexTableConfig{DictTable: "dict"}
	cdc := vectorindex.NewVectorIndexCdc[float32](4)
	for i, v := range quantizeTestVectors(3, 4) {
		cdc.Insert(int64(i), v, nil)
	}

	loaded := QuantizeBounds{Min: -3, Max: 3, Set: true}
	s := &HnswSync[float32]{idxcfg: cfg, tblcfg: tblcfg, bounds: loaded, tmpDir: t.TempDir()}
	s.setupBounds(cdc)
	require.Equal(t, loaded, s.bounds)
	require.False(t, s.newBounds, "a range already in the dict table is not rewritten")

	s = &HnswSync[float32]{idxcfg: cfg, tblcfg: tblcfg, tmpDir: t.TempDir()}
	s.setupBounds(cdc)
	require.True(t, s.bounds.Set)
	require.True(t, s.newBounds)
	s.headroom = 3
	m, err := s.newModel(16)
	require.NoError(t, err)
	defer m.Destroy()
	require.Equal(t, s.bounds, m.Bounds)
	require.Equal(t, uint(16), m.MaxCapacity)
	capacity, err := m.Index.Capacity()
	require.NoError(t, err)
	require.Less(t, capacity, uint(16), "a sync model reserves room for the change, not its capacity")

	// a model created by a chunk with no vectors takes the bounds a later chunk trains
	empty := vectorindex.NewVectorIndexCdc[float32](4)
	empty.Delete(100)
	s = &HnswSync[float32]{idxcfg: cfg, tblcfg: tblcfg, tmpDir: t.TempDir()}
	s.setupBounds(empty)
	require.False(t, s.bounds.Set)
	require.False(t, s.newBounds)
	first, err := s.newModel(16)
	require.NoError(t, err)
	defer first.Destroy()
	s.indexes = append(s.indexes, first)
	s.setupBounds(cdc)
	require.True(t, s.bounds.Set)
	require.Equal(t, s.bounds, first.Bounds)
	require.NoError(t, first.Add(1, cdc.Data[0].Vec))

	cfg.Usearch.Quantization = usearch.F16
	s = &HnswSync[float32]{idxcfg: cfg, tblcfg: tblcfg}
	s.setupBounds(cdc)
	require.False(t, s.bounds.Set, "a float index has no bounds")
	require.False(t, s.newBounds)
}

// A model holds what fits 75% of available host memory, capped by an explicit capacity.
func TestPlanCapacity(t *testing.T) {
	old := hostRowsFitting
	t.Cleanup(func() { hostRowsFitting = old })
	cfg := usearch.DefaultConfig(128)
	cfg.Quantization = usearch.I8
	require.Equal(t, uint64(128+4*33+4*17/15+64), ModelBytesPerRow(cfg))

	var perRow uint64
	hostRowsFitting = func(bytes, _ uint64) (int64, uint64, error) {
		perRow = bytes
		return 5000, 0, nil
	}
	got, err := PlanCapacity(0, cfg)
	require.NoError(t, err)
	require.EqualValues(t, 5000, got)
	require.Equal(t, ModelBytesPerRow(cfg), perRow)
	got, err = PlanCapacity(100, cfg)
	require.NoError(t, err)
	require.EqualValues(t, 100, got)
	got, err = PlanCapacity(9000, cfg)
	require.NoError(t, err)
	require.EqualValues(t, 5000, got, "an explicit capacity is capped by what fits")

	// unmeasured host memory
	hostRowsFitting = func(uint64, uint64) (int64, uint64, error) { return 0, 0, nil }
	got, err = PlanCapacity(0, cfg)
	require.NoError(t, err)
	require.EqualValues(t, unmeasuredCapacity, got)
	got, err = PlanCapacity(42, cfg)
	require.NoError(t, err)
	require.EqualValues(t, 42, got)

	hostRowsFitting = func(uint64, uint64) (int64, uint64, error) { return 0, 0, moerr.NewInternalErrorNoCtx("no room") }
	_, err = PlanCapacity(0, cfg)
	require.Error(t, err)
}

// A sync applying several batches grows the room of the model it keeps adding to.
func TestSyncBatchesGrowModelRoom(t *testing.T) {
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(4), IndexCapacity: 1000}
	cfg.Usearch.Metric = usearch.L2sq
	s := &HnswSync[float32]{idxcfg: cfg, tblcfg: vectorindex.IndexTableConfig{ThreadsBuild: 2}, tmpDir: t.TempDir(), uid: "cdc:1:0"}
	defer s.Destroy()
	vectors := quantizeTestVectors(300, 4)
	for batch := 0; batch < 3; batch++ {
		cdc := vectorindex.NewVectorIndexCdc[float32](100)
		for i := batch * 100; i < (batch+1)*100; i++ {
			cdc.Insert(int64(i), vectors[i], nil)
		}
		require.NoError(t, s.Update(nil, cdc), "batch %d", batch)
	}
	require.Len(t, s.indexes, 1)
	require.EqualValues(t, 300, s.indexes[0].Len.Load())
}
