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

package sqlreader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/diagnostic"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func statsTestBatch(mp *mpool.MPool, vectors ...*vector.Vector) executor.Result {
	bat := batch.NewWithSize(len(vectors))
	bat.Vecs = vectors
	if len(vectors) > 0 {
		bat.SetRowCount(vectors[0].Length())
	}
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func statsTestInts(t *testing.T, mp *mpool.MPool, values ...int64) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_int64.ToType())
	for _, x := range values {
		require.NoError(t, vector.AppendFixed(v, x, false, mp))
	}
	return v
}

func statsTestVectors(t *testing.T, mp *mpool.MPool, values ...[]float64) *vector.Vector {
	t.Helper()
	v := vector.NewVec(types.T_array_float64.ToType())
	for _, x := range values {
		require.NoError(t, vector.AppendBytes(v, types.ArrayToBytes(x), x == nil, mp))
	}
	return v
}

func TestMoIndexStatsReadOnlyCurrentVersionAndOutput(t *testing.T) {
	proc := testutil.NewProc(t)
	oldSQL, oldStreaming := indexStatsRunSQL, indexStatsRunStreaming
	t.Cleanup(func() { indexStatsRunSQL, indexStatsRunStreaming = oldSQL, oldStreaming })
	oldStorage := indexStatsReadStorage
	t.Cleanup(func() { indexStatsReadStorage = oldStorage })
	indexStatsReadStorage = func(_ *process.Process, _ *sqlexec.SqlProcess, _ indexstats.Config) (map[string]any, error) {
		return map[string]any{"size_bytes": uint64(100), "object_count": 2}, nil
	}
	indexStatsRunSQL = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		require.True(t, strings.HasPrefix(sql, "SELECT "))
		if strings.Contains(sql, "`meta`") {
			return statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 7)), nil
		}
		require.Contains(t, sql, "WHERE `__mo_index_centroid_version` = 7")
		return statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 2, 1),
			statsTestVectors(t, proc.Mp(), []float64{100}, []float64{0})), nil
	}
	entries := statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 1, 1),
		statsTestVectors(t, proc.Mp(), []float64{9}, []float64{11}))
	indexStatsRunStreaming = func(_ context.Context, _ *sqlexec.SqlProcess, sql string, results chan executor.Result, _ chan error) (executor.Result, error) {
		if !strings.Contains(sql, "WHERE `e`.`__mo_index_centroid_fk_version` = 7") {
			entries.Close()
			return executor.Result{}, errors.New("missing current version filter")
		}
		results <- entries
		return executor.Result{}, nil
	}
	cfg := indexstats.Config{Database: "db", Table: "source", Metadata: "meta", Centroids: "centers", Entries: "entries", OpType: "vector_l2_ops",
		Options: map[string]string{"lists": "2", "op_type": "vector_l2_ops", "quantization": "none", "async": "false", "kmeans_train_percent": "100"}}
	rows, err := ReadIVF(sqlexec.NewSqlProcess(proc), cfg)
	require.NoError(t, err)
	require.Equal(t, 3, rows.Count)
	require.Equal(t, "index", rows.At(0).Kind)
	require.Empty(t, rows.At(0).ObjectID)
	decode := func(i int) map[string]any {
		encoded, err := json.Marshal(rows.At(i).Metrics)
		require.NoError(t, err)
		var metrics map[string]any
		require.NoError(t, json.Unmarshal(encoded, &metrics))
		return metrics
	}
	summary := decode(0)
	require.Equal(t, float64(2), summary["vector_count"])
	require.Equal(t, float64(1), summary["empty_centroid_count"])
	require.Equal(t, float64(7), summary["index_version"])
	require.Equal(t, float64(1), summary["avg_bucket_size"])
	require.Equal(t, "sync", summary["maintenance_mode"])
	require.NotContains(t, summary, "async")
	require.NotContains(t, summary, "diagnosis")
	require.NotContains(t, summary, "status")
	require.Equal(t, map[string]any{"kmeans_train_percent": "100"}, summary["index_options"])
	require.Equal(t, 0.5, summary["empty_centroid_ratio"])
	require.Equal(t, float64(2), summary["max_bucket_size_multiple"])
	for _, key := range []string{"mean_residual_distance", "rms_residual_distance", "center_shift"} {
		require.NotContains(t, summary, key)
	}
	require.InDelta(t, 100.0/101, summary["center_mismatch_ratio"], 1e-12)
	require.Equal(t, "1", rows.At(1).ObjectID)
	require.Equal(t, "2", rows.At(2).ObjectID)
	centroid := decode(1)
	for _, key := range []string{"index_version", "centroid_count", "empty_centroid_count", "min_bucket_size", "max_bucket_size", "bucket_size_cv"} {
		require.NotContains(t, centroid, key, "index-wide fields must not be repeated on centroid rows")
	}
	require.Equal(t, float64(2), centroid["vector_count"])
	require.Equal(t, float64(2), centroid["bucket_size_multiple"])
	for _, key := range []string{"mean_residual_distance", "rms_residual_distance", "center_shift"} {
		require.NotContains(t, centroid, key)
	}
	empty := decode(2)
	require.Equal(t, map[string]any{"vector_count": float64(0), "bucket_size_multiple": float64(0)}, empty)
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMoIndexStatsZeroGeometryIsPresentButUnavailableGeometryIsOmitted(t *testing.T) {
	bucket := diagnostic.NewBucket(1, []float64{0})
	require.NoError(t, bucket.Add([]float64{0}))
	state := &ivfStats{}
	metrics := state.rowMetrics(bucket)
	require.NotContains(t, metrics, "center_shift")
	require.Contains(t, metrics, "center_mismatch_ratio")
	require.Equal(t, float64(0), metrics["center_mismatch_ratio"])
	require.NotContains(t, metrics, "status")
	require.NoError(t, bucket.Add(nil))
	metrics = state.rowMetrics(bucket)
	require.NotContains(t, metrics, "center_mismatch_ratio")
	require.NotContains(t, metrics, "center_shift")
	require.Equal(t, "incomplete_vectors", metrics["status"])
}

func TestMoIndexStatsEntrySQLUsesBoundedIndexReads(t *testing.T) {
	cfg := indexstats.Config{Database: "db`name", Table: "source", Entries: "entries`name"}
	query := buildIndexStatsEntriesSQL(cfg, 3, true)
	require.Contains(t, query, "`db``name`.`entries``name`")
	require.NotContains(t, query, "JOIN")
	require.Contains(t, query, "`e`.`__mo_index_centroid_fk_entry`")
	require.Contains(t, query, "WHERE `e`.`__mo_index_centroid_fk_version` = 3")
	query = buildIndexStatsEntriesSQL(cfg, 0, false)
	require.Contains(t, query, "COUNT(*)")
	require.Contains(t, query, "GROUP BY `e`.`__mo_index_centroid_fk_id`")
	require.NotContains(t, query, "__mo_index_centroid_fk_entry")
}

func TestMoIndexStatsStreamingPropagatesErrorsAndReleasesBatches(t *testing.T) {
	for _, failConsume := range []bool{false, true} {
		t.Run(map[bool]string{false: "executor", true: "consumer"}[failConsume], func(t *testing.T) {
			proc := testutil.NewProc(t)
			previous := indexStatsRunStreaming
			t.Cleanup(func() { indexStatsRunStreaming = previous })
			want := errors.New("injected stream error")
			res := statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 1))
			indexStatsRunStreaming = func(ctx context.Context, _ *sqlexec.SqlProcess, _ string, results chan executor.Result, errs chan error) (executor.Result, error) {
				results <- res
				if failConsume {
					<-ctx.Done()
				} else {
					errs <- want
				}
				return executor.Result{}, nil
			}
			err := streamIndexStats(proc, sqlexec.NewSqlProcess(proc), "SELECT 1", func(_ *batch.Batch) error {
				if failConsume {
					return want
				}
				return nil
			})
			require.ErrorIs(t, err, want)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestMoIndexStatsVectorDecodingCopiesInputAndRejectsInvalidValues(t *testing.T) {
	proc := testutil.NewProc(t)
	v := statsTestVectors(t, proc.Mp(), []float64{1, 2}, nil, []float64{math.NaN()})
	x, err := indexStatsVector(v, 0)
	require.NoError(t, err)
	require.Equal(t, []float64{1, 2}, x)
	x[0] = 99
	y, err := indexStatsVector(v, 0)
	require.NoError(t, err)
	require.Equal(t, float64(1), y[0])
	x, err = indexStatsVector(v, 1)
	require.NoError(t, err)
	require.Nil(t, x)
	_, err = indexStatsVector(v, 2)
	require.ErrorContains(t, err, "non-finite")
	v.Free(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMoIndexStatsBlockScaledVectorDecoding(t *testing.T) {
	for _, oid := range []types.T{types.T_array_float8, types.T_array_float4} {
		for _, dim := range []int{1, 17, 33, 65} {
			t.Run(fmt.Sprintf("%s/%d", oid, dim), func(t *testing.T) {
				proc := testutil.NewProc(t)
				format, _ := oid.BlockScaledFormat()
				source := make([]float32, dim)
				want := make([]float64, dim)
				for i := range source {
					source[i] = float32(uint64(1) << (3 * (i / 32)))
					if i%2 != 0 {
						source[i] = -source[i]
					}
					want[i] = float64(source[i])
				}
				cell, err := types.AppendBlockScaled(nil, format, source)
				require.NoError(t, err)
				v := vector.NewVec(types.New(oid, int32(dim), 0))
				defer v.Free(proc.Mp())
				require.NoError(t, vector.AppendBytes(v, cell, false, proc.Mp()))
				require.NoError(t, vector.AppendBytes(v, nil, true, proc.Mp()))

				scratch := make([]float64, dim+4)
				got, err := indexStatsVectorInto(v, 0, scratch)
				require.NoError(t, err)
				require.InDeltaSlice(t, want, got, 1e-5)
				require.Same(t, &scratch[0], &got[0])
				got[0] = 99
				require.Equal(t, cell, v.GetBytesAt(0))
				got, err = indexStatsVector(v, 0)
				require.NoError(t, err)
				require.InDeltaSlice(t, want, got, 1e-5)
				got, err = indexStatsVectorInto(v, 1, scratch)
				require.NoError(t, err)
				require.Nil(t, got)
				dynamic := vector.NewVec(oid.ToType())
				require.NoError(t, vector.AppendBytes(dynamic, cell, false, proc.Mp()))
				got, err = indexStatsVector(dynamic, 0)
				dynamic.Free(proc.Mp())
				require.NoError(t, err)
				require.InDeltaSlice(t, want, got, 1e-5)

				badScale := append([]byte(nil), cell...)
				badScale[types.BlockScaledHeaderSize] = 0xff
				otherFormat := types.BlockScaledMXFP8
				if format == otherFormat {
					otherFormat = types.BlockScaledNVFP4
				}
				wrongFormat, err := types.AppendBlockScaled(nil, otherFormat, source)
				require.NoError(t, err)
				for _, invalid := range []struct {
					cell  []byte
					width int32
				}{
					{cell[:len(cell)-1], int32(dim)},
					{badScale, int32(dim)},
					{wrongFormat, int32(dim)},
					{cell, int32(dim + 1)},
				} {
					bad := vector.NewVec(types.New(oid, invalid.width, 0))
					require.NoError(t, vector.AppendBytes(bad, invalid.cell, false, proc.Mp()))
					_, err = indexStatsVector(bad, 0)
					bad.Free(proc.Mp())
					require.Error(t, err)
				}
			})
		}
	}
}

func TestMoIndexStatsCountOnlyUsesAggregates(t *testing.T) {
	proc := testutil.NewProc(t)
	oldSQL, oldStream, oldStorage := indexStatsRunSQL, indexStatsRunStreaming, indexStatsReadStorage
	t.Cleanup(func() {
		indexStatsRunSQL, indexStatsRunStreaming, indexStatsReadStorage = oldSQL, oldStream, oldStorage
	})
	indexStatsReadStorage = func(*process.Process, *sqlexec.SqlProcess, indexstats.Config) (map[string]any, error) {
		return nil, nil
	}
	indexStatsRunSQL = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "`meta`") {
			return statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 7)), nil
		}
		return statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 1, 2), statsTestVectors(t, proc.Mp(), []float64{0}, []float64{1})), nil
	}
	indexStatsRunStreaming = func(ctx context.Context, _ *sqlexec.SqlProcess, sql string, results chan executor.Result, _ chan error) (executor.Result, error) {
		require.Contains(t, sql, "COUNT(*)")
		require.Contains(t, sql, "GROUP BY")
		results <- statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), 1), statsTestInts(t, proc.Mp(), 1000000))
		return executor.Result{}, nil
	}

	state := &ivfStats{metadataState: metadataState{config: indexstats.Config{Metadata: "meta", OpType: "vector_cosine_ops"}}}
	require.NoError(t, state.startIVF(proc, sqlexec.NewSqlProcess(proc)))
	require.Equal(t, int64(1000000), state.summary.Count)
	require.Equal(t, int64(1), state.summary.Empty)
	require.Equal(t, "center_stats_unsupported_distance", state.rowMetrics(nil)["status"])

	require.Zero(t, proc.Mp().CurrNB())
}
