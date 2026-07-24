// Copyright 2025 Matrix Origin
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

package ivfflat

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// mockQuantizeBoundsResult builds a (key, val) result mirroring the single
// `WHERE key IN ('quantize_min','quantize_max')` query loadQuantizeBounds issues.
func mockQuantizeBoundsResult(m *mpool.MPool, qmin, qmax float64) executor.Result {
	bat := batch.NewWithSize(2)
	keyVec := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendBytes(keyVec, []byte(catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin), false, m)
	_ = vector.AppendBytes(keyVec, []byte(catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax), false, m)
	valVec := vector.NewVec(types.T_float64.ToType())
	_ = vector.AppendFixed(valVec, qmin, false, m)
	_ = vector.AppendFixed(valVec, qmax, false, m)
	bat.Vecs[0] = keyVec
	bat.Vecs[1] = valVec
	bat.SetRowCount(2)
	return executor.Result{Mp: m, Batches: []*batch.Batch{bat}}
}

func TestLoadQuantizeBounds(t *testing.T) {
	defer func() { runSql = sqlexec.RunSql }()

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	var tblcfg vectorindex.IndexTableConfig

	const qmin, qmax = -2.0, 6.0

	// both bounds present → params derived per element type
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return mockQuantizeBoundsResult(m, qmin, qmax), nil
	}
	for _, vt := range []types.T{types.T_array_int8, types.T_array_uint8} {
		idx := &IvfflatSearchIndex[float32]{QuantMul: 1, QuantAdd: 0}
		require.NoError(t, idx.loadQuantizeBounds(sqlproc, tblcfg, vt))

		wantMul, wantAdd := quantizer.Int8Params(qmin, qmax)
		if vt == types.T_array_uint8 {
			wantMul, wantAdd = quantizer.Uint8Params(qmin, qmax)
		}
		require.Equal(t, wantMul, idx.QuantMul)
		require.Equal(t, wantAdd, idx.QuantAdd)
	}

	// bounds absent → params left at identity (1,0)
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, nil
	}
	idx := &IvfflatSearchIndex[float32]{QuantMul: 1, QuantAdd: 0}
	require.NoError(t, idx.loadQuantizeBounds(sqlproc, tblcfg, types.T_array_int8))
	require.Equal(t, 1.0, idx.QuantMul)
	require.Equal(t, 0.0, idx.QuantAdd)

	// sql error propagates
	runSql = mock_runSql_parser_error
	require.Error(t, (&IvfflatSearchIndex[float32]{}).loadQuantizeBounds(sqlproc, tblcfg, types.T_array_int8))
}

// TestScoreFromQuantized verifies that the distance the entries query measures in
// the quantized domain is rescaled back to source units. The regression: with
// QUANTIZATION='int8' over [0,1] (mul=255), a source L2 of 0.5 was reported near
// 128 (mul*0.5) and range predicates in source units were off by mul^2.
func TestScoreFromQuantized(t *testing.T) {
	const sourceL2 = 0.5
	const sourceSq = sourceL2 * sourceL2 // 0.25

	// Identity quantizer (f32/f64/bf16/f16): QuantMul==1 → pass through, with the
	// squared->L2 conversion applied when the user asked for l2_distance.
	id := &IvfflatSearchIndex[float32]{QuantMul: 1, QuantAdd: 0}
	require.InDelta(t, sourceL2, id.scoreFromQuantized(sourceSq, metric.DistFn_L2Distance, metric.Metric_L2sqDistance), 1e-9)
	require.InDelta(t, sourceSq, id.scoreFromQuantized(sourceSq, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance), 1e-9)

	// int8 quantizer for [0,1] → mul=255 (the reviewer's example). The entries
	// query returns the squared L2 in the quantized domain = mul^2 * sourceSq.
	mul, _ := quantizer.Int8Params(0, 1)
	require.Equal(t, 255.0, mul)
	q := &IvfflatSearchIndex[float32]{QuantMul: mul, QuantAdd: -128}
	quantizedSq := mul * mul * sourceSq

	// Returned distance (user asked l2_distance) must be the SOURCE L2 (0.5),
	// NOT ~mul*0.5 (~127.5) as before the rescale.
	got := q.scoreFromQuantized(quantizedSq, metric.DistFn_L2Distance, metric.Metric_L2sqDistance)
	require.InDelta(t, sourceL2, got, 1e-6)
	require.Less(t, got, 1.0, "regression: quantized L2 must not be reported near mul*0.5")

	// Range predicate in source SQUARED units (user asked l2_distance_sq).
	gotSq := q.scoreFromQuantized(quantizedSq, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance)
	require.InDelta(t, sourceSq, gotSq, 1e-6)
}
