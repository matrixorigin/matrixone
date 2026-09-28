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

package function

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// The C kernels accumulate in double, so an _xc distance can be a finite float64 that its scalar
// counterpart rejects. checkXCallDistanceDomain holds each overload to the domain that counterpart
// uses: float32 for l2_distance on either base type and for l2_distance_sq on vecf32, float64 for
// l2_distance_sq on vecf64, which returns the raw square by design.
func TestXCallDistanceDomain(t *testing.T) {
	proc := testutil.NewProcess(t)

	newResult := func(vals []float64, nullAt int) *vector.Vector {
		v := vector.NewVec(types.T_float64.ToType())
		for i, d := range vals {
			if i == nullAt {
				require.NoError(t, vector.AppendFixed(v, float64(0), true, proc.Mp()))
				continue
			}
			require.NoError(t, vector.AppendFixed(v, d, false, proc.Mp()))
		}
		return v
	}

	// 1e40 is a finite float64 but float32(1e40) is +Inf, so it is outside the public float32
	// distance domain. 1e38 is inside it. 1e300 is outside even the float64 square domain.
	for _, tc := range []struct {
		name    string
		funcId  int64
		vals    []float64
		wantErr bool
	}{
		{"l2 f32 inf", XCALL_L2DISTANCE_F32, []float64{math.Inf(1)}, true},
		{"l2 f32 beyond f32", XCALL_L2DISTANCE_F32, []float64{1e40}, true},
		{"l2 f32 ordinary", XCALL_L2DISTANCE_F32, []float64{5, 1e38}, false},

		{"l2 f64 beyond f32", XCALL_L2DISTANCE_F64, []float64{1e40}, true},
		{"l2 f64 nan", XCALL_L2DISTANCE_F64, []float64{math.NaN()}, true},
		{"l2 f64 ordinary", XCALL_L2DISTANCE_F64, []float64{5, 1e38}, false},

		{"l2sq f32 beyond f32", XCALL_L2DISTANCE_SQ_F32, []float64{1e40}, true},
		{"l2sq f32 ordinary", XCALL_L2DISTANCE_SQ_F32, []float64{25}, false},

		// vecf64 l2_distance_sq keeps the raw float64 square, so 1e80 is in domain.
		{"l2sq f64 keeps raw square", XCALL_L2DISTANCE_SQ_F64, []float64{1e80}, false},
		{"l2sq f64 inf", XCALL_L2DISTANCE_SQ_F64, []float64{math.Inf(1)}, true},

		// an unrelated xcall result is not a distance and is left alone
		{"other func id", 99, []float64{math.Inf(1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newResult(tc.vals, -1)
			defer v.Free(proc.Mp())
			err := checkXCallDistanceDomain(tc.funcId, v, len(tc.vals))
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "overflows the element domain")
				return
			}
			require.NoError(t, err)
		})
	}

	// a null entry carries no distance, so it is skipped rather than read
	t.Run("null skipped", func(t *testing.T) {
		v := newResult([]float64{1e40, 5}, 0)
		defer v.Free(proc.Mp())
		require.NoError(t, checkXCallDistanceDomain(XCALL_L2DISTANCE_F64, v, 2))
	})

	// the result vector may be longer than the batch; only the batch is checked
	t.Run("length bounds the scan", func(t *testing.T) {
		v := newResult([]float64{5, 1e40}, -1)
		defer v.Free(proc.Mp())
		require.NoError(t, checkXCallDistanceDomain(XCALL_L2DISTANCE_F64, v, 1))
		require.Error(t, checkXCallDistanceDomain(XCALL_L2DISTANCE_F64, v, 2))
	})
}
