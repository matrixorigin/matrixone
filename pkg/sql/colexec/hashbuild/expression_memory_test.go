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

package hashbuild

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRecoveryProjectionPreservesLogicalVarlenaMultiplicity(t *testing.T) {
	mp := mpool.MustNewZero()
	const rows = 4
	payload := strings.Repeat("x", 64)
	constant, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte(payload), 1, mp)
	require.NoError(t, err)
	defer constant.Free(mp)
	require.Equal(t, 1, constant.Length(), "the value is physically stored once")
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = constant
	bat.SetRowCount(rows)

	selected, err := projectedSelectedRange(bat, 0, rows)
	require.NoError(t, err)
	require.Equal(t, uint64(rows*(types.VarlenaSize+len(payload))), selected)

	selected, err = projectedSelectedRange(bat, 2, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2*(types.VarlenaSize+len(payload))), selected)
}

func TestRecoveryProjectionTreatsBroadcastConstNullAsZeroPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	constant, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte(strings.Repeat("x", 64)), 1, mp,
	)
	require.NoError(t, err)
	constant.SetNull(0)
	source := batch.NewWithSize(1)
	source.Vecs[0] = constant
	source.SetRowCount(4)

	window, err := source.Window(2, 4)
	require.NoError(t, err)
	require.True(t, window.Vecs[0].IsConstNull())
	require.Equal(t, 2, window.RowCount())

	projection, err := (&HashmapBuilder{}).projectRetainedRecovery(window)
	require.NoError(t, err)
	require.Equal(t, 2, projection.maxRows)
	require.Equal(t, uint64(2*types.VarlenaSize), projection.maxSelected)
	require.Equal(t, projection.maxSelected, projection.nextTailSelected)

	window.Clean(nil)
	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestRecoveryProjectionIgnoresReusableConstNullArea(t *testing.T) {
	mp := mpool.MustNewZero()
	constant, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte(strings.Repeat("x", 64)), 1, mp,
	)
	require.NoError(t, err)
	require.NotEmpty(t, constant.GetArea())
	require.NoError(t, vector.SetConstNull(constant, 4, mp))
	require.True(t, constant.IsConstNull())
	require.NotEmpty(t, constant.GetArea(), "SetConstNull retains reusable area")
	source := batch.NewWithSize(1)
	source.Vecs[0] = constant
	source.SetRowCount(4)

	physical, selected, err := unionBatchAreaProjection(constant, 0, 4)
	require.NoError(t, err)
	require.Zero(t, physical)
	require.Zero(t, selected)

	projection, err := (&HashmapBuilder{}).projectRetainedRecovery(source)
	require.NoError(t, err)
	require.Equal(t, 4, projection.maxRows)
	require.Equal(t, uint64(4*types.VarlenaSize), projection.maxSelected)
	require.Equal(t, projection.maxSelected, projection.nextTailSelected)

	source.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestRecoveryProjectionRejectsMissingPhysicalRows(t *testing.T) {
	for _, test := range []struct {
		name string
		rows int
		vec  func(*testing.T, *mpool.MPool) *vector.Vector
	}{
		{
			name: "empty constant",
			rows: 1,
			vec: func(_ *testing.T, mp *mpool.MPool) *vector.Vector {
				return vector.NewConstNull(types.T_varchar.ToType(), 0, mp)
			},
		},
		{
			name: "short flat vector",
			rows: 2,
			vec: func(t *testing.T, mp *mpool.MPool) *vector.Vector {
				t.Helper()
				vec := vector.NewVec(types.T_varchar.ToType())
				require.NoError(t, vector.AppendBytes(vec, []byte("value"), false, mp))
				return vec
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			source := batch.NewWithSize(1)
			source.Vecs[0] = test.vec(t, mp)
			source.SetRowCount(test.rows)

			_, err := projectedSelectedRange(source, 0, test.rows)
			require.ErrorIs(t, err, process.ErrExecutionResourceInvalid)

			source.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestRecoveryProjectionUsesIncrementalPartialTail(t *testing.T) {
	mp := mpool.MustNewZero()
	tail := batch.NewWithSize(1)
	tail.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2, 3}, nil, mp)
	tail.SetRowCount(3)
	defer tail.Clean(mp)
	source := batch.NewWithSize(1)
	source.Vecs[0] = testutil.MakeInt64Vector([]int64{4, 5, 6, 7}, nil, mp)
	source.SetRowCount(4)
	defer source.Clean(mp)

	builder := HashmapBuilder{}
	builder.Batches.Buf = []*batch.Batch{tail}
	builder.retainedSpillTailSelected = 3 * 8
	projection, err := builder.projectRetainedRecovery(source)
	require.NoError(t, err)
	require.Equal(t, 7, projection.maxRows)
	require.Equal(t, uint64(7*8), projection.maxSelected)
	require.Equal(t, uint64(7*8), projection.nextTailSelected)
}

func TestExpressionRecoveryIncludesReplacementOverlap(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := makeIssue26454ConcatKey(t, proc)
	peak, err := expressionVectorPeak(proc, expr, 1024, false)
	require.NoError(t, err)
	recovery, err := expressionRecoveryBytes(proc, []*plan.Expr{expr}, 1024, false)
	require.NoError(t, err)
	require.Equal(t, 2*peak, recovery)
}
