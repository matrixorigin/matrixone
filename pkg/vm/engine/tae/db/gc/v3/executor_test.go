// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestBuildBloomfilter(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rows         int
		probability  float64
		readFailure  bool
		cancelBefore bool
		cancelDuring bool
	}{
		{name: "explicit sizing", rows: 64, probability: 0.000001},
		// A high explicit probability keeps the default-row contract test small.
		{name: "default rows", probability: 0.9},
		{name: "default probability", rows: 64},
		{name: "reader failure", rows: 64, probability: 0.01, readFailure: true},
		{name: "cancel before acquisition", rows: 64, probability: 0.01, cancelBefore: true},
		{name: "cancel during read", rows: 64, probability: 0.01, cancelDuring: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			typ := types.T_uint64.ToType()
			buffer := containers.NewOneSchemaBatchBuffer(1024, []string{"id"}, []types.Type{typ}, false)
			t.Cleanup(func() { buffer.Close(mp) })
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("GC reader stopped")
			if tc.cancelBefore {
				cancel(cause)
			}
			calls := 0
			values := []uint64{1, 7, 17}
			source := func(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, bat *batch.Batch) (bool, error) {
				calls++
				if tc.readFailure {
					return false, cause
				}
				if calls > 1 {
					return true, nil
				}
				if err := vector.AppendFixedList(bat.Vecs[0], values, nil, mp); err != nil {
					return false, err
				}
				bat.SetRowCount(len(values))
				if tc.cancelDuring {
					cancel(cause)
				}
				return false, nil
			}
			bf, err := BuildBloomfilter(ctx, tc.rows, tc.probability, 0, source, buffer, mp)
			if bf != nil {
				t.Cleanup(bf.Free)
			}
			if tc.cancelBefore {
				require.ErrorIs(t, err, cause)
				require.Nil(t, bf)
				require.Zero(t, calls)
				require.Zero(t, buffer.Len(), "cancelled work must not acquire a batch")
				return
			}
			require.Equal(t, 1, buffer.Len(), "the acquired batch must be returned on every terminal path")
			if tc.readFailure || tc.cancelDuring {
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, calls)
				return
			}
			require.NoError(t, err)
			require.True(t, bf.Valid())
			require.Equal(t, 2, calls)
			probe := vector.NewVec(typ)
			t.Cleanup(func() { probe.Free(mp) })
			require.NoError(t, vector.AppendFixedList(probe, values, nil, mp))
			checked := 0
			bf.Test(probe, func(exists bool, _ int) { require.True(t, exists); checked++ })
			require.Equal(t, len(values), checked)
		})
	}
}
