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

package window

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestMaterializedRowNumberFitsOutputBudget(t *testing.T) {
	for _, tc := range []struct {
		name              string
		limit             uint64
		wantCapacityError bool
	}{
		{name: "output fits without scratch", limit: 96 << 10},
		{name: "output exceeds budget", limit: 32 << 10, wantCapacityError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc, generation, account := localRankTestBudget(t, tc.limit)
			w := new(Window)
			w.WinSpecList = []*plan.Expr{makeWindowSpec()}
			w.WinSpecList[0].GetW().Name = "row_number"
			w.ctr.bat = batch.NewWithSize(0)
			w.ctr.bat.SetRowCount(8192)
			w.ctr.ps = []int64{0, 2048, 6144}
			require.NoError(t, w.SetAllocationAccount(account))
			defer func() {
				w.Free(proc, false, nil)
				require.NoError(t, w.ClearAllocationAccount(account))
			}()

			result, err := w.ctr.processOrderFuncRange(0, w, proc, 0, 8192)
			if tc.wantCapacityError {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				func() {
					defer result.Free(proc.Mp())
					values := vector.MustFixedColWithTypeCheck[uint64](result)
					require.Len(t, values, 8192)
					require.Equal(t, uint64(1), values[0])
					require.Equal(t, uint64(2048), values[2047])
					require.Equal(t, uint64(1), values[2048])
					require.Equal(t, uint64(4096), values[6143])
					require.Equal(t, uint64(1), values[6144])
					require.Equal(t, uint64(2048), values[8191])
				}()
			}
			require.Zero(t, generation.Used())
			require.Zero(t, account.Snapshot().Used)
		})
	}
}
