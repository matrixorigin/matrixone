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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestConstGroupingSelectionCopies(t *testing.T) {
	for _, shape := range []string{"fixed", "bytes", "null", "rollup", "flat-control"} {
		for _, method := range []string{"union", "union-int32", "batch", "batch-flags", "batch-preflighted", "batch-preflighted-flags", "empty-flags", "multi-control"} {
			t.Run(shape+"/"+method, func(t *testing.T) {
				mp := mpool.MustNewZero()
				typ := types.T_varchar.ToType()
				if shape == "fixed" {
					typ = types.T_int64.ToType()
				}
				var source *Vector
				var err error
				switch shape {
				case "fixed":
					source, err = NewConstFixed(typ, int64(7), 5, mp)
				case "bytes":
					source, err = NewConstBytes(typ, []byte("constant payload"), 5, mp)
				case "null":
					source = NewConstNull(typ, 5, mp)
				case "rollup":
					source = NewRollupConst(typ, 5, mp)
				case "flat-control":
					source = NewVec(typ)
					for range 5 {
						require.NoError(t, AppendBytes(source, []byte("constant payload"), false, mp))
					}
				}
				require.NoError(t, err)
				if shape != "rollup" {
					source.GetGrouping().Add(0, 2, 8)
				}
				state := newTestVectorAllocationAccount(t, 1<<20, 64)
				dst := newAccountedTestVector(t, typ)
				dmp := state.view(t, mp)
				defer func() {
					source.Free(mp)
					dst.Free(mp)
					finalizeTestVectorAllocationAccount(t, state)
					require.Zero(t, mp.CurrNB())
				}()
				// Existing data and marks must survive repeated appends.
				require.NoError(t, dst.UnionOne(source, 0, dmp))
				// At most five selected rows are appended in each of two rounds.
				want := make([]bool, 1, 11)
				want[0] = true
				for range 2 {
					var rows []int64
					switch method {
					case "union":
						rows = []int64{4, 2, 0, 2, 1}
						err = dst.Union(source, rows, dmp)
					case "union-int32":
						rows = []int64{4, 2, 0, 2, 1}
						err = dst.UnionInt32(source, []int32{4, 2, 0, 2, 1}, dmp)
					case "batch":
						rows = []int64{1, 2, 3, 4}
						err = dst.UnionBatch(source, 1, 4, nil, dmp)
					case "batch-flags":
						rows = []int64{1, 2, 4}
						err = dst.UnionBatch(source, 1, 4, []uint8{1, 1, 0, 1}, dmp)
					case "batch-preflighted":
						rows = []int64{1, 2, 3, 4}
						flags := []uint8{1, 1, 1, 1}
						var preflight SelectedBatchPreflight
						preflight, err = dst.PreExtendSelectedBatch(source, 1, 4, flags, dst.Length()+4, dmp)
						require.NoError(t, err)
						err = dst.UnionBatchPreflighted(source, 1, 4, flags, &preflight, dmp)
					case "batch-preflighted-flags":
						rows = []int64{1, 2, 4}
						flags := []uint8{1, 1, 0, 1}
						var preflight SelectedBatchPreflight
						preflight, err = dst.PreExtendSelectedBatch(source, 1, 4, flags, dst.Length()+3, dmp)
						require.NoError(t, err)
						err = dst.UnionBatchPreflighted(source, 1, 4, flags, &preflight, dmp)
					case "empty-flags":
						err = dst.UnionBatch(source, 1, 4, []uint8{0, 0, 0, 0}, dmp)
					case "multi-control":
						rows = []int64{2, 2, 2}
						err = dst.UnionMulti(source, 2, 3, dmp)
					}
					require.NoError(t, err)
					for _, row := range rows {
						want = append(want, shape == "rollup" || row == 0 || row == 2)
					}
				}
				require.Equal(t, len(want), dst.Length())
				for row, grouping := range want {
					require.Equal(t, grouping, dst.GetGrouping().Contains(uint64(row)), "row %d", row)
					require.Equal(t, shape == "null" || shape == "rollup", dst.IsNull(uint64(row)))
					if shape == "fixed" {
						require.Equal(t, int64(7), GetFixedAtNoTypeCheck[int64](dst, row))
					}
					if shape == "bytes" || shape == "flat-control" {
						require.Equal(t, "constant payload", dst.GetStringAt(row))
					}
				}
				marked := 0
				for _, grouping := range want {
					if grouping {
						marked++
					}
				}
				require.EqualValues(t, marked, dst.GetGrouping().Count())
			})
		}
	}
}
