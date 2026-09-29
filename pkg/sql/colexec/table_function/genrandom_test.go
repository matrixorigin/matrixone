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

package table_function

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestGenerateRandomProjectedColumns(t *testing.T) {
	proc := testutil.NewProc(t)
	for _, method := range []rMethod{
		rMethodInt64, rMethodInt64N, rMethodFloat64,
		rMethodExpFloat64, rMethodNormalFloat64,
	} {
		intValues := method == rMethodInt64 || method == rMethodInt64N
		valueName := "f64"
		valueType := types.T_float64.ToType()
		if intValues {
			valueName = "i64"
			valueType = types.T_int64.ToType()
		}
		for _, layout := range [][]string{
			{"nth", valueName}, {"nth"}, {valueName}, {valueName, "nth"}, nil,
		} {
			for _, total := range []int64{0, 3, 8193} {
				tf := &TableFunction{Attrs: layout}
				for _, name := range layout {
					if name == "nth" {
						tf.ctr.retSchema = append(tf.ctr.retSchema, types.T_int64.ToType())
					} else {
						tf.ctr.retSchema = append(tf.ctr.retSchema, valueType)
					}
				}
				st := &genRandomState{
					total: total, genInt64: intValues, method: method,
					iMax: 10, r: rand.New(rand.NewSource(42)),
					batch: tf.createResultBatch(),
				}
				want := rand.New(rand.NewSource(42))
				var seen int64
				batches := 0
				for {
					result, err := st.call(tf, proc)
					require.NoError(t, err)
					if result.Status == vm.ExecStop {
						break
					}
					rows := result.Batch.RowCount()
					require.Greater(t, rows, 0)
					require.LessOrEqual(t, rows, 8192)
					batches++
					for i := 0; i < rows; i++ {
						var intValue int64
						var floatValue float64
						switch method {
						case rMethodInt64:
							intValue = want.Int63()
						case rMethodInt64N:
							intValue = want.Int63n(10)
						case rMethodFloat64:
							floatValue = want.Float64()
						case rMethodExpFloat64:
							floatValue = want.ExpFloat64()
						case rMethodNormalFloat64:
							floatValue = want.NormFloat64()
						}
						for col, name := range layout {
							if name == "nth" {
								require.Equal(t, seen+int64(i)+1,
									vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[col])[i])
							} else if intValues {
								require.Equal(t, intValue,
									vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[col])[i])
							} else {
								require.Equal(t, floatValue,
									vector.MustFixedColWithTypeCheck[float64](result.Batch.Vecs[col])[i])
							}
						}
					}
					for _, vec := range result.Batch.Vecs {
						require.Equal(t, rows, vec.Length())
					}
					seen += int64(rows)
				}
				require.Equal(t, total, seen)
				require.Equal(t, int((total+8191)/8192), batches)
				require.Equal(t, want.Int63(), st.r.Int63(),
					"pruned columns must not change the seeded random sequence")
				st.free(tf, proc, false, nil)
			}
		}
	}
}
