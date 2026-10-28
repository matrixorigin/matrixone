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

package table_scan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkBooleanFilterSelection(b *testing.B) {
	for _, rows := range []int{0, 1, 256, 8192} {
		for _, shape := range []string{"true", "false", "mixed", "nullable", "const_true", "const_null"} {
			b.Run(fmt.Sprintf("rows=%d/%s", rows, shape), func(b *testing.B) {
				proc := testutil.NewProc(b)
				boolType := types.T_bool.ToType()
				scan := &TableScan{Attrs: []string{"mask"}, Types: []pb.Type{{Id: int32(types.T_bool)}}, FilterExprs: []*pb.Expr{lateTestCol(0, boolType)}}
				b.Cleanup(func() { scan.Free(proc, false, nil) })
				require.NoError(b, scan.Prepare(proc))
				input := batch.NewWithSize(1)
				b.Cleanup(func() { input.Clean(proc.Mp()) })
				values := make([]bool, rows)
				for i := range values {
					values[i] = shape == "true" || (shape == "mixed" || shape == "nullable") && i%3 != 1
				}
				switch shape {
				case "const_true":
					v, err := vector.NewConstFixed(boolType, true, rows, proc.Mp())
					require.NoError(b, err)
					input.Vecs[0] = v
				case "const_null":
					input.Vecs[0] = vector.NewConstNull(boolType, rows, proc.Mp())
				default:
					input.Vecs[0] = vector.NewVec(boolType)
					require.NoError(b, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					input.Vecs[0].SetLength(rows)
					// Restore the mask compacted by the preceding call. This
					// bounded input refresh is included in both owner timings.
					if !input.Vecs[0].IsConst() {
						copy(vector.MustFixedColNoTypeCheck[bool](input.Vecs[0]), values)
						input.Vecs[0].GetNulls().Reset()
						if shape == "nullable" {
							for i := 0; i < rows; i += 4 {
								input.Vecs[0].GetNulls().Add(uint64(i))
							}
						}
					}
					input.SetRowCount(rows)
					if _, err := scan.evalFilter(proc, input, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
