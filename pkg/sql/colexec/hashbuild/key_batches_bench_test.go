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
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Keep this benchmark on the existing public builder/operator entry points so
// the identical fixture can compare the parent and the bounded implementation.
func BenchmarkHashBuildRuntimeKeys(b *testing.B) {
	for _, oid := range []types.T{types.T_int64, types.T_float64, types.T_varchar} {
		for _, rows := range []int{1024, 8192, 65536} {
			for _, mode := range []string{"off", "in", "membership"} {
				b.Run(fmt.Sprintf("%s/rows=%d/%s", oid.String(), rows, mode), func(b *testing.B) {
					typ := oid.ToType()
					tc := newTestCase(b, []types.Type{typ}, []*plan.Expr{newExpr(0, typ)})
					defer tc.proc.Free()
					defer tc.arg.Free(tc.proc, false, nil)
					input := batch.NewWithSize(1)
					input.Vecs[0] = vector.NewVec(typ)
					defer input.Clean(tc.proc.Mp())
					for _, key := range rand.New(rand.NewSource(42)).Perm(rows) {
						if oid == types.T_int64 {
							require.NoError(b, vector.AppendFixed(input.Vecs[0], int64(key), false, tc.proc.Mp()))
						} else if oid == types.T_float64 {
							require.NoError(b, vector.AppendFixed(input.Vecs[0], float64(key), false, tc.proc.Mp()))
						} else {
							require.NoError(b, vector.AppendBytes(input.Vecs[0], []byte(fmt.Sprintf("a non-inline runtime key %016d", key)), false, tc.proc.Mp()))
						}
					}
					input.SetRowCount(rows)
					if mode == "in" {
						tc.arg.RuntimeFilterSpec = rawRuntimeFilterSpec(101, int32(rows*2), typ)
						if oid == types.T_float64 {
							tc.arg.RuntimeFilterSpec.KeyEncoding = plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1
						}
					} else if mode == "membership" {
						tc.arg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: 101, Expr: newExpr(0, typ), UseMembershipFilter: true}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						require.NoError(b, tc.arg.Prepare(tc.proc))
						builder := &tc.arg.ctr.hashmapBuilder
						builder.InputBatchRowCount = rows
						require.NoError(b, builder.CopyBuildBatch(input, tc.proc))
						require.NoError(b, builder.BuildHashmap(true, false, mode != "off", tc.proc))
						if mode != "off" {
							require.NoError(b, tc.arg.handleRuntimeFilter(tc.proc))
						}
						tc.arg.Reset(tc.proc, false, nil)
						tc.proc.GetMessageBoard().Reset()
					}
				})
			}
		}
	}
}
