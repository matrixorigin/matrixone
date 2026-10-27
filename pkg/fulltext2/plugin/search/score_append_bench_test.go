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

package search

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"testing"
)

// Frozen2465 complete reader consumer, keeping the same caller/inlining scope.
func (u *reader) appendOutputRangeBeforeBulk(res *vectorindex.SearchOutput, start, n int, mp *mpool.MPool, out *batch.Batch) error {
	if u.pkVecIdx >= 0 {
		if err := vectorindex.AppendColumnBufferRange(res.Keys, out.Vecs[u.pkVecIdx], start, n, mp); err != nil {
			return err
		}
	}
	if u.scoreVecIdx >= 0 {
		vec := out.Vecs[u.scoreVecIdx]
		for i := start; i < start+n; i++ {
			if err := vector.AppendFixed[float32](vec, res.Dists[i], false, mp); err != nil {
				return err
			}
		}
	}
	for _, ic := range u.includeOut {
		vec := out.Vecs[ic.vecIdx]
		if ic.segPos >= 0 && ic.segPos < len(res.Include) {
			if err := vectorindex.AppendColumnBufferRange(res.Include[ic.segPos], vec, start, n, mp); err != nil {
				return err
			}
		} else {
			for j := 0; j < n; j++ {
				if err := vector.AppendAny(vec, nil, true, mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var scoreAppendBenchmarkSink int

func BenchmarkReaderScoreAppend(b *testing.B) {
	for _, n := range []int{1, 15, 16, 8192} {
		for _, pruned := range []bool{false, true} {
			for _, fresh := range []bool{false, true} {
				for _, bulk := range []bool{false, true} {
					b.Run(fmt.Sprintf("rows=%d/pruned=%t/fresh=%t/bulk=%t", n, pruned, fresh, bulk), func(b *testing.B) {
						mp := mpool.MustNewZero()
						defer mpool.DeleteMPool(mp)
						out := batch.NewWithSize(1)
						out.Vecs[0] = vector.NewVec(types.T_float32.ToType())
						// Fresh cases replace this vector after releasing all owned data. The
						// deferred batch cleanup covers assertion/error exits as well.
						defer out.Clean(mp)
						scores := make([]float32, n+1)
						for i := range scores {
							scores[i] = float32(i) + 0.5
						}
						res := &vectorindex.SearchOutput{Dists: scores}
						u := &reader{pkVecIdx: -1, scoreVecIdx: 0}
						if pruned {
							u.scoreVecIdx = -1
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if fresh {
								out.Vecs[0].Free(mp)
								out.Vecs[0] = vector.NewVec(types.T_float32.ToType())
							}
							out.CleanOnlyData()
							var err error
							if bulk {
								err = u.appendOutputRange(res, 1, n, mp, out)
							} else {
								err = u.appendOutputRangeBeforeBulk(res, 1, n, mp, out)
							}
							if err != nil {
								b.Fatal(err)
							}
							scoreAppendBenchmarkSink = out.Vecs[0].Length()
						}
					})
				}
			}
		}
	}
}
