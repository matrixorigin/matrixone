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

package incrservice

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// This diagnoses reservation amplification with actual bulk demand, without
// SQL, a live service or a wall-clock assertion. The historical control changes
// only the speculative prefetch call that preceded #29527. It is not a proposed
// restoration of that policy for ordinary user-visible AUTO_INCREMENT columns.
// An uncommitted cache matches COPY's allocation mode, but the in-memory store
// deliberately does not simulate the SQL store's growing transaction workspace.
func BenchmarkAutoIDCacheBulkAllocationRequests(b *testing.B) {
	const (
		actualRows = 1_000_000
		batchRows  = 8192
		estimate   = int64(1_000_000_000)
	)
	for _, committed := range []bool{false, true} {
		for _, legacyEstimate := range []bool{false, true} {
			b.Run(fmt.Sprintf("committed=%t/legacy-estimate=%t", committed, legacyEstimate), func(b *testing.B) {
				runtime.RunTest("", func(runtime.Runtime) {
					ctx := defines.AttachAccountId(b.Context(), catalog.System_Account)
					allocationRequests := 0
					reservedValues := uint64(0)
					for range b.N {
						func() {
							store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
							// Use the production CREATE qualification so this fixture also
							// measures an unmodified main allocator with the same input.
							cols := GetAutoColumnFromDef(&plan.TableDef{Cols: []*plan.ColDef{{
								Name: catalog.FakePrimaryKeyColName, Hidden: true,
								Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
							}}})
							require.NoError(b, store.Create(ctx, 0, cols, nil))
							cfg := Config{}
							cfg.adjust()
							allocator := newValueAllocator("", store)
							defer allocator.close()
							cache, err := newTableCache(ctx, "", 0, 0, cols, cfg, allocator, nil, committed)
							require.NoError(b, err)
							defer cache.close()
							cc := cache.(*tableCache).getColumnCache(catalog.FakePrimaryKeyColName)
							mp := mpool.MustNewZero()
							defer mpool.DeleteMPool(mp)
							next := uint64(1)
							for consumed := 0; consumed < actualRows; {
								rows := min(batchRows, actualRows-consumed)
								func() {
									vec := vector.NewVec(types.T_uint64.ToType())
									defer vec.Free(mp)
									require.NoError(b, vector.AppendFixedList(vec, make([]uint64, rows), nil, mp))
									vec.GetNulls().AddRange(0, uint64(rows))
									if legacyEstimate {
										// Exactly the old tableCache prefetch request. The
										// current bounded call below cannot shrink it.
										cc.preAllocate(ctx, 0, int(estimate), nil)
									}
									_, err := cache.insertAutoValues(ctx, 0, []*vector.Vector{vec}, rows, estimate)
									require.NoError(b, err)
									for _, value := range vector.MustFixedColWithTypeCheck[uint64](vec) {
										require.Equal(b, next, value)
										next++
									}
								}()
								consumed += rows
							}
							cc.Lock()
							err = cc.waitPrevAllocatingLocked(ctx)
							cc.Unlock()
							require.NoError(b, err)
							require.Equal(b, uint64(actualRows+1), next)
							requests := store.requests()
							allocationRequests += len(requests)
							for _, count := range requests {
								reservedValues += uint64(count)
							}
							require.Zero(b, mp.CurrNB())
						}()
					}
					b.ReportMetric(float64(allocationRequests)/float64(b.N), "range-requests/op")
					b.ReportMetric(float64(reservedValues)/float64(b.N), "reserved-values/op")
				})
			})
		}
	}
}
