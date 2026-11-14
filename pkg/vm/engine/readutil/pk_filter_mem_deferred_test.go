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

package readutil

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestMemPKFilterDeferredINConcurrentRead(t *testing.T) {
	for _, disjunction := range []bool{false, true} {
		name := "IN"
		if disjunction {
			name = "OR"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			var vectors []*vector.Vector
			t.Cleanup(func() {
				for _, v := range vectors {
					v.Free(mp)
				}
				require.Zero(t, mp.CurrNB())
			})
			makeValues := func(values ...int64) *vector.Vector {
				v := vector.NewVec(types.T_int64.ToType())
				vectors = append(vectors, v)
				require.NoError(t, vector.AppendFixedList(v, values, nil, mp))
				return v
			}
			pool := fileservice.NewPool(1, func() *types.Packer { p := types.NewPacker(); t.Cleanup(p.Close); return p }, func(p *types.Packer) { p.Reset() }, nil)
			base := BasePKFilter{Valid: true, Op: function.IN, Oid: types.T_int64, Vec: makeValues(7, 1, 3, 1)}
			want := uint64(1<<1 | 1<<3 | 1<<7)
			if disjunction {
				base = BasePKFilter{Valid: true, Disjuncts: []BasePKFilter{base, {Valid: true, Op: function.IN, Oid: types.T_int64, Vec: makeValues(4, 0, 2)}}}
				want |= 1<<0 | 1<<2 | 1<<4
			}
			def := &plan.TableDef{Name: "t", Pkey: &plan.PrimaryKeyDef{Names: []string{"pk"}, PkeyColName: "pk"}, Cols: []*plan.ColDef{{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}}}}
			filter, err := newMemPKFilter(def, types.MaxTs().ToTimestamp(), pool, base, engine.FilterHint{}, true)
			require.NoError(t, err)
			require.True(t, filter.Valid())
			copied := filter
			input := makeValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
			const workers = 16
			start := make(chan struct{})
			results := make(chan uint64, workers)
			var wg sync.WaitGroup
			for worker := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					f := &filter
					if worker%2 == 1 {
						f = &copied
					}
					if worker%3 == 0 {
						f.PrepareWorkspaceFilter()
					}
					packer := types.NewPacker()
					defer packer.Close()
					mask := objectio.GetReusableBitmap()
					defer mask.Release()
					f.FilterVector(input, packer, &mask)
					var kept uint64
					for i := range 10 {
						if !mask.Contains(uint64(i)) {
							kept |= 1 << i
						}
					}
					results <- kept
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			for got := range results {
				require.Equal(t, want, got)
			}
			filter.PrepareWorkspaceFilter()
			copied.PrepareWorkspaceFilter()
		})
	}
}

func TestMemPKFilterPublicINSnapshot(t *testing.T) {
	packer := types.NewPacker()
	defer packer.Close()
	encode := func(values ...int64) [][]byte {
		keys := make([][]byte, 0, len(values))
		for _, v := range values {
			keys = append(keys, EncodePrimaryKey(v, packer))
		}
		return keys
	}
	var filter MemPKFilter
	filter.SetFullData(function.IN, true, encode(1, 3, 7)...)
	copied := filter
	filter.SetFullData(function.IN, true, encode(2)...)
	for _, v := range []int64{1, 2, 3, 7} {
		require.Equal(t, v == 2, filter.matches(encode(v)[0]))
		require.Equal(t, v != 2, copied.matches(encode(v)[0]))
	}
	filter.SetFullData(function.IN, true)
	filter.PrepareWorkspaceFilter()
	require.False(t, filter.matches(encode(2)[0]))
}
