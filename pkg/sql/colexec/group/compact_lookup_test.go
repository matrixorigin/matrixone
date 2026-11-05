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

package group

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestCompactGroupCanonicalKeysAndSpill(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns int
		spill   int64
	}{
		{"single", 1, 0}, {"tuple", 2, 0}, {"spill", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			typ := types.New(types.T_char, 1, 0)
			input := batch.NewWithSize(tc.columns)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			exprs := make([]*plan.Expr, tc.columns)
			for col := range tc.columns {
				input.Vecs[col] = vector.NewVec(typ)
				exprs[col] = colExprWithType(int32(col), typ)
			}
			const rows = 4 * hashmap.UnitLimit
			expected := make(map[string]int64)
			for row := 0; row < rows; row++ {
				// The last chunk introduces a new key after accelerated hits.
				key := []string{"", " ", "\x00", "A"}[row%4]
				if row >= 3*hashmap.UnitLimit && row%4 == 3 {
					key = "B"
				}
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(key), false, proc.Mp()))
				canonical := key
				if key == " " {
					canonical = ""
				}
				if tc.columns == 2 {
					other := string([]byte{byte('x' + row%2)})
					require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(other), false, proc.Mp()))
					canonical += "|" + other
				}
				expected[canonical]++
			}
			input.SetRowCount(rows)
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
			g := newGroupOp(proc, exprs, []aggexec.AggFuncExecExpression{countStarAgg()})
			g.SpillMem = tc.spill
			g.AppendChild(child)
			allocation := installGroupTestAllocation(t, g, proc, 64<<20)
			t.Cleanup(func() {
				g.Free(proc, false, nil)
				finalizeGroupTestAllocation(t, g, allocation)
			})
			require.NoError(t, g.Prepare(proc))
			got := make(map[string]int64)
			for {
				result, err := vm.Exec(g, proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				counts := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[tc.columns])
				for row, count := range counts {
					key := string(result.Batch.Vecs[0].GetBytesAt(row))
					if key == " " {
						key = ""
					}
					if tc.columns == 2 {
						key += "|" + string(result.Batch.Vecs[1].GetBytesAt(row))
					}
					got[key] += count
				}
			}
			require.Equal(t, expected, got)
			require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupCompactLookupRows"])
			if tc.spill != 0 {
				require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
			}
		})
	}
}

func TestCompactGroupFallbackAndGeneration(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	vec := vector.NewVec(types.New(types.T_char, 1, 0))
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vector.AppendBytes(vec, []byte("A"), false, mp))
	ctr := container{mp: mp}
	ctr.hr.mp = mp
	t.Cleanup(ctr.hr.Free0)
	ctr.hr.compactRows = 2 * hashmap.UnitLimit
	_, hit, err := ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.False(t, hit)
	require.NotNil(t, ctr.hr.compact)
	// No publication: repeated preview must remain a miss.
	_, hit, err = ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.False(t, hit)
	ctr.hr.publishCompactGroups([]uint64{7})
	preview, hit, err := ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.True(t, hit)
	require.True(t, preview.existingOnly)
	require.Equal(t, []uint64{7}, preview.values)
	ctr.groupKeyStringSourceMetadata = true
	_, hit, err = ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.False(t, hit, "resident source metadata keeps the generic publication path")
	ctr.groupKeyStringSourceMetadata = false
	vec.GetNulls().Add(0)
	_, hit, err = ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.False(t, hit)
	vec.GetNulls().Reset()
	require.NoError(t, vector.SetBytesAt(vec, 0, []byte("é"), mp))
	_, hit, err = ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.False(t, hit)
	ctr.hr.freeCompactGroups()
	require.Nil(t, ctr.hr.compact)
	require.Zero(t, ctr.hr.compactRows)
}

func TestCompactGroupAdmissionFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	vec := vector.NewVec(types.New(types.T_char, 1, 0))
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vector.AppendBytes(vec, []byte("A"), false, mp))
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(128)
	require.NoError(t, err)
	ctr := container{mp: mp, allocationAccount: account}
	ctr.hr.mp = mp
	t.Cleanup(ctr.hr.Free0)
	ctr.hr.compactRows = 2 * hashmap.UnitLimit
	_, hit, err := ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.NoError(t, err)
	require.False(t, hit)
	require.Nil(t, ctr.hr.compact)
	require.True(t, ctr.hr.compactDisabled)
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	ctr.hr.freeCompactGroups()
	ctr.hr.compactRows = 2 * hashmap.UnitLimit
	_, _, err = ctr.previewCompactGroups([]*vector.Vector{vec}, 0, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
}
