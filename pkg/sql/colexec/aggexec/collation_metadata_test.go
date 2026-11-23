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

package aggexec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAggregateCollationAdmission(t *testing.T) {
	typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
	for _, id := range []int64{AggIdOfMin, AggIdOfMax} {
		for _, spill := range []bool{false, true} {
			for _, revision := range []uint8{0, 1, 2} {
				// Compact spills carry no type tag: their type is admitted by
				// MakeAgg, rather than decoded from the vector payload.
				if spill && revision != 0 {
					continue
				}
				t.Run(fmt.Sprintf("agg_%d/spill_%t/revision_%d", id, spill, revision), func(t *testing.T) {
					mp := mpool.MustNewZero()
					defer func() {
						require.Zero(t, mp.CurrNB())
						mpool.DeleteMPool(mp)
					}()
					input := vector.NewVec(typ)
					defer input.Free(mp)
					require.NoError(t, vector.AppendBytes(input, []byte("A"), false, mp))
					source, err := MakeAgg(mp, id, false, typ)
					require.NoError(t, err)
					defer source.Free()
					require.NoError(t, source.GroupGrow(1))
					require.NoError(t, source.BulkFill(0, []*vector.Vector{input}))
					// The declared argument stays legacy; only the inner wire vector changes.
					source.(PrepareParamKindStateAccessor).PrepareParamKindVectorForChunk(0).GetType().CollationVersion = revision
					var wire bytes.Buffer
					if spill {
						require.NoError(t, source.(SpillStateCodec).SaveSpillIntermediateRows(0, []int32{0}, &wire))
					} else {
						require.NoError(t, source.SaveIntermediateResultOfChunk(0, &wire))
					}
					target, err := MakeAgg(mp, id, false, typ)
					require.NoError(t, err)
					defer target.Free()
					before := mp.CurrNB()
					if spill {
						err = target.(SpillStateCodec).UnmarshalSpillFromReader(&wire, mp)
					} else {
						err = target.UnmarshalFromReader(&wire, mp)
					}
					if revision != 0 {
						require.ErrorContains(t, err, "collation")
						require.Equal(t, before, mp.CurrNB(), "rejected state must be freed")
						return
					}
					require.NoError(t, err)
					result, err := target.Flush()
					require.NoError(t, err)
					defer func() {
						for _, vec := range result {
							vec.Free(mp)
						}
					}()
					require.Equal(t, "A", result[0].GetStringAt(0))
				})
			}
		}
	}
}

func TestAggregateFactoryRejectsDisabledCollation(t *testing.T) {
	typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
	typ.CollationVersion = 1
	exec, err := MakeAgg(nil, AggIdOfMin, false, typ)
	require.ErrorContains(t, err, "disabled")
	require.Nil(t, exec)
}
