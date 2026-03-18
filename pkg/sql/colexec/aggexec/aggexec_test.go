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

package aggexec

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// trivialGroupCtx satisfies AggGroupExecContext with no-op marshal/unmarshal.
type trivialGroupCtx struct{}

func (trivialGroupCtx) Marshal() []byte                { return nil }
func (trivialGroupCtx) MarshalBinary() ([]byte, error) { return nil, nil }
func (trivialGroupCtx) Unmarshal([]byte)               {}
func (trivialGroupCtx) Size() int64                    { return 0 }

// TestGrowGroupContexts exercises growGroupContexts via an agg registered with
// a non-nil initGroupContext.
func TestGrowGroupContexts(t *testing.T) {
	mp := mpool.MustNewZero()

	const testAggID = -9901
	param := types.T_int64.ToType()
	ret := types.T_int64.ToType()

	RegisterAggFromFixedRetFixed(
		MakeSingleColumnAggInformation(testAggID, param, func([]types.Type) types.Type { return ret }, true),
		nil,
		func(resultType types.Type, parameters ...types.Type) AggGroupExecContext { return trivialGroupCtx{} },
		func(resultType types.Type, parameters ...types.Type) int64 { return 0 },
		func(_ AggGroupExecContext, _ AggCommonExecContext, v int64, empty bool, get AggGetter[int64], set AggSetter[int64]) error {
			set(get() + v)
			return nil
		},
		nil, nil, nil,
	)

	info := singleAggInfo{
		aggID:     testAggID,
		argType:   param,
		retType:   ret,
		emptyNull: false,
	}
	impl := registeredAggFunctions[generateKeyOfSingleColumnAgg(testAggID, param)]
	exec := newAggregatorFromFixedToFixed[int64](mp, info, impl)

	// First grow allocates beyond cap.
	require.NoError(t, exec.GroupGrow(2))
	// PreAllocate sets cap > len, so next grow fits within cap (else branch).
	require.NoError(t, exec.PreAllocateGroups(4))
	require.NoError(t, exec.GroupGrow(2))
	exec.Free()
}

// TestVectorsUnmarshalFromReader exercises Vectors.UnmarshalFromReader via a
// median exec roundtrip.
func TestVectorsUnmarshalFromReader(t *testing.T) {
	mp := mpool.MustNewZero()

	param := types.T_float64.ToType()
	exec, err := makeMedian(mp, 0, false, param)
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(2))

	v := vector.NewVec(param)
	require.NoError(t, vector.AppendFixed(v, float64(1), false, mp))
	require.NoError(t, vector.AppendFixed(v, float64(3), false, mp))
	require.NoError(t, exec.Fill(0, 0, []*vector.Vector{v}))
	require.NoError(t, exec.Fill(1, 1, []*vector.Vector{v}))
	v.Free(mp)

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(2, [][]uint8{{1}, {1}}, &buf))

	exec2, err := makeMedian(mp, 0, false, param)
	require.NoError(t, err)
	r := bytes.NewReader(buf.Bytes())
	require.NoError(t, exec2.UnmarshalFromReader(r, mp))
	// 8 trailing bytes = extra_cnt=0 written by SaveIntermediateResult, not consumed by median
	require.LessOrEqual(t, r.Len(), 8)

	exec.Free()
	exec2.Free()
}
