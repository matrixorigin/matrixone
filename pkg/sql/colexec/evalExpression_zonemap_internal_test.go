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

package colexec

import (
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestAnyNotEqualZoneMap(t *testing.T) {
	key := makeInternalVarcharZoneMap("key")
	keep := makeInternalVarcharZoneMap("keep")

	res, ok := anyNotEqualZoneMap(key, keep)
	require.True(t, ok)
	require.True(t, res)

	res, ok = anyNotEqualZoneMap(key, key)
	require.True(t, ok)
	require.False(t, res)

	res, ok = anyNotEqualZoneMap(makeInternalVarcharZoneMap("key", "keep"), key)
	require.True(t, ok)
	require.True(t, res)

	_, ok = anyNotEqualZoneMap(key, objectio.NewZM(types.T_varchar, 0))
	require.False(t, ok)
}

func TestFoldBooleanZoneMapUnknownPaths(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	trueExpr := makeInternalBoolLitExpr(true, 0)
	falseExpr := makeInternalBoolLitExpr(false, 1)
	unknownExpr := &plan.Expr{
		Typ:   plan.Type{Id: int32(types.T_bool)},
		AuxId: 2,
	}

	zms := make([]objectio.ZoneMap, 4)
	vecs := make([]*vector.Vector, 4)
	require.False(t, foldAndZoneMap(ctx, proc, nil, nil, zms, vecs, []*plan.Expr{trueExpr, unknownExpr}, 3))
	require.False(t, zms[3].IsInited())

	zms = make([]objectio.ZoneMap, 4)
	vecs = make([]*vector.Vector, 4)
	require.False(t, foldOrZoneMap(ctx, proc, nil, nil, zms, vecs, []*plan.Expr{falseExpr, unknownExpr}, 3))
	require.False(t, zms[3].IsInited())

	zms = make([]objectio.ZoneMap, 4)
	vecs = make([]*vector.Vector, 4)
	require.True(t, foldOrZoneMap(ctx, proc, nil, nil, zms, vecs, []*plan.Expr{falseExpr}, 3))
	require.True(t, zms[3].IsInited())
	require.False(t, types.DecodeBool(zms[3].GetMaxBuf()))
}

func makeInternalBoolLitExpr(value bool, auxID int32) *plan.Expr {
	return &plan.Expr{
		Typ:   plan.Type{Id: int32(types.T_bool)},
		AuxId: auxID,
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: value},
			},
		},
	}
}

func makeInternalVarcharZoneMap(values ...string) objectio.ZoneMap {
	zm := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(zm, []byte(value))
	}
	return zm
}

// The metadata proof may fail; it must not hide internal failures or retain
// temporary results, including a partially written result.
func TestEvaluateZoneMapFunctionCleanup(t *testing.T) {
	arithmetic := moerr.NewOutOfRangeNoCtx("int64", "ROUND")
	internal := moerr.NewInternalErrorNoCtx("metadata test")
	for _, tc := range []struct {
		name         string
		returned     error
		panicked     any
		cleanupPanic bool
		known        bool
	}{
		{name: "success", known: true},
		{name: "returned error", returned: internal},
		{name: "arithmetic panic", panicked: arithmetic},
		{name: "internal panic", panicked: internal},
		{name: "runtime panic", panicked: &runtime.TypeAssertionError{}},
		{name: "untyped panic", panicked: "unexpected"},
		{name: "cleanup panic", panicked: arithmetic, cleanupPanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			baseline := proc.Mp().CurrNB()
			zm := index.NewZM(types.T_int64, 0)
			old := int64(123)
			index.UpdateZM(zm, types.EncodeInt64(&old))
			frees := 0
			var result objectio.ZoneMap
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				result = evaluateZoneMapFunction(proc, types.T_int64.ToType(), zm, func(result vector.FunctionResultWrapper) error {
					require.NoError(t, vector.MustFunctionResult[int64](result).Append(1, false))
					if tc.panicked != nil && !tc.cleanupPanic {
						panic(tc.panicked)
					}
					if tc.known {
						require.NoError(t, vector.MustFunctionResult[int64](result).Append(2, false))
					}
					return tc.returned
				}, func() error {
					frees++
					if tc.cleanupPanic {
						panic(tc.panicked)
					}
					return nil
				}, nil)
			}()
			require.Equal(t, 1, frees)
			require.Equal(t, baseline, proc.Mp().CurrNB(), "assert before process teardown")
			if tc.cleanupPanic || tc.panicked != nil && tc.panicked != arithmetic {
				require.Equal(t, tc.panicked, recovered)
			} else {
				require.Nil(t, recovered)
				require.Equal(t, tc.known, result.IsInited())
				if tc.known {
					require.Equal(t, int64(1), types.DecodeInt64(result.GetMinBuf()))
				}
			}
		})
	}
	t.Run("allocation failure", func(t *testing.T) {
		const capacity = 1024 * 1024
		mp, err := mpool.NewMPool("zone map allocation failure", capacity, mpool.NoFixed)
		require.NoError(t, err)
		proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
		defer proc.Free()
		pressure, err := mp.Alloc(capacity-8, true)
		require.NoError(t, err)
		defer mp.Free(pressure)
		baseline := mp.CurrNB()
		frees := 0
		result := evaluateZoneMapFunction(proc, types.T_int64.ToType(), index.NewZM(types.T_int64, 0), func(vector.FunctionResultWrapper) error {
			t.Error("callback must not run after allocation failure")
			return nil
		}, func() error { frees++; return nil }, nil)
		require.False(t, result.IsInited())
		require.Equal(t, 1, frees)
		require.Equal(t, baseline, mp.CurrNB())
	})
}
