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

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func checkIntegerMembership[T constraints.Integer](t *testing.T, typ types.T, members, probes []T, sorted, nullable bool) {
	t.Helper()
	proc := testutil.NewProcess(t)
	list := vector.NewVec(typ.ToType())
	defer list.Free(proc.Mp())
	for i, v := range members {
		require.NoError(t, vector.AppendFixed(list, v, nullable && i == 0, proc.Mp()))
	}
	list.SetSorted(sorted)
	for _, name := range []string{"in", "not_in"} {
		t.Run(name, func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{typ.ToType(), typ.ToType()})
			require.NoError(t, err)
			ov, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
			require.NoError(t, err)
			fn, _, _, _ := ov.GetExecuteMethod()
			for _, mode := range []string{"flat", "all_null", "constant", "const_null"} {
				constant := mode == "constant" || mode == "const_null"
				t.Run(mode, func(t *testing.T) {
					input := vector.NewVec(typ.ToType())
					defer input.Free(proc.Mp())
					for _, v := range probes {
						require.NoError(t, vector.AppendFixed(input, v, false, proc.Mp()))
					}
					// Constant vectors may retain irrelevant NULL bits beyond row zero.
					input.GetNulls().Add(uint64(len(probes) - 1))
					if mode == "all_null" {
						input.GetNulls().AddRange(0, uint64(len(probes)))
					}
					if mode == "const_null" {
						input.GetNulls().Add(0)
					}
					if constant {
						input.ToConst()
					}
					out := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
					defer out.Free()
					// The same closure processes multiple batches and vector shapes.
					for _, rows := range []int{len(probes), 0, len(probes)} {
						require.NoError(t, out.PreExtendAndReset(rows))
						require.NoError(t, fn([]*vector.Vector{input, list}, out, proc, rows, nil))
						result := out.GetResultVector()
						for i := 0; i < rows; i++ {
							v := probes[i]
							if constant {
								v = probes[0]
							}
							// Independent equality oracle, without production set helpers.
							found := false
							for j, member := range members {
								if (!nullable || j != 0) && member == v {
									found = true
								}
							}
							wantNull := mode == "all_null" || mode == "const_null" || !constant && i == len(probes)-1 || !found && nullable
							require.Equal(t, wantNull, result.IsNull(uint64(i)), "row %d", i)
							if !wantNull {
								require.Equal(t, found != (name == "not_in"), vector.GetFixedAtWithTypeCheck[bool](result, i), "row %d", i)
							}
						}
					}
				})
			}
		})
	}
}

func checkIntegerMembershipBoundary[T constraints.Integer](t *testing.T, typ types.T, lower T) {
	t.Helper()
	members := make([]T, 16)
	for i := range members {
		members[i] = lower + T(i)*2
	}
	probes := []T{lower, lower + 1, lower + 30, lower + 31, lower - 1, 0}
	checkIntegerMembership(t, typ, members, probes, true, false)
}

func TestIntegerMembershipBoundaries(t *testing.T) {
	t.Run("int8", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_int8, int8(-15)) })
	t.Run("int16", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_int16, int16(math.MinInt16)) })
	t.Run("int32", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_int32, int32(math.MaxInt32-30)) })
	t.Run("int64_min", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_int64, int64(math.MinInt64)) })
	t.Run("int64_cross_zero", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_int64, int64(-15)) })
	t.Run("uint8", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_uint8, uint8(math.MaxUint8-30)) })
	t.Run("uint16", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_uint16, uint16(0)) })
	t.Run("uint32", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_uint32, uint32(math.MaxUint32-30)) })
	t.Run("uint64", func(t *testing.T) { checkIntegerMembershipBoundary(t, types.T_uint64, uint64(math.MaxUint64-30)) })
}

func TestIntegerMembershipFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name             string
		members          []int64
		sorted, nullable bool
	}{
		{"empty", nil, true, false},
		{"single", []int64{2}, true, false},
		{"all_null", []int64{2}, false, true},
		{"nullable", []int64{2, 4, 8}, false, true},
		{"unsorted", []int64{8, 2, 4, 0, 10, 12, 16, 14, 18, 20, 22, 24, 26, 28, 30, 32}, false, false},
		{"duplicate", []int64{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, true, false},
		{"full_signed_span", []int64{math.MinInt64, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, math.MaxInt64}, true, false},
		{"sparse", []int64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 1000000}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkIntegerMembership(t, types.T_int64, tc.members, []int64{2, 3, math.MinInt64, math.MaxInt64, 0}, tc.sorted, tc.nullable)
		})
	}
}

// Admission is a storage bound, independent of the number of probe batches.
func TestIntegerMembershipBitmapAdmission(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		last  int64
		want  bool
	}{
		{"below_minimum", 15, 28, false},
		{"minimum", 16, 30, true},
		{"density_boundary", 16, 1023, true},
		{"too_sparse", 16, 1024, false},
		{"memory_boundary", 8192, 524287, true},
		{"over_memory_bound", 8193, 524288, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			list := vector.NewVec(types.T_int64.ToType())
			defer list.Free(proc.Mp())
			for i := 0; i < tc.count-1; i++ {
				require.NoError(t, vector.AppendFixed(list, int64(i), false, proc.Mp()))
			}
			require.NoError(t, vector.AppendFixed(list, tc.last, false, proc.Mp()))
			list.SetSorted(true)
			op := newOpOperatorIntegerIn[int64]()
			require.Equal(t, tc.want, op.initBitmap(list))
			if tc.want {
				require.True(t, op.bits.Contains(uint64(tc.last)-op.lower))
				require.False(t, op.bits.Contains(uint64(tc.last+1)-op.lower))
				// The retained set owns its storage, independently of input reuse.
				vector.MustFixedColWithTypeCheck[int64](list)[tc.count-1] = tc.last + 1
				require.True(t, op.bits.Contains(uint64(tc.last)-op.lower))
				require.False(t, op.bits.Contains(uint64(tc.last+1)-op.lower))
			}
		})
	}
}
