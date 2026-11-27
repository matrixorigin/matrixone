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

package containers

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Types the switch implementations left out, and so panicked on.
var (
	legacyGetMissing    = map[types.T]bool{types.T_decimal256: true, types.T_geometry: true, types.T_geometry32: true}
	legacyUpdateMissing = map[types.T]bool{types.T_decimal256: true, types.T_geometry: true, types.T_geometry32: true, types.T_year: true}
)

// randomTAEVector returns a vector of typ with n rows of random contents and
// about one row in five NULL.
func randomTAEVector(t *testing.T, rng *rand.Rand, typ types.Type, n int, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ, mp)
	if typ.IsVarlen() {
		for i := 0; i < n; i++ {
			b := make([]byte, rng.Intn(48)) // some inline, some in the area
			rng.Read(b)
			require.NoError(t, vector.AppendBytes(v, b, false, mp))
		}
	} else {
		require.NoError(t, v.PreExtend(n, mp))
		v.SetLength(n)
		for i := 0; i < n; i++ {
			cell := v.GetRawBytesAt(i)
			rng.Read(cell)
			if typ.Oid == types.T_bool {
				cell[0] &= 1
			}
		}
	}
	for i := 0; i < n; i++ {
		if rng.Intn(5) == 0 {
			v.SetNull(uint64(i))
		}
	}
	return v
}

func outcome(f func() any) (s string) {
	defer func() {
		if r := recover(); r != nil {
			s = fmt.Sprintf("panic(%v)", r)
		}
	}()
	return fmt.Sprintf("%#v", f())
}

func requireSameRows(t *testing.T, want, got *vector.Vector, msg string) {
	t.Helper()
	require.Equal(t, want.Length(), got.Length(), msg)
	for i := 0; i < want.Length(); i++ {
		require.Equal(t, want.IsNull(uint64(i)), got.IsNull(uint64(i)), "%s: row %d null", msg, i)
		if !want.IsNull(uint64(i)) {
			require.True(t, bytes.Equal(want.GetRawBytesAt(i), got.GetRawBytesAt(i)), "%s: row %d", msg, i)
		}
	}
}

// TestValueAccessMatchesLegacy compares getNonNullValue and UpdateValue, now
// on the vector type-ops table, with their switch implementations for every
// type id: the same values, vectors and panics, except that the types the
// switches left out now work instead of panicking.
func TestValueAccessMatchesLegacy(t *testing.T) {
	mp := mpool.MustNewZero()
	rng := rand.New(rand.NewSource(33336))
	const n = 17

	for i := 0; i < 256; i++ {
		oid := types.T(i)
		name := fmt.Sprintf("T(%d) %s", i, oid.OidString())
		typ, ok := oid.TryToType()
		if !ok || !vector.SupportsAny(oid) {
			if !ok {
				typ = types.Type{Oid: oid}
			}
			empty := vector.NewVec(typ, mp)
			require.Equal(t, outcome(func() any { return legacyGetNonNullValue(empty, 0) }),
				outcome(func() any { return getNonNullValue(empty, 0) }), "%s: get", name)
			for _, isNull := range []bool{false, true} {
				require.Equal(t, outcome(func() any { legacyUpdateValue(empty, 0, 1, isNull, mp); return nil }),
					outcome(func() any { UpdateValue(empty, 0, 1, isNull, mp); return nil }), "%s: update", name)
			}
			empty.Free(mp)
			continue
		}

		src := randomTAEVector(t, rng, typ, n, mp)
		for row := 0; row < n; row++ {
			got := outcome(func() any { return getNonNullValue(src, uint32(row)) })
			require.NotContains(t, got, "panic(", "%s: get row %d", name, row)
			want := outcome(func() any { return legacyGetNonNullValue(src, uint32(row)) })
			if legacyGetMissing[oid] {
				require.Contains(t, want, "panic(No Support)", "%s: the switch left it out", name)
			} else {
				require.Equal(t, want, got, "%s: get row %d", name, row)
			}
		}

		// update each row to another row's value, and a few to NULL
		for _, isNull := range []bool{false, true} {
			a, err := src.Dup(mp)
			require.NoError(t, err)
			b, err := src.Dup(mp)
			require.NoError(t, err)
			for row := 0; row < n; row++ {
				val := src.GetValue((row + 3) % n)
				if val == nil || src.IsNull(uint64((row+3)%n)) {
					continue
				}
				null := isNull && row%4 == 0
				if !legacyUpdateMissing[oid] {
					legacyUpdateValue(a, uint32(row), val, null, mp)
				}
				UpdateValue(b, uint32(row), val, null, mp)
			}
			if legacyUpdateMissing[oid] {
				require.Contains(t, outcome(func() any { legacyUpdateValue(a, 0, nil, true, mp); return nil }),
					"not supported", "%s: the switch left it out", name)
				// the update took effect: each written row now holds the
				// value it was set to
				for row := 0; row < n; row++ {
					from := (row + 3) % n
					if src.IsNull(uint64(from)) || (isNull && row%4 == 0) {
						continue
					}
					require.Equal(t, src.GetValue(from), b.GetValue(row), "%s: updated row %d", name, row)
				}
			} else {
				requireSameRows(t, a, b, name+": update")
			}
			a.Free(mp)
			b.Free(mp)
		}
		src.Free(mp)
	}
	require.Zero(t, mp.CurrNB(), "vectors leaked")
}
