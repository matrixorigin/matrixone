//go:build amd64 && go1.27 && goexperiment.simd

// Copyright 2021 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"simd/archsimd"
	"testing"
)

func TestDecimal128PreparedExact(t *testing.T) {
	if !archsimd.X86.AVX2() {
		t.Skip("AVX2 unavailable")
	}
	for _, op := range []int32{PLUS, MINUS} {
		for _, rt := range []types.T{types.T_decimal128, types.T_decimal256} {
			t.Run(fmt.Sprintf("op%d/%s", op, rt.String()), func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
				x, y := vector.NewVec(types.New(types.T_decimal128, 38, 0)), vector.NewVec(types.New(types.T_decimal128, 38, 0))
				got, want := vector.NewVec(types.New(rt, 65, 0)), vector.NewVec(types.New(rt, 65, 0))
				for _, v := range []*vector.Vector{x, y, got, want} {
					t.Cleanup(func() { v.Free(mp) })
				}
				for i := 0; i < 263; i++ {
					a := types.Decimal128FromInt64(int64(i - 130))
					b := types.Decimal128FromInt64(int64(i%7 - 3))
					if rt == types.T_decimal256 {
						a = types.Decimal128{B0_63: ^uint64(0), B64_127: uint64(1)<<63 - 1}
						if i%2 != 0 {
							a = types.Decimal128{B64_127: uint64(1) << 63}
						}
						b = a
						if op == MINUS {
							if i%2 == 0 {
								b = types.Decimal128{B64_127: uint64(1) << 63}
							} else {
								b = types.Decimal128{B0_63: ^uint64(0), B64_127: uint64(1)<<63 - 1}
							}
						}
					}
					require.NoError(t, vector.AppendFixed(x, a, false, mp))
					require.NoError(t, vector.AppendFixed(y, b, false, mp))
				}
				require.NoError(t, got.PreExtend(263, mp))
				got.SetLength(263)
				require.NoError(t, want.PreExtend(263, mp))
				want.SetLength(263)
				require.NoError(t, DecimalArithmeticChunk(op, x, y, want, 1, 1, 1, 261))
				require.True(t, TryDecimalArithmeticSIMD(op, x, y, got, 1, 1, 1, 261))
				require.Equal(t, want.GetData()[rt.ToType().TypeSize():262*rt.ToType().TypeSize()], got.GetData()[rt.ToType().TypeSize():262*rt.ToType().TypeSize()])
			})
		}
	}
}
