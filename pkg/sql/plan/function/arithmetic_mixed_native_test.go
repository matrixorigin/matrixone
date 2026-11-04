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

package function

import (
	"math"
	"math/big"
	"math/rand/v2"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Independent unbounded arithmetic decides both values and unsigned rejection.
func mixedUnsignedOracle(s int64, u uint64, multiply bool) (uint64, bool) {
	a, b := big.NewInt(s), new(big.Int).SetUint64(u)
	if multiply {
		a.Mul(a, b)
	} else {
		a.Add(a, b)
	}
	return a.Uint64(), a.Sign() >= 0 && a.BitLen() <= 64
}

func mixedUnsignedDecimalReference(multiply bool) executeLogicOfOverload {
	return func(p []*vector.Vector, r vector.FunctionResultWrapper, proc *process.Process, n int, selected *FunctionSelectList) error {
		if multiply {
			return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "*", types.Decimal128.Mul128)
		}
		return checkedIntegerArithmetic[uint64](p, r, proc, n, selected, "+", types.Decimal128.Add128)
	}
}

func TestMixedUnsignedAddMulExactValues(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, multiply := range []bool{false, true} {
		t.Run(map[bool]string{false: "add", true: "multiply"}[multiply], func(t *testing.T) {
			fn := mixedUnsignedPlusFn
			if multiply {
				fn = mixedUnsignedMultiFn
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{0}, nil),
			}, NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil), fn)
			defer fc.Free()
			signed, unsigned := fc.parameters[0], fc.parameters[1]
			check := func(s int64, u uint64) {
				t.Helper()
				vector.MustFixedColNoTypeCheck[int64](signed)[0] = s
				vector.MustFixedColNoTypeCheck[uint64](unsigned)[0] = u
				want, ok := mixedUnsignedOracle(s, u, multiply)
				for _, reverse := range []bool{false, true} {
					fc.parameters[0], fc.parameters[1] = signed, unsigned
					if reverse {
						fc.parameters[0], fc.parameters[1] = unsigned, signed
					}
					got, err := fc.DebugRun()
					if ok {
						require.NoError(t, err, "s=%d u=%d reversed=%t", s, u, reverse)
						require.Equal(t, []uint64{want}, vector.MustFixedColNoTypeCheck[uint64](got))
						require.False(t, got.IsNull(0))
					} else {
						require.Error(t, err, "s=%d u=%d", s, u)
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
						require.Equal(t, moerr.ER_DATA_OUT_OF_RANGE, err.(*moerr.Error).MySQLCode())
						// Diagnostic compatibility is a separate oracle from big.Int values.
						fc.fn = mixedUnsignedDecimalReference(multiply)
						_, oldErr := fc.DebugRun()
						require.Error(t, oldErr)
						require.EqualError(t, err, oldErr.Error())
						fc.fn = fn
					}
				}
			}
			for _, s := range []int64{math.MinInt64, math.MinInt64 + 1, -2, -1, 0, 1, 2, 3, math.MaxInt64 - 1, math.MaxInt64} {
				for _, u := range []uint64{0, 1, 2, 3, math.MaxInt64, 1 << 63, 1<<63 + 1, math.MaxUint64 - 1, math.MaxUint64} {
					check(s, u)
				}
			}
			// Nearest multiplication boundaries, rather than mostly random overflows.
			for _, s := range []int64{2, 3, 7, 46341, math.MaxInt64} {
				limit := uint64(math.MaxUint64) / uint64(s)
				check(s, limit)
				check(s, limit+1)
			}
			rng := rand.New(rand.NewPCG(31036, 1))
			for i := 0; i < 256; i++ {
				check(int64(rng.Uint64()), rng.Uint64())
			}
		})
	}
}

func TestMixedUnsignedAddMulSignedFallback(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, multiply := range []bool{false, true} {
		for _, pair := range [][2]int64{{3, 4}, {-2, -3}} {
			func() {
				fn := mixedUnsignedPlusFn
				if multiply {
					fn = mixedUnsignedMultiFn
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestInput(types.T_int64.ToType(), []int64{pair[0]}, nil),
					NewFunctionTestInput(types.T_int64.ToType(), []int64{pair[1]}, nil),
				}, NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil), fn)
				defer fc.Free()
				got, err := fc.DebugRun()
				// This compatibility entry preserves the old overload-2 behavior,
				// including diagnostics; normal same-sign SQL uses overload 0.
				values := append([]uint64(nil), vector.MustFixedColNoTypeCheck[uint64](got)...)
				fc.fn = mixedUnsignedDecimalReference(multiply)
				old, oldErr := fc.DebugRun()
				if oldErr == nil {
					require.NoError(t, err)
					require.Equal(t, vector.MustFixedColNoTypeCheck[uint64](old), values)
				} else {
					require.EqualError(t, err, oldErr.Error())
				}
			}()
		}
	}
}

func TestMixedUnsignedAddMulVectorReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, multiply := range []bool{false, true} {
		t.Run(map[bool]string{false: "add", true: "multiply"}[multiply], func(t *testing.T) {
			fn := mixedUnsignedPlusFn
			if multiply {
				fn = mixedUnsignedMultiFn
			}
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2, 3, 4}, nil),
				NewFunctionTestInput(types.T_uint64.ToType(), []uint64{3, 4, 5}, nil),
			}, NewFunctionTestResult(types.T_uint64.ToType(), false, nil, nil), fn)
			defer fc.Free()
			signed, unsigned := fc.parameters[0], fc.parameters[1]
			for _, shape := range []struct {
				name                                                                 string
				constS, constU, nullS, nullU, masked, allMasked, ignoreAll, overflow bool
				rows                                                                 int
			}{
				{name: "dense", rows: 3},
				{name: "signed constant", constS: true, rows: 3},
				{name: "unsigned constant", constU: true, rows: 3},
				{name: "both constant", constS: true, constU: true, rows: 3},
				{name: "NULL hides overflow", nullU: true, overflow: true, rows: 3},
				{name: "all NULL", constS: true, nullS: true, overflow: true, rows: 3},
				{name: "sparse mask hides overflow", masked: true, overflow: true, rows: 3},
				{name: "both constants all masked without AllNull", constS: true, constU: true, allMasked: true, overflow: true, rows: 3},
				{name: "IgnoreAllRow", ignoreAll: true, overflow: true, rows: 3},
				{name: "empty overflowing constants", constS: true, constU: true, overflow: true, rows: 0},
				{name: "unmasked overflow", overflow: true, rows: 3},
				{name: "reset after error to dense", rows: 3},
			} {
				t.Run(shape.name, func(t *testing.T) {
					signed.SetClass(vector.FLAT)
					unsigned.SetClass(vector.FLAT)
					signed.GetNulls().Reset()
					unsigned.GetNulls().Reset()
					ss, us := vector.MustFixedColNoTypeCheck[int64](signed), vector.MustFixedColNoTypeCheck[uint64](unsigned)
					copy(ss, []int64{2, 3, 4})
					copy(us, []uint64{3, 4, 5})
					if shape.overflow {
						us[1] = math.MaxUint64
						if shape.constU {
							us[0] = math.MaxUint64
						}
					}
					if shape.nullS {
						signed.GetNulls().Add(0)
					}
					if shape.nullU {
						unsigned.GetNulls().Add(1)
					}
					if shape.constS {
						signed.SetClass(vector.CONSTANT)
					}
					if shape.constU {
						unsigned.SetClass(vector.CONSTANT)
					}
					fc.selectList = nil
					if shape.masked {
						fc.selectList = &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
					}
					if shape.allMasked {
						fc.selectList = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, false}}
					}
					if shape.ignoreAll {
						fc.selectList = &FunctionSelectList{AllNull: true}
					}
					fc.fnLength = shape.rows
					for _, reverse := range []bool{false, true} {
						fc.parameters[0], fc.parameters[1] = signed, unsigned
						if reverse {
							fc.parameters[0], fc.parameters[1] = unsigned, signed
						}
						got, err := fc.DebugRun()
						if shape.name == "unmasked overflow" {
							require.Error(t, err)
							require.Equal(t, moerr.ER_DATA_OUT_OF_RANGE, err.(*moerr.Error).MySQLCode())
							// Compare the first reachable error, not unpublished partial
							// vector length (the caller has already pre-extended it).
							fc.fn = mixedUnsignedDecimalReference(multiply)
							_, oldErr := fc.DebugRun()
							require.Error(t, oldErr)
							require.EqualError(t, err, oldErr.Error())
							fc.fn = fn
							continue
						}
						require.NoError(t, err)
						require.Equal(t, shape.rows, got.Length())
						for row := 0; row < shape.rows; row++ {
							si, ui := row, row
							if shape.constS {
								si = 0
							}
							if shape.constU {
								ui = 0
							}
							wantNull := shape.allMasked || shape.ignoreAll || (shape.masked && row == 1) || (shape.nullS && si == 0) || (shape.nullU && ui == 1)
							require.Equal(t, wantNull, got.IsNull(uint64(row)))
							if !wantNull {
								want, ok := mixedUnsignedOracle(ss[si], us[ui], multiply)
								require.True(t, ok)
								require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint64](got, row))
							}
						}
					}
				})
			}
		})
	}
}
