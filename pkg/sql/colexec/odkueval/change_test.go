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

package odkueval

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type updateComparisonCase struct {
	name     string
	typ      types.Type
	safe     bool
	same     bool
	appendAt func(*vector.Vector, bool, bool, *process.Process) error
}

func updateFixedCase[T types.FixedSizeTExceptStrType](name string, typ types.Type, left, right T, safe, same bool) updateComparisonCase {
	return updateComparisonCase{name, typ, safe, same, func(v *vector.Vector, second, null bool, p *process.Process) error {
		value := left
		if second {
			value = right
		}
		return vector.AppendFixed(v, value, null, p.Mp())
	}}
}

func updateBytesCase(name string, typ types.Type, left, right []byte, safe, same bool) updateComparisonCase {
	return updateComparisonCase{name, typ, safe, same, func(v *vector.Vector, second, null bool, p *process.Process) error {
		value := left
		if second {
			value = right
		}
		return vector.AppendBytes(v, value, null, p.Mp())
	}}
}

type observedUpdateExecutor struct {
	colexec.ExpressionExecutor
	calls int
	err   error
}

func (e *observedUpdateExecutor) Eval(p *process.Process, inputs []*batch.Batch, sels []bool) (*vector.Vector, error) {
	e.calls++
	if e.err != nil {
		return nil, e.err
	}
	return e.ExpressionExecutor.Eval(p, inputs, sels)
}

func TestUpdateChangedMatchesTypedSQLEquality(t *testing.T) {
	jsonBytes := func(s string) []byte {
		v, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		encoded, err := types.EncodeJson(v)
		require.NoError(t, err)
		return encoded
	}
	cases := []updateComparisonCase{
		updateFixedCase("bool", types.T_bool.ToType(), true, true, true, true),
		updateFixedCase("bit", types.T_bit.ToType(), uint64(1), uint64(2), true, false),
		updateFixedCase("int8", types.T_int8.ToType(), int8(-1), int8(1), true, false),
		updateFixedCase("int64", types.T_int64.ToType(), int64(-9), int64(-9), true, true),
		updateFixedCase("malformed-integer-size", types.T_int64.ToType(), int64(1), int64(257), false, false),
		updateFixedCase("uint64", types.T_uint64.ToType(), uint64(math.MaxUint64), uint64(math.MaxUint64), true, true),
		updateFixedCase("enum", types.T_enum.ToType(), types.Enum(2), types.Enum(2), true, true),
		updateFixedCase("float-scale", types.New(types.T_float32, 0, 2), float32(1.231), float32(1.234), false, false),
		updateFixedCase("float-same", types.T_float32.ToType(), float32(1), float32(1), false, true),
		updateFixedCase("signed-zero", types.T_float64.ToType(), float64(0), math.Copysign(0, -1), false, false),
		updateFixedCase("nan", types.T_float64.ToType(), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000001), false, true),
		updateFixedCase("infinity", types.T_float64.ToType(), math.Inf(1), math.Inf(1), false, true),
		updateBytesCase("json-numeric", types.T_json.ToType(), jsonBytes("1"), jsonBytes("1.0"), false, false),
		updateBytesCase("json-same", types.T_json.ToType(), jsonBytes("{}"), jsonBytes("{}"), false, true),
		updateBytesCase("array", types.T_array_float32.ToType(), types.ArrayToBytes([]float32{1, 2}), types.ArrayToBytes([]float32{1, 2}), false, true),
		updateBytesCase("varchar-change", types.T_varchar.ToType(), []byte("a"), []byte("b"), true, false),
		updateBytesCase("varchar-metadata-equal", types.T_varchar.ToType(), []byte("a"), []byte("a"), true, true),
		updateBytesCase("varchar-metadata-changed", types.T_varchar.ToType(), []byte("a"), []byte("b"), true, false),
		updateBytesCase("varchar-oid-mismatch", types.T_varchar.ToType(), []byte("a"), []byte("a"), false, true),
	}
	for _, oid := range []types.T{types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink} {
		for _, size := range []int{1, 512} {
			value := []byte(strings.Repeat("x", size))
			cases = append(cases, updateBytesCase(fmt.Sprintf("%s/%d", oid, size), oid.ToType(), value, value, true, true))
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := testutil.NewProc(t)
			defer p.Free()
			account := checkTestAccount(t)
			execs, err := CompileUpdateChanges([]int32{0}, []types.Type{tc.typ}, p, account)
			require.NoError(t, err)
			defer func() { execs[0].Free(); require.Zero(t, account.Snapshot().Used) }()
			for _, constant := range []bool{false, true} {
				for nulls := range 4 {
					func() {
						operands := []*batch.Batch{batch.NewWithSize(1), batch.NewWithSize(1)}
						for i, b := range operands {
							defer b.Clean(p.Mp())
							null := nulls&(1<<i) != 0
							if constant && null {
								b.Vecs[0] = vector.NewConstNull(tc.typ, 1, p.Mp())
							} else {
								b.Vecs[0] = vector.NewVec(tc.typ)
								require.NoError(t, tc.appendAt(b.Vecs[0], i == 1, null, p))
								if constant {
									b.Vecs[0].ToConst()
								}
							}
							b.SetRowCount(1)
						}
						if strings.Contains(tc.name, "metadata") {
							operands[1].Vecs[0].GetType().Width++
							operands[1].Vecs[0].GetType().Scale++
						}
						if tc.name == "varchar-oid-mismatch" {
							operands[1].Vecs[0].GetType().Oid = types.T_text
						}
						if tc.name == "malformed-integer-size" {
							for _, b := range operands {
								b.Vecs[0].GetType().Size = 1
							}
						}
						operands[0].Vecs[0].GetGrouping().Add(0) // GROUPING is not SQL NULL/equality.
						r, err := execs[0].Eval(p, operands, nil)
						require.NoError(t, err)
						want := r.IsNull(0) || !vector.GetFixedAtNoTypeCheck[bool](r, 0)
						observed := &observedUpdateExecutor{ExpressionExecutor: execs[0]}
						changed, err := UpdateChanged([]colexec.ExpressionExecutor{observed}, []int32{0}, operands[0], operands[1], p)
						require.NoError(t, err)
						require.Equal(t, want, changed, "const=%t nulls=%d", constant, nulls)
						fallback := !tc.safe || (nulls == 0 && !tc.same)
						require.Equal(t, fallback, observed.calls == 1, "fallback const=%t nulls=%d", constant, nulls)
						if fallback {
							cause := moerr.NewInternalErrorNoCtx("comparison failed")
							observed.err = cause
							_, err = UpdateChanged([]colexec.ExpressionExecutor{observed}, []int32{0}, operands[0], operands[1], p)
							require.Same(t, cause, err)
						}
					}()
				}
				// The same prepared executor must accept new flat/constant data.
				execs[0].ResetForNextQuery()
			}
		})
	}
}
