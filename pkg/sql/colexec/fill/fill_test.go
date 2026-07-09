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

package fill

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// add unit tests for cases
type fillTestCase struct {
	arg  *Fill
	proc *process.Process
}

func makeTestCases(t *testing.T) []fillTestCase {
	return []fillTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_VALUE,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_PREV,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_NONE,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},

		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_NEXT,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Fill{
				AggIds:   []int32{function.MAX},
				FillType: plan.Node_LINEAR,
				FillVal: []*plan.Expr{
					{
						Expr: &plan.Expr_Lit{Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_I64Val{
								I64Val: 1,
							},
						}},
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
					},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestFill(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		tc.arg.ctr.bats = make([]*batch.Batch, 10)
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = vm.Exec(tc.arg, tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestProcessLinearDecimal128(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	typ := types.New(types.T_decimal128, 38, 0)
	vec := vector.NewVec(typ)
	defer vec.Free(proc.Mp())

	left := types.Decimal128FromInt64(100)
	right := types.Decimal128FromInt64(300)
	require.NoError(t, vector.AppendFixed(vec, left, false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(vec, types.Decimal128{}, true, proc.Mp()))
	require.NoError(t, vector.AppendFixed(vec, right, false, proc.Mp()))

	bat := batch.NewWithSize(1)
	bat.SetVector(0, vec)
	bat.SetRowCount(3)

	arg := &Fill{ColLen: 1, FillType: plan.Node_LINEAR}
	ctr := &arg.ctr
	ctr.bats = []*batch.Batch{bat}
	ctr.doneIdx = make([]int, 1)
	ctr.endBatch = make([]bool, 1)

	require.NoError(t, processLinearCol(ctr, arg, proc, 0, nil))
	require.False(t, vec.IsNull(1))
	require.Equal(t, types.Decimal128FromInt64(200), vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 1))
}

func resetChildren(arg *Fill, m *mpool.MPool) {
	bat1 := colexec.MakeMockBatchsWithNullVec1(m)
	bat := colexec.MakeMockBatchsWithNullVec(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat1, bat, bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func Test_appendValue(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	// bool
	{
		v1 := vector.NewVec(types.T_bool.ToType())
		v2 := vector.NewVec(types.T_bool.ToType())
		err := vector.AppendFixed(v2, true, false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, true, vector.MustFixedColWithTypeCheck[bool](v1)[0])
	}

	// bit
	{
		v1 := vector.NewVec(types.T_bit.ToType())
		v2 := vector.NewVec(types.T_bit.ToType())
		err := vector.AppendFixed(v2, uint64(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint64(1), vector.MustFixedColWithTypeCheck[uint64](v1)[0])
	}

	// int8
	{
		v1 := vector.NewVec(types.T_int8.ToType())
		v2 := vector.NewVec(types.T_int8.ToType())
		err := vector.AppendFixed(v2, int8(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int8(1), vector.MustFixedColWithTypeCheck[int8](v1)[0])
	}

	// int16
	{
		v1 := vector.NewVec(types.T_int16.ToType())
		v2 := vector.NewVec(types.T_int16.ToType())
		err := vector.AppendFixed(v2, int16(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int16(1), vector.MustFixedColWithTypeCheck[int16](v1)[0])
	}

	// int32
	{
		v1 := vector.NewVec(types.T_int32.ToType())
		v2 := vector.NewVec(types.T_int32.ToType())
		err := vector.AppendFixed(v2, int32(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int32(1), vector.MustFixedColWithTypeCheck[int32](v1)[0])
	}

	// int64
	{
		v1 := vector.NewVec(types.T_int64.ToType())
		v2 := vector.NewVec(types.T_int64.ToType())
		err := vector.AppendFixed(v2, int64(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](v1)[0])
	}

	// uint8
	{
		v1 := vector.NewVec(types.T_uint8.ToType())
		v2 := vector.NewVec(types.T_uint8.ToType())
		err := vector.AppendFixed(v2, uint8(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint8(1), vector.MustFixedColWithTypeCheck[uint8](v1)[0])
	}

	// uint16
	{
		v1 := vector.NewVec(types.T_uint16.ToType())
		v2 := vector.NewVec(types.T_uint16.ToType())
		err := vector.AppendFixed(v2, uint16(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint16(1), vector.MustFixedColWithTypeCheck[uint16](v1)[0])
	}

	// uint32
	{
		v1 := vector.NewVec(types.T_uint32.ToType())
		v2 := vector.NewVec(types.T_uint32.ToType())
		err := vector.AppendFixed(v2, uint32(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint32(1), vector.MustFixedColWithTypeCheck[uint32](v1)[0])
	}

	// uint64
	{
		v1 := vector.NewVec(types.T_uint64.ToType())
		v2 := vector.NewVec(types.T_uint64.ToType())
		err := vector.AppendFixed(v2, uint64(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint64(1), vector.MustFixedColWithTypeCheck[uint64](v1)[0])
	}

	// float32
	{
		v1 := vector.NewVec(types.T_float32.ToType())
		v2 := vector.NewVec(types.T_float32.ToType())
		err := vector.AppendFixed(v2, float32(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, float32(1), vector.MustFixedColWithTypeCheck[float32](v1)[0])
	}

	// float64
	{
		v1 := vector.NewVec(types.T_float64.ToType())
		v2 := vector.NewVec(types.T_float64.ToType())
		err := vector.AppendFixed(v2, float64(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, float64(1), vector.MustFixedColWithTypeCheck[float64](v1)[0])
	}

	// date
	{
		v1 := vector.NewVec(types.T_date.ToType())
		v2 := vector.NewVec(types.T_date.ToType())
		err := vector.AppendFixed(v2, types.Date(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Date(1), vector.MustFixedColWithTypeCheck[types.Date](v1)[0])
	}

	// datetime
	{
		v1 := vector.NewVec(types.T_datetime.ToType())
		v2 := vector.NewVec(types.T_datetime.ToType())
		err := vector.AppendFixed(v2, types.Datetime(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Datetime(1), vector.MustFixedColWithTypeCheck[types.Datetime](v1)[0])
	}

	// time
	{
		v1 := vector.NewVec(types.T_time.ToType())
		v2 := vector.NewVec(types.T_time.ToType())
		err := vector.AppendFixed(v2, types.Time(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Time(1), vector.MustFixedColWithTypeCheck[types.Time](v1)[0])
	}

	// timestamp
	{
		v1 := vector.NewVec(types.T_timestamp.ToType())
		v2 := vector.NewVec(types.T_timestamp.ToType())
		err := vector.AppendFixed(v2, types.Timestamp(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Timestamp(1), vector.MustFixedColWithTypeCheck[types.Timestamp](v1)[0])
	}

	// enum
	{
		v1 := vector.NewVec(types.T_enum.ToType())
		v2 := vector.NewVec(types.T_enum.ToType())
		err := vector.AppendFixed(v2, types.Enum(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Enum(1), vector.MustFixedColWithTypeCheck[types.Enum](v1)[0])
	}

	// decimal64
	{
		v1 := vector.NewVec(types.T_decimal64.ToType())
		v2 := vector.NewVec(types.T_decimal64.ToType())
		err := vector.AppendFixed(v2, types.Decimal64(1), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Decimal64(1), vector.MustFixedColWithTypeCheck[types.Decimal64](v1)[0])
	}

	// decimal128
	{
		v1 := vector.NewVec(types.T_decimal128.ToType())
		v2 := vector.NewVec(types.T_decimal128.ToType())
		dec128 := types.Decimal128{B0_63: 1, B64_127: 0}
		err := vector.AppendFixed(v2, dec128, false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, dec128, vector.MustFixedColWithTypeCheck[types.Decimal128](v1)[0])
	}

	// uuid
	{
		v1 := vector.NewVec(types.T_uuid.ToType())
		v2 := vector.NewVec(types.T_uuid.ToType())
		uuid := types.Uuid{}
		uuid[0] = 1
		err := vector.AppendFixed(v2, uuid, false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uuid, vector.MustFixedColWithTypeCheck[types.Uuid](v1)[0])
	}

	// TS
	{
		v1 := vector.NewVec(types.T_TS.ToType())
		v2 := vector.NewVec(types.T_TS.ToType())
		ts := types.TS{}
		ts[0] = 1
		err := vector.AppendFixed(v2, ts, false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, ts, vector.MustFixedColWithTypeCheck[types.TS](v1)[0])
	}

	// Rowid
	{
		v1 := vector.NewVec(types.T_Rowid.ToType())
		v2 := vector.NewVec(types.T_Rowid.ToType())
		rowid := types.Rowid{}
		rowid[0] = 1
		err := vector.AppendFixed(v2, rowid, false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, rowid, vector.MustFixedColWithTypeCheck[types.Rowid](v1)[0])
	}

	// char
	{
		v1 := vector.NewVec(types.T_char.ToType())
		v2 := vector.NewVec(types.T_char.ToType())
		err := vector.AppendBytes(v2, []byte("1"), false, proc.Mp())
		require.NoError(t, err)
		err = appendValue(v1, v2, 0, proc)
		require.NoError(t, err)
		require.Equal(t, []byte("1"), v1.GetBytesAt(0))
	}
}

func Test_setValue(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())

	// bool
	{
		v1 := vector.NewVec(types.T_bool.ToType())
		v2 := vector.NewVec(types.T_bool.ToType())
		err := vector.AppendFixed(v2, true, false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, false, false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, true, vector.MustFixedColWithTypeCheck[bool](v1)[0])
	}

	// bit
	{
		v1 := vector.NewVec(types.T_bit.ToType())
		v2 := vector.NewVec(types.T_bit.ToType())
		err := vector.AppendFixed(v2, uint64(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, uint64(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint64(1), vector.MustFixedColWithTypeCheck[uint64](v1)[0])
	}

	// int8
	{
		v1 := vector.NewVec(types.T_int8.ToType())
		v2 := vector.NewVec(types.T_int8.ToType())
		err := vector.AppendFixed(v2, int8(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, int8(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int8(1), vector.MustFixedColWithTypeCheck[int8](v1)[0])
	}

	// int16
	{
		v1 := vector.NewVec(types.T_int16.ToType())
		v2 := vector.NewVec(types.T_int16.ToType())
		err := vector.AppendFixed(v2, int16(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, int16(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int16(1), vector.MustFixedColWithTypeCheck[int16](v1)[0])
	}

	// int32
	{
		v1 := vector.NewVec(types.T_int32.ToType())
		v2 := vector.NewVec(types.T_int32.ToType())
		err := vector.AppendFixed(v2, int32(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, int32(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int32(1), vector.MustFixedColWithTypeCheck[int32](v1)[0])
	}

	// int64
	{
		v1 := vector.NewVec(types.T_int64.ToType())
		v2 := vector.NewVec(types.T_int64.ToType())
		err := vector.AppendFixed(v2, int64(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, int64(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](v1)[0])
	}

	// uint8
	{
		v1 := vector.NewVec(types.T_uint8.ToType())
		v2 := vector.NewVec(types.T_uint8.ToType())
		err := vector.AppendFixed(v2, uint8(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, uint8(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint8(1), vector.MustFixedColWithTypeCheck[uint8](v1)[0])
	}

	// uint16
	{
		v1 := vector.NewVec(types.T_uint16.ToType())
		v2 := vector.NewVec(types.T_uint16.ToType())
		err := vector.AppendFixed(v2, uint16(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, uint16(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint16(1), vector.MustFixedColWithTypeCheck[uint16](v1)[0])
	}

	// uint32
	{
		v1 := vector.NewVec(types.T_uint32.ToType())
		v2 := vector.NewVec(types.T_uint32.ToType())
		err := vector.AppendFixed(v2, uint32(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, uint32(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint32(1), vector.MustFixedColWithTypeCheck[uint32](v1)[0])
	}

	// uint64
	{
		v1 := vector.NewVec(types.T_uint64.ToType())
		v2 := vector.NewVec(types.T_uint64.ToType())
		err := vector.AppendFixed(v2, uint64(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, uint64(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uint64(1), vector.MustFixedColWithTypeCheck[uint64](v1)[0])
	}

	// float32
	{
		v1 := vector.NewVec(types.T_float32.ToType())
		v2 := vector.NewVec(types.T_float32.ToType())
		err := vector.AppendFixed(v2, float32(1.0), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, float32(0.0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, float32(1.0), vector.MustFixedColWithTypeCheck[float32](v1)[0])
	}

	// float64
	{
		v1 := vector.NewVec(types.T_float64.ToType())
		v2 := vector.NewVec(types.T_float64.ToType())
		err := vector.AppendFixed(v2, float64(1.0), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, float64(0.0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, float64(1.0), vector.MustFixedColWithTypeCheck[float64](v1)[0])
	}

	// date
	{
		v1 := vector.NewVec(types.T_date.ToType())
		v2 := vector.NewVec(types.T_date.ToType())
		err := vector.AppendFixed(v2, types.Date(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Date(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Date(1), vector.MustFixedColWithTypeCheck[types.Date](v1)[0])
	}

	// datetime
	{
		v1 := vector.NewVec(types.T_datetime.ToType())
		v2 := vector.NewVec(types.T_datetime.ToType())
		err := vector.AppendFixed(v2, types.Datetime(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Datetime(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Datetime(1), vector.MustFixedColWithTypeCheck[types.Datetime](v1)[0])
	}

	// time
	{
		v1 := vector.NewVec(types.T_time.ToType())
		v2 := vector.NewVec(types.T_time.ToType())
		err := vector.AppendFixed(v2, types.Time(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Time(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Time(1), vector.MustFixedColWithTypeCheck[types.Time](v1)[0])
	}

	// timestamp
	{
		v1 := vector.NewVec(types.T_timestamp.ToType())
		v2 := vector.NewVec(types.T_timestamp.ToType())
		err := vector.AppendFixed(v2, types.Timestamp(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Timestamp(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Timestamp(1), vector.MustFixedColWithTypeCheck[types.Timestamp](v1)[0])
	}

	// enum
	{
		v1 := vector.NewVec(types.T_enum.ToType())
		v2 := vector.NewVec(types.T_enum.ToType())
		err := vector.AppendFixed(v2, types.Enum(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Enum(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Enum(1), vector.MustFixedColWithTypeCheck[types.Enum](v1)[0])
	}

	// decimal64
	{
		v1 := vector.NewVec(types.T_decimal64.ToType())
		v2 := vector.NewVec(types.T_decimal64.ToType())
		err := vector.AppendFixed(v2, types.Decimal64(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Decimal64(0), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, types.Decimal64(1), vector.MustFixedColWithTypeCheck[types.Decimal64](v1)[0])
	}

	// decimal128
	{
		v1 := vector.NewVec(types.T_decimal128.ToType())
		v2 := vector.NewVec(types.T_decimal128.ToType())
		dec128 := types.Decimal128{B0_63: 1, B64_127: 0}
		err := vector.AppendFixed(v2, dec128, false, proc.Mp())
		require.NoError(t, err)
		dec128Zero := types.Decimal128{B0_63: 0, B64_127: 0}
		err = vector.AppendFixed(v1, dec128Zero, false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, dec128, vector.MustFixedColWithTypeCheck[types.Decimal128](v1)[0])
	}

	// decimal128 target with int64 fill value
	{
		targetType := types.T_decimal128.ToTypeWithScale(2)
		v1 := vector.NewVec(targetType)
		v2 := vector.NewVec(types.T_int64.ToType())
		err := vector.AppendFixed(v2, int64(1), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Decimal128{}, true, proc.Mp()) // Pre-allocate null target slot
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		expected, err := types.Decimal128FromInt64(1).Scale(2)
		require.NoError(t, err)
		require.False(t, v1.IsNull(0))
		require.Equal(t, expected, vector.MustFixedColWithTypeCheck[types.Decimal128](v1)[0])
	}

	// decimal128 target with different-scale decimal128 fill value
	{
		targetType := types.T_decimal128.ToTypeWithScale(4)
		sourceType := types.T_decimal128.ToTypeWithScale(2)
		v1 := vector.NewVec(targetType)
		v2 := vector.NewVec(sourceType)
		source, err := types.ParseDecimal128("1.23", sourceType.Width, sourceType.Scale)
		require.NoError(t, err)
		err = vector.AppendFixed(v2, source, false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendFixed(v1, types.Decimal128{}, true, proc.Mp()) // Pre-allocate null target slot
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		expected, err := types.ParseDecimal128("1.2300", targetType.Width, targetType.Scale)
		require.NoError(t, err)
		require.False(t, v1.IsNull(0))
		require.Equal(t, expected, vector.MustFixedColWithTypeCheck[types.Decimal128](v1)[0])
	}

	// uuid
	{
		v1 := vector.NewVec(types.T_uuid.ToType())
		v2 := vector.NewVec(types.T_uuid.ToType())
		uuid := types.Uuid{}
		uuid[0] = 1
		err := vector.AppendFixed(v2, uuid, false, proc.Mp())
		require.NoError(t, err)
		uuidZero := types.Uuid{}
		err = vector.AppendFixed(v1, uuidZero, false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, uuid, vector.MustFixedColWithTypeCheck[types.Uuid](v1)[0])
	}

	// TS
	{
		v1 := vector.NewVec(types.T_TS.ToType())
		v2 := vector.NewVec(types.T_TS.ToType())
		ts := types.TS{}
		ts[0] = 1
		err := vector.AppendFixed(v2, ts, false, proc.Mp())
		require.NoError(t, err)
		tsZero := types.TS{}
		err = vector.AppendFixed(v1, tsZero, false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, ts, vector.MustFixedColWithTypeCheck[types.TS](v1)[0])
	}

	// Rowid
	{
		v1 := vector.NewVec(types.T_Rowid.ToType())
		v2 := vector.NewVec(types.T_Rowid.ToType())
		rowid := types.Rowid{}
		rowid[0] = 1
		err := vector.AppendFixed(v2, rowid, false, proc.Mp())
		require.NoError(t, err)
		rowidZero := types.Rowid{}
		err = vector.AppendFixed(v1, rowidZero, false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, rowid, vector.MustFixedColWithTypeCheck[types.Rowid](v1)[0])
	}

	// char
	{
		v1 := vector.NewVec(types.T_char.ToType())
		v2 := vector.NewVec(types.T_char.ToType())
		err := vector.AppendBytes(v2, []byte("1"), false, proc.Mp())
		require.NoError(t, err)
		err = vector.AppendBytes(v1, []byte("0"), false, proc.Mp()) // Pre-allocate space
		require.NoError(t, err)
		err = setValue(v1, v2, 0, 0, proc)
		require.NoError(t, err)
		require.Equal(t, []byte("1"), v1.GetBytesAt(0))
	}
}

func TestSetDecimal128ValueCastsSourceTypes(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	targetType := types.T_decimal128.ToTypeWithScale(2)

	run := func(t *testing.T, src *vector.Vector, want string) {
		t.Helper()
		defer src.Free(proc.Mp())

		dst := vector.NewVec(targetType)
		defer dst.Free(proc.Mp())
		require.NoError(t, vector.AppendFixed(dst, types.Decimal128{}, true, proc.Mp()))

		require.NoError(t, setValue(dst, src, 0, 0, proc))
		require.False(t, dst.IsNull(0))

		wantDecimal, err := types.ParseDecimal128(want, targetType.Width, targetType.Scale)
		require.NoError(t, err)
		require.Equal(t, wantDecimal, vector.GetFixedAtNoTypeCheck[types.Decimal128](dst, 0))
	}

	t.Run("int8", func(t *testing.T) {
		src := vector.NewVec(types.T_int8.ToType())
		require.NoError(t, vector.AppendFixed(src, int8(-12), false, proc.Mp()))
		run(t, src, "-12.00")
	})
	t.Run("int16", func(t *testing.T) {
		src := vector.NewVec(types.T_int16.ToType())
		require.NoError(t, vector.AppendFixed(src, int16(-123), false, proc.Mp()))
		run(t, src, "-123.00")
	})
	t.Run("int32", func(t *testing.T) {
		src := vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(src, int32(-1234), false, proc.Mp()))
		run(t, src, "-1234.00")
	})
	t.Run("int64", func(t *testing.T) {
		src := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(src, int64(-12345), false, proc.Mp()))
		run(t, src, "-12345.00")
	})
	t.Run("uint8", func(t *testing.T) {
		src := vector.NewVec(types.T_uint8.ToType())
		require.NoError(t, vector.AppendFixed(src, uint8(12), false, proc.Mp()))
		run(t, src, "12.00")
	})
	t.Run("uint16", func(t *testing.T) {
		src := vector.NewVec(types.T_uint16.ToType())
		require.NoError(t, vector.AppendFixed(src, uint16(123), false, proc.Mp()))
		run(t, src, "123.00")
	})
	t.Run("uint32", func(t *testing.T) {
		src := vector.NewVec(types.T_uint32.ToType())
		require.NoError(t, vector.AppendFixed(src, uint32(1234), false, proc.Mp()))
		run(t, src, "1234.00")
	})
	t.Run("uint64", func(t *testing.T) {
		src := vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed(src, uint64(18446744073709551615), false, proc.Mp()))
		run(t, src, "18446744073709551615.00")
	})
	t.Run("float32", func(t *testing.T) {
		src := vector.NewVec(types.T_float32.ToType())
		require.NoError(t, vector.AppendFixed(src, float32(12.25), false, proc.Mp()))
		run(t, src, "12.25")
	})
	t.Run("float64", func(t *testing.T) {
		src := vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(src, float64(123.45), false, proc.Mp()))
		run(t, src, "123.45")
	})
	t.Run("decimal64", func(t *testing.T) {
		srcType := types.T_decimal64.ToTypeWithScale(1)
		src := vector.NewVec(srcType)
		require.NoError(t, vector.AppendFixed(src, types.Decimal64(123), false, proc.Mp()))
		run(t, src, "12.30")
	})
	t.Run("decimal128_different_scale", func(t *testing.T) {
		srcType := types.T_decimal128.ToTypeWithScale(4)
		src := vector.NewVec(srcType)
		value, err := types.ParseDecimal128("12.3400", srcType.Width, srcType.Scale)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixed(src, value, false, proc.Mp()))
		run(t, src, "12.34")
	})
}

func TestSetDecimal128ValueRejectsUnsupportedSourceType(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	dst := vector.NewVec(types.T_decimal128.ToTypeWithScale(2))
	defer dst.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(dst, types.Decimal128{}, true, proc.Mp()))

	src := vector.NewVec(types.T_bool.ToType())
	defer src.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(src, true, false, proc.Mp()))

	err := setValue(dst, src, 0, 0, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot set decimal128 fill value")
}

func TestOpType(t *testing.T) {
	require.Equal(t, vm.Fill, (&Fill{}).OpType())
}

func TestResetColRefRecursesIntoFunctionArgs(t *testing.T) {
	expr := &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Args: []*plan.Expr{
					{
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{RelPos: 3, ColPos: 4},
						},
					},
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{RelPos: 5, ColPos: 6},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resetColRef(expr, 0)

	outerCol := expr.GetF().Args[0].GetCol()
	require.Equal(t, int32(-1), outerCol.RelPos)
	require.Equal(t, int32(0), outerCol.ColPos)

	innerCol := expr.GetF().Args[1].GetF().Args[0].GetCol()
	require.Equal(t, int32(-1), innerCol.RelPos)
	require.Equal(t, int32(0), innerCol.ColPos)
}

func TestProcessValueFillsNullsWithConstantVector(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.SetVector(0, testutil.MakeInt64Vector([]int64{0, 2}, []uint64{0}, proc.Mp()))
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())

	fillValue := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(fillValue, int64(9), false, proc.Mp()))

	arg := &Fill{ColLen: 1}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	arg.ctr.valVecs = []*vector.Vector{fillValue}
	defer arg.Free(proc, false, nil)

	result, err := processValue(&arg.ctr, arg, proc, process.NewAnalyzer(0, false, false, "fill"))
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.NotNil(t, result.Batch)

	values := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])
	require.Equal(t, []int64{9, 2}, values)
	require.False(t, result.Batch.Vecs[0].IsNull(0))
}

func TestProcessValueFillsDecimal128NullsWithIntegerConstant(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	typ := types.T_decimal128.ToTypeWithScale(0)
	decimalVec := vector.NewVec(typ)
	require.NoError(t, vector.AppendFixed(decimalVec, types.Decimal128{}, true, proc.Mp()))
	require.NoError(t, vector.AppendFixed(decimalVec, types.Decimal128FromInt64(-2109048), false, proc.Mp()))

	input := batch.NewWithSize(1)
	input.SetVector(0, decimalVec)
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())

	fillValue := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(fillValue, int64(100), false, proc.Mp()))

	arg := &Fill{ColLen: 1}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	arg.ctr.valVecs = []*vector.Vector{fillValue}
	defer arg.Free(proc, false, nil)

	result, err := processValue(&arg.ctr, arg, proc, process.NewAnalyzer(0, false, false, "fill"))
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.NotNil(t, result.Batch)

	values := vector.MustFixedColWithTypeCheck[types.Decimal128](result.Batch.Vecs[0])
	require.Equal(t, types.Decimal128FromInt64(100), values[0])
	require.Equal(t, types.Decimal128FromInt64(-2109048), values[1])
	require.False(t, result.Batch.Vecs[0].IsNull(0))
}

func TestProcessPrevFillsFromPreviousValueAcrossBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	first := batch.NewWithSize(1)
	first.SetVector(0, testutil.MakeInt64Vector([]int64{0, 10, 0}, []uint64{0, 2}, proc.Mp()))
	first.SetRowCount(3)
	defer first.Clean(proc.Mp())

	second := batch.NewWithSize(1)
	second.SetVector(0, testutil.MakeInt64Vector([]int64{0, 30}, []uint64{0}, proc.Mp()))
	second.SetRowCount(2)
	defer second.Clean(proc.Mp())

	arg := &Fill{ColLen: 1}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second}))
	arg.ctr.prevVecs = make([]*vector.Vector, 1)
	defer arg.Free(proc, false, nil)

	analyzer := process.NewAnalyzer(0, false, false, "fill")

	result, err := processPrev(&arg.ctr, arg, proc, analyzer)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	firstValues := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])
	require.True(t, result.Batch.Vecs[0].IsNull(0))
	require.Equal(t, int64(10), firstValues[1])
	require.Equal(t, int64(10), firstValues[2])

	result, err = processPrev(&arg.ctr, arg, proc, analyzer)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	secondValues := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])
	require.Equal(t, []int64{10, 30}, secondValues)
	require.False(t, result.Batch.Vecs[0].IsNull(0))

	result, err = processPrev(&arg.ctr, arg, proc, analyzer)
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Nil(t, result.Batch)
}

func TestProcessDefaultPassesThroughBatchAndStops(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.SetVector(0, testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp()))
	input.SetRowCount(2)
	defer input.Clean(proc.Mp())

	arg := &Fill{}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))

	result, err := processDefault(&arg.ctr, arg, proc, process.NewAnalyzer(0, false, false, "fill"))
	require.NoError(t, err)
	require.Equal(t, vm.ExecNext, result.Status)
	require.Same(t, input, result.Batch)

	result, err = processDefault(&arg.ctr, arg, proc, process.NewAnalyzer(0, false, false, "fill"))
	require.NoError(t, err)
	require.Equal(t, vm.ExecStop, result.Status)
	require.Nil(t, result.Batch)
}

func TestLinearFillValueUsesExpressionForNonDecimal128(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()

	input := batch.NewWithSize(1)
	input.SetVector(0, testutil.MakeInt64Vector([]int64{10, 0, 30}, []uint64{1}, proc.Mp()))
	input.SetRowCount(3)
	defer input.Clean(proc.Mp())

	resultVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(resultVec, int64(20), false, proc.Mp()))
	defer resultVec.Free(proc.Mp())

	ctr := &container{
		bats:   []*batch.Batch{input},
		preIdx: 0,
		preRow: 0,
		curIdx: 0,
		curRow: 2,
		exes: []colexec.ExpressionExecutor{
			&fillStubExpressionExecutor{result: resultVec},
		},
	}

	vec, owned, err := linearFillValue(ctr, proc, 0)
	require.NoError(t, err)
	require.False(t, owned)
	require.Same(t, resultVec, vec)
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](vec, 0))
}

type fillStubExpressionExecutor struct {
	result *vector.Vector
}

func (s *fillStubExpressionExecutor) Eval(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return s.result, nil
}

func (s *fillStubExpressionExecutor) EvalWithoutResultReusing(*process.Process, []*batch.Batch, []bool) (*vector.Vector, error) {
	return s.result, nil
}

func (s *fillStubExpressionExecutor) ResetForNextQuery() {}

func (s *fillStubExpressionExecutor) Free() {}

func (s *fillStubExpressionExecutor) IsColumnExpr() bool {
	return false
}

func (s *fillStubExpressionExecutor) TypeName() string {
	return "fillStubExpressionExecutor"
}
