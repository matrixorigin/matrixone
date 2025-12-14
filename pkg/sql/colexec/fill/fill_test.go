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
