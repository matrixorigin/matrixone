// Copyright 2021 - 2022 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func Test_BuiltIn_CurrentSessionInfo(t *testing.T) {
	proc := testutil.NewProcess()
	proc.Base.SessionInfo = process.SessionInfo{
		User:      "test_user1",
		UserId:    135,
		Account:   "test_account2",
		AccountId: 246,
		Role:      "test_role3",
		RoleId:    147,
	}

	{
		tc := tcTemp{
			info:   "select current_user_id()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_uint32.ToType(), false,
				[]uint32{135}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentUserID)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_user_name()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_user1"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentUserName)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_account_id()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_uint32.ToType(), false,
				[]uint32{246}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentAccountID)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_account_name()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_account2"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentAccountName)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_role_id()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_uint32.ToType(), false,
				[]uint32{147}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentRoleID)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_role_name()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_role3"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentRoleName)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info:   "select current_role()",
			inputs: []FunctionTestInput{},
			expect: NewFunctionTestResult(
				types.T_varchar.ToType(), false,
				[]string{"test_role3"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCurrentRole)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_BuiltIn_Rpad(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "test rpad('hello', num, '#') with num = 0, 1, 10",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "hello", "hello"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 1, 10}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"#", "#", "#"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "h", "hello#####"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInRpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test rpad('hello', num, '#@&') with num = 15",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{15}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"#@&"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello#@&#@&#@&#"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInRpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test rpad('hello', num, '#@&') with num = 15, -1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "hello"}, []bool{false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{15, -1}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"#@&", "#@&"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"hello#@&#@&#@&#", ""}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInRpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test rpad('你好', num, '再见') with num = 10",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"你好"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"再见"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"你好再见再见再见再见"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInRpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_BuiltIn_Lpad(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "test lpad('hello', num, '#') with num = 1, 10 \n" +
				"test lpad('hello', num, '#@&') with num = 15",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "hello", "hello"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 10, 15}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"#", "#", "#@&"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"h", "#####hello", "#@&#@&#@&#hello"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test lpad('12345678', num, 'abcdefgh') with num = 10",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"12345678"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"abcdefgh"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"ab12345678"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test lpad('你好', num, '再见') with num = 10",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"你好"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"再见"}, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"再见再见再见再见你好"}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLpad)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_BuiltIn_Repeat(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "test repeat('ab', num) with num = -1, 0, 1, 3, null, 1000000000000",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_varchar.ToType(),
					[]string{"ab"}, nil),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1, 0, 1, 3, 0, 1000000000000}, []bool{false, false, false, false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "", "ab", "ababab", "", ""}, []bool{false, false, false, false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInRepeat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test repeat(null, num) with num = -1, 0, 1, 3, null, 1000000000000",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "", "", "", "", ""}, []bool{true, true, true, true, true, true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1, 0, 1, 3, 0, 1000000000000}, []bool{false, false, false, false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "", "", "", "", ""}, []bool{true, true, true, true, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInRepeat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_BuiltIn_Serial(t *testing.T) {
	proc := testutil.NewProcess()

	{
		input1 := []bool{true, false}
		input2 := []int8{10, 1}

		tc := tcTemp{
			info: "test serial(bool, int8)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					input1, nil),
				NewFunctionTestInput(types.T_int8.ToType(),
					input2, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"serial(true, 10)", "serial(false, 1)"}, nil),
		}
		opSerial := newOpSerial()
		defer opSerial.Close()
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opSerial.BuiltInSerial)
		tcc.Run()

		vec := tcc.GetResultVectorDirectly()
		p1 := vector.GenerateFunctionStrParameter(vec)
		{
			v, null := p1.GetStrValue(0)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Equal(t, input1[0], tuple[0], tc.info)
			require.Equal(t, input2[0], tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(1)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Equal(t, input1[1], tuple[0], tc.info)
			require.Equal(t, input2[1], tuple[1], tc.info)
		}
	}

	// test for uuid
	{
		// copy from pkg/container/types/uuid_test.go
		input1 := []types.Uuid{
			// "0d5687da-2a67-11ed-99e0-000c29847904"
			{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
			// "6119dffd-2a6b-11ed-99e0-000c29847904"
			{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
		}
		input2 := []bool{true, false}

		tc := tcTemp{
			info: "test serial(uuid, bool)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uuid.ToType(),
					input1, nil),
				NewFunctionTestInput(types.T_bool.ToType(),
					input2, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"serial('0d5687da-2a67-11ed-99e0-000c29847904', true)", "serial('6119dffd-2a6b-11ed-99e0-000c29847904', false)"}, nil),
		}
		opSerial := newOpSerial()
		defer opSerial.Close()
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opSerial.BuiltInSerial)
		tcc.Run()

		vec := tcc.GetResultVectorDirectly()
		p1 := vector.GenerateFunctionStrParameter(vec)
		{
			v, null := p1.GetStrValue(0)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "0d5687da-2a67-11ed-99e0-000c29847904", ustr, tc.info)
			require.Equal(t, true, tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(1)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "6119dffd-2a6b-11ed-99e0-000c29847904", ustr, tc.info)
			require.Equal(t, false, tuple[1], tc.info)
		}
	}

	// test for rows that contain null
	{
		input1 := []types.Uuid{
			// "0d5687da-2a67-11ed-99e0-000c29847904"
			{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
			// "6119dffd-2a6b-11ed-99e0-000c29847904"
			{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
		}
		input2 := []bool{true, false}

		tc := tcTemp{
			info: "test serial(uuid, bool)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uuid.ToType(),
					input1, []bool{false, true}),
				NewFunctionTestInput(types.T_bool.ToType(),
					input2, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"serial('0d5687da-2a67-11ed-99e0-000c29847904', true)", "serial('6119dffd-2a6b-11ed-99e0-000c29847904', false)"}, nil),
		}
		opSerial := newOpSerial()
		defer opSerial.Close()
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opSerial.BuiltInSerial)
		tcc.Run()

		vec := tcc.GetResultVectorDirectly()
		p1 := vector.GenerateFunctionStrParameter(vec)
		{
			v, null := p1.GetStrValue(0)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "0d5687da-2a67-11ed-99e0-000c29847904", ustr, tc.info)
			require.Equal(t, true, tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(1)
			require.True(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Nil(t, tuple, tc.info)
		}
	}
}

func Test_BuiltIn_SerialFull(t *testing.T) {
	proc := testutil.NewProcess()

	{
		// serial_full functionality (preserving nulls)
		input1 := []bool{true, false, true, true}
		input1Nulls := []bool{true, false, true, true}
		input2 := []int8{10, 1, 120, -1}
		input2Nulls := []bool{false, true, false, true}

		tc := tcTemp{
			info: "test serial_full(input1, input2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(), input1, input1Nulls),
				NewFunctionTestInput(types.T_int8.ToType(), input2, input2Nulls),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"serial_full(null, 10)", "serial_full(false, null)", "serial_full(null, 120)", "serial_full(null, null)"}, nil),
		}
		opSerial := newOpSerial()
		defer opSerial.Close()
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opSerial.BuiltInSerialFull)
		tcc.Run()

		vec := tcc.GetResultVectorDirectly()
		p1 := vector.GenerateFunctionStrParameter(vec)
		{
			v, null := p1.GetStrValue(0)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Equal(t, nil, tuple[0], tc.info) // note: nulls are preserved
			require.Equal(t, input2[0], tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(1)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Equal(t, input1[1], tuple[0], tc.info)
			require.Equal(t, nil, tuple[1], tc.info) // note: nulls are preserved
		}
		{
			v, null := p1.GetStrValue(2)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Equal(t, nil, tuple[0], tc.info) // note: nulls are preserved
			require.Equal(t, input2[2], tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(3)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			require.Equal(t, nil, tuple[0], tc.info) // note: nulls are preserved
			require.Equal(t, nil, tuple[1], tc.info) // note: nulls are preserved
		}
	}

	{
		// copy from pkg/container/types/uuid_test.go
		input1 := []types.Uuid{
			// "0d5687da-2a67-11ed-99e0-000c29847904"
			{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
			// "6119dffd-2a6b-11ed-99e0-000c29847904"
			{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
		}
		input2 := []bool{true, false}

		tc := tcTemp{
			info: "test serial(uuid, bool)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uuid.ToType(),
					input1, nil),
				NewFunctionTestInput(types.T_bool.ToType(),
					input2, nil),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"serial('0d5687da-2a67-11ed-99e0-000c29847904', true)", "serial('6119dffd-2a6b-11ed-99e0-000c29847904', false)"}, nil),
		}
		opSerial := newOpSerial()
		defer opSerial.Close()
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opSerial.BuiltInSerialFull)
		tcc.Run()

		vec := tcc.GetResultVectorDirectly()
		p1 := vector.GenerateFunctionStrParameter(vec)
		{
			v, null := p1.GetStrValue(0)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "0d5687da-2a67-11ed-99e0-000c29847904", ustr, tc.info)
			require.Equal(t, true, tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(1)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "6119dffd-2a6b-11ed-99e0-000c29847904", ustr, tc.info)
			require.Equal(t, false, tuple[1], tc.info)
		}
	}

	// test for rows that contain null
	{
		input1 := []types.Uuid{
			// "0d5687da-2a67-11ed-99e0-000c29847904"
			{13, 86, 135, 218, 42, 103, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
			// "6119dffd-2a6b-11ed-99e0-000c29847904"
			{97, 25, 223, 253, 42, 107, 17, 237, 153, 224, 0, 12, 41, 132, 121, 4},
		}
		input2 := []bool{true, false}

		tc := tcTemp{
			info: "test serial(uuid, bool)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uuid.ToType(),
					input1, nil),
				NewFunctionTestInput(types.T_bool.ToType(),
					input2, []bool{false, true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"serial('0d5687da-2a67-11ed-99e0-000c29847904', true)", "serial('6119dffd-2a6b-11ed-99e0-000c29847904', false)"}, nil),
		}
		opSerial := newOpSerial()
		defer opSerial.Close()
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, opSerial.BuiltInSerialFull)
		tcc.Run()

		vec := tcc.GetResultVectorDirectly()
		p1 := vector.GenerateFunctionStrParameter(vec)
		{
			v, null := p1.GetStrValue(0)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "0d5687da-2a67-11ed-99e0-000c29847904", ustr, tc.info)
			require.Equal(t, true, tuple[1], tc.info)
		}
		{
			v, null := p1.GetStrValue(1)
			require.False(t, null, tc.info)
			tuple, err := types.Unpack(v)
			require.NoError(t, err, tc.info)
			ustr, err := types.UuidToString(tuple[0].(types.Uuid))
			require.NoError(t, err, tc.info)
			require.Equal(t, "6119dffd-2a6b-11ed-99e0-000c29847904", ustr, tc.info)
			require.Nil(t, tuple[1], tc.info)
		}
	}
}

func initSerialExtractTestCase() []tcTemp {
	ps := types.NewPacker()
	defer ps.Close()
	ps.EncodeInt8(10)
	ps.EncodeStringType([]byte("adam"))

	return []tcTemp{
		{
			info: "test serial_extract( serial(10,'adam'), 0 as Int8)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{convertByteSliceToString(ps.Bytes())},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0},
					[]bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{0},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{10},
				[]bool{false}),
		},
		{
			info: "test serial_extract( serial(10,'adam'), 1 as varchar)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{convertByteSliceToString(ps.Bytes())},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"adam"},
				[]bool{false}),
		},
		{
			info: "test serial_extract( serial(10,'adam'), 2 as varchar)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{convertByteSliceToString(ps.Bytes())},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2},
					[]bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), true,
				[]string{"adam"},
				[]bool{false}),
		},
	}
}

func TestSerialExtract(t *testing.T) {
	testCases := initSerialExtractTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSerialExtract)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func Test_BuiltIn_Math(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "test ln",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1, math.Exp(0), math.Exp(1), math.Exp(10), math.Exp(100), math.Exp(99), math.Exp(-1),
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0, 1, 10, 100, 99, -1}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test exp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-1, 0, 1, 2, 10, 100,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{math.Exp(-1), math.Exp(0), math.Exp(1), math.Exp(2), math.Exp(10), math.Exp(100)}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInExp)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test sin",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-math.Pi / 2, 0, math.Pi / 2,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-1, 0, 1}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSin)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test cos",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-math.Pi, 0, math.Pi,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-1, 1, -1}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCos)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test tan",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						0,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInTan)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test sinh",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						0,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSinh)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test acos",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInACos)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test atan",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						0,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInATan)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test atan with 2 args",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-1, 1, 1, 1, 1.0, 1.0,
					},
					nil),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1, 0, -1, 1, -1.0, 1.0,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-0.7853981633974483, 0, -0.7853981633974483, 0.7853981633974483, -0.7853981633974483, 0.7853981633974483}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInATan2)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						5,
					},
					nil),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						3,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0.6826061944859853}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log with err",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1,
					},
					nil),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				nil, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						8,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog2)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log2 with err",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-10,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				nil, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog2)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log10",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						100,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{2}, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog10)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log10 with err",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-10,
					},
					nil),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				nil, nil),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog10)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
