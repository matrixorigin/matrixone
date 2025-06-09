package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestHnswCdcUpdateFn(t *testing.T) {
	tcs := []tcTemp{
		{
			info: "nargs invalid",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "dbname null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "table name null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "dimension null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{0}, []bool{true}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "cdc json null",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{""}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},

		{
			info: "cdc json invalid",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2}, []bool{false}),
				NewFunctionTestInput(types.T_json.ToType(),
					[]string{"{..."}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		},
	}

	proc := testutil.NewProcess()
	for _, tc := range tcs {
		fcTC := NewFunctionTestCase(proc,
			tc.inputs, tc.expect, hnswCdcUpdate)
		s, info := fcTC.Run()
		require.True(t, s, info, tc.info)
	}
}
