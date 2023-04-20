package function2

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSleep(t *testing.T) {
	testCases := []tcTemp{
		{
			info: "sleep uint64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1}, []bool{false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0}, []bool{false}),
		},
		{
			info: "sleep uint64 with null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_uint64.ToType(), []uint64{1, 0, 1}, []bool{false, true, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0, 0, 0}, []bool{false, true, true}),
		},
	}

	// do the test work.
	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Sleep[uint64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}

	testCases2 := []tcTemp{
		{
			info: "sleep float64",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{0.1, 0.2}, []bool{false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0, 0}, []bool{false, false}),
		},
		{
			info: "sleep float64 with null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{0.1, 0, 0.1}, []bool{false, true, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false, []uint8{0, 0, 0}, []bool{false, true, true}),
		},
		{
			info: "sleep float64 with null",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(), []float64{0.1, -1.0, 0.1}, []bool{false, false, true}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), true, []uint8{0, 0, 0}, []bool{false, true, true}),
		},
	}

	for _, tc := range testCases2 {
		fcTC := testutil.NewFunctionTestCase(proc,
			tc.inputs, tc.expect, Sleep[float64])
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}
