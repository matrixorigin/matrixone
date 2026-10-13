// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCastExactJSONDecimal(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct{ value, want string }{{"9007199254740993.001", "9007199254740993.001"}, {"18446744073709551615", "18446744073709551615.000"}, {"1e-3", "0.001"}, {"-9007199254740993.001", "-9007199254740993.001"}} {
		bj, e := bytejson.ParseFromStringExact(tc.value)
		require.NoError(t, e)
		data, e := types.EncodeJson(bj)
		require.NoError(t, e)
		target := types.New(types.T_decimal128, 38, 3)
		d, e := types.ParseDecimal128(tc.want, 38, 3)
		require.NoError(t, e)
		fc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_json.ToType(), []string{string(data)}, nil), NewFunctionTestInput(target, []types.Decimal128{}, nil)}, NewFunctionTestResult(target, false, []types.Decimal128{d}, nil), NewCast)
		ok, info := fc.RunAndFree()
		require.True(t, ok, info)
	}
}
