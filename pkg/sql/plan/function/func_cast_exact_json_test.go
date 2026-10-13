// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
