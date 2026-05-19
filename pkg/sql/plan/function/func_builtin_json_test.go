// Copyright 2025 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestJsonLengthCheckFn(t *testing.T) {
	// Valid: 1 arg (json)
	ret := jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType()})
	require.Equal(t, succeedMatched, ret.status)

	// Valid: 1 arg (varchar)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_varchar.ToType()})
	require.Equal(t, succeedMatched, ret.status)

	// Valid: 2 args (json + varchar path)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_varchar.ToType()})
	require.Equal(t, succeedMatched, ret.status)

	// Valid: 2 args with cast (int -> varchar cast for first arg)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
	require.Equal(t, succeedWithCast, ret.status)

	// Invalid: 0 args
	ret = jsonLengthCheckFn(nil, []types.Type{})
	require.Equal(t, failedFunctionParametersWrong, ret.status)

	// Invalid: first arg cannot cast to varchar
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_geometry.ToType()})
	require.Equal(t, failedFunctionParametersWrong, ret.status)

	// Valid: 2 args with second arg castable to varchar (succeedWithCast on path)
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_int64.ToType()})
	require.Equal(t, succeedWithCast, ret.status)

	// Invalid: second arg cannot cast to varchar
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_geometry.ToType()})
	require.Equal(t, failedFunctionParametersWrong, ret.status)

	// Invalid: 3 args
	ret = jsonLengthCheckFn(nil, []types.Type{types.T_json.ToType(), types.T_varchar.ToType(), types.T_int64.ToType()})
	require.Equal(t, failedFunctionParametersWrong, ret.status)
}
