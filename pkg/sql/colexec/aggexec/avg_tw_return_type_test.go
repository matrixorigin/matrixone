// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestAvgTwCacheReturnType(t *testing.T) {
	require.Equal(t, types.T_char.ToType(), AvgTwCacheReturnType([]types.Type{types.T_int32.ToType()}))
	require.Equal(t, types.New(types.T_varchar, types.MaxVarcharLen, 10), AvgTwCacheReturnType([]types.Type{types.New(types.T_decimal64, 0, 4)}))
}

func TestAvgTwResultReturnType(t *testing.T) {
	require.Equal(t, types.T_float64.ToType(), AvgTwResultReturnType([]types.Type{types.T_char.ToType()}))
	require.Equal(t, types.New(types.T_decimal128, 18, 12), AvgTwResultReturnType([]types.Type{types.New(types.T_varchar, types.MaxVarcharLen, 12)}))
}
