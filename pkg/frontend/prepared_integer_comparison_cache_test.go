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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestPreparedIntegerComparisonCacheKey(t *testing.T) {
	bindings := []plan2.PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}}
	key := func(value any) string {
		return preparedExecutionBindingKey(bindings, []any{plan2.ParamValue{Value: value, IsBinaryProtocol: true}}, true)
	}
	require.Equal(t, key(int64(42)), key(int64(43)), "ordinary rebinding reuses the range specialization")
	require.NotEqual(t, key(int64(127)), key(int64(128)))
	require.NotEqual(t, key(int64(32767)), key(int64(32768)))
	require.NotEqual(t, key(int64(2147483647)), key(int64(2147483648)))
	require.NotEqual(t, key(int64(-2147483648)), key(int64(-2147483649)))
	require.NotEqual(t, key(int64(0)), key(nil))
	require.Equal(t,
		preparedExecutionBindingKey(bindings, []any{int64(42)}, false),
		preparedExecutionBindingKey(bindings, []any{int64(2147483648)}, false),
		"plans without comparison narrowing retain source-type cache identity")
}
