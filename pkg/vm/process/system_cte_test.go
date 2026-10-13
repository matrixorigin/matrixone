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

package process

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSystemCTELimitsOverrideOnlyRecursiveBudgets(t *testing.T) {
	ctx := WithSystemCTELimits(context.Background())
	require.True(t, HasSystemCTELimits(ctx))
	resolver := SystemCTEResolver(func(name string, _, _ bool) (interface{}, error) {
		return "user:" + name, nil
	})
	depth, err := resolver("cte_max_recursion_depth", true, false)
	require.NoError(t, err)
	require.Equal(t, SystemCTEMaxRecursionDepth, depth)
	memory, err := resolver(CTEMemoryQuotaVariable, true, false)
	require.NoError(t, err)
	require.Equal(t, int64(DefaultCTEMemoryQuotaBytes), memory)
	other, err := resolver("sql_mode", true, false)
	require.NoError(t, err)
	require.Equal(t, "user:sql_mode", other)
}
