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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// TestDefaultResolveVariableWired asserts that pkg/frontend's init()
// populated executor.DefaultResolveVariable, that it returns
// gSysVarsDefs[name].Default for a known system variable, and that
// it errors on user variables and unknown names — mirroring the
// nil-resolver-replacement contract that ProcessInitSQL relies on.
func TestDefaultResolveVariableWired(t *testing.T) {
	require.NotNil(t, executor.DefaultResolveVariable,
		"pkg/frontend/init.go must wire executor.DefaultResolveVariable")

	// Known system var → default value.
	v, err := executor.DefaultResolveVariable("kmeans_train_percent", true, false)
	require.NoError(t, err)
	require.Equal(t, float64(10), v)

	v, err = executor.DefaultResolveVariable("kmeans_max_iteration", true, false)
	require.NoError(t, err)
	require.Equal(t, int64(20), v)

	// Case-insensitive (Mixed-case input must still resolve).
	v, err = executor.DefaultResolveVariable("Kmeans_Train_Percent", true, false)
	require.NoError(t, err)
	require.Equal(t, float64(10), v)

	// Unknown system var → error.
	_, err = executor.DefaultResolveVariable("definitely_not_a_real_var", true, false)
	require.Error(t, err)

	// User variables are not supported by the background resolver.
	_, err = executor.DefaultResolveVariable("kmeans_train_percent", false, false)
	require.Error(t, err)
}
