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

package plugin

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

func mustParams(t *testing.T, m map[string]string) string {
	t.Helper()
	s, err := catalog.IndexParamsMapToJsonString(m)
	require.NoError(t, err)
	return s
}

// TestAsyncParamPath covers the async resolution using an UNREGISTERED algo so
// the plugin-identity path is out of the picture (that path is exercised by each
// plugin's own SyncDescriptor test). AlwaysAsync is identity-only; the async
// opt-in param feeds IsAsync but is NOT an always-async source.
func TestAsyncParamPath(t *testing.T) {
	const noAlgo = "no_such_algo" // Get() misses ⇒ identity always-async is false

	asyncParam := mustParams(t, map[string]string{catalog.Async: "true"})

	// AlwaysAsync = identity only (false here for an unregistered algo). The async
	// param is opt-in, NOT an always-async source.
	require.False(t, AlwaysAsync(noAlgo, asyncParam), "async param is opt-in, not always-async")
	require.False(t, AlwaysAsync(noAlgo, ""), "no params ⇒ not always async")

	// IsAsync = AlwaysAsync OR async param.
	for _, tc := range []struct {
		name   string
		params string
		want   bool
	}{
		{"asyncParam", asyncParam, true},
		{"empty", "", false},
	} {
		got, err := IsAsync(noAlgo, tc.params)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
}
