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

// TestAsyncVersionAndParamPaths covers the two param-derived inputs to the
// async resolution — engine version (AlwaysAsync) and the async opt-in param —
// using an UNREGISTERED algo so the plugin-identity path is out of the picture
// (that path is exercised by each plugin's own SyncDescriptor test). Together
// with those, this pins the union logic of AlwaysAsync / IsAsync.
func TestAsyncVersionAndParamPaths(t *testing.T) {
	const noAlgo = "no_such_algo" // Get() misses ⇒ identity always-async is false

	v2 := mustParams(t, map[string]string{catalog.IndexAlgoParamVersion: "2"})
	v1 := mustParams(t, map[string]string{catalog.IndexAlgoParamVersion: "1"})
	asyncParam := mustParams(t, map[string]string{catalog.Async: "true"})
	both := mustParams(t, map[string]string{catalog.IndexAlgoParamVersion: "2", catalog.Async: "true"})

	// AlwaysAsync = identity(false here) OR version>=2. The async param is NOT
	// an always-async source.
	require.True(t, AlwaysAsync(noAlgo, v2), "version>=2 ⇒ always async")
	require.False(t, AlwaysAsync(noAlgo, v1), "version 1 ⇒ not always async")
	require.False(t, AlwaysAsync(noAlgo, asyncParam), "async param is opt-in, not always-async")
	require.False(t, AlwaysAsync(noAlgo, ""), "no params ⇒ not always async")

	// IsAsync = AlwaysAsync OR async param.
	for _, tc := range []struct {
		name   string
		params string
		want   bool
	}{
		{"version2", v2, true},
		{"version1", v1, false},
		{"asyncParam", asyncParam, true},
		{"versionAndParam", both, true},
		{"empty", "", false},
	} {
		got, err := IsAsync(noAlgo, tc.params)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
}

// TestIndexAlgoParamVersionOf pins the version reader: explicit value, default
// to 1 when the key is absent or the params are empty.
func TestIndexAlgoParamVersionOf(t *testing.T) {
	require.Equal(t, int64(2), catalog.IndexAlgoParamVersionOf(
		mustParams(t, map[string]string{catalog.IndexAlgoParamVersion: "2"})))
	require.Equal(t, int64(1), catalog.IndexAlgoParamVersionOf(
		mustParams(t, map[string]string{catalog.IndexAlgoParamVersion: "1"})))
	require.Equal(t, int64(1), catalog.IndexAlgoParamVersionOf(
		mustParams(t, map[string]string{"parser": "gojieba"})), "no version key ⇒ v1")
	require.Equal(t, int64(1), catalog.IndexAlgoParamVersionOf(""), "empty params ⇒ v1")
}
