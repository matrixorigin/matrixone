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

package bytejson

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryWithMemorySemanticsAndLifetime(t *testing.T) {
	tests := []struct {
		name, document, path, result string
		exists                       bool
	}{
		{"root scalar autowrap", `1`, `$[0]`, `1`, true},
		{"object autowrap", `{"k":1}`, `$[0].k`, `1`, true},
		{"missing", `{"a":[1,2]}`, `$.missing[*]`, `null`, false},
		{"null", `{"a":null}`, `$.a`, `null`, true},
		{"wildcard", `{"a":[1,2]}`, `$.a[*]`, `[1,2]`, true},
		{"single wildcard", `{"a":[1]}`, `$.a[*]`, `[1]`, true},
		{"range", `{"a":[1,2]}`, `$.a[0 to 1]`, `[1,2]`, true},
		{"last", `{"a":[1,2]}`, `$.a[last]`, `2`, true},
		{"nested wildcard", `[{"a":[1,2]},{"a":[3]}]`, `$[*].a[*]`, `[1,2,3]`, true},
		{"recursive", `{"a":1,"nested":{"a":2}}`, `$**.a`, `[1,2]`, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			doc, err := ParseFromString(test.document)
			require.NoError(t, err)
			path, err := ParseJsonPath(test.path)
			require.NoError(t, err)
			var live uint64
			reserve := func(n uint64) (func(), error) {
				live += n
				return func() { live -= n }, nil
			}
			result, exists, release, err := doc.QueryWithMemory(&path, reserve)
			require.NoError(t, err)
			defer func() {
				if release != nil {
					release()
				}
			}()
			require.Equal(t, test.exists, exists)
			require.JSONEq(t, test.result, result.String())
			if !path.IsSimple() && exists {
				require.Positive(t, live, "a merged result retains its backing reservation")
			} else {
				require.Zero(t, live)
			}
			if release != nil {
				release()
				release = nil
			}
			require.Zero(t, live)
		})
	}
}

func TestQueryWithMemoryReleasesPartialFailures(t *testing.T) {
	doc, err := ParseFromString(`[1,2]`)
	require.NoError(t, err)
	path, err := ParseJsonPath(`$[*]`)
	require.NoError(t, err)
	want := errors.New("admission failed")
	for failAt := 1; failAt <= 3; failAt++ {
		t.Run(string(rune('0'+failAt)), func(t *testing.T) {
			calls := 0
			var live uint64
			reserve := func(n uint64) (func(), error) {
				calls++
				live += n
				release := func() { live -= n }
				if calls == failAt {
					return release, want
				}
				return release, nil
			}
			_, _, release, err := doc.QueryWithMemory(&path, reserve)
			require.ErrorIs(t, err, want)
			require.Nil(t, release)
			require.Zero(t, live)
		})
	}
}
