// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
)

func TestAdvertisedCollationsAreExecutable(t *testing.T) {
	want := map[string]string{
		"binary":             "binary",
		"utf8_bin":           "utf8",
		"utf8_general_ci":    "utf8",
		"utf8mb4_bin":        "utf8mb4",
		"utf8mb4_general_ci": "utf8mb4",
	}
	defaults := map[string]string{
		"binary":  "binary",
		"utf8":    "utf8_general_ci",
		"utf8mb4": "utf8mb4_general_ci",
	}

	require.Len(t, Collations, len(want))
	seenDefaults := make(map[string]string)
	for _, collation := range Collations {
		require.Equal(t, want[collation.collationName], collation.charset,
			"SHOW COLLATION must list only implemented collation identities")
		if collation.isDefault == "YES" {
			require.NotContains(t, seenDefaults, collation.charset)
			seenDefaults[collation.charset] = collation.collationName
		}
	}
	require.Equal(t, defaults, seenDefaults)
}
