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
	type advertisedCollation struct {
		charset      string
		padAttribute string
	}
	want := map[string]advertisedCollation{
		"binary":             {charset: "binary", padAttribute: "NO PAD"},
		"utf8_bin":           {charset: "utf8", padAttribute: "PAD SPACE"},
		"utf8_general_ci":    {charset: "utf8", padAttribute: "PAD SPACE"},
		"utf8mb4_bin":        {charset: "utf8mb4", padAttribute: "PAD SPACE"},
		"utf8mb4_general_ci": {charset: "utf8mb4", padAttribute: "PAD SPACE"},
	}
	defaults := map[string]string{
		"binary":  "binary",
		"utf8":    "utf8_general_ci",
		"utf8mb4": "utf8mb4_general_ci",
	}

	require.Len(t, Collations, len(want))
	seenDefaults := make(map[string]string)
	for _, collation := range Collations {
		expected, ok := want[collation.collationName]
		require.True(t, ok, "SHOW COLLATION advertised an unsupported identity")
		require.Equal(t, expected.charset, collation.charset,
			"SHOW COLLATION must list only implemented collation identities")
		require.Equal(t, expected.padAttribute, collation.padAttribute,
			"SHOW COLLATION pad metadata must match executable comparison semantics")
		if collation.isDefault == "YES" {
			require.NotContains(t, seenDefaults, collation.charset)
			seenDefaults[collation.charset] = collation.collationName
		}
	}
	require.Equal(t, defaults, seenDefaults)
}
