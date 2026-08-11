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

func TestObjectKeyRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name      string
		database  string
		relation  string
		legacyKey string
	}{
		{name: "ordinary", database: "db", relation: "tbl", legacyKey: "db#tbl"},
		{name: "database separator", database: "db#part", relation: "tbl"},
		{name: "relation separator", database: "db", relation: "tbl#part"},
		{name: "both components", database: "db#part", relation: "tbl#part"},
		{name: "adjacent separators", database: "db##", relation: "##tbl"},
		{name: "boundary separators", database: "#db#", relation: "#tbl#"},
		{name: "backslashes", database: `db\#part`, relation: `tbl\\#part`},
		{name: "literal legacy backslash", database: `db\part`, relation: `tbl\part`, legacyKey: `db\part#tbl\part`},
		{name: "unicode byte length", database: "数据库#一", relation: "表#二"},
		{name: "empty database", database: "", relation: "tbl"},
		{name: "empty relation", database: "db", relation: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := genKey(tc.database, tc.relation)
			if tc.legacyKey != "" {
				require.Equal(t, tc.legacyKey, key)
			}
			database, relation := splitKey(key)
			require.Equal(t, tc.database, database)
			require.Equal(t, tc.relation, relation)
		})
	}

	database, relation := splitKey("legacy#relation#suffix")
	require.Equal(t, "legacy", database)
	require.Equal(t, "relation#suffix", relation)
}
