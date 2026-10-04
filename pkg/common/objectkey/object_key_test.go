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

package objectkey

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		database string
		object   string
	}{
		{database: "db", object: "table"},
		{database: "db#part", object: "table"},
		{database: "#db#", object: "#table#"},
		{database: "数据库#一", object: "表#二"},
		{database: "\x001", object: "x"},
		{database: "", object: "table"},
		{database: "db", object: ""},
	} {
		key := Encode(tc.database, tc.object)
		database, object := Decode(key)
		require.Equal(t, tc.database, database)
		require.Equal(t, tc.object, object)
	}
}

func TestLegacyKeyCompatibility(t *testing.T) {
	database, object := Decode("legacy#relation#suffix")
	require.Equal(t, "legacy", database)
	require.Equal(t, "relation#suffix", object)

	database, object = Decode("\x001#x")
	require.Equal(t, "\x001", database)
	require.Equal(t, "x", object)
}
