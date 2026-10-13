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

package vectorindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDescribeFilterJSON(t *testing.T) {
	name := func(col int) string {
		if col == -1 {
			return "id"
		}
		return []string{"category", "note"}[col]
	}
	got, err := DescribeFilterJSON(`[{"col":-1,"op":"between","lo":501,"hi":1000},{"col":0,"op":"=","val":1},{"col":1,"op":"in","vals":["a","b'c"]},{"col":0,"op":"is_null"},{"col":1,"op":"is_not_null"},{"col":1,"op":"prefix","val":"x"},{"col":0,"op":">=","val":2.5}]`, name)
	require.NoError(t, err)
	require.Equal(t, "id BETWEEN 501 AND 1000 AND category = 1 AND note IN ('a', 'b''c') AND category IS NULL AND note IS NOT NULL AND note LIKE 'x%' AND category >= 2.5", got)

	got, err = DescribeFilterJSON("", name)
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = DescribeFilterJSON("[]", name)
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = DescribeFilterJSON("{", name)
	require.Error(t, err)
	_, err = DescribeFilterJSON(`[{"col":0,"op":"like"}]`, name)
	require.Error(t, err)
}
