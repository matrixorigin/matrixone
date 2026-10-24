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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// DESCRIBE tbl col_name must behave like SHOW COLUMNS FROM tbl LIKE 'col_name'
// (MySQL semantics) instead of silently listing every column.
func TestDescribeColumnNameFiltersColumns(t *testing.T) {
	build := func(sql string) string {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		p, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		return p.String()
	}

	all := build("desc nation")
	like := build("show columns from nation like 'n_name'")
	require.NotEqual(t, all, like)

	for _, sql := range []string{
		"desc nation n_name",
		"describe nation N_NAME",
		"explain nation n_name",
		"desc tpch.nation n_name",
	} {
		require.Equal(t, like, build(sql), sql)
	}

	require.Equal(t, build("show columns from nation like 'nosuch'"), build("desc nation nosuch"))
}
