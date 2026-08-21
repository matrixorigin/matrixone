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

package cdc

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestISCPLogSQLBuilderEscapesLiteralValues(t *testing.T) {
	jobName := "materialized_view_db_mv'quoted"
	jobSpec := `{"RefreshSQL":"select date_trunc('minute', ts) where status >= 500","Path":"a\\b"}`
	jobStatus := `{"ErrorMsg":"can't refresh"}`

	insertSQL := CDCSQLBuilder.ISCPLogInsertSQL(1, 2, jobName, 3, jobSpec, 4, types.BuildTS(5, 6), jobStatus)
	require.Contains(t, insertSQL, "materialized_view_db_mv''quoted")
	require.Contains(t, insertSQL, "date_trunc(''minute'', ts)")
	require.Contains(t, insertSQL, `"Path":"a\\\\b"`)
	require.Contains(t, insertSQL, "can''t refresh")

	updateSQL := CDCSQLBuilder.ISCPLogUpdateResultSQL(1, 2, jobName, 3, types.BuildTS(7, 8), jobStatus, 4, 9)
	require.Contains(t, updateSQL, "materialized_view_db_mv''quoted")
	require.Contains(t, updateSQL, "can''t refresh")

	dropSQL := CDCSQLBuilder.ISCPLogUpdateDropAtSQL(1, 2, jobName, 3)
	require.Contains(t, dropSQL, "materialized_view_db_mv''quoted")
}
