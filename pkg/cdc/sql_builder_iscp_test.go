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

func TestISCPLogUpdateResultSQLRejectsStageRegression(t *testing.T) {
	sql := CDCSQLBuilder.ISCPLogUpdateResultSQL(
		1, 2, "job", 3, types.BuildTS(4, 0),
		`{"LSN":5,"Stage":1}`, 1, 3, 4,
	)

	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.LSN') = '4'")
	require.Contains(t, sql, "CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.Stage')), '0') AS SIGNED) <= 1")
}

func TestISCPLogRepairLegacyWatermarkStageSQLIsConservative(t *testing.T) {
	sql := CDCSQLBuilder.ISCPLogRepairLegacyWatermarkStageSQL(3, 1)

	require.Contains(t, sql, "job_status = JSON_SET(job_status, '$.Stage', 1)")
	require.Contains(t, sql, "job_state = 3")
	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.Stage') = '0'")
	require.Contains(t, sql, "CAST(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.LSN')) AS UNSIGNED) > 0")
	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.ErrorCode')")
	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.ErrorMsg')")
}
