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
		`{"LSN":5,"Stage":1,"LifecycleVersion":1}`, 1, 1, 3, 4,
	)

	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.LSN') = '4'")
	require.Contains(t, sql, "CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.Stage')), '0') AS SIGNED) <= 1")
	require.Contains(t, sql, "AND (3 = 4 OR")
	require.Contains(t, sql, "GREATEST(CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.Stage')), '0') AS SIGNED), 1)")
	require.Contains(t, sql, "GREATEST(CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.LifecycleVersion')), '0') AS UNSIGNED), 1)")
}

func TestISCPLogUpdateResultSQLLetsTerminalErrorWinWithoutRegressingStage(t *testing.T) {
	sql := CDCSQLBuilder.ISCPLogUpdateResultSQL(
		1, 2, "job", 3, types.BuildTS(4, 0),
		`{"LSN":5,"Stage":0,"LifecycleVersion":1}`, 0, 1, 4, 4,
	)

	require.Contains(t, sql, "AND (4 = 4 OR")
	require.Contains(t, sql, "GREATEST(CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.Stage')), '0') AS SIGNED), 0)")
}

func TestISCPLogUpdateResultSQLPreservesDurableLifecycleVersion(t *testing.T) {
	sql := CDCSQLBuilder.ISCPLogUpdateResultSQL(
		1, 2, "job", 3, types.BuildTS(4, 0),
		`{"LSN":5,"Stage":1}`, 1, 0, 3, 4,
	)

	// Ordinary and synthesized statuses may omit the marker. The catalog value
	// remains authoritative and cannot be replaced by the incoming zero value.
	require.Contains(t, sql,
		"GREATEST(CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.LifecycleVersion')), '0') AS UNSIGNED), 0)")
}

func TestISCPLogAdvanceWatermarkSQLCarriesMonotonicStage(t *testing.T) {
	sql := CDCSQLBuilder.ISCPLogAdvanceWatermarkSQL(
		1, 2, "job", 3, types.BuildTS(4, 0), 5, 1, 3, 4,
	)

	require.Contains(t, sql, "JSON_SET(job_status, '$.LSN', 5, '$.Stage'")
	require.Contains(t, sql, "GREATEST(CAST(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(job_status, '$.Stage')), '0') AS SIGNED), 1)")
	require.Contains(t, sql, "JSON_EXTRACT(job_status, '$.LSN') = '4'")
}

func TestISCPLogSQLBuildersEscapeStringLiterals(t *testing.T) {
	jobName := `job_'name\path`
	jobSpec := `{"InitSQL":"select 'quoted' from C:\\data"}`
	jobStatus := `{"ErrorMsg":"can't open C:\\data","LSN":5,"Stage":1}`
	watermark := types.BuildTS(4, 0)

	insertSQL := CDCSQLBuilder.ISCPLogInsertSQL(
		1, 2, jobName, 3, jobSpec, 1, watermark, jobStatus,
	)
	require.Contains(t, insertSQL, `'job_''name\\path'`)
	require.Contains(t, insertSQL, `'{"InitSQL":"select ''quoted'' from C:\\\\data"}'`)
	require.Contains(t, insertSQL, `'{"ErrorMsg":"can''t open C:\\\\data","LSN":5,"Stage":1}'`)

	updateSQL := CDCSQLBuilder.ISCPLogUpdateResultSQL(
		1, 2, jobName, 3, watermark, jobStatus, 1, 1, 1, 4,
	)
	require.Contains(t, updateSQL, `JSON_SET('{"ErrorMsg":"can''t open C:\\\\data","LSN":5,"Stage":1}'`)
	require.Contains(t, updateSQL, `job_name = 'job_''name\\path'`)

	advanceSQL := CDCSQLBuilder.ISCPLogAdvanceWatermarkSQL(
		1, 2, jobName, 3, watermark, 5, 1, 1, 4,
	)
	require.Contains(t, advanceSQL, `job_name = 'job_''name\\path'`)

	require.Contains(t,
		CDCSQLBuilder.ISCPLogUpdateDropAtSQL(1, 2, jobName, 3),
		`job_name = 'job_''name\\path'`,
	)
	require.Contains(t,
		CDCSQLBuilder.ISCPLogUpdateJobSpecSQL(1, 2, jobName, 3, jobSpec),
		`job_spec = '{"InitSQL":"select ''quoted'' from C:\\\\data"}'`,
	)
	require.Contains(t,
		CDCSQLBuilder.ISCPLogSelectByTableSQL(1, 2, jobName),
		`job_name = 'job_''name\\path'`,
	)
}
