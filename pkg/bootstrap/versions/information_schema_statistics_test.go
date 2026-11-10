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

package versions

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

// TestInformationSchemaStatisticsUpgradeEntryMatchesVersionedContract pins
// the shared STATISTICS repair entry (#30910/#31164): it publishes the current
// identity - the engine's per-part cardinality estimates composed with the
// retained index prefix lengths (#30935) - behind the v107 protocol gate, and
// its exact-equality definition check can never accept the zero-cardinality
// definitions that pre-fix binaries published. The byte-stable historical
// template itself is never the entry's target.
func TestInformationSchemaStatisticsUpgradeEntryMatchesVersionedContract(t *testing.T) {
	entry := InformationSchemaStatisticsUpgradeEntry()
	require.Equal(t, strings.Replace(sysview.InformationSchemaStatisticsPrefixSubPartDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1), entry.UpgSql)
	require.Equal(t, defines.MORPCVersion107, entry.RequiredProtocolVersion)
	require.Empty(t, entry.PreSql)

	// The pre-#30910 current definition (prefix lengths without the estimate)
	// and the byte-stable historical template must never pass the check.
	preEstimateCurrent := strings.NewReplacer(
		"NULL AS `SUB_PART`,",
		"mo_index_sub_part(`idx`.`algo_params`, `idx`.`column_name`) AS `SUB_PART`,",
		"`idx`.`ordinal_position`, `idx`.`column_name`, `tcl`.`attnotnull`,",
		"`idx`.`ordinal_position`, `idx`.`column_name`, `idx`.`algo_params`, `tcl`.`attnotnull`,",
	).Replace(sysview.InformationSchemaStatisticsDDL)
	require.NotEqual(t, entry.UpgSql, strings.Replace(preEstimateCurrent, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1))
	require.NotEqual(t, entry.UpgSql, strings.Replace(sysview.InformationSchemaStatisticsDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1))

	// The published estimate covers each index part's own prefix: part one
	// reads the part column, later parts render NULL because the engine
	// publishes no multi-column prefix counts, a character-prefix first part
	// passes its index metadata to mo_table_col_ndv so the truncated
	// population renders NULL instead of the full-column estimate, and the
	// visible first part adds the published NULL population (mo_table_col_nullcnt)
	// as one MySQL index-key value group on top of the distinct estimate.
	require.Contains(t, entry.UpgSql, "if(`idx`.`ordinal_position` = 1, case when mo_table_col_ndv(`tbl`.`reldatabase`, `tbl`.`relname`, `idx`.`column_name`, ifnull(`idx`.`algo_params`, '')) is null then null else mo_table_col_ndv(`tbl`.`reldatabase`, `tbl`.`relname`, `idx`.`column_name`, ifnull(`idx`.`algo_params`, '')) + if(coalesce(mo_table_col_nullcnt(`tbl`.`reldatabase`, `tbl`.`relname`, `idx`.`column_name`), 0) > 0, 1, 0) end, NULL)) AS `CARDINALITY`")
	// The pre-revision projection published every part's column NDV; the
	// ordinal gate must keep that form out of the shipped definition.
	require.NotContains(t, entry.UpgSql, "`idx`.`column_name`)) AS `CARDINALITY`")
	require.NotContains(t, entry.UpgSql, "0 AS `CARDINALITY`")
	// The retained prefix-length projection of #30935 rides the same current
	// identity; the entry must not regress it.
	require.Contains(t, entry.UpgSql, "mo_index_sub_part(`idx`.`algo_params`, `idx`.`column_name`) AS `SUB_PART`,")
}
