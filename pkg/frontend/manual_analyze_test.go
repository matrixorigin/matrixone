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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestManualAnalyzeFeatureGateDefaultsOff(t *testing.T) {
	ses := &Session{}
	enabled, err := manualAnalyzeEnabled(ses)
	require.NoError(t, err)
	require.False(t, enabled)

	ses.sesSysVars = &SystemVariables{mp: map[string]any{manualAnalyzeVariable: int8(1)}}
	enabled, err = manualAnalyzeEnabled(ses)
	require.NoError(t, err)
	require.True(t, enabled)
}

func TestBuildAnalyzeAuthorizationProbeQuotesIdentifiers(t *testing.T) {
	entry := &tree.AnalyzeTableEntry{Table: tree.NewTableName(
		"tick`table",
		tree.ObjectNamePrefix{SchemaName: "select-db", ExplicitSchema: true},
		nil,
	)}
	probe := buildAnalyzeAuthorizationProbe(
		entry, tree.IdentifierList{"select", "a-b", "tick`name"})
	require.Equal(t,
		"select `select`,`a-b`,`tick``name` from `select-db`.`tick``table` where false",
		probe)
}

func TestAddManualAnalyzeResultColumns(t *testing.T) {
	mrs := &MysqlResultSet{}
	addManualAnalyzeResultColumns(mrs)
	require.Equal(t, uint64(11), mrs.GetColumnCount())
	column, err := mrs.GetColumn(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, "table_name", column.Name())
	column, err = mrs.GetColumn(t.Context(), 4)
	require.NoError(t, err)
	require.Equal(t, defines.MYSQL_TYPE_LONGLONG, column.ColumnType())
}
