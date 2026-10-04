// Copyright 2021 - 2026 Matrix Origin
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
	"strings"
	"testing"

	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// The planner looks this token up inside sql_mode. Nothing else connects the
// accepted-value list here to that lookup, so a rename on either side would
// silently leave the mode with no effect rather than failing.
func TestSQLModeEnableBoolSumAvgContract(t *testing.T) {
	sysVar, ok := gSysVarsDefs["sql_mode"]
	require.True(t, ok)
	setType, ok := sysVar.Type.(SystemVariableSetType)
	require.True(t, ok, "sql_mode must stay a SET so modes compose")

	// SET rejects any value outside its list, so the token the planner reads
	// must be settable.
	require.Contains(t, setType.Values(), mysqlparser.SQLModeEnableBoolSumAvg)
	converted, err := sysVar.Type.Convert(mysqlparser.SQLModeEnableBoolSumAvg)
	require.NoError(t, err)
	require.Equal(t, mysqlparser.SQLModeEnableBoolSumAvg, converted)

	// MySQL-compatible BOOL aggregation is the product default. Removing the
	// token remains an explicit opt-out for callers that need strict typing.
	defaultMode, ok := sysVar.Default.(string)
	require.True(t, ok)
	require.True(t, mysqlparser.HasEnableBoolSumAvgSQLMode(defaultMode),
		"sql_mode default must enable bool SUM/AVG")

	// It composes with the modes a session already has rather than replacing
	// them, which is the reason for putting it in sql_mode at all.
	strictMode := strings.ReplaceAll(defaultMode, ","+mysqlparser.SQLModeEnableBoolSumAvg, "")
	combined := strictMode + "," + mysqlparser.SQLModeEnableBoolSumAvg
	normalized, err := sysVar.Type.Convert(combined)
	require.NoError(t, err)
	normalizedMode, ok := normalized.(string)
	require.True(t, ok)
	require.True(t, mysqlparser.HasEnableBoolSumAvgSQLMode(normalizedMode))
	for _, mode := range strings.Split(defaultMode, ",") {
		require.True(t, mysqlparser.HasSQLMode(normalizedMode, mode),
			"%s must survive enabling bool SUM/AVG", mode)
	}
}

// Appending keeps the bit index of every existing value stable: SET bit
// indexes are positional.
func TestSQLModeExistingValueBitsAreStable(t *testing.T) {
	setType, ok := gSysVarsDefs["sql_mode"].Type.(SystemVariableSetType)
	require.True(t, ok)
	values := setType.Values()
	require.Equal(t, mysqlparser.SQLModeEnableBoolSumAvg, values[len(values)-1],
		"a new sql_mode value must be appended, never inserted")

	// The bit index of a pre-existing value is unchanged: MATRIXONE_NATIVE, the
	// other MatrixOne-specific mode, still sits at index 7.
	require.Equal(t, mysqlparser.SQLModeMatrixOneNative, values[7])
}
