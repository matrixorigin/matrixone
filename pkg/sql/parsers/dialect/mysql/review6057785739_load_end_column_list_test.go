// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// LOAD DATA's column list must accept the same unquoted END spelling the
// bare-END declaration rule admits: columns_or_variable now reuses the
// existing column_decl_name owner (the column_name arms plus END) instead
// of the END-excluding column_name_unresolved. Each form below must parse
// into the Load statement (the deterministic AST mapping at this owner).
func parseLoadColumnList(t *testing.T, sql string) *tree.Load {
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	l, ok := stmt.(*tree.Load)
	require.True(t, ok, "expected *tree.Load for: %s", sql)
	require.NotNil(t, l.Table)
	return l
}

func TestLoadColumnListAcceptsBareEndColumn(t *testing.T) {
	// bare END — previously rejected with 1064, now accepted like the quoted form
	parseLoadColumnList(t, "load data infile '/tmp/x.csv' into table t (id, end)")
	// quoted control
	parseLoadColumnList(t, "load data infile '/tmp/x.csv' into table t (id, `end`)")
	// ordinary control
	parseLoadColumnList(t, "load data infile '/tmp/x.csv' into table t (id, k)")
	// qualified two/three-part forms still parse through the shared column_name arms
	parseLoadColumnList(t, "load data infile '/tmp/x.csv' into table t (t.k)")
	parseLoadColumnList(t, "load data infile '/tmp/x.csv' into table t (db.t.k)")
	// the user_variable arm remains a distinct element of the same list
	parseLoadColumnList(t, "load data infile '/tmp/x.csv' into table t (id, @k)")
}

func TestLoadColumnListCompoundEndStaysKeyword(t *testing.T) {
	// The compound-statement END keeps its keyword role: the LOAD owner change
	// must not widen END in any other production.
	_, err := ParseOne(context.Background(), "begin select 1; end;", 1)
	require.NoError(t, err)
}
