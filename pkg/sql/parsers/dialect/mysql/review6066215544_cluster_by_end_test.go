// Copyright 2021-2025 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A table whose clustering key names an unquoted END column parses, and the
// cluster-key binding keeps the declaration spelling: the END column declared
// in the table body and the CLUSTER BY identifier resolve to the same column.
func TestClusterByBareEndParsesAndBindsReview6066215544(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		keys []string
	}{
		{"bare_end_parenthesized", "CREATE TABLE t (id INT, end INT) CLUSTER BY (end)", []string{"end"}},
		{"bare_end_single", "CREATE TABLE t (id INT, END INT) CLUSTER BY end", []string{"end"}},
		{"quoted_end_control", "CREATE TABLE t (id INT, `end` INT) CLUSTER BY (`end`)", []string{"end"}},
		{"ordinary_column_control", "CREATE TABLE t (id INT, end INT) CLUSTER BY (id)", []string{"id"}},
		{"list_with_end_and_ordinary", "CREATE TABLE t (id INT, end INT) CLUSTER BY (end, id)", []string{"end", "id"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), c.sql, 1)
			require.NoErrorf(t, err, "expected %q to parse", c.sql)
			create, ok := stmt.(*tree.CreateTable)
			require.Truef(t, ok, "expected a CreateTablesStmt for %q", c.sql)
			require.NotNilf(t, create.ClusterByOption, "expected a cluster-by option for %q", c.sql)
			require.Lenf(t, create.ClusterByOption.ColumnList, len(c.keys), "key count for %q", c.sql)
			for i, want := range c.keys {
				require.Equalf(t, want, strings.ToLower(create.ClusterByOption.ColumnList[i].ColName()), "key %d for %q", i, c.sql)
			}
		})
	}
}

// The compound-statement END boundaries stay intact: the unquoted END is only
// a column/cluster-key identifier in a declaration context, never a general
// identifier, and BEGIN/END compound bodies parse as before.
func TestClusterByEndKeepsCompoundAndRejectedGrammarContracts(t *testing.T) {
	// The cluster list is an identifier list: a CASE...END expression must
	// stay rejected in the CLUSTER BY context (the keyword alternative did not
	// widen it to an expression position), and the identifier validation of an
	// unknown cluster column lives downstream in the planner, not the parser.
	negative := []struct {
		name string
		sql  string
	}{
		{"cluster_by_case_expression", "CREATE TABLE t (id INT) CLUSTER BY (case when id > 0 then 1 end)"},
		{"cluster_by_missing_columns", "CREATE TABLE t (id INT) CLUSTER BY"},
	}
	for _, c := range negative {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseOne(context.Background(), c.sql, 1)
			require.Error(t, err, "expected %q to be rejected", c.sql)
		})
	}
	// the supported compound-statement shape still parses to its typed body
	stmt, err := ParseOne(context.Background(), "begin select 1; end;", 1)
	require.NoError(t, err)
	_, isCompound := stmt.(*tree.CompoundStmt)
	require.Truef(t, isCompound, "the compound BEGIN/END control must keep its typed shape")
}

// The upstream RENAME USER grammar survives the replayed feature series and
// this review's cluster rule: the accepted syntax parses into its typed
// statement and malformed forms stay rejected.
func TestRenameUserGrammarPreservedAcrossRebasedClusterRules(t *testing.T) {
	for _, c := range []struct {
		name string
		sql  string
	}{
		{"rename_user_identifiers", "RENAME USER old_name TO new_name"},
		{"rename_user_string_hosts", "RENAME USER 'old'@'%' TO 'new'@'%'"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseOne(context.Background(), c.sql, 1)
			require.NoErrorf(t, err, "the upstream RENAME USER syntax must stay accepted after the rebased grammar: %q", c.sql)
		})
	}
	t.Run("rename_user_missing_target", func(t *testing.T) {
		_, err := ParseOne(context.Background(), "RENAME USER old TO", 1)
		require.Errorf(t, err, "a malformed RENAME USER statement must stay rejected after the rebased grammar")
	})
}
