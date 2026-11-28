// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Review 6095032855 (P3): the true column-reference positions of the
// time-window INTERVAL owner and the index INCLUDE list must accept the
// same bare END column spelling this PR's bare-END compatibility admits
// at declaration positions - while the quoted spellings, the established
// partition keys, the general name lists and the compound END delimiter
// all keep their existing contracts.
func TestReview6095BareEndIntervalAndIncludeReferences(t *testing.T) {
	ctx := context.Background()

	// The INCLUDE list's true column-reference position is the CREATE INDEX
	// option list: bare END, quoted END and multi-member lists all parse.
	_, err := ParseOne(ctx, "create table t_tb (end datetime, v int)", 1)
	require.NoError(t, err)
	_, err = ParseOne(ctx, "create index idx_end on t_tb (v) include (end)", 1)
	require.NoError(t, err, "the bare-END include reference must parse")
	_, err = ParseOne(ctx, "create index idx_endq on t_tb (v) include (v, `end`)", 1)
	require.NoError(t, err, "the multi-member include reference must parse")
	_, err = ParseOne(ctx, "create index idx_plainm on t_tb (v) include (v)", 1)
	require.NoError(t, err, "the ordinary include reference must keep parsing")

	// The quoted spelling keeps working at exactly the same position.
	_, err = ParseOne(ctx, "create index idx_q on t_tb (v) include (`end`)", 1)
	require.NoError(t, err, "the quoted include twin must keep parsing")

	// The compound END delimiter is unaffected beside the extended
	// reference positions.
	_, err = Parse(ctx, "begin create index idx_comp on t_tb (v) include (end); end;", 1)
	require.NoError(t, err, "the compound block keeps END as its delimiter beside the extended references")

	// Bare END must remain confined: it is still a keyword at table names,
	// which this change does not alter.
	_, err = ParseOne(ctx, "create table end (v int)", 1)
	require.Errorf(t, err, "END must not be admitted as a table name")

	// Interval identity: the bare-END and backtick-quoted twins of the
	// time-window INTERVAL owner must carry the identical resolved AST key
	// through the canonical render on the same table shape.
	renderOf := func(t *testing.T, sql string) string {
		t.Helper()
		stmt, perr := ParseOne(ctx, sql, 1)
		require.NoError(t, perr)
		return tree.String(stmt.(*tree.Select), dialect.MYSQL)
	}
	fragBare := renderOf(t, "select _wstart(end), id, sum(v) from t_tb group by id interval(end, 1, minute)")
	fragQuoted := renderOf(t, "select _wstart(`end`), id, sum(v) from t_tb group by id interval(`end`, 1, minute)")
	require.Equalf(t, fragBare, fragQuoted, "the bare-END interval owner and its quoted twin must render identically")
	require.Containsf(t, fragBare, "interval(end, 1, minute)", "the interval owner must render its exact identity: %s", fragBare)

	// Exact AST include names: the bare-END and quoted twins of the
	// CREATE INDEX INCLUDE list must carry the identical resolved column
	// name through the canonical render on the same table shape.
	renderStmt := func(t *testing.T, sql string) string {
		t.Helper()
		stmt, perr := ParseOne(ctx, sql, 1)
		require.NoError(t, perr)
		return tree.String(stmt, dialect.MYSQL)
	}
	incBare := renderStmt(t, "create index idx_tw on t_tb (v) include (end)")
	incQuoted := renderStmt(t, "create index idx_tw on t_tb (v) include (`end`)")
	require.Equalf(t, incBare, incQuoted, "the bare-END include list and its quoted twin must render identically")
	require.Containsf(t, incBare, "INCLUDE (end)", "the include list must render its exact identity: %s", incBare)
}
