// Copyright 2026 Matrix Origin
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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

// Review 6094583688: a column comment quoting the SHOW CREATE display
// fragment "  SPATIAL KEY `idx_g` (`g`)" made the old first-occurrence text
// replacement rewrite the comment instead of the index declaration, so the
// ALTER COPY replay kept the unparsable SPATIAL KEY spelling and the copy
// failed. The replay must be derived from the recorded construction offsets,
// leaving the comment bytes untouched.
func TestAlterCopyReplayIgnoresQuotedIndexFragmentInColumnComment(t *testing.T) {
	ddl := "CREATE TABLE t_collide (g POINT NOT NULL, id INT PRIMARY KEY, note VARCHAR(64) DEFAULT NULL COMMENT '  SPATIAL KEY `idx_g` (`g`)', KEY idx_g USING RTREE(g))"
	mock := newCheckAlterOptimizer(t, ddl)

	p, err := runOneStmt(mock, t, "ALTER TABLE t_collide ADD COLUMN marker INT DEFAULT 7 FIRST")
	require.NoError(t, err)
	alter := p.GetDdl().GetAlterTable()
	require.NotNil(t, alter)

	require.NotEmpty(t, alter.CreateTmpTableSql)
	require.Contains(t, alter.CreateTmpTableSql, "KEY `idx_g` USING rtree (")
	require.Contains(t, alter.CreateTmpTableSql,
		"COMMENT '  SPATIAL KEY `idx_g` (`g`)'")
	require.NotContains(t, alter.CreateTmpTableSql,
		"COMMENT '  KEY `idx_g` USING rtree (`g`)'")
	require.NotContains(t, alter.CreateTmpTableSql, "SPATIAL KEY `idx_g` (`g`)\n)")

	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, alter.CreateTmpTableSql, 1)
	require.NoError(t, err, alter.CreateTmpTableSql)
	require.NotNil(t, stmt)
}
