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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// TestBinaryPrefixOperatorSyntax probes how the `BINARY <expr>` prefix
// operator is accepted across operand classes, clause positions and
// neighboring BINARY/VARBINARY syntax (issue #31563).
func TestBinaryPrefixOperatorSyntax(t *testing.T) {
	okCases := []struct {
		input, want string
	}{
		// legacy narrow operand forms must keep their exact AST and formatting
		{`select binary 'x'`, `select binary('x')`},
		{`select binary 123`, `select binary(123)`},
		{`select binary col from t`, `select binary(col) from t`},
		{`select binary('x')`, `select binary('x')`},
		{`select binary (( 'x' ))`, `select binary(('x'))`},
		// issue #31563 forms: general expression operands now parse
		{`select binary cast('x' as char)`, `select binary(cast('x' as varchar))`},
		{`select binary (concat('a','b'))`, `select binary(concat('a', 'b'))`},
		{`select binary concat('a','b')`, `select binary(concat('a', 'b'))`},
		{`select binary if(true, 'a', 'b')`, `select binary(if(true, 'a', 'b'))`},
		{`select binary case when true then 'a' else 'b' end`, `select binary(case when true then 'a' else 'b' end)`},
		{`select binary (case when true then 'a' else 'b' end)`, `select binary(case when true then 'a' else 'b' end)`},
		{`select binary -1`, `select binary(-1)`},
		{`select binary (select 'a')`, `select binary((select 'a'))`},
		{`select binary exists (select 1)`, `select binary(exists (select 1))`},
		// precedence: BINARY binds tighter than arithmetic/comparison
		{`select binary 'a' = 'A'`, `select binary('a') = 'A'`},
		{`select binary 1 + 1`, `select binary(1) + 1`},
		{`select binary a = binary b from t`, `select binary(a) = binary(b) from t`},
		// clause coverage
		{`select col from t where binary col = 'x'`, `select col from t where binary(col) = 'x'`},
		{`select col from t where binary (concat(col, 'x')) = 'y'`, `select col from t where binary(concat(col, 'x')) = 'y'`},
		{`select col from t order by binary col`, `select col from t order by binary(col)`},
		{`insert into t values (binary 'x'), (binary ('y'))`, `insert into t values (binary('x')), (binary('y'))`},
		{`insert into t (a) select binary col from s`, `insert into t (a) select binary(col) from s`},
		{`update t set a = binary 'x' where binary a = 'x'`, `update t set a = binary('x') where binary(a) = 'x'`},
		// introducers and operand variety
		{`select binary _binary 'x'`, `select binary(_binary 'x')`},
		{`select _binary 'x'`, `select _binary 'x'`},
		{`select binary @a`, `select binary(@a)`},
		{`select _binary 'x' collate binary`, `select (_binary 'x') collate binary`},
		{`select binary 'a' collate utf8mb4_bin`, `select (binary('a')) collate utf8mb4_bin`},
		// DDL/predicate neighbors must keep parsing
		{`create table t (a binary(10), b varbinary(20), c binary, d char(10) default 'x')`, `create table t (a binary(10), b varbinary(20), c binary, d char(10) default 'x')`},
		{`select cast('x' as binary)`, `select cast('x' as binary)`},
		{`select cast('x' as binary(10))`, `select cast('x' as binary(10))`},
		{`select convert('x' using binary)`, `select convert('x' using binary)`},
		// legacy multi-arg functional form stays parseable (tuple operand)
		{`select binary('a', 'b')`, `select binary(('a', 'b'))`},
	}
	for _, test := range okCases {
		t.Run(test.input, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.input, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			require.Equal(t, test.want, formatted)
			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			t.Cleanup(roundTrip.Free)
			require.Equal(t, formatted,
				tree.StringWithOpts(roundTrip, dialect.MYSQL, tree.WithSingleQuoteString()))
		})
	}

	errCases := []string{
		// dangling / reserved-word operands must stay syntax errors
		`select binary from t`,
		`select binary`,
		`select binary * from t`,
		`select binary collate utf8mb4_bin`,
		`select binary , 1`,
	}
	for _, sql := range errCases {
		t.Run("ERR: "+sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err)
		})
	}
}
