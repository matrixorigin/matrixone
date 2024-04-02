// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSplitRecursiveMember(t *testing.T) {
	sql := "with recursive c as (select a from t1 union all select a+1 from c where a < 3 union all select a+1 from c where a < 4) select * from c"
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1, 0)
	if err != nil {
		t.Errorf("Parse(%q) err: %v", sql, err)
		return
	}
	c := stmts[0].(*tree.Select).With.CTEs[0]
	name := string(c.Name.Alias)
	stmt := c.Stmt.(*tree.Select).Select
	b := &QueryBuilder{}

	var ss []tree.SelectStatement
	left, err := b.splitRecursiveMember(&stmt, name, &ss)
	if err != nil {
		t.Errorf("splitRecursiveMember err: %v", err)
		return
	}

	require.Equal(t, 2, len(ss))
	require.Equal(t, true, left != nil)
}
