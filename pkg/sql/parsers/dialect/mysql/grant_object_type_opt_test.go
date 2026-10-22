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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// MySQL's GRANT/REVOKE object type keyword is optional and defaults to TABLE:
// GRANT priv ON [TABLE] {*|*.*|db.*|db.tbl|tbl}. See #30874.
func TestGrantRevokeObjectTypeOptional(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"grant select on qa2.* to qa_u1", "grant select on table qa2.* to qa_u1"},
		{"grant select, insert on qa2.t1 to r1 with grant option", "grant select, insert on table qa2.t1 to r1 with grant option"},
		{"grant select on t1 to r1", "grant select on table t1 to r1"},
		{"grant select on *.* to r1", "grant select on table *.* to r1"},
		{"grant select on * to r1", "grant select on table * to r1"},
		{"revoke select on qa2.* from qa_u1", "revoke select on table qa2.* from qa_u1"},
		{"revoke if exists select on qa2.t1 from r1", "revoke if exists select on table qa2.t1 from r1"},
		// Explicit object types keep working.
		{"grant select on table qa2.* to qa_u1", "grant select on table qa2.* to qa_u1"},
		{"grant select on database * to r1", "grant select on database * to r1"},
		{"grant all on account * to r1", "grant all on account * to r1"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), c.sql, 1)
			require.NoError(t, err)
			require.Equal(t, c.want, tree.String(stmt, 0))
		})
	}
}
