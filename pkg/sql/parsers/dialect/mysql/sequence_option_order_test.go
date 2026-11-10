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

// CREATE SEQUENCE options are order-independent (MariaDB-compatible). See #30873.
func TestCreateSequenceOptionOrder(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"create sequence s2 start with 10 increment by 2", "create sequence s2 as bigint increment by 2 start with 10 no cycle"},
		{"create sequence s2 increment by 2 start with 10", "create sequence s2 as bigint increment by 2 start with 10 no cycle"},
		{"create sequence s cycle start 5 maxvalue 100 minvalue -3 increment -1",
			"create sequence s as bigint increment by -1 minvalue -3 maxvalue 100 start with 5 cycle"},
		{"create sequence s maxvalue 100 no cycle minvalue 1", "create sequence s as bigint minvalue 1 maxvalue 100 no cycle"},
		{"create sequence if not exists s as int start with -5 increment by 3",
			"create sequence if not exists s as int increment by 3 start with -5 no cycle"},
		{"create sequence s3 start with 1", "create sequence s3 as bigint start with 1 no cycle"},
		{"create sequence s4", "create sequence s4 as bigint no cycle"},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), c.sql, 1)
			require.NoError(t, err)
			require.Equal(t, c.want, tree.String(stmt, 0))
		})
	}
}

func TestCreateSequenceDuplicateOptionRejected(t *testing.T) {
	for _, sql := range []string{
		"create sequence s start with 1 start with 2",
		"create sequence s increment 1 start 1 increment 2",
		"create sequence s minvalue 1 minvalue 2",
		"create sequence s maxvalue 1 maxvalue 2",
		"create sequence s cycle no cycle",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err)
		})
	}
}
