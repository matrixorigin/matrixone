// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

func TestParseAlterAccountWorkloadPolicy(t *testing.T) {
	tests := []struct {
		sql         string
		accountName string
		reset       bool
		value       string
		formatted   string
	}{
		{
			sql:       `alter account config set query_workload_policy = '{"version":1}'`,
			value:     `{"version":1}`,
			formatted: `alter account config set query_workload_policy = '{"version":1}'`,
		},
		{
			sql:         `alter account config for tenant_a set query_workload_policy = '{"version":1}'`,
			accountName: "tenant_a",
			value:       `{"version":1}`,
			formatted:   `alter account config for tenant_a set query_workload_policy = '{"version":1}'`,
		},
		{
			sql:       "alter account config reset query_workload_policy",
			reset:     true,
			formatted: "alter account config reset query_workload_policy",
		},
		{
			sql:         "alter account config for tenant_a reset query_workload_policy",
			accountName: "tenant_a",
			reset:       true,
			formatted:   "alter account config for tenant_a reset query_workload_policy",
		},
	}

	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
			statements, err := Parse(context.Background(), test.sql, 1)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			defer statements[0].Free()

			statement, ok := statements[0].(*tree.AlterAccountConfig)
			require.True(t, ok)
			require.Equal(t, test.accountName, statement.AccountName)
			require.Equal(t, "query_workload_policy", statement.ConfigName)
			require.Equal(t, test.value, statement.Value)
			require.Equal(t, test.reset, statement.Reset)
			require.Equal(t, tree.QueryTypeDCL, statement.GetQueryType())
			require.Equal(t, test.formatted, tree.String(statement, dialect.MYSQL))
		})
	}
}
