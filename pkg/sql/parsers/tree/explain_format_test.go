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

package tree

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestExplainFormatVariants(t *testing.T) {
	tests := []struct {
		name    string
		stmt    Statement
		options []OptionElem
		want    string
	}{
		{name: "empty", want: "explain"},
		{name: "options", options: []OptionElem{{Name: "analyze", Value: "NULL"}, {Name: "format", Value: "json"}}, want: "explain (analyze,format json)"},
		{name: "ordinary statement", stmt: &ShowDatabases{}, want: "explain show databases"},
		{name: "empty show columns", stmt: &ShowColumns{}, want: "explain"},
		{name: "show table and column", stmt: &ShowColumns{Table: NewUnresolvedObjectName("db", "tbl"), ColName: NewUnresolvedName(NewCStr("col", 0))}, want: "explain db.tbl col"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			impl := explainImpl{Statement: tt.stmt, Options: tt.options}
			for _, node := range []NodeFormatter{&ExplainStmt{explainImpl: impl}, &ExplainAnalyze{explainImpl: impl}, &ExplainPhyPlan{explainImpl: impl}} {
				require.Equal(t, tt.want, String(node, dialect.MYSQL))
			}
		})
	}
}
