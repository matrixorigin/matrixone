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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIndexOptionOpTypeRoundTrip(t *testing.T) {
	for _, sql := range []string{
		`create index ix using hnsw on t(v) op_type "vector_l2_ops" m=4 ef_construction=16 ef_search=16`,
		`create index ix using hnsw on t(v) op_type 'VECTOR_L2SQ_OPS'`,
		`create index ix using hnsw on t(v) op_type 'vector_ip_ops' m=1 ef_construction=1 ef_search=1`,
		`create index ix using hnsw on t(v) op_type 'vector_cosine_ops'`,
		`create table t(id bigint primary key, v vecf32(4), index ix using hnsw(v) op_type 'vector_l2_ops' m=4)`,
		`alter table t add index ix using hnsw(v) op_type 'vector_l2_ops' m=4`,
		`create index ix using ivfflat on t(v) op_type 'vector_l2_ops' lists=2`,
		`create index ix using hnsw on t(v) m=4`,
	} {
		t.Run(sql, func(t *testing.T) {
			for _, opts := range [][]tree.FmtCtxOption{
				nil, {tree.WithQuoteString(true)}, {tree.WithSingleQuoteString()},
			} {
				func() {
					stmt, err := ParseOne(context.Background(), sql, 1)
					require.NoError(t, err)
					defer stmt.Free()
					formatted := tree.StringWithOpts(stmt, dialect.MYSQL, opts...)
					reparsed, err := ParseOne(context.Background(), formatted, 1)
					require.NoError(t, err, formatted)
					defer reparsed.Free()
					require.Equal(t, formatted, tree.StringWithOpts(reparsed, dialect.MYSQL, opts...))
					if first, ok := stmt.(*tree.CreateIndex); ok {
						require.Equal(t, first.IndexOption, reparsed.(*tree.CreateIndex).IndexOption)
					}
				}()
			}
		})
	}
}

func TestIndexOptionOpTypeEscapesRoundTrip(t *testing.T) {
	// Invalid operators still have to reach semantic validation as one intact
	// string, without quote/backslash drift or extra SQL statements.
	for _, value := range []string{
		"bad'operator", "bad\"operator", `bad\operator`, "bad\noperator",
		`bad\'; drop table t; --`, `bad\_operator`, `bad\%operator`,
	} {
		for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES"} {
			t.Run(fmt.Sprintf("%q/%s", value, mode), func(t *testing.T) {
				option := &tree.IndexOption{AlgoParamVectorOpType: value}
				var opts []tree.FmtCtxOption
				if mode == "NO_BACKSLASH_ESCAPES" {
					opts = append(opts, tree.WithNoBackslashEscape())
				}
				formatted := "create index ix using hnsw on t (v)" + tree.StringWithOpts(option, dialect.MYSQL, opts...)
				stmt, err := ParseOneWithSQLMode(context.Background(), formatted, 1, mode)
				require.NoError(t, err, formatted)
				defer stmt.Free()
				require.Equal(t, value, stmt.(*tree.CreateIndex).IndexOption.AlgoParamVectorOpType)
				require.Equal(t, formatted, tree.StringWithOpts(stmt, dialect.MYSQL, opts...))
			})
		}
	}
}
