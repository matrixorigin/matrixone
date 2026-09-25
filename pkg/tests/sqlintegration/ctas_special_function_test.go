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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestCTASSpecialFunctionSyntax(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		const schema = "ctas_special_function_29330"
		_, err = db.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		_, err = db.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table "+schema+".s(a varchar(8), b varchar(32))")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "insert into "+schema+".s values ('ab', 'ababXabab')")
		require.NoError(t, err)

		for _, tc := range []struct {
			name  string
			table string
			expr  string
			want  any
		}{
			{"both", "both_trim", "trim(both a from b)", "X"},
			{"leading", "leading_trim", "trim(leading a from b)", "Xabab"},
			{"trailing", "trailing_trim", "trim(trailing a from b)", "ababX"},
			{"position column", "position_column", "position(a in b)", int64(1)},
			{"position literal", "position_literal", "position('ab' in b)", int64(1)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				table := schema + "." + tc.table
				_, err := db.ExecContext(ctx, "create table "+table+" as select "+tc.expr+" as v from "+schema+".s")
				require.NoError(t, err)
				var actual any
				switch tc.want.(type) {
				case string:
					var value string
					require.NoError(t, db.QueryRowContext(ctx, "select v from "+table).Scan(&value))
					actual = value
				case int64:
					var value int64
					require.NoError(t, db.QueryRowContext(ctx, "select v from "+table).Scan(&value))
					actual = value
				}
				require.Equal(t, tc.want, actual)
			})
		}
	})
}
