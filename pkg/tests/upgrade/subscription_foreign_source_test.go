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

package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testHistoricalSubscriptionForeignSource(t *testing.T, sys, tenant *sql.DB) {
	for i, tc := range []struct {
		name, publisherName, ownerName string
		publisher, owner               *sql.DB
	}{
		{"system publisher", "sys", "view_description_sub", sys, tenant},
		{"owner-only targets", "view_description_sub", "sys", tenant, sys},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			exec := func(db *sql.DB, query string) {
				t.Helper()
				_, err := db.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			prefix := fmt.Sprintf("view_description_foreign_%d", i)
			original, source, alias := prefix+"_original", prefix+"_source", prefix+"_alias"
			publication, snapshot := prefix+"_pub", prefix+"_history"
			createSource := func(db *sql.DB, database string, width int, value string) {
				t.Helper()
				exec(db, "create database "+database)
				t.Cleanup(func() { cleanupViewDescriptionSQL(t, db, "drop database if exists "+database) })
				exec(db, fmt.Sprintf("create table %s.src(x varchar(%d)) comment='%s'", database, width, value))
				exec(db, fmt.Sprintf("insert into %s.src values ('%s')", database, value))
				exec(db, "create view "+database+".v as select x from "+database+".src")
			}
			createSource(tc.publisher, original, 8, "initial")
			createSource(tc.owner, source, 37, "source-then")
			createSource(tc.publisher, source, 8, "decoy-T")
			createPublication := func(database string) {
				exec(tc.publisher, fmt.Sprintf("create publication %s database %s account %s", publication, database, tc.ownerName))
			}
			createPublication(original)
			t.Cleanup(func() { cleanupViewDescriptionSQL(t, tc.publisher, "drop publication if exists "+publication) })
			exec(tc.owner, fmt.Sprintf("create database %s from %s publication %s", alias, tc.publisherName, publication))
			t.Cleanup(func() { cleanupViewDescriptionSQL(t, tc.owner, "drop database "+alias) })
			exec(tc.owner, "create snapshot "+snapshot+" for account")
			t.Cleanup(func() { cleanupViewDescriptionSQL(t, tc.owner, "drop snapshot "+snapshot) })

			// Remove the current publisher decoy only while resolving the new
			// publication. The historical decoy remains visible at data T.
			exec(tc.publisher, "drop publication "+publication)
			exec(tc.publisher, "drop database "+source)
			createPublication(source)
			createSource(tc.publisher, source, 16, "decoy-now")
			exec(tc.owner, "alter table "+source+".src modify x varchar(51)")
			exec(tc.owner, "update "+source+".src set x='source-now'")

			for _, object := range []string{"src", "v"} {
				for _, historical := range []bool{true, false} {
					hint, value, width := "", "source-now", 51
					if historical {
						hint, value, width = " {snapshot='"+snapshot+"'}", "source-then", 37
					}
					var actual string
					query := "select x from " + alias + "." + object + hint
					require.NoError(t, tc.owner.QueryRowContext(ctx, query).Scan(&actual))
					require.Equal(t, value, actual, query)
					func() {
						prepared, err := tc.owner.PrepareContext(ctx, query)
						require.NoError(t, err)
						defer prepared.Close()
						require.NoError(t, prepared.QueryRowContext(ctx).Scan(&actual))
						require.Equal(t, value, actual)
					}()
					var actualWidth, count int
					require.NoError(t, tc.owner.QueryRowContext(ctx,
						"select character_maximum_length from information_schema.columns"+hint+
							" where table_schema=? and table_name=? and column_name='x'", alias, object).Scan(&actualWidth))
					require.Equal(t, width, actualWidth, "metadata must describe SELECT's admitted physical source at the same T")
					require.NoError(t, tc.owner.QueryRowContext(ctx,
						"select count(*) from information_schema.tables"+hint+" where table_schema=? and table_name=?", alias, object).Scan(&count))
					require.Equal(t, 1, count)
					if object == "src" {
						var comment string
						require.NoError(t, tc.owner.QueryRowContext(ctx,
							"select table_comment from information_schema.tables"+hint+" where table_schema=? and table_name='src'", alias).Scan(&comment))
						require.Equal(t, "source-then", comment, "table metadata must not come from the publisher decoy")
					}
				}
			}
			exec(tc.publisher, "alter publication "+publication+" account drop "+tc.ownerName)
			var count int
			require.NoError(t, tc.owner.QueryRowContext(ctx,
				"select count(*) from information_schema.columns {snapshot='"+snapshot+"'} where table_schema=?", alias).Scan(&count))
			require.Zero(t, count, "old source ownership cannot revive current membership")
			var value string
			err := tc.owner.QueryRowContext(ctx, "select x from "+alias+".src {snapshot='"+snapshot+"'}").Scan(&value)
			require.Error(t, err)
		})
	}
}
