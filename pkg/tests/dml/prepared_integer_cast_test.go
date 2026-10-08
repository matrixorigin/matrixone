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

package dml

import (
	"context"
	"errors"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPreparedArithmeticPreservesExplicitCasts(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		for _, tc := range []struct {
			query               string
			reverse, negativeOK bool
		}{
			{"select cast(? as unsigned)+?", false, false},
			{"select ?+cast(? as unsigned)", true, false},
			{"select cast(cast(? as signed) as unsigned)+?", false, false},
			{"select cast(cast(? as unsigned) as signed)+?", false, true},
		} {
			stmt, err := db.PrepareContext(ctx, tc.query)
			require.NoError(t, err)
			func() {
				defer stmt.Close()
				for _, negative := range []bool{false, true, false} {
					a, b := int64(2), int64(1)
					want := "3"
					if negative {
						a, b = 0, -1
						want = "-1"
					}
					if tc.reverse {
						a, b = b, a
					}
					var value string
					err := stmt.QueryRowContext(ctx, a, b).Scan(&value)
					if negative && !tc.negativeOK {
						var sqlErr *mysql.MySQLError
						require.True(t, errors.As(err, &sqlErr), "%s: %v", tc.query, err)
						require.Equal(t, uint16(1690), sqlErr.Number, tc.query)
					} else {
						require.NoError(t, err, tc.query)
						require.Equal(t, want, value, tc.query)
					}
				}
			}()
		}
	})
}
