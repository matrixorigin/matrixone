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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue28792BinaryBitwiseLengthMismatchUsesMySQLError(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()

		for _, operator := range []string{"&", "|", "^"} {
			t.Run(operator, func(t *testing.T) {
				var got string
				err := db.QueryRowContext(ctx,
					"SELECT HEX(_binary X'01' "+operator+" _binary X'0001')").Scan(&got)
				require.Error(t, err)

				var mysqlErr *mysqlDriver.MySQLError
				require.ErrorAs(t, err, &mysqlErr)
				require.Equal(t, uint16(3513), mysqlErr.Number)
				require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
				require.Equal(t,
					"Binary operands of bitwise operators must be of equal length",
					mysqlErr.Message)
			})
		}
	})
}
