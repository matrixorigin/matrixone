// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkRoTx benchmarks the performance of read-only, noop transactions.
func BenchmarkRoTx(b *testing.B) {
	RunBaseClusterTests(
		func(c Cluster) {

			cn0, err := c.GetCNService(0)
			require.NoError(b, err)

			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn0.GetServiceConfig().CN.Frontend.Port,
			)

			db, err := sql.Open("mysql", dsn)
			require.NoError(b, err)
			defer db.Close()

			b.ResetTimer()
			for range b.N {
				tx, err := db.Begin()
				require.NoError(b, err)
				err = tx.Commit()
				require.NoError(b, err)
			}
			b.StopTimer()
		},
	)
}

// BenchmarkSelect1Conn benchmarks the performance of running select 1
// from one connection, in autocommit mode.
func BenchmarkSelect1Conn(b *testing.B) {
	RunBaseClusterTests(
		func(c Cluster) {

			cn0, err := c.GetCNService(0)
			require.NoError(b, err)

			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn0.GetServiceConfig().CN.Frontend.Port,
			)

			db, err := sql.Open("mysql", dsn)
			require.NoError(b, err)
			defer db.Close()

			ctx := context.Background()
			conn, err := db.Conn(ctx)
			require.NoError(b, err)
			defer conn.Close()

			_, err = conn.ExecContext(ctx, `set debug_break=on`)
			require.NoError(b, err)

			b.ResetTimer()
			for range b.N {
				rows, err := conn.QueryContext(ctx, `select 1`)
				require.NoError(b, err)
				rows.Close()
			}
			b.StopTimer()
		},
	)
}
