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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestOrderExpressionErrorKeepsPublicMessage(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const database = "order_expression_error"
		const table = database + ".vectors"
		execSQLRequire(t, ctx, db, "create database "+database)
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, _ = db.ExecContext(cleanupCtx, "drop database "+database)
		}()
		execSQLRequire(t, ctx, db, "create table "+table+" (b vecf32(128))")
		execSQLRequire(t, ctx, db, "insert into "+table+" values ('["+strings.Repeat("0,", 127)+"0]')")
		var value string
		err = db.QueryRowContext(ctx, "select b from "+table+
			" order by l2_distance(b, '[1,0,1,6,6,17,47]')").Scan(&value)
		require.ErrorContains(t, err, "invalid input: vector ops between different dimensions (128, 7) is not permitted.")
		require.NotContains(t, err.Error(), "order phase=")
	})
}
