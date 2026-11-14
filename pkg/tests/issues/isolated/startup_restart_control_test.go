// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Same constructor and SQL readiness as the failed P1 probe, without branch
// operations, fault injection, lifecycle hooks, or workload DDL/DML.
func TestStartupRestartControl(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	check := func() {
		t.Helper()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, waitSystemBootstrap(ctx, db))
		var value int
		require.NoError(t, db.QueryRowContext(ctx, "select 1").Scan(&value))
		require.Equal(t, 1, value)
	}
	check()
	require.NoError(t, cluster.Stop())
	require.NoError(t, cluster.Start())
	check()
}
