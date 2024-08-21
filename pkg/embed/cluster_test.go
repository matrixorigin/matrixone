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
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestBasicCluster(t *testing.T) {
	c, err := NewCluster(WithCNCount(3))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	validCNCanWork(t, c, 0)
	validCNCanWork(t, c, 1)
	validCNCanWork(t, c, 2)

	require.NoError(t, c.Close())
}

func TestMultiClusterCanWork(t *testing.T) {
	new := func() Cluster {
		c, err := NewCluster(WithCNCount(3))
		require.NoError(t, err)
		require.NoError(t, c.Start())

		validCNCanWork(t, c, 0)
		validCNCanWork(t, c, 1)
		validCNCanWork(t, c, 2)
		return c
	}

	c1 := new()
	c2 := new()

	require.NoError(t, c1.Close())
	require.NoError(t, c2.Close())
}

func TestBaseClusterCanWorkWithNewCluster(t *testing.T) {
	RunBaseClusterTests(
		func(c Cluster) {
			validCNCanWork(t, c, 0)
			validCNCanWork(t, c, 1)
			validCNCanWork(t, c, 2)
		},
	)

	c, err := NewCluster(WithCNCount(3))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	validCNCanWork(t, c, 0)
	validCNCanWork(t, c, 1)
	validCNCanWork(t, c, 2)
}

func TestBaseClusterOnlyStartOnce(t *testing.T) {
	var id1, id2 uint64
	RunBaseClusterTests(
		func(c Cluster) {
			id1 = c.ID()
		},
	)

	RunBaseClusterTests(
		func(c Cluster) {
			id2 = c.ID()
		},
	)

	require.Equal(t, id1, id2)
}

func TestRestartCN(t *testing.T) {
	// TODO: wait #17668 fixed
	t.SkipNow()
	RunBaseClusterTests(
		func(c Cluster) {
			svc, err := c.GetCNService(0)
			require.NoError(t, err)
			require.NoError(t, svc.Close())

			require.NoError(t, svc.Start())
			validCNCanWork(t, c, 0)
		},
	)
}

func TestRunSQLWithFrontend(t *testing.T) {
	RunBaseClusterTests(
		func(c Cluster) {
			cn0, err := c.GetCNService(0)
			require.NoError(t, err)

			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn0.GetServiceConfig().CN.Frontend.Port,
			)

			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			_, err = db.Exec("show databases")
			require.NoError(t, err)
		},
	)
}

func validCNCanWork(
	t *testing.T,
	c Cluster,
	index int,
) {
	svc, err := c.GetCNService(index)
	require.NoError(t, err)

	sql := svc.(*operator).reset.svc.(cnservice.Service).GetSQLExecutor()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := sql.Exec(
		ctx,
		"select count(1) from mo_catalog.mo_tables",
		executor.Options{},
	)
	require.NoError(t, err)
	defer res.Close()

	n := int64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			n = executor.GetFixedRows[int64](cols[0])[0]
			return true
		},
	)
	require.True(t, n > 0)
}
