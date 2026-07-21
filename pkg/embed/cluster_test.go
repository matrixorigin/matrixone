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
	"sort"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicCluster(t *testing.T) {
	c, err := NewCluster(
		WithCNCount(3),
		WithPreStart(
			func(svc ServiceOperator) {
				if svc.ServiceType() == metadata.ServiceType_CN {
					svc.Adjust(
						func(config *ServiceConfig) {
							config.CN.AutomaticUpgrade = true
						},
					)
				}
			},
		),
	)
	require.NoError(t, err)
	require.NoError(t, c.Start())

	validCNCanWork(t, c, 0)
	validCNCanWork(t, c, 1)
	validCNCanWork(t, c, 2)

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	v, err := c.GetService(cn.ServiceID())
	require.NoError(t, err)
	require.Equal(t, cn, v)

	require.NoError(t, c.Close())
}

func TestSingleCNCluster(t *testing.T) {
	c, err := NewCluster()
	require.NoError(t, err)
	require.NoError(t, c.Start())
	require.Error(t, c.Start())

	validCNCanWork(t, c, 0)

	_, err = c.GetService("no")
	require.Error(t, err)

	_, err = c.GetCNService(1)
	require.Error(t, err)

	require.NoError(t, c.Close())
}

func TestClusterCanStartNewCNServices(t *testing.T) {
	c, err := NewCluster(WithCNCount(3))
	require.NoError(t, err)
	require.NoError(t, c.Start())

	validCNCanWork(t, c, 0)
	validCNCanWork(t, c, 1)
	validCNCanWork(t, c, 2)

	require.NoError(t, c.StartNewCNService(1))
	validCNCanWork(t, c, 3)

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

func TestRowCountOverMySQLProtocol(t *testing.T) {
	require.NoError(t, RunBaseClusterTests(
		func(c Cluster) {
			cn0, err := c.GetCNService(0)
			require.NoError(t, err)

			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/",
				cn0.GetServiceConfig().CN.Frontend.Port,
			)
			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			_, err = conn.ExecContext(ctx, "drop database if exists row_count_protocol_test")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "create database row_count_protocol_test")
			require.NoError(t, err)
			defer conn.ExecContext(ctx, "drop database if exists row_count_protocol_test")
			_, err = conn.ExecContext(ctx, "use row_count_protocol_test")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "create table t (id int primary key)")
			require.NoError(t, err)

			_, err = conn.ExecContext(ctx, "insert into t values (1), (2)")
			require.NoError(t, err)
			stmt, err := conn.PrepareContext(ctx, "select row_count()")
			require.NoError(t, err)
			defer stmt.Close()

			var rowCount int64
			require.NoError(t, stmt.QueryRowContext(ctx).Scan(&rowCount))
			require.Equal(t, int64(2), rowCount)

			result, err := conn.ExecContext(ctx, "insert into t values (3)")
			require.NoError(t, err)
			affectedRows, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(1), affectedRows)

			_, err = conn.ExecContext(ctx, "create procedure insert_rows() 'begin insert into t values (4), (5); end'")
			require.NoError(t, err)
			result, err = conn.ExecContext(ctx, "call insert_rows()")
			require.NoError(t, err)
			affectedRows, err = result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(2), affectedRows)
			require.NoError(t, stmt.QueryRowContext(ctx).Scan(&rowCount))
			require.Equal(t, int64(2), rowCount)

			_, err = conn.ExecContext(ctx, "create procedure caller_count() 'begin select row_count(); end'")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into t values (6), (7), (8), (9), (10), (11)")
			require.NoError(t, err)
			rows, err := conn.QueryContext(ctx, "call caller_count()")
			require.NoError(t, err)
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(&rowCount))
			require.Equal(t, int64(6), rowCount)
			require.NoError(t, rows.Close())

			_, err = conn.ExecContext(ctx, "create procedure inner_results() 'begin select 20; select 21; end'")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "create procedure outer_results() 'begin select 10; call inner_results(); select 30; end'")
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, "call outer_results()")
			require.NoError(t, err)
			var got []int64
			for {
				for rows.Next() {
					var value int64
					require.NoError(t, rows.Scan(&value))
					got = append(got, value)
				}
				require.NoError(t, rows.Err())
				if !rows.NextResultSet() {
					break
				}
			}
			require.Equal(t, []int64{10, 20, 21, 30}, got)
			require.NoError(t, rows.Close())

			_, err = conn.ExecContext(ctx, "insert into t values (1)")
			require.Error(t, err)
			require.NoError(t, stmt.QueryRowContext(ctx).Scan(&rowCount))
			require.Equal(t, int64(-1), rowCount)
		},
	))
}

func TestGetInitValue(t *testing.T) {
	var wg sync.WaitGroup
	var ports []uint64
	var lock sync.Mutex
	add := func(v uint64) {
		lock.Lock()
		defer lock.Unlock()
		ports = append(ports, v)
	}

	n := 4
	name := fmt.Sprintf("%d.port", time.Now().Nanosecond())
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			port := getInitValue(name)
			add(port)
		}()
	}

	wg.Wait()
	sort.Slice(ports, func(i, j int) bool {
		return ports[i] < ports[j]
	})
	require.Equal(t, []uint64{10000, 11000, 12000, 13000}, ports)
}

func TestGetInitValueWithEmptyNameMustPanic(t *testing.T) {
	defer func() {
		err := recover()
		require.NotNil(t, err)
	}()
	getInitValue("")
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

func TestCreateDB(t *testing.T) {
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

			_, err = db.Exec("create database foo")
			require.NoError(t, err)

			_, err = db.Exec("use foo")
			require.NoError(t, err)

			_, err = db.Exec("create table bar (id int)")
			require.NoError(t, err)

			_, err = db.Exec("insert into bar values (1)")
			require.NoError(t, err)

			rows, err := db.Query("select id from bar")
			require.NoError(t, err)
			require.NoError(t, rows.Err())
			defer rows.Close()

			var id int
			for rows.Next() {
				rows.Scan(&id)
				require.Equal(t, 1, id)
			}
		},
	)
}

// TestDoStartLockedErrorPaths exercises the error-handling branches in
// doStartLocked that are not reached by normal cluster startup tests.
func TestDoStartLockedErrorPaths(t *testing.T) {
	t.Run("non-CN service error returns immediately", func(t *testing.T) {
		// A non-CN operator whose state is already 'started' will return
		// an error from Start(), exercising the direct-return path at
		// cluster.go line 119-121.
		op := &operator{
			serviceType: metadata.ServiceType_LOG,
			state:       started, // forces Start() to return error
		}
		c := &cluster{
			services: []*operator{op},
		}
		err := c.doStartLocked(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already started")
	})

	t.Run("CN service error captured via atomic.Value", func(t *testing.T) {
		// A CN operator whose state is already 'started' will return an
		// error from Start(), exercising the goroutine error-capture path
		// at cluster.go lines 128-133 and the error-return at 138-140.
		op := &operator{
			serviceType: metadata.ServiceType_CN,
			state:       started,
		}
		c := &cluster{
			services: []*operator{op},
		}
		err := c.doStartLocked(0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already started")
	})

	t.Run("Start propagates doStartLocked error", func(t *testing.T) {
		// Exercises the error propagation in Start() at line 107-109.
		op := &operator{
			serviceType: metadata.ServiceType_LOG,
			state:       started,
		}
		c := &cluster{
			state:    stopped,
			services: []*operator{op},
		}
		err := c.Start()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already started")
	})

	t.Run("Start rejects double start", func(t *testing.T) {
		c := &cluster{state: started}
		err := c.Start()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "embed mo cluster already started")
	})

	t.Run("happy path with no services", func(t *testing.T) {
		c := &cluster{
			state:    stopped,
			services: []*operator{},
		}
		err := c.doStartLocked(0)
		assert.NoError(t, err)
	})
}
