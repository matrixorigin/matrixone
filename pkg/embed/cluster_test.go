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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/flock"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeTrackingService struct {
	closeCount atomic.Int32
	startErr   error
	closeErr   error
}

func (s *closeTrackingService) Start() error {
	return s.startErr
}

func (s *closeTrackingService) Close() error {
	s.closeCount.Add(1)
	return s.closeErr
}

type closeTrackingFileService struct {
	closeCount atomic.Int32
}

func (s *closeTrackingFileService) Close(context.Context) {
	s.closeCount.Add(1)
}

func TestOperatorOwnsConstructedServiceBeforeStart(t *testing.T) {
	startErr := errors.New("service partially started")
	svc := &closeTrackingService{startErr: startErr}
	op := &operator{}

	err := op.startConstructedServiceLocked(svc)
	require.ErrorIs(t, err, startErr)
	require.Same(t, svc, op.reset.svc)

	require.NoError(t, op.Close())
	require.Equal(t, int32(1), svc.closeCount.Load())
	require.False(t, op.needsCleanup())
}

func TestBasicCluster(t *testing.T) {
	c, err := StartTestCluster(
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
	defer func() {
		require.NoError(t, c.Close())
	}()

	validCNCanWork(t, c, 0)
	validCNCanWork(t, c, 1)
	validCNCanWork(t, c, 2)

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	v, err := c.GetService(cn.ServiceID())
	require.NoError(t, err)
	require.Equal(t, cn, v)
}

func TestWithHAKeeperHeartbeatTimeout(t *testing.T) {
	timeout := 15 * time.Second
	clusterValue, err := NewCluster(
		WithCNCount(2),
		WithHAKeeperHeartbeatTimeout(timeout),
	)
	require.NoError(t, err)
	c := clusterValue.(*cluster)
	defer func() {
		require.NoError(t, c.Close())
		require.NoError(t, os.RemoveAll(c.options.dataPath))
	}()

	for _, svc := range c.services {
		cfg := svc.GetServiceConfig()
		switch svc.ServiceType() {
		case metadata.ServiceType_CN:
			require.Equal(t, timeout, cfg.CN.HAKeeper.HeatbeatTimeout.Duration)
		case metadata.ServiceType_TN:
			require.NotNil(t, cfg.TN_please_use_getTNServiceConfig)
			require.Equal(t, timeout, cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration)
		}
	}
}

func TestHAKeeperHeartbeatTimeoutHonorsLegacyTNConfig(t *testing.T) {
	timeout := 15 * time.Second
	cfg := &ServiceConfig{TNCompatible: &tnservice.Config{}}

	applyHAKeeperHeartbeatTimeout(cfg, metadata.ServiceType_TN, timeout)

	require.Same(t, cfg.TNCompatible, cfg.TN_please_use_getTNServiceConfig)
	require.Equal(t, timeout, cfg.getTNServiceConfig().HAKeeper.HeatbeatTimeout.Duration)
}

func TestSingleCNCluster(t *testing.T) {
	c, err := NewCluster(WithTesting())
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() {
		require.NoError(t, c.Close())
	}()
	require.Error(t, c.Start())

	validCNCanWork(t, c, 0)

	_, err = c.GetService("no")
	require.Error(t, err)

	_, err = c.GetCNService(1)
	require.Error(t, err)
}

func TestClusterCanStartNewCNServices(t *testing.T) {
	c, err := StartTestCluster(WithCNCount(3))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close())
	}()

	validCNCanWork(t, c, 0)
	validCNCanWork(t, c, 1)
	validCNCanWork(t, c, 2)

	require.NoError(t, c.StartNewCNService(1))
	validCNCanWork(t, c, 3)
}

func TestMultiClusterCanWork(t *testing.T) {
	new := func() *cluster {
		value, err := StartTestCluster(WithCNCount(1))
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, value.Close())
		})
		return value.(*cluster)
	}

	first := new()
	second := new()
	require.NotEqual(t, first.ID(), second.ID())
	require.NotEqual(t, first.options.dataPath, second.options.dataPath)
	require.NotEqual(t, first.portLease.base, second.portLease.base)
	validCNCanWork(t, first, 0)
	validCNCanWork(t, second, 0)
}

func TestBaseClusterCanWorkWithNewCluster(t *testing.T) {
	RunBaseClusterTests(t,
		func(c Cluster) {
			validCNCanWork(t, c, 0)
		},
	)

	c, err := StartTestCluster(WithCNCount(1))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close())
	}()

	validCNCanWork(t, c, 0)
}

func TestBaseClusterOnlyStartOnce(t *testing.T) {
	var id1, id2 uint64
	RunBaseClusterTests(t,
		func(c Cluster) {
			id1 = c.ID()
		},
	)

	RunBaseClusterTests(t,
		func(c Cluster) {
			id2 = c.ID()
		},
	)

	require.Equal(t, id1, id2)
}

func TestRestartCN(t *testing.T) {
	t.SkipNow()
	RunBaseClusterTests(t,
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
	RunBaseClusterTests(t,
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
	RunBaseClusterTests(t,
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
			func() {
				rows, err := conn.QueryContext(ctx, "call caller_count()")
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(&rowCount))
				require.NoError(t, rows.Err())
				require.Equal(t, int64(6), rowCount)
			}()

			_, err = conn.ExecContext(ctx, "create procedure inner_results() 'begin select 20; select 21; end'")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "create procedure outer_results() 'begin select 10; call inner_results(); select 30; end'")
			require.NoError(t, err)
			func() {
				rows, err := conn.QueryContext(ctx, "call outer_results()")
				require.NoError(t, err)
				defer rows.Close()
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
			}()

			_, err = conn.ExecContext(ctx, "insert into t values (1)")
			require.Error(t, err)
			require.NoError(t, stmt.QueryRowContext(ctx).Scan(&rowCount))
			require.Equal(t, int64(-1), rowCount)
		},
	)
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

func TestClusterPortLeasesAreExclusive(t *testing.T) {
	first, err := acquireClusterPortLease()
	require.NoError(t, err)
	firstCluster := &cluster{
		id:            1,
		portLease:     first,
		portLeaseBase: first.base,
		portLeaseNext: first.base,
	}
	t.Cleanup(func() {
		if firstCluster.portLease != nil {
			require.NoError(t, firstCluster.releasePortLeaseLocked())
		}
	})

	second, err := acquireClusterPortLease()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, second.lock.Close()) })

	require.NotEqual(t, first.base, second.base)
	if first.base < second.base {
		require.GreaterOrEqual(t, second.base-first.base, portLeaseSpan)
	} else {
		require.GreaterOrEqual(t, first.base-second.base, portLeaseSpan)
	}

	firstPort := firstCluster.nextBasePort()
	require.Greater(t, firstPort, int(first.base))
	require.Less(t, firstPort, int(first.base+portLeaseSpan))

	require.NoError(t, firstCluster.releasePortLeaseLocked())
	contender, locked, err := tryAcquireClusterPortLease(first.base)
	require.NoError(t, err)
	require.True(t, locked)
	require.Error(t, firstCluster.ensurePortLeaseLocked())
	require.NoError(t, contender.lock.Close())
	require.NoError(t, firstCluster.ensurePortLeaseLocked())
	require.Equal(t, firstPort+int(basePortStep), firstCluster.nextBasePort())
	require.NoError(t, firstCluster.releasePortLeaseLocked())
}

func TestClusterStartupLeaseIsExclusive(t *testing.T) {
	first, err := acquireClusterStartupLease(context.Background())
	require.NoError(t, err)
	firstClosed := false
	t.Cleanup(func() {
		if !firstClosed {
			require.NoError(t, first.Close())
		}
	})

	contender := flock.New(filepath.Join(os.TempDir(), clusterStartupLeaseFilename))
	locked, err := contender.TryLock()
	require.NoError(t, err)
	require.False(t, locked)
	require.NoError(t, contender.Close())
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acquireClusterStartupLease(canceledCtx)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, first.Close())
	firstClosed = true
	next, err := acquireClusterStartupLease(context.Background())
	require.NoError(t, err)
	require.NoError(t, next.Close())
}

func TestWithTestingUsesCoherentHAKeeperTimeouts(t *testing.T) {
	clusterValue, err := NewCluster(WithTesting())
	require.NoError(t, err)
	c := clusterValue.(*cluster)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	for _, svc := range c.services {
		cfg := svc.GetServiceConfig()
		switch svc.ServiceType() {
		case metadata.ServiceType_CN:
			require.Equal(t, testHAKeeperHeartbeatTimeout, cfg.CN.HAKeeper.HeatbeatTimeout.Duration)
		case metadata.ServiceType_TN:
			require.Equal(t, testHAKeeperHeartbeatTimeout,
				cfg.getTNServiceConfig().HAKeeper.HeatbeatTimeout.Duration)
		case metadata.ServiceType_LOG:
			require.Equal(t, testHAKeeperStoreTimeout,
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration)
			require.Equal(t, testHAKeeperStoreTimeout,
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration)
		}
	}
}

func validCNCanWork(
	t *testing.T,
	c Cluster,
	index int,
) {
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		svc, err := c.GetCNService(index)
		if !assert.NoError(collect, err) {
			return
		}

		sql := svc.(*operator).reset.svc.(cnservice.Service).GetSQLExecutor()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := sql.Exec(
			ctx,
			"select count(1) from mo_catalog.mo_tables",
			executor.Options{},
		)
		if !assert.NoError(collect, err) {
			return
		}
		defer res.Close()

		var n int64
		res.ReadRows(
			func(rows int, cols []*vector.Vector) bool {
				n = executor.GetFixedRows[int64](cols[0])[0]
				return true
			},
		)
		assert.Positive(collect, n)
	}, 30*time.Second, 100*time.Millisecond)
}

func TestCreateDB(t *testing.T) {
	RunBaseClusterTests(t,
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

func TestClusterStartRollbackClosesPartiallyStartedServices(t *testing.T) {
	startErr := errors.New("TN wait for HAKeeper timed out")
	portLease, err := acquireClusterPortLease()
	require.NoError(t, err)
	logService := &closeTrackingService{}
	logFS := &closeTrackingFileService{}
	tnFS := &closeTrackingFileService{}
	logStopper := stopper.NewStopper("rollback-log")
	tnStopper := stopper.NewStopper("rollback-tn")
	tnTaskStopped := make(chan struct{})
	require.NoError(t, tnStopper.RunTask(func(ctx context.Context) {
		<-ctx.Done()
		close(tnTaskStopped)
	}))

	logOp := &operator{serviceType: metadata.ServiceType_LOG}
	tnOp := &operator{serviceType: metadata.ServiceType_TN}
	cnOp := &operator{serviceType: metadata.ServiceType_CN}
	c := &cluster{
		services:      []*operator{logOp, tnOp, cnOp},
		portLease:     portLease,
		portLeaseBase: portLease.base,
		portLeaseNext: portLease.base,
	}
	t.Cleanup(func() {
		if c.portLease != nil {
			require.NoError(t, c.releasePortLeaseLocked())
		}
	})
	c.startFn = func(op *operator) error {
		switch op.serviceType {
		case metadata.ServiceType_LOG:
			op.state = started
			op.reset.svc = logService
			op.reset.stopper = logStopper
			op.reset.fs = logFS
			return nil
		case metadata.ServiceType_TN:
			op.reset.stopper = tnStopper
			op.reset.fs = tnFS
			return startErr
		case metadata.ServiceType_CN:
			t.Fatal("CN must not start after TN startup fails")
		default:
			t.Fatalf("unexpected service type %s", op.serviceType)
		}
		return nil
	}

	err = c.Start()
	require.ErrorIs(t, err, startErr)
	require.Equal(t, int32(1), logService.closeCount.Load())
	require.Equal(t, int32(1), logFS.closeCount.Load())
	require.Equal(t, int32(1), tnFS.closeCount.Load())
	require.Equal(t, stopped, logOp.state)
	require.Equal(t, stopped, tnOp.state)
	require.Equal(t, stopped, cnOp.state)
	require.Nil(t, logOp.reset.stopper)
	require.Nil(t, tnOp.reset.stopper)
	select {
	case <-tnTaskStopped:
	default:
		t.Fatal("partially initialized TN stopper was not stopped")
	}

	// Cleanup is idempotent, so a caller's deferred Close does not obscure the
	// original startup error or close an already rolled-back service twice.
	require.NoError(t, c.Close())
	require.Equal(t, int32(1), logService.closeCount.Load())
	require.Equal(t, int32(1), logFS.closeCount.Load())
	require.Equal(t, int32(1), tnFS.closeCount.Load())
}

func TestClusterCloseContinuesAfterServiceError(t *testing.T) {
	first := &closeTrackingService{}
	secondErr := errors.New("close second")
	second := &closeTrackingService{closeErr: secondErr}
	firstOp := &operator{state: started}
	firstOp.reset.svc = first
	secondOp := &operator{state: started}
	secondOp.reset.svc = second
	c := &cluster{
		state:    started,
		services: []*operator{firstOp, secondOp},
	}

	err := c.Close()
	require.ErrorIs(t, err, secondErr)
	require.Equal(t, int32(1), first.closeCount.Load())
	require.Equal(t, int32(1), second.closeCount.Load())
	require.Equal(t, stopped, c.state)

	second.closeErr = nil
	require.NoError(t, c.Close())
	require.Equal(t, int32(1), first.closeCount.Load())
	require.Equal(t, int32(2), second.closeCount.Load())
}

func TestRollbackNewServicesKeepsRunningCluster(t *testing.T) {
	existingService := &closeTrackingService{}
	existingOp := &operator{state: started}
	existingOp.reset.svc = existingService

	newStopper := stopper.NewStopper("rollback-new-cn")
	newTaskStopped := make(chan struct{})
	require.NoError(t, newStopper.RunTask(func(ctx context.Context) {
		<-ctx.Done()
		close(newTaskStopped)
	}))
	newOp := &operator{serviceType: metadata.ServiceType_CN}
	newOp.reset.stopper = newStopper

	c := &cluster{
		state:    started,
		files:    []string{"existing.toml", "new.toml"},
		services: []*operator{existingOp, newOp},
	}
	c.options.cn = 2

	require.NoError(t, c.rollbackNewServicesLocked(1, 1))
	require.Equal(t, started, c.state)
	require.Len(t, c.services, 1)
	require.Same(t, existingOp, c.services[0])
	require.Equal(t, []string{"existing.toml"}, c.files)
	require.Equal(t, 1, c.options.cn)
	require.Equal(t, int32(0), existingService.closeCount.Load())
	select {
	case <-newTaskStopped:
	default:
		t.Fatal("partially initialized new CN stopper was not stopped")
	}
}

func TestRollbackNewServicesDropsTopologyAfterCloseError(t *testing.T) {
	startErr := errors.New("start new CN")
	closeErr := errors.New("close new CN")
	newService := &closeTrackingService{closeErr: closeErr}
	clusterValue, err := NewCluster(WithCNCount(1))
	require.NoError(t, err)
	c := clusterValue.(*cluster)
	c.state = started
	servicesBefore := append([]*operator(nil), c.services...)
	filesBefore := append([]string(nil), c.files...)
	c.startFn = func(op *operator) error {
		require.Equal(t, metadata.ServiceType_CN, op.serviceType)
		op.state = started
		op.reset.svc = newService
		return startErr
	}

	err = c.StartNewCNService(1)
	require.ErrorIs(t, err, startErr)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, started, c.state)
	require.Equal(t, servicesBefore, c.services)
	require.Equal(t, filesBefore, c.files)
	require.Equal(t, 1, c.options.cn)
	require.Len(t, c.pendingCleanup, 1)
	_, err = c.GetCNService(1)
	require.Error(t, err)

	err = c.StartNewCNService(1)
	require.ErrorIs(t, err, closeErr)
	require.Equal(t, servicesBefore, c.services)
	require.Equal(t, filesBefore, c.files)
	require.Equal(t, 1, c.options.cn)
	require.Len(t, c.pendingCleanup, 1)

	newService.closeErr = nil
	require.NoError(t, c.StartNewCNService(0))
	require.Empty(t, c.pendingCleanup)
	require.NoError(t, c.Close())
	require.Equal(t, int32(3), newService.closeCount.Load())
}
