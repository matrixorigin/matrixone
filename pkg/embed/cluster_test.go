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
	"sort"
	"sync"
	"sync/atomic"
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

type closeTrackingService struct {
	closeCount atomic.Int32
	closeErr   error
}

func (s *closeTrackingService) Start() error { return nil }

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
	svc := &closeTrackingService{}
	op := &operator{}

	require.NoError(t, op.startConstructedServiceLocked(svc))
	require.Same(t, svc, op.reset.svc)

	require.NoError(t, op.Close())
	require.Equal(t, int32(1), svc.closeCount.Load())
	require.False(t, op.needsCleanup())
	require.NoError(t, op.Close())
}

func TestOperatorCloseRetainsDependenciesAfterServiceCloseFailure(t *testing.T) {
	closeErr := errors.New("service close failed")
	svc := &closeTrackingService{closeErr: closeErr}
	fs := &closeTrackingFileService{}
	op := &operator{state: stopped}
	op.reset.svc = svc
	op.reset.fs = fs

	require.ErrorIs(t, op.Close(), closeErr)
	require.Equal(t, int32(1), svc.closeCount.Load())
	require.Zero(t, fs.closeCount.Load())
	require.Same(t, svc, op.reset.svc)
	require.Same(t, fs, op.reset.fs)

	svc.closeErr = nil
	require.NoError(t, op.Close())
	require.Equal(t, int32(2), svc.closeCount.Load())
	require.Equal(t, int32(1), fs.closeCount.Load())
	require.False(t, op.needsCleanup())
}

func TestClusterStartRollbackClosesPartiallyConstructedServices(t *testing.T) {
	startErr := errors.New("service startup failed")
	logService := &closeTrackingService{}
	logFS := &closeTrackingFileService{}
	tnFS := &closeTrackingFileService{}

	logOp := &operator{serviceType: metadata.ServiceType_LOG}
	tnOp := &operator{serviceType: metadata.ServiceType_TN}
	cnOp := &operator{serviceType: metadata.ServiceType_CN}
	c := &cluster{
		services: []*operator{logOp, tnOp, cnOp},
		startFn: func(op *operator) error {
			switch op.serviceType {
			case metadata.ServiceType_LOG:
				op.state = started
				op.reset.svc = logService
				op.reset.fs = logFS
				return nil
			case metadata.ServiceType_TN:
				op.reset.fs = tnFS
				return startErr
			default:
				t.Fatalf("service %s must not start after rollback", op.serviceType)
				return nil
			}
		},
	}

	err := c.Start()
	require.ErrorIs(t, err, startErr)
	require.Equal(t, int32(1), logService.closeCount.Load())
	require.Equal(t, int32(1), logFS.closeCount.Load())
	require.Equal(t, int32(1), tnFS.closeCount.Load())
	require.Equal(t, stopped, logOp.state)
	require.Equal(t, stopped, tnOp.state)
	require.Equal(t, stopped, cnOp.state)

	// Startup rollback is idempotent; a deferred Close must not double-close.
	require.NoError(t, c.Close())
	require.Equal(t, int32(1), logService.closeCount.Load())
	require.Equal(t, int32(1), logFS.closeCount.Load())
	require.Equal(t, int32(1), tnFS.closeCount.Load())
}

func TestClusterStartRetriesFailedInitialCleanupBeforeRestart(t *testing.T) {
	startErr := errors.New("service startup failed")
	closeErr := errors.New("service close failed")
	firstService := &closeTrackingService{closeErr: closeErr}
	secondService := &closeTrackingService{}
	op := &operator{serviceType: metadata.ServiceType_LOG}
	c := &cluster{
		services: []*operator{op},
		startFn: func(op *operator) error {
			op.state = started
			op.reset.svc = firstService
			return startErr
		},
	}

	err := c.Start()
	require.ErrorIs(t, err, startErr)
	require.ErrorIs(t, err, closeErr)
	require.Len(t, c.pendingCleanup, 1)
	require.Equal(t, int32(1), firstService.closeCount.Load())

	startCalled := false
	c.startFn = func(op *operator) error {
		startCalled = true
		op.state = started
		op.reset.svc = secondService
		return nil
	}
	require.ErrorIs(t, c.Start(), closeErr)
	require.False(t, startCalled)
	require.Equal(t, int32(2), firstService.closeCount.Load())

	firstService.closeErr = nil
	c.startFn = func(op *operator) error {
		require.Equal(t, int32(3), firstService.closeCount.Load())
		op.state = started
		op.reset.svc = secondService
		return nil
	}
	require.NoError(t, c.Start())
	require.Empty(t, c.pendingCleanup)
	require.Equal(t, started, c.state)
	require.NoError(t, c.Close())
	require.Equal(t, int32(1), secondService.closeCount.Load())
}

func TestRollbackNewServicesRetriesCleanupBeforeRestart(t *testing.T) {
	closeErr := errors.New("close failed")
	newService := &closeTrackingService{closeErr: closeErr}
	existingService := &closeTrackingService{}
	existingOp := &operator{state: started}
	existingOp.reset.svc = existingService
	newOp := &operator{state: started, serviceType: metadata.ServiceType_CN}
	newOp.reset.svc = newService

	c := &cluster{
		state:    started,
		files:    []string{"existing.toml", "new.toml"},
		services: []*operator{existingOp, newOp},
	}
	c.options.cn = 2

	err := c.rollbackNewServicesLocked(1, 1)
	require.ErrorIs(t, err, closeErr)
	require.Len(t, c.pendingCleanup, 1)
	require.Len(t, c.services, 1)
	require.Equal(t, 1, c.options.cn)

	newService.closeErr = nil
	require.NoError(t, c.StartNewCNService(0))
	require.Empty(t, c.pendingCleanup)
	require.Equal(t, int32(2), newService.closeCount.Load())
	require.NoError(t, c.Close())
	require.Equal(t, int32(1), existingService.closeCount.Load())
}

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
	RunBaseClusterTests(t,
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
