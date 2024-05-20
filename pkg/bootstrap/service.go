// Copyright 2023 Matrix Origin
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

package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

var (
	_ Service = (*service)(nil)
)

var (
	bootstrapKey = "_mo_bootstrap"
)

var (
	bootstrappedCheckerDB = catalog.MOTaskDB
	// Note: The following tables belong to data dictionary table, and system tables's creation will depend on
	// the following system tables. Therefore, when creating tenants, they must be created first
	step1InitSQLs = []string{
		frontend.MoCatalogMoIndexesDDL,
		frontend.MoCatalogMoTablePartitionsDDL,
		frontend.MoCatalogMoAutoIncrTableDDL,
		frontend.MoCatalogMoForeignKeysDDL,
	}

	step2InitSQLs = []string{
		fmt.Sprintf(`create database %s`, catalog.MOTaskDB),
		frontend.MoTaskSysAsyncTaskDDL,
		frontend.MoTaskSysCronTaskDDL,
		frontend.MoTaskSysDaemonTaskDDL,
		fmt.Sprintf(`create index idx_task_status on %s.sys_async_task(task_status)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create index idx_task_runner on %s.sys_async_task(task_runner)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create index idx_task_executor on %s.sys_async_task(task_metadata_executor)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create index idx_task_epoch on %s.sys_async_task(task_epoch)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create index idx_account_id on %s.sys_daemon_task(account_id)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create index idx_last_heartbeat on %s.sys_daemon_task(last_heartbeat)`,
			catalog.MOTaskDB),
	}

	step3InitSQLs = []string{
		frontend.MoCatalogMoVersionDDL,
		frontend.MoCatalogMoUpgradeDDL,
		frontend.MoCatalogMoUpgradeTenantDDL,
	}

	initMoVersionFormat = `insert into %s.%s values ('%s', %d, %d, current_timestamp(), current_timestamp())`

	initSQLs []string
)

func init() {
	initSQLs = append(initSQLs, step1InitSQLs...)
	initSQLs = append(initSQLs, step2InitSQLs...)
	initSQLs = append(initSQLs, step3InitSQLs...)

	// generate system cron tasks sql
	sql, err := predefine.GenInitCronTaskSQL()
	if err != nil {
		panic(err)
	}
	initSQLs = append(initSQLs, sql)

	initSQLs = append(initSQLs, trace.InitSQLs...)
}

type service struct {
	lock    Locker
	clock   clock.Clock
	client  client.TxnClient
	exec    executor.SQLExecutor
	stopper *stopper.Stopper
	handles []VersionHandle

	mu struct {
		sync.RWMutex
		tenants map[int32]bool
	}

	upgrade struct {
		upgradeTenantBatch         int
		checkUpgradeDuration       time.Duration
		checkUpgradeTenantDuration time.Duration
		upgradeTenantTasks         int
		finalVersionCompleted      atomic.Bool
	}
}

// NewService create service to bootstrap mo database
func NewService(
	lock Locker,
	clock clock.Clock,
	client client.TxnClient,
	exec executor.SQLExecutor,
	opts ...Option) Service {
	s := &service{
		clock:   clock,
		exec:    exec,
		lock:    lock,
		client:  client,
		stopper: stopper.NewStopper("upgrade", stopper.WithLogger(getLogger().RawLogger())),
	}
	s.mu.tenants = make(map[int32]bool)
	s.initUpgrade()

	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *service) Bootstrap(ctx context.Context) error {
	getLogger().Info("start to check bootstrap state")

	if ok, err := s.checkAlreadyBootstrapped(ctx); ok {
		getLogger().Info("mo already bootstrapped")
		return nil
	} else if err != nil {
		return err
	}

	ok, err := s.lock.Get(ctx, bootstrapKey)
	if err != nil {
		return err
	}

	// current node get the bootstrap privilege
	if ok {
		// the auto-increment service has already been initialized at current time
		return s.execBootstrap(ctx)
	}

	// otherwise, wait bootstrap completed
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
		if ok, err := s.checkAlreadyBootstrapped(ctx); ok || err != nil {
			getLogger().Info("waiting bootstrap completed",
				zap.Bool("result", ok),
				zap.Error(err))
			return err
		}
	}
}

func (s *service) checkAlreadyBootstrapped(ctx context.Context) (bool, error) {
	res, err := s.exec.Exec(ctx, "show databases", executor.Options{}.WithMinCommittedTS(s.now()))
	if err != nil {
		return false, err
	}
	defer res.Close()

	var dbs []string
	res.ReadRows(func(_ int, cols []*vector.Vector) bool {
		dbs = append(dbs, executor.GetStringRows(cols[0])...)
		return true
	})
	for _, db := range dbs {
		if strings.EqualFold(db, bootstrappedCheckerDB) {
			return true, nil
		}
	}
	return false, nil
}

func (s *service) execBootstrap(ctx context.Context) error {
	opts := executor.Options{}.
		WithMinCommittedTS(s.now()).
		WithDisableTrace().
		WithWaitCommittedLogApplied().
		WithTimeZone(time.Local).
		WithAccountID(catalog.System_Account)

	err := s.exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		if err := initPreprocessSQL(ctx, txn, s.GetFinalVersion(), s.GetFinalVersionOffset()); err != nil {
			return err
		}
		if err := frontend.InitSysTenant(ctx, txn, s.GetFinalVersion()); err != nil {
			return err
		}
		if err := sysview.InitSchema(ctx, txn); err != nil {
			return err
		}
		if err := mometric.InitSchema(ctx, txn); err != nil {
			return err
		}
		if err := motrace.InitSchemaWithTxn(ctx, txn); err != nil {
			return err
		}
		return nil
	}, opts)

	if err != nil {
		getLogger().Error("bootstrap system init failed", zap.Error(err))
		return err
	}
	getLogger().Info("bootstrap system init completed")

	if s.client != nil {
		getLogger().Info("wait bootstrap logtail applied")

		// if we bootstrapped, in current cn, we must wait logtails to be applied. All subsequence operations need to see the
		// bootstrap data.
		s.client.SyncLatestCommitTS(s.now())
	}

	getLogger().Info("successfully completed bootstrap")
	return nil
}

func (s *service) now() timestamp.Timestamp {
	n, _ := s.clock.Now()
	return n
}

func (s *service) Close() error {
	s.stopper.Stop()
	return nil
}

// initPreprocessSQL  Execute preprocessed SQL, which typically must be completed before system tenant initialization
func initPreprocessSQL(ctx context.Context, txn executor.TxnExecutor, finalVersion string, finalVersonOffset int32) error {
	var timeCost time.Duration
	defer func() {
		logutil.Debugf("Initialize system pre SQL: create cost %d ms", timeCost.Milliseconds())
	}()

	begin := time.Now()
	var initMoVersion string
	for _, sql := range initSQLs {
		if _, err := txn.Exec(sql, executor.StatementOption{}); err != nil {
			return err
		}
	}

	initMoVersion = fmt.Sprintf(initMoVersionFormat, catalog.MO_CATALOG, catalog.MOVersionTable, finalVersion, finalVersonOffset, versions.StateReady)
	if _, err := txn.Exec(initMoVersion, executor.StatementOption{}); err != nil {
		return err
	}

	timeCost = time.Since(begin)
	return nil
}
