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
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/predefine"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	//"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	_ Service = (*service)(nil)
)

var (
	bootstrapKey = "_mo_bootstrap"
)

var (
	bootstrappedCheckerDB = catalog.MOTaskDB
	step1InitSQLs         = []string{
		fmt.Sprintf(`create table %s.%s(
			id 			bigint unsigned not null,
			table_id 	bigint unsigned not null,
			database_id bigint unsigned not null,
			name 		varchar(64) not null,
			type        varchar(11) not null,
    		algo	varchar(11),
    		algo_table_type varchar(11),
			algo_params varchar(2048),
			is_visible  tinyint not null,
			hidden      tinyint not null,
			comment 	varchar(2048) not null,
			column_name    varchar(256) not null,
			ordinal_position  int unsigned  not null,
			options     text,
			index_table_name varchar(5000),
			primary key(id, column_name)
		);`, catalog.MO_CATALOG, catalog.MO_INDEXES),

		fmt.Sprintf(`CREATE TABLE %s.%s (
			  table_id bigint unsigned NOT NULL,
			  database_id bigint unsigned not null,
			  number smallint unsigned NOT NULL,
			  name varchar(64) NOT NULL,
    		  partition_type varchar(50) NOT NULL,
              partition_expression varchar(2048) NULL,
			  description_utf8 text,
			  comment varchar(2048) NOT NULL,
			  options text,
			  partition_table_name varchar(1024) NOT NULL,
    		  PRIMARY KEY table_id (table_id, name)
			);`, catalog.MO_CATALOG, catalog.MO_TABLE_PARTITIONS),

		fmt.Sprintf(`create table %s.%s (
			table_id   bigint unsigned, 
			col_name   varchar(770), 
			col_index  int,
			offset     bigint unsigned, 
			step       bigint unsigned,  
			primary key(table_id, col_name)
		);`, catalog.MO_CATALOG, catalog.MOAutoIncrTable),
	}

	step2InitSQLs = []string{
		fmt.Sprintf(`create database %s`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create table %s.sys_async_task (
			task_id                     bigint primary key auto_increment,
			task_metadata_id            varchar(50) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			task_parent_id              varchar(50),
			task_status                 int,
			task_runner                 varchar(50),
			task_epoch                  int,
			last_heartbeat              bigint,
			result_code                 int null,
			error_msg                   varchar(1000) null,
			create_at                   bigint,
			end_at                      bigint)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create table %s.sys_cron_task (
			cron_task_id				bigint primary key auto_increment,
    		task_metadata_id            varchar(50) unique not null,
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option 		varchar(1000),
			cron_expr					varchar(100) not null,
			next_time					bigint,
			trigger_times				int,
			create_at					bigint,
			update_at					bigint)`,
			catalog.MOTaskDB),

		fmt.Sprintf(`create table %s.sys_daemon_task (
			task_id                     bigint primary key auto_increment,
			task_metadata_id            varchar(50),
			task_metadata_executor      int,
			task_metadata_context       blob,
			task_metadata_option        varchar(1000),
			account_id                  int unsigned not null,
			account                     varchar(128) not null,
			task_type                   varchar(64) not null,
			task_runner                 varchar(64),
			task_status                 int not null,
			last_heartbeat              timestamp,
			create_at                   timestamp not null,
			update_at                   timestamp not null,
			end_at                      timestamp,
			last_run                    timestamp,
			details                     blob)`,
			catalog.MOTaskDB),
	}

	initSQLs []string
)

func init() {
	initSQLs = append(initSQLs, step1InitSQLs...)
	initSQLs = append(initSQLs, step2InitSQLs...)

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
		if err := initPreprocessSQL(ctx, txn); err != nil {
			return err
		}
		if err := frontend.InitSysTenant2(ctx, txn, s.GetFinalVersion()); err != nil {
			return err
		}
		if err := sysview.InitSchema(ctx, txn); err != nil {
			return err
		}
		if err := mometric.InitSchema(ctx, txn); err != nil {
			return err
		}
		if err := motrace.InitSchema2(ctx, txn); err != nil {
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
func initPreprocessSQL(ctx context.Context, txn executor.TxnExecutor) error {
	var timeCost time.Duration
	defer func() {
		logutil.Debugf("Initialize system pre SQL: create cost %d ms", timeCost.Milliseconds())
	}()

	begin := time.Now()

	for _, sql := range initSQLs {
		if _, err := txn.Exec(sql, executor.StatementOption{}); err != nil {
			return err
		}
	}

	timeCost = time.Since(begin)
	return nil
}
