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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

var (
	_ Bootstrapper = (*bootstrapper)(nil)
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
			is_visible  tinyint not null,
			hidden      tinyint not null,
			comment 	varchar(2048) not null,
			column_name    varchar(256) not null,
			ordinal_position  int unsigned  not null,
			options     text,
			index_table_name varchar(5000),
			primary key(id, column_name)
		);`, catalog.MO_CATALOG, catalog.MO_INDEXES),

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
			task_id                     int primary key auto_increment,
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
			cron_task_id				int primary key auto_increment,
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
	}
)

type bootstrapper struct {
	lock   Locker
	clock  clock.Clock
	client client.TxnClient
	exec   executor.SQLExecutor
}

// NewBootstrapper create bootstrapper to bootstrap mo database
func NewBootstrapper(
	lock Locker,
	clock clock.Clock,
	client client.TxnClient,
	exec executor.SQLExecutor) Bootstrapper {
	return &bootstrapper{clock: clock, exec: exec, lock: lock, client: client}
}

func (b *bootstrapper) Bootstrap(ctx context.Context) error {
	getLogger().Info("start to check bootstrap state")

	ok, err := b.checkAlreadyBootstrapped(ctx)
	if ok {
		getLogger().Info("mo already boostrapped")
		return nil
	}
	if err != nil {
		return err
	}

	ok, err = b.lock.Get(ctx)
	if err != nil {
		return err
	}

	// current node get the bootstrap privilege
	if ok {
		getLogger().Info("start to bootstrap mo in 2 steps")

		opts := executor.Options{}
		err := b.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				for _, sql := range step1InitSQLs {
					res, err := te.Exec(sql)
					if err != nil {
						return err
					}
					res.Close()
				}
				return nil
			},
			opts)
		if err != nil {
			return err
		}

		getLogger().Info("bootstrap mo step 1 completed")

		now, _ := b.clock.Now()
		// make sure txn start at now, and make sure can see the data of step1
		opts = opts.WithMinCommittedTS(now)
		err = b.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				for _, sql := range step2InitSQLs {
					res, err := te.Exec(sql)
					if err != nil {
						return err
					}
					res.Close()
				}
				return nil
			},
			opts)
		if err != nil {
			getLogger().Error("bootstrap mo step 2 failed",
				zap.Error(err))
			return err
		}

		getLogger().Info("bootstrap mo step 2 completed")

		if b.client != nil {
			getLogger().Info("wait bootstrap logtail applied")

			// if we bootstrapped, in current cn, we must wait logtails to be applied. All subsequence operations need to see the
			// bootstrap data.
			now, _ = b.clock.Now()
			b.client.(client.TxnClientWithCtl).SetLatestCommitTS(now)
		}

		getLogger().Info("successfully completed bootstrap")
		return nil
	}

	// otherwrise, wait bootstrap completed
	getLogger().Info("another cn is in bootstrapping, waiting")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if ok, err := b.checkAlreadyBootstrapped(ctx); ok || err != nil {
				getLogger().Info("waiting bootstrap completed",
					zap.Bool("result", ok),
					zap.Error(err))
				return err
			}
			time.Sleep(time.Second)
		}
	}
}

func (b *bootstrapper) checkAlreadyBootstrapped(ctx context.Context) (bool, error) {
	now, _ := b.clock.Now()
	opts := executor.Options{}
	res, err := b.exec.Exec(
		ctx,
		"show databases",
		opts.WithMinCommittedTS(now))
	if err != nil {
		return false, err
	}
	defer res.Close()

	var dbs []string
	res.ReadRows(func(cols []*vector.Vector) bool {
		dbs = append(dbs, executor.GetStringRows(cols[0])...)
		return true
	})
	for _, db := range dbs {
		if strings.EqualFold(db, bootstrappedCheckerDB) {
			go func() {
				for {
					if b.showTables() {
						return
					}
					time.Sleep(time.Second * 10)
				}
			}()
			return true, nil
		}
	}
	return false, nil
}

func (b *bootstrapper) showTables() bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	now, _ := b.clock.Now()
	opts := executor.Options{}
	res, err := b.exec.Exec(
		ctx,
		"select relname from "+catalog.MO_TABLES,
		opts.WithMinCommittedTS(now).WithDatabase(catalog.MO_CATALOG))
	if err != nil {
		return false
	}
	defer res.Close()

	var tables []string
	res.ReadRows(func(cols []*vector.Vector) bool {
		tables = append(tables, executor.GetStringRows(cols[0])...)
		return true
	})
	find := false
	for _, t := range tables {
		if strings.EqualFold(t, catalog.MOAutoIncrTable) {
			find = true
		}
	}
	if !find {
		getLogger().Fatal("table in mo_tables, BUGBUGBUGBUGBUGBUGBUGBUGBUGBUGBUG", zap.Any("tables", tables))
	} else {
		getLogger().Fatal("table in mo_tables", zap.Any("tables", tables))
	}
	return find
}
