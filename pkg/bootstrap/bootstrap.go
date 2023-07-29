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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

		fmt.Sprintf(`insert into %s.sys_async_task(
                              task_metadata_id,
                              task_metadata_executor,
                              task_metadata_context,
		                      task_metadata_option,
		                      task_parent_id,
		                      task_status,
		                      task_runner,
		                      task_epoch,
		                      last_heartbeat,
		                      create_at,
		                      end_at) values ("SystemInit", 1, "", "{}", 0, 0, 0, 0, 0, %d, 0)`,
			catalog.MOTaskDB, time.Now().UnixNano()),
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

		opts := executor.Options{}
		if err := b.exec.ExecTxn(ctx, execFunc(step1InitSQLs), opts); err != nil {
			return err
		}

		getLogger().Info("bootstrap mo step 1 completed")

		// make sure txn start at now, and make sure can see the data of step1
		opts = opts.WithMinCommittedTS(b.now()).WithDatabase(catalog.MOTaskDB).WithWaitCommittedLogApplied()
		if err := b.exec.ExecTxn(ctx, execFunc(step2InitSQLs), opts); err != nil {
			getLogger().Error("bootstrap mo step 2 failed",
				zap.Error(err))
			return err
		}
		getLogger().Info("bootstrap mo step 2 completed")

		if b.client != nil {
			getLogger().Info("wait bootstrap logtail applied")

			// if we bootstrapped, in current cn, we must wait logtails to be applied. All subsequence operations need to see the
			// bootstrap data.
			b.client.(client.TxnClientWithCtl).SetLatestCommitTS(b.now())
		}

		getLogger().Info("successfully completed bootstrap")
		return nil
	}

	// otherwrise, wait bootstrap completed
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			if ok, err := b.checkAlreadyBootstrapped(ctx); ok || err != nil {
				getLogger().Info("waiting bootstrap completed",
					zap.Bool("result", ok),
					zap.Error(err))
				return err
			}
		}
	}
}

func (b *bootstrapper) checkAlreadyBootstrapped(ctx context.Context) (bool, error) {
	opts := executor.Options{}
	res, err := b.exec.Exec(
		ctx,
		"show databases",
		opts.WithMinCommittedTS(b.now()))
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
			return true, nil
		}
	}
	return false, nil
}

func (b *bootstrapper) now() timestamp.Timestamp {
	n, _ := b.clock.Now()
	return n
}

func execFunc(sql []string) func(executor.TxnExecutor) error {
	return func(e executor.TxnExecutor) error {
		for _, s := range sql {
			r, err := e.Exec(s)
			if err != nil {
				return err
			}
			r.Close()
		}
		return nil
	}
}
