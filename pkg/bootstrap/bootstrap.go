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
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var (
	_ Bootstrapper = (*bootstrapper)(nil)
)

var (
	bootstrappedCheckerDB = catalog.MOTaskDB
	initSQLs              = []string{
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

		fmt.Sprintf("create table `%s`.`%s`(name varchar(770) primary key, offset bigint unsigned, step bigint unsigned);",
			catalog.MO_CATALOG,
			catalog.AutoIncrTableName),

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
	lock  Locker
	clock clock.Clock
	exec  executor.SQLExecutor
}

// NewBootstrapper create bootstrapper to bootstrap mo database
func NewBootstrapper(
	lock Locker,
	clock clock.Clock,
	exec executor.SQLExecutor) Bootstrapper {
	return &bootstrapper{clock: clock, exec: exec, lock: lock}
}

func (b *bootstrapper) Bootstrap(ctx context.Context) error {
	if ok, err := b.checkAlreadyBootstrapped(ctx); ok || err != nil {
		return err
	}

	ok, err := b.lock.Get(ctx)
	if err != nil {
		return err
	}

	// current node get the bootstrap privilege
	if ok {
		opts := executor.Options{}
		return b.exec.ExecTxn(
			ctx,
			func(te executor.TxnExecutor) error {
				for _, sql := range initSQLs {
					res, err := te.Exec(sql)
					if err != nil {
						return err
					}
					res.Close()
				}
				return nil
			},
			opts)
	}

	// otherwrise, wait bootstrap completed
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			if ok, err := b.checkAlreadyBootstrapped(ctx); ok || err != nil {
				return err
			}
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
			return true, nil
		}
	}
	return false, nil
}
