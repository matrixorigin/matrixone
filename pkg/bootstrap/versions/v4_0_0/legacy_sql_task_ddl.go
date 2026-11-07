// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v4_0_0

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
)

// Keep the released 4.0.0 definitions independent of fresh-bootstrap DDL.
// Subsequent upgrade entries add the account indexes and ownership fields.
var originalSQLTaskDDL = fmt.Sprintf(`create table %s.%s (
                task_id bigint primary key auto_increment,
                task_name varchar(128) not null,
                account_id int unsigned not null,
                database_name varchar(128) not null default '',
                cron_expr varchar(128) not null default '',
                `+"`timezone`"+` varchar(64) not null default 'UTC',
                sql_body text not null,
                gate_condition text not null default '',
                retry_limit int not null default 0,
                timeout_seconds int not null default 0,
                enabled tinyint not null default 1,
                next_fire_time bigint not null default 0,
                trigger_count bigint not null default 0,
                creator varchar(128) not null default '',
                creator_user_id int unsigned not null default 0,
                creator_role_id int unsigned not null default 0,
                created_at timestamp not null default current_timestamp,
                updated_at timestamp not null default current_timestamp,
                unique key uk_task_name_account (task_name, account_id))`,
	catalog.MOTaskDB, catalog.MOSQLTask)

var originalSQLTaskRunDDL = fmt.Sprintf(`create table %s.%s (
                run_id bigint primary key auto_increment,
                task_id bigint not null,
                task_name varchar(128) not null,
                account_id int unsigned not null,
                scheduled_at timestamp null,
                started_at timestamp null,
                finished_at timestamp null,
                duration_seconds double not null default 0,
                status varchar(20) not null default 'RUNNING',
                trigger_type varchar(20) not null default 'SCHEDULED',
                attempt_number int not null default 1,
                rows_affected bigint not null default 0,
                error_code int not null default 0,
                error_message text,
                gate_result tinyint not null default 1,
                runner_cn varchar(128) not null default '',
                index idx_task_id (task_id),
                index idx_status (status),
                index idx_started_at (started_at))`,
	catalog.MOTaskDB, catalog.MOSQLTaskRun)
