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

package catalog

const MO_VIEW_RECOVERY = "mo_view_recovery"
const MO_VIEW_RECOVERY_WORK = "mo_view_recovery_work"

// Recovery is cluster control state, not tenant snapshot data. The singleton
// owns the worker fence, work quota and three fixed transactional outbox slots.
const MoViewRecoveryDDL = `create table mo_catalog.mo_view_recovery (
 id int unsigned not null primary key,
 revision bigint unsigned not null,
 mutation_revision bigint unsigned not null default 0,
 completion_fence bool not null default false,
 state text not null,
 lease_expires_at timestamp null
)`

const MoViewRecoveryInitSQL = `insert into mo_catalog.mo_view_recovery
 (id,revision,state) select 1,0,'{"version":1}' where not exists
 (select 1 from mo_catalog.mo_view_recovery where id=1)`

// Completed nodes remain as the generation's visited set until bounded cleanup.
// Neither a cycle nor a diamond in the dependency graph creates duplicate work.
const MoViewRecoveryWorkDDL = `create table mo_catalog.mo_view_recovery_work (
 generation bigint unsigned not null,
 kind varchar(16) not null,
 account_id int unsigned not null,
 relation_id bigint unsigned not null,
 database_id bigint unsigned not null default 0,
 logical_id bigint unsigned not null default 0,
 database_name varchar(5000) not null default '',
 relation_name varchar(5000) not null default '',
 cursor_account bigint unsigned not null default 0,
 cursor_relation bigint unsigned not null default 0,
 visits bigint unsigned not null default 0,
 done bool not null default false,
 primary key(generation,kind,account_id,relation_id),
 index idx_view_recovery_work_pending(generation,done,visits)
)`
