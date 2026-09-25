// Copyright 2026 Matrix Origin
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

package catalog

const MODatabaseDefaults = "mo_database_defaults"

// Database identity, rather than its reusable name, owns this row. This is a
// tenant catalog table, also qualified by account_id for restore and visibility.
const MoDatabaseDefaultsDDL = `create table mo_catalog.mo_database_defaults (
    account_id int unsigned not null,
    database_id bigint unsigned not null,
    character_set varchar(64) not null,
    collation_name varchar(64) not null,
    version bigint unsigned not null,
    primary key(account_id, database_id)
)`
