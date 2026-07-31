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

package mongodb

import "github.com/matrixorigin/matrixone/pkg/catalog"

const ConnectionsDDL = `create table mo_catalog.mo_mongodb_connections (
	account_id int unsigned not null,
	connection_id bigint unsigned not null auto_increment,
	name varchar(300) not null,
	discovery_mode varchar(16) not null,
	hosts text,
	srv_host varchar(1024),
	replica_set varchar(256),
	auth_source varchar(256) not null default 'admin',
	auth_mechanism varchar(64) not null default 'SCRAM-SHA-256',
	credential_secret_ref text not null,
	tls_mode varchar(32) not null default 'required',
	tls_ca_secret_ref text,
	read_preference varchar(64) not null default 'secondaryPreferred',
	read_concern varchar(64) not null default 'majority',
	max_staleness_seconds bigint not null default 0,
	options_json json,
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	disabled_at timestamp default null,
	version bigint unsigned not null default 1,
	primary key(account_id, connection_id),
	unique key(account_id, name)
)`

const MappingsDDL = `create table mo_catalog.mo_mongodb_tables (
	account_id int unsigned not null,
	db_id bigint unsigned not null,
	table_id bigint unsigned not null,
	mapping_id bigint unsigned not null auto_increment,
	connection_id bigint unsigned not null,
	database_name varchar(1024) not null,
	collection_name varchar(1024) not null,
	schema_mode varchar(32) not null default 'explicit',
	conversion_mode varchar(32) not null default 'strict',
	split_key varchar(1024),
	max_parallelism int not null default 1,
	columns_json json not null,
	options_json json,
	created_at timestamp not null default utc_timestamp,
	updated_at timestamp not null default utc_timestamp,
	version bigint unsigned not null default 1,
	primary key(account_id, db_id, table_id),
	unique key(account_id, mapping_id)
)`

type SystemTableDDL struct {
	Schema string
	Name   string
	DDL    string
}

var SystemTableDDLs = []SystemTableDDL{
	{Schema: catalog.MO_CATALOG, Name: TableConnections, DDL: ConnectionsDDL},
	{Schema: catalog.MO_CATALOG, Name: TableMappings, DDL: MappingsDDL},
}
