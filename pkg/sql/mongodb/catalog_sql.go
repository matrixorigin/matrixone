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

import (
	"encoding/json"
	"fmt"
	"strings"
)

func quoteSQLString(value string) string {
	// BackgroundExec parses these generated statements with backslash escapes
	// enabled. Escape backslashes before quote doubling so a user-controlled
	// backslash cannot consume either quote and terminate the SQL literal.
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}

func nullOrSQLString(value string) string {
	if strings.TrimSpace(value) == "" {
		return "null"
	}
	return quoteSQLString(value)
}

func GetConnectionByNameSQL(accountID uint32, name string) string {
	return fmt.Sprintf(
		"select account_id,connection_id,name,discovery_mode,hosts,srv_host,replica_set,auth_source,auth_mechanism,credential_secret_ref,tls_mode,tls_ca_secret_ref,read_preference,read_concern,max_staleness_seconds,coalesce(cast(options_json as char),''),version,cast(disabled_at is not null as unsigned) from mo_catalog.%s where account_id = %d and name = %s",
		TableConnections, accountID, quoteSQLString(name),
	)
}

func GetConnectionByNameForUpdateSQL(accountID uint32, name string) string {
	return GetConnectionByNameSQL(accountID, name) + " for update"
}

func GetConnectionByIDSQL(accountID uint32, connectionID, version uint64) string {
	return fmt.Sprintf(
		"select account_id,connection_id,name,discovery_mode,hosts,srv_host,replica_set,auth_source,auth_mechanism,credential_secret_ref,tls_mode,tls_ca_secret_ref,read_preference,read_concern,max_staleness_seconds,coalesce(cast(options_json as char),''),version,cast(disabled_at is not null as unsigned) from mo_catalog.%s where account_id = %d and connection_id = %d and version = %d",
		TableConnections, accountID, connectionID, version,
	)
}

func GetMappingByTableIDSQL(accountID uint32, tableID uint64) string {
	return fmt.Sprintf(
		"select m.mapping_id,m.connection_id,c.version,m.database_name,m.collection_name,cast(m.columns_json as char),m.max_parallelism,cast(c.disabled_at is not null as unsigned),m.version from mo_catalog.%s m join mo_catalog.%s c on c.account_id = m.account_id and c.connection_id = m.connection_id where m.account_id = %d and m.table_id = %d",
		TableMappings, TableConnections, accountID, tableID,
	)
}

func GetMappingByIDSQL(accountID uint32, mappingID, version uint64) string {
	return fmt.Sprintf(
		"select account_id,db_id,table_id,mapping_id,connection_id,database_name,collection_name,schema_mode,conversion_mode,coalesce(split_key,''),max_parallelism,cast(columns_json as char),coalesce(cast(options_json as char),''),version from mo_catalog.%s where account_id = %d and mapping_id = %d and version = %d",
		TableMappings, accountID, mappingID, version,
	)
}

func InsertTableMappingSQL(mapping TableMapping) string {
	columns, _ := json.Marshal(mapping.Columns)
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,db_id,table_id,connection_id,database_name,collection_name,schema_mode,conversion_mode,split_key,max_parallelism,columns_json,options_json,version) values (%d,%d,%d,%d,%s,%s,%s,%s,%s,%d,%s,%s,%d)",
		TableMappings,
		mapping.AccountID, mapping.DatabaseID, mapping.TableID, mapping.ConnectionID,
		quoteSQLString(mapping.Database), quoteSQLString(mapping.Collection),
		quoteSQLString(defaultString(mapping.SchemaMode, SchemaExplicit)),
		quoteSQLString(defaultString(mapping.Conversion, ConversionStrict)),
		nullOrSQLString(mapping.SplitKey), defaultParallelism(mapping.MaxParallelism),
		quoteSQLString(string(columns)), nullOrSQLString(mapping.OptionsJSON),
		defaultVersion(mapping.Version),
	)
}

func DeleteTableMappingSQL(accountID uint32, databaseID, tableID uint64) string {
	return fmt.Sprintf(
		"delete from mo_catalog.%s where account_id = %d and db_id = %d and table_id = %d",
		TableMappings, accountID, databaseID, tableID,
	)
}

func InsertConnectionSQL(connection Connection) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,name,discovery_mode,hosts,srv_host,replica_set,auth_source,auth_mechanism,credential_secret_ref,tls_mode,tls_ca_secret_ref,read_preference,read_concern,max_staleness_seconds,options_json,version) values (%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d,%s,%d)",
		TableConnections, connection.AccountID, quoteSQLString(connection.Name),
		quoteSQLString(connection.DiscoveryMode), nullOrSQLString(connection.Hosts), nullOrSQLString(connection.SRVHost),
		nullOrSQLString(connection.ReplicaSet), quoteSQLString(defaultString(connection.AuthSource, "admin")),
		quoteSQLString(defaultString(connection.AuthMechanism, "SCRAM-SHA-256")), quoteSQLString(connection.CredentialSecretRef),
		quoteSQLString(defaultString(connection.TLSMode, "required")), nullOrSQLString(connection.TLSCASecretRef),
		quoteSQLString(defaultString(connection.ReadPreference, "secondaryPreferred")), quoteSQLString(defaultString(connection.ReadConcern, "majority")),
		connection.MaxStalenessSeconds, nullOrSQLString(connection.OptionsJSON), defaultVersion(connection.Version),
	)
}

func UpdateConnectionSQL(connection Connection) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set discovery_mode=%s,hosts=%s,srv_host=%s,replica_set=%s,auth_source=%s,auth_mechanism=%s,credential_secret_ref=%s,tls_mode=%s,tls_ca_secret_ref=%s,read_preference=%s,read_concern=%s,max_staleness_seconds=%d,options_json=%s,updated_at=utc_timestamp,version=version+1 where account_id=%d and name=%s",
		TableConnections, quoteSQLString(connection.DiscoveryMode), nullOrSQLString(connection.Hosts), nullOrSQLString(connection.SRVHost), nullOrSQLString(connection.ReplicaSet),
		quoteSQLString(defaultString(connection.AuthSource, "admin")), quoteSQLString(defaultString(connection.AuthMechanism, "SCRAM-SHA-256")),
		quoteSQLString(connection.CredentialSecretRef), quoteSQLString(defaultString(connection.TLSMode, "required")), nullOrSQLString(connection.TLSCASecretRef),
		quoteSQLString(defaultString(connection.ReadPreference, "secondaryPreferred")), quoteSQLString(defaultString(connection.ReadConcern, "majority")),
		connection.MaxStalenessSeconds, nullOrSQLString(connection.OptionsJSON), connection.AccountID, quoteSQLString(connection.Name),
	)
}

func DisableConnectionSQL(accountID uint32, name string) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set disabled_at=utc_timestamp,updated_at=utc_timestamp,version=version+1 where account_id=%d and name=%s",
		TableConnections, accountID, quoteSQLString(name),
	)
}

func EnableConnectionSQL(accountID uint32, name string) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set disabled_at=null,updated_at=utc_timestamp,version=version+1 where account_id=%d and name=%s",
		TableConnections, accountID, quoteSQLString(name),
	)
}

func DeleteConnectionSQL(accountID uint32, connectionID uint64) string {
	return fmt.Sprintf("delete from mo_catalog.%s where account_id=%d and connection_id=%d", TableConnections, accountID, connectionID)
}

func ConnectionDependencyCountSQL(accountID uint32, connectionID uint64) string {
	return fmt.Sprintf("select count(*) from mo_catalog.%s where account_id=%d and connection_id=%d", TableMappings, accountID, connectionID)
}

func defaultVersion(version uint64) uint64 {
	if version == 0 {
		return 1
	}
	return version
}
