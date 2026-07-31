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

package frontend

import (
	"context"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func handleCreateMongoDBConnection(ctx context.Context, ses *Session, stmt *tree.CreateMongoDBConnection) (err error) {
	if err := ensureMongoDBFeatureEnabledForSession(ctx, ses); err != nil {
		return err
	}
	options, err := mongoDBOptionsToMap(ctx, stmt.Options)
	if err != nil {
		return err
	}
	connection, err := mongoDBConnectionFromOptions(ctx, ses.GetAccountId(), string(stmt.Name), options, nil, mongoDBParametersForService(ses.GetService()))
	if err != nil {
		return err
	}
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() { err = finishTxn(ctx, bh, err) }()
	existing, found, err := queryMongoDBConnectionByNameForUpdate(ctx, bh, connection.AccountID, connection.Name)
	if err != nil {
		return err
	}
	if found {
		if stmt.IfNotExists {
			return nil
		}
		return moerr.NewInvalidInputf(ctx, "MongoDB connection %s already exists", existing.Name)
	}
	return bh.Exec(ctx, mongodb.InsertConnectionSQL(connection))
}

func handleAlterMongoDBConnection(ctx context.Context, ses *Session, stmt *tree.AlterMongoDBConnection) (err error) {
	if err := ensureMongoDBFeatureEnabledForSession(ctx, ses); err != nil {
		return err
	}
	var options map[string]string
	if stmt.Action == tree.AlterMongoDBConnectionSet {
		options, err = mongoDBOptionsToMap(ctx, stmt.Options)
		if err != nil {
			return err
		}
		if len(options) == 0 {
			return moerr.NewInvalidInput(ctx, "ALTER MONGODB CONNECTION SET requires at least one option")
		}
	} else if len(stmt.Options) != 0 {
		return moerr.NewInvalidInput(ctx, "ALTER MONGODB CONNECTION ENABLE/DISABLE does not accept options")
	}
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	var retireConnectionID, retireBeforeVersion uint64
	defer func() {
		err = finishTxn(ctx, bh, err)
		if err == nil && retireConnectionID != 0 {
			retireMongoDBClients(ctx, ses.GetService(), mongodb.ClientRetirement{
				AccountID: ses.GetAccountId(), ConnectionID: retireConnectionID,
				VersionExclusive: retireBeforeVersion,
			})
		}
	}()
	existing, found, err := queryMongoDBConnectionByNameForUpdate(ctx, bh, ses.GetAccountId(), string(stmt.Name))
	if err != nil {
		return err
	}
	if !found {
		return moerr.NewInvalidInputf(ctx, "MongoDB connection %s does not exist", string(stmt.Name))
	}
	switch stmt.Action {
	case tree.AlterMongoDBConnectionDisable:
		if existing.Disabled {
			return nil
		}
		err = bh.Exec(ctx, mongodb.DisableConnectionSQL(existing.AccountID, existing.Name))
	case tree.AlterMongoDBConnectionEnable:
		if !existing.Disabled {
			return nil
		}
		if err := mongodb.ValidateConnection(ctx, existing, mongoDBRuntimeConfig(mongoDBParametersForService(ses.GetService()))); err != nil {
			return err
		}
		err = bh.Exec(ctx, mongodb.EnableConnectionSQL(existing.AccountID, existing.Name))
	case tree.AlterMongoDBConnectionSet:
		var updated mongodb.Connection
		updated, err = mongoDBConnectionFromOptions(ctx, ses.GetAccountId(), string(stmt.Name), options, &existing, mongoDBParametersForService(ses.GetService()))
		if err == nil {
			err = bh.Exec(ctx, mongodb.UpdateConnectionSQL(updated))
		}
	default:
		return moerr.NewInvalidInput(ctx, "ALTER MONGODB CONNECTION has an invalid action")
	}
	if err == nil {
		retireConnectionID = existing.ConnectionID
		retireBeforeVersion = existing.Version + 1
	}
	return err
}

func handleDropMongoDBConnection(ctx context.Context, ses *Session, stmt *tree.DropMongoDBConnection) (err error) {
	if err := ensureMongoDBFeatureEnabledForSession(ctx, ses); err != nil {
		return err
	}
	accountID := ses.GetAccountId()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	var retiredConnectionID uint64
	defer func() {
		err = finishTxn(ctx, bh, err)
		if err == nil && retiredConnectionID != 0 {
			retireMongoDBClients(ctx, ses.GetService(), mongodb.ClientRetirement{
				AccountID: accountID, ConnectionID: retiredConnectionID,
			})
		}
	}()
	connection, found, err := queryMongoDBConnectionByNameForUpdate(ctx, bh, accountID, string(stmt.Name))
	if err != nil {
		return err
	}
	if !found {
		if stmt.IfExists {
			return nil
		}
		return moerr.NewInvalidInputf(ctx, "MongoDB connection %s does not exist", string(stmt.Name))
	}
	results, err := ExeSqlInBgSes(ctx, bh, mongodb.ConnectionDependencyCountSQL(accountID, connection.ConnectionID))
	if err != nil {
		return err
	}
	if execResultArrayHasData(results) {
		count, countErr := results[0].GetUint64(ctx, 0, 0)
		if countErr != nil {
			return countErr
		}
		if count != 0 {
			return moerr.NewInvalidInputf(ctx, "MongoDB connection %s is referenced by %d external table mappings", connection.Name, count)
		}
	}
	err = bh.Exec(ctx, mongodb.DeleteConnectionSQL(accountID, connection.ConnectionID))
	if err == nil {
		retiredConnectionID = connection.ConnectionID
	}
	return err
}

func retireMongoDBClients(ctx context.Context, service string, retirement mongodb.ClientRetirement) {
	if dependencies := mongoDBRuntimeDependencies(service); dependencies != nil && dependencies.Pool != nil {
		_ = retirement.Apply(dependencies.Pool)
	}
	pu := getPuIfPresent(service)
	if pu == nil || pu.QueryClient == nil {
		return
	}
	value, ok := moruntime.ServiceRuntime(pu.QueryClient.ServiceID()).GetGlobalVariables(moruntime.ClusterService)
	if !ok {
		return
	}
	cluster, ok := value.(clusterservice.MOCluster)
	if !ok || cluster == nil {
		return
	}
	mongodb.ClusterRemoteClientRetirer{
		Cluster: cluster, QueryClient: pu.QueryClient,
	}.Retire(ctx, retirement)
}

func finishTxnAndRetireMongoDBAccounts(
	ctx context.Context,
	bh BackgroundExec,
	service string,
	accountIDs []uint32,
	err error,
) error {
	err = finishTxn(ctx, bh, err)
	if err != nil {
		return err
	}
	for _, accountID := range accountIDs {
		retireMongoDBClients(ctx, service, mongodb.ClientRetirement{AccountID: accountID})
	}
	return nil
}

func markMongoDBAccountForRetirement(accountIDs *[]uint32, accountID uint32) {
	if accountIDs == nil {
		return
	}
	for _, existing := range *accountIDs {
		if existing == accountID {
			return
		}
	}
	*accountIDs = append(*accountIDs, accountID)
}

func mongoDBRuntimeDependencies(service string) *mongodb.RuntimeDependencies {
	for _, serviceID := range []string{service, ""} {
		runtime := moruntime.ServiceRuntime(serviceID)
		if runtime == nil {
			continue
		}
		value, ok := runtime.GetGlobalVariables(mongodb.RuntimeDependenciesKey)
		if !ok {
			continue
		}
		if dependencies, typeOK := value.(*mongodb.RuntimeDependencies); typeOK {
			return dependencies
		}
	}
	return nil
}

func handleShowMongoDBConnections(ctx context.Context, ses *Session, stmt *tree.ShowMongoDBConnections) error {
	if stmt != nil && (stmt.Like != nil || stmt.Where != nil) {
		return moerr.NewNotSupported(ctx, "SHOW MONGODB CONNECTIONS with LIKE/WHERE")
	}
	if err := ensureMongoDBFeatureEnabledForSession(ctx, ses); err != nil {
		return err
	}
	sql := "select name,discovery_mode,auth_mechanism,tls_mode,read_preference,read_concern,version,cast(disabled_at is not null as unsigned) " +
		"from mo_catalog." + mongodb.TableConnections + " where account_id=" + strconv.FormatUint(uint64(ses.GetAccountId()), 10) + " order by name"
	return showIcebergQuery(ctx, ses, sql, []icebergShowColumn{
		{name: "name", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "discovery_mode", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "auth_mechanism", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "tls_mode", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "read_preference", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "read_concern", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "version", typ: defines.MYSQL_TYPE_LONGLONG},
		{name: "disabled", typ: defines.MYSQL_TYPE_LONGLONG},
	})
}

func ensureMongoDBFeatureEnabledForSession(ctx context.Context, ses interface {
	GetService() string
	GetAccountId() uint32
}) error {
	if ses == nil {
		return moerr.NewInvalidInput(ctx, "MongoDB connection DDL requires a session")
	}
	pu := icebergParameterUnitForService(ses.GetService())
	if pu == nil || pu.SV == nil {
		return moerr.NewNotSupported(ctx, "MongoDB external tables are disabled because runtime configuration is unavailable")
	}
	parameters := pu.SV.MongoDB
	if !parameters.Enable {
		return moerr.NewNotSupported(ctx, "MongoDB external tables are disabled")
	}
	if !parameters.EnablePerAccount {
		return nil
	}
	if ses.GetAccountId() == 0 {
		return nil
	}
	for _, accountID := range parameters.AllowedAccounts {
		if accountID == ses.GetAccountId() {
			return nil
		}
	}
	return moerr.NewNotSupported(ctx, "MongoDB external tables are disabled for this account")
}

func mongoDBOptionsToMap(ctx context.Context, options tree.MongoDBOptions) (map[string]string, error) {
	values := make(map[string]string, len(options))
	for _, option := range options {
		if option == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(string(option.Key)))
		value := strings.TrimSpace(option.Val)
		if key == "" || value == "" && !mongoDBConnectionOptionMayBeCleared(key) {
			return nil, moerr.NewInvalidInput(ctx, "MongoDB connection option key and value cannot be empty")
		}
		if _, exists := values[key]; exists {
			return nil, moerr.NewInvalidInputf(ctx, "duplicate MongoDB connection option %s", key)
		}
		switch key {
		case "hosts", "srv_host", "replica_set", "auth_source", "auth_mechanism", "credential_secret_ref",
			"tls_mode", "tls_ca_secret_ref", "read_preference", "read_concern", "max_staleness_seconds", "options_json":
		default:
			return nil, moerr.NewInvalidInputf(ctx, "unsupported MongoDB connection option %s", key)
		}
		values[key] = value
	}
	return values, nil
}

func mongoDBConnectionOptionMayBeCleared(key string) bool {
	switch key {
	case "hosts", "srv_host", "replica_set", "tls_ca_secret_ref", "options_json":
		return true
	default:
		return false
	}
}

func mongoDBConnectionFromOptions(
	ctx context.Context,
	accountID uint32,
	name string,
	options map[string]string,
	existing *mongodb.Connection,
	parameters config.MongoDBParameters,
) (mongodb.Connection, error) {
	connection := mongodb.Connection{AccountID: accountID, Name: name, Version: 1}
	if existing != nil {
		connection = *existing
	}
	setString := func(key string, target *string) {
		if value, ok := options[key]; ok {
			*target = value
		}
	}
	setString("hosts", &connection.Hosts)
	setString("srv_host", &connection.SRVHost)
	setString("replica_set", &connection.ReplicaSet)
	setString("auth_source", &connection.AuthSource)
	setString("auth_mechanism", &connection.AuthMechanism)
	setString("credential_secret_ref", &connection.CredentialSecretRef)
	setString("tls_mode", &connection.TLSMode)
	setString("tls_ca_secret_ref", &connection.TLSCASecretRef)
	setString("read_preference", &connection.ReadPreference)
	setString("read_concern", &connection.ReadConcern)
	setString("options_json", &connection.OptionsJSON)
	_, readPreferenceChanged := options["read_preference"]
	_, maxStalenessSpecified := options["max_staleness_seconds"]
	if value, ok := options["max_staleness_seconds"]; ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 {
			return mongodb.Connection{}, moerr.NewInvalidInput(ctx, "MongoDB max_staleness_seconds must be non-negative")
		}
		connection.MaxStalenessSeconds = parsed
	}
	if connection.AuthSource == "" {
		connection.AuthSource = "admin"
	}
	if connection.AuthMechanism == "" {
		connection.AuthMechanism = "SCRAM-SHA-256"
	}
	if connection.TLSMode == "" {
		connection.TLSMode = "required"
	}
	if connection.ReadPreference == "" {
		connection.ReadPreference = "secondaryPreferred"
	}
	if !maxStalenessSpecified && (existing == nil || readPreferenceChanged) {
		switch strings.ToLower(connection.ReadPreference) {
		case "secondary", "secondarypreferred", "nearest":
			connection.MaxStalenessSeconds = 120
		default:
			connection.MaxStalenessSeconds = 0
		}
	}
	if connection.ReadConcern == "" {
		connection.ReadConcern = "majority"
	}
	if connection.SRVHost != "" {
		connection.DiscoveryMode = mongodb.DiscoverySRV
	} else {
		connection.DiscoveryMode = mongodb.DiscoverySeeds
	}
	runtimeConfig := mongoDBRuntimeConfig(parameters)
	if err := mongodb.ValidateConnection(ctx, connection, runtimeConfig); err != nil {
		return mongodb.Connection{}, err
	}
	return connection, nil
}

func mongoDBParametersForService(service string) config.MongoDBParameters {
	if pu := icebergParameterUnitForService(service); pu != nil && pu.SV != nil {
		return pu.SV.MongoDB
	}
	return config.MongoDBParameters{}
}

func mongoDBRuntimeConfig(parameters config.MongoDBParameters) mongodb.RuntimeConfig {
	allowed := make(map[uint32]struct{}, len(parameters.AllowedAccounts))
	for _, accountID := range parameters.AllowedAccounts {
		allowed[accountID] = struct{}{}
	}
	return mongodb.RuntimeConfig{
		Enable: parameters.Enable, EnablePerAccount: parameters.EnablePerAccount, AllowedAccounts: allowed,
		AllowLoopback: parameters.AllowLoopback, AllowedHostSuffixes: parameters.AllowedHostSuffixes, AllowedCIDRs: parameters.AllowedCIDRs,
	}
}

func queryMongoDBConnectionByNameForUpdate(ctx context.Context, bh BackgroundExec, accountID uint32, name string) (mongodb.Connection, bool, error) {
	return queryMongoDBConnectionByNameSQL(ctx, bh, mongodb.GetConnectionByNameForUpdateSQL(accountID, name))
}

func queryMongoDBConnectionByNameSQL(ctx context.Context, bh BackgroundExec, sql string) (mongodb.Connection, bool, error) {
	results, err := ExeSqlInBgSes(ctx, bh, sql)
	if err != nil || !execResultArrayHasData(results) {
		return mongodb.Connection{}, false, err
	}
	result := results[0]
	stringAt := func(column uint64) (string, error) {
		isNull, err := result.ColumnIsNull(ctx, 0, column)
		if err != nil || isNull {
			return "", err
		}
		return result.GetString(ctx, 0, column)
	}
	accountID, err := result.GetUint64(ctx, 0, 0)
	if err != nil || accountID > uint64(^uint32(0)) {
		return mongodb.Connection{}, false, moerr.NewInternalError(ctx, "MongoDB catalog connection has an invalid account ID")
	}
	connection := mongodb.Connection{AccountID: uint32(accountID)}
	connection.ConnectionID, err = result.GetUint64(ctx, 0, 1)
	if err != nil {
		return mongodb.Connection{}, false, err
	}
	fields := []*string{&connection.Name, &connection.DiscoveryMode, &connection.Hosts, &connection.SRVHost, &connection.ReplicaSet,
		&connection.AuthSource, &connection.AuthMechanism, &connection.CredentialSecretRef, &connection.TLSMode, &connection.TLSCASecretRef,
		&connection.ReadPreference, &connection.ReadConcern}
	for i, target := range fields {
		*target, err = stringAt(uint64(i + 2))
		if err != nil {
			return mongodb.Connection{}, false, err
		}
	}
	maxStaleness, err := result.GetInt64(ctx, 0, 14)
	if err != nil {
		return mongodb.Connection{}, false, err
	}
	connection.MaxStalenessSeconds = maxStaleness
	connection.OptionsJSON, err = stringAt(15)
	if err != nil {
		return mongodb.Connection{}, false, err
	}
	connection.Version, err = result.GetUint64(ctx, 0, 16)
	if err != nil {
		return mongodb.Connection{}, false, err
	}
	disabled, err := result.GetUint64(ctx, 0, 17)
	if err != nil {
		return mongodb.Connection{}, false, err
	}
	connection.Disabled = disabled != 0
	return connection, true, nil
}
