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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

const mongoDBTestAccountID = uint32(7)

var mongoDBTestServiceSequence atomic.Uint64

func newMongoDBHandlerTestSession(t *testing.T) (*Session, *backgroundExecTest) {
	t.Helper()
	service := "mongodb-handler-" + t.Name()
	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{MongoDB: config.MongoDBParameters{
		Enable:        true,
		AllowLoopback: true,
	}}})
	ses := &Session{feSessionImpl: feSessionImpl{service: service}}
	ses.SetAccountId(mongoDBTestAccountID)
	bh := &backgroundExecTest{}
	bh.init()
	previous := NewBackgroundExec
	NewBackgroundExec = func(context.Context, FeSession, ...*BackgroundExecOption) BackgroundExec {
		return bh
	}
	t.Cleanup(func() { NewBackgroundExec = previous })
	return ses, bh
}

func mongoDBTestConnection(disabled bool) mongodb.Connection {
	return mongodb.Connection{
		AccountID:           mongoDBTestAccountID,
		ConnectionID:        23,
		Name:                "source",
		DiscoveryMode:       mongodb.DiscoverySeeds,
		Hosts:               "127.0.0.1:27017",
		AuthSource:          "admin",
		AuthMechanism:       "SCRAM-SHA-256",
		CredentialSecretRef: "secret://env/MONGO",
		TLSMode:             "disabled",
		ReadPreference:      "primary",
		ReadConcern:         "majority",
		Version:             4,
		Disabled:            disabled,
	}
}

func mongoDBConnectionResult(connection *mongodb.Connection) *MysqlResultSet {
	result := &MysqlResultSet{}
	for range 18 {
		result.AddColumn(&MysqlColumn{})
	}
	if connection == nil {
		return result
	}
	disabled := uint64(0)
	if connection.Disabled {
		disabled = 1
	}
	result.AddRow([]interface{}{
		uint64(connection.AccountID), connection.ConnectionID, connection.Name, connection.DiscoveryMode,
		connection.Hosts, connection.SRVHost, connection.ReplicaSet, connection.AuthSource,
		connection.AuthMechanism, connection.CredentialSecretRef, connection.TLSMode, connection.TLSCASecretRef,
		connection.ReadPreference, connection.ReadConcern, connection.MaxStalenessSeconds, connection.OptionsJSON,
		connection.Version, disabled,
	})
	return result
}

func mongoDBCountResult(value interface{}) *MysqlResultSet {
	result := &MysqlResultSet{}
	result.AddColumn(&MysqlColumn{})
	if value != nil {
		result.AddRow([]interface{}{value})
	}
	return result
}

func stubMongoDBCatalogQueries(
	t *testing.T,
	connection *mongodb.Connection,
	lookupErr error,
	dependencyResult *MysqlResultSet,
	dependencyErr error,
) {
	t.Helper()
	previous := ExeSqlInBgSes
	ExeSqlInBgSes = func(_ context.Context, _ BackgroundExec, sql string) ([]ExecResult, error) {
		switch sql {
		case mongodb.GetConnectionByNameForUpdateSQL(mongoDBTestAccountID, "source"):
			if lookupErr != nil {
				return nil, lookupErr
			}
			return []ExecResult{mongoDBConnectionResult(connection)}, nil
		case mongodb.ConnectionDependencyCountSQL(mongoDBTestAccountID, 23):
			if dependencyErr != nil {
				return nil, dependencyErr
			}
			if dependencyResult == nil {
				dependencyResult = mongoDBCountResult(nil)
			}
			return []ExecResult{dependencyResult}, nil
		default:
			t.Fatalf("unexpected MongoDB catalog query: %s", sql)
			return nil, nil
		}
	}
	t.Cleanup(func() { ExeSqlInBgSes = previous })
}

func mongoDBCreateOptions() tree.MongoDBOptions {
	return tree.MongoDBOptions{
		tree.NewMongoDBOption("hosts", "127.0.0.1:27017"),
		tree.NewMongoDBOption("credential_secret_ref", "secret://env/MONGO"),
		tree.NewMongoDBOption("tls_mode", "disabled"),
		tree.NewMongoDBOption("read_preference", "primary"),
	}
}

func TestMongoDBConnectionCreateLifecycle(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		stubMongoDBCatalogQueries(t, nil, nil, nil, nil)
		stmt := &tree.CreateMongoDBConnection{Name: "source", Options: mongoDBCreateOptions()}
		require.NoError(t, handleCreateMongoDBConnection(t.Context(), ses, stmt))
		expected := mongoDBTestConnection(false)
		expected.Version = 1
		require.Equal(t, []string{"begin;", mongodb.InsertConnectionSQL(expected), "commit;"}, bh.executedSQLs)
	})

	t.Run("existing if not exists", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		connection := mongoDBTestConnection(false)
		stubMongoDBCatalogQueries(t, &connection, nil, nil, nil)
		err := handleCreateMongoDBConnection(t.Context(), ses, &tree.CreateMongoDBConnection{
			Name: "source", IfNotExists: true, Options: mongoDBCreateOptions(),
		})
		require.NoError(t, err)
		require.Equal(t, []string{"begin;", "commit;"}, bh.executedSQLs)
	})

	t.Run("duplicate rolls back", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		connection := mongoDBTestConnection(false)
		stubMongoDBCatalogQueries(t, &connection, nil, nil, nil)
		err := handleCreateMongoDBConnection(t.Context(), ses, &tree.CreateMongoDBConnection{Name: "source", Options: mongoDBCreateOptions()})
		require.ErrorContains(t, err, "already exists")
		require.Equal(t, []string{"begin;", "rollback;"}, bh.executedSQLs)
	})

	t.Run("begin and lookup errors", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		beginErr := errors.New("begin failed")
		bh.sql2err["begin;"] = beginErr
		err := handleCreateMongoDBConnection(t.Context(), ses, &tree.CreateMongoDBConnection{Name: "source", Options: mongoDBCreateOptions()})
		require.ErrorIs(t, err, beginErr)
		require.Equal(t, []string{"begin;"}, bh.executedSQLs)
	})

	t.Run("lookup error rolls back", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		lookupErr := errors.New("lookup failed")
		stubMongoDBCatalogQueries(t, nil, lookupErr, nil, nil)
		err := handleCreateMongoDBConnection(t.Context(), ses, &tree.CreateMongoDBConnection{Name: "source", Options: mongoDBCreateOptions()})
		require.ErrorIs(t, err, lookupErr)
		require.Equal(t, []string{"begin;", "rollback;"}, bh.executedSQLs)
	})
}

func TestMongoDBConnectionAlterLifecycle(t *testing.T) {
	t.Run("disable and already disabled", func(t *testing.T) {
		for _, disabled := range []bool{false, true} {
			t.Run(map[bool]string{false: "disable", true: "no-op"}[disabled], func(t *testing.T) {
				ses, bh := newMongoDBHandlerTestSession(t)
				connection := mongoDBTestConnection(disabled)
				stubMongoDBCatalogQueries(t, &connection, nil, nil, nil)
				err := handleAlterMongoDBConnection(t.Context(), ses, &tree.AlterMongoDBConnection{
					Name: "source", Action: tree.AlterMongoDBConnectionDisable,
				})
				require.NoError(t, err)
				if disabled {
					require.Equal(t, []string{"begin;", "commit;"}, bh.executedSQLs)
				} else {
					require.Equal(t, []string{"begin;", mongodb.DisableConnectionSQL(mongoDBTestAccountID, "source"), "commit;"}, bh.executedSQLs)
				}
			})
		}
	})

	t.Run("enable and already enabled", func(t *testing.T) {
		for _, disabled := range []bool{true, false} {
			t.Run(map[bool]string{true: "enable", false: "no-op"}[disabled], func(t *testing.T) {
				ses, bh := newMongoDBHandlerTestSession(t)
				connection := mongoDBTestConnection(disabled)
				stubMongoDBCatalogQueries(t, &connection, nil, nil, nil)
				err := handleAlterMongoDBConnection(t.Context(), ses, &tree.AlterMongoDBConnection{
					Name: "source", Action: tree.AlterMongoDBConnectionEnable,
				})
				require.NoError(t, err)
				if disabled {
					require.Equal(t, []string{"begin;", mongodb.EnableConnectionSQL(mongoDBTestAccountID, "source"), "commit;"}, bh.executedSQLs)
				} else {
					require.Equal(t, []string{"begin;", "commit;"}, bh.executedSQLs)
				}
			})
		}
	})

	t.Run("set", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		connection := mongoDBTestConnection(false)
		stubMongoDBCatalogQueries(t, &connection, nil, nil, nil)
		err := handleAlterMongoDBConnection(t.Context(), ses, &tree.AlterMongoDBConnection{
			Name: "source", Action: tree.AlterMongoDBConnectionSet,
			Options: tree.MongoDBOptions{tree.NewMongoDBOption("read_concern", "local")},
		})
		require.NoError(t, err)
		connection.ReadConcern = "local"
		require.Equal(t, []string{"begin;", mongodb.UpdateConnectionSQL(connection), "commit;"}, bh.executedSQLs)
	})

	t.Run("missing and invalid action roll back", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			connection *mongodb.Connection
			action     tree.AlterMongoDBConnectionAction
			want       string
		}{
			{name: "missing", action: tree.AlterMongoDBConnectionDisable, want: "does not exist"},
			{name: "invalid action", connection: func() *mongodb.Connection { c := mongoDBTestConnection(false); return &c }(), action: 99, want: "invalid action"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ses, bh := newMongoDBHandlerTestSession(t)
				stubMongoDBCatalogQueries(t, tc.connection, nil, nil, nil)
				err := handleAlterMongoDBConnection(t.Context(), ses, &tree.AlterMongoDBConnection{Name: "source", Action: tc.action})
				require.ErrorContains(t, err, tc.want)
				require.Equal(t, []string{"begin;", "rollback;"}, bh.executedSQLs)
			})
		}
	})

	t.Run("statement validation", func(t *testing.T) {
		ses, _ := newMongoDBHandlerTestSession(t)
		err := handleAlterMongoDBConnection(t.Context(), ses, &tree.AlterMongoDBConnection{Name: "source", Action: tree.AlterMongoDBConnectionSet})
		require.ErrorContains(t, err, "requires at least one option")
		err = handleAlterMongoDBConnection(t.Context(), ses, &tree.AlterMongoDBConnection{
			Name: "source", Action: tree.AlterMongoDBConnectionEnable,
			Options: tree.MongoDBOptions{tree.NewMongoDBOption("hosts", "127.0.0.1:27017")},
		})
		require.ErrorContains(t, err, "does not accept options")
	})
}

func TestMongoDBConnectionDropLifecycle(t *testing.T) {
	t.Run("drop", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		connection := mongoDBTestConnection(false)
		stubMongoDBCatalogQueries(t, &connection, nil, mongoDBCountResult(uint64(0)), nil)
		require.NoError(t, handleDropMongoDBConnection(t.Context(), ses, &tree.DropMongoDBConnection{Name: "source"}))
		require.Equal(t, []string{"begin;", mongodb.DeleteConnectionSQL(mongoDBTestAccountID, 23), "commit;"}, bh.executedSQLs)
	})

	t.Run("missing", func(t *testing.T) {
		for _, ifExists := range []bool{true, false} {
			t.Run(map[bool]string{true: "if exists", false: "error"}[ifExists], func(t *testing.T) {
				ses, bh := newMongoDBHandlerTestSession(t)
				stubMongoDBCatalogQueries(t, nil, nil, nil, nil)
				err := handleDropMongoDBConnection(t.Context(), ses, &tree.DropMongoDBConnection{Name: "source", IfExists: ifExists})
				if ifExists {
					require.NoError(t, err)
					require.Equal(t, []string{"begin;", "commit;"}, bh.executedSQLs)
				} else {
					require.ErrorContains(t, err, "does not exist")
					require.Equal(t, []string{"begin;", "rollback;"}, bh.executedSQLs)
				}
			})
		}
	})

	t.Run("dependencies block drop", func(t *testing.T) {
		ses, bh := newMongoDBHandlerTestSession(t)
		connection := mongoDBTestConnection(false)
		stubMongoDBCatalogQueries(t, &connection, nil, mongoDBCountResult(uint64(2)), nil)
		err := handleDropMongoDBConnection(t.Context(), ses, &tree.DropMongoDBConnection{Name: "source"})
		require.ErrorContains(t, err, "referenced by 2")
		require.Equal(t, []string{"begin;", "rollback;"}, bh.executedSQLs)
	})

	t.Run("dependency query and conversion errors", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			result *MysqlResultSet
			err    error
		}{
			{name: "query", err: errors.New("dependency query failed")},
			{name: "conversion", result: mongoDBCountResult(struct{}{})},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ses, bh := newMongoDBHandlerTestSession(t)
				connection := mongoDBTestConnection(false)
				stubMongoDBCatalogQueries(t, &connection, nil, tc.result, tc.err)
				err := handleDropMongoDBConnection(t.Context(), ses, &tree.DropMongoDBConnection{Name: "source"})
				require.Error(t, err)
				require.Equal(t, []string{"begin;", "rollback;"}, bh.executedSQLs)
			})
		}
	})
}

func TestMongoDBConnectionCatalogResultValidation(t *testing.T) {
	previous := ExeSqlInBgSes
	t.Cleanup(func() { ExeSqlInBgSes = previous })

	result := mongoDBConnectionResult(nil)
	result.AddRow([]interface{}{uint64(^uint32(0)) + 1, uint64(23), "source", "seeds", "host", "", "", "admin", "SCRAM-SHA-256", "ref", "required", "", "primary", "majority", int64(0), "", uint64(1), uint64(0)})
	ExeSqlInBgSes = func(context.Context, BackgroundExec, string) ([]ExecResult, error) {
		return []ExecResult{result}, nil
	}
	_, found, err := queryMongoDBConnectionByNameSQL(t.Context(), nil, "select invalid account")
	require.False(t, found)
	require.ErrorContains(t, err, "invalid account ID")
}

func TestShowMongoDBConnectionsValidation(t *testing.T) {
	require.ErrorContains(t, handleShowMongoDBConnections(t.Context(), nil, &tree.ShowMongoDBConnections{Like: &tree.ComparisonExpr{}}), "LIKE/WHERE")
	service := "mongodb-show-" + t.Name()
	InitServerLevelVars(service)
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{}})
	require.ErrorContains(t, handleShowMongoDBConnections(t.Context(), &Session{
		feSessionImpl: feSessionImpl{service: service},
	}, nil), "are disabled")
}

func TestMongoDBTableMappingRequiresConnectionUsageFallback(t *testing.T) {
	stmt := &tree.CreateTable{MongoDBParam: &tree.MongoDBTableParam{}}
	require.False(t, canCreateMongoDBTableMapping(stmt, &TenantInfo{Tenant: "tenant", DefaultRole: "readonly"}))
	require.True(t, canCreateMongoDBTableMapping(stmt, &TenantInfo{Tenant: "tenant", DefaultRole: accountAdminRoleName}))
	require.True(t, canCreateMongoDBTableMapping(&tree.CreateTable{}, &TenantInfo{Tenant: "tenant", DefaultRole: "readonly"}))
}

func TestMongoDBReadPreferenceStalenessDefaults(t *testing.T) {
	parameters := config.MongoDBParameters{AllowLoopback: true}
	base := map[string]string{
		"hosts": "127.0.0.1:27017", "credential_secret_ref": "secret://env/MONGO", "tls_mode": "disabled",
	}
	primary := cloneStringMap(base)
	primary["read_preference"] = "primary"
	connection, err := mongoDBConnectionFromOptions(t.Context(), 1, "source", primary, nil, parameters)
	require.NoError(t, err)
	require.Zero(t, connection.MaxStalenessSeconds)

	secondary, err := mongoDBConnectionFromOptions(t.Context(), 1, "source", base, nil, parameters)
	require.NoError(t, err)
	require.Equal(t, int64(120), secondary.MaxStalenessSeconds)

	invalid := cloneStringMap(primary)
	invalid["max_staleness_seconds"] = "120"
	_, err = mongoDBConnectionFromOptions(t.Context(), 1, "source", invalid, nil, parameters)
	require.Error(t, err)
}

func TestMongoDBConnectionCanSwitchDiscoveryMode(t *testing.T) {
	parameters := config.MongoDBParameters{
		AllowLoopback:       true,
		AllowedHostSuffixes: []string{"mongo.example"},
	}
	existing, err := mongoDBConnectionFromOptions(t.Context(), 1, "source", map[string]string{
		"hosts":                 "127.0.0.1:27017",
		"credential_secret_ref": "secret://env/MONGO",
		"tls_mode":              "disabled",
		"read_preference":       "primary",
	}, nil, parameters)
	require.NoError(t, err)

	options, err := mongoDBOptionsToMap(t.Context(), tree.MongoDBOptions{
		tree.NewMongoDBOption("hosts", ""),
		tree.NewMongoDBOption("srv_host", "cluster.mongo.example"),
		tree.NewMongoDBOption("options_json", ""),
	})
	require.NoError(t, err)
	updated, err := mongoDBConnectionFromOptions(t.Context(), 1, "source", options, &existing, parameters)
	require.NoError(t, err)
	require.Empty(t, updated.Hosts)
	require.Equal(t, "cluster.mongo.example", updated.SRVHost)
	require.Equal(t, mongodb.DiscoverySRV, updated.DiscoveryMode)
}

func TestMongoDBSystemTablesAreInitializedAndDroppedForTenants(t *testing.T) {
	for _, table := range mongodb.SystemTableDDLs {
		require.Contains(t, predefinedTables, table.Name)
		require.Contains(t, createSqls, table.DDL)
		require.NotContains(t, dropSqls, "drop table if exists mo_catalog."+table.Name+";")
		require.Contains(t, dropMongoDBSqls, "drop table if exists mo_catalog."+table.Name+";")
	}
}

func TestMongoDBConnectionOptionValidation(t *testing.T) {
	ctx := t.Context()
	values, err := mongoDBOptionsToMap(ctx, tree.MongoDBOptions{
		nil,
		tree.NewMongoDBOption(" HOSTS ", " mongo.example:27017 "),
		tree.NewMongoDBOption("options_json", ""),
	})
	require.NoError(t, err)
	require.Equal(t, "mongo.example:27017", values["hosts"])
	require.Contains(t, values, "options_json")
	require.True(t, mongoDBConnectionOptionMayBeCleared("srv_host"))
	require.False(t, mongoDBConnectionOptionMayBeCleared("credential_secret_ref"))

	for _, tc := range []struct {
		name    string
		options tree.MongoDBOptions
		want    string
	}{
		{name: "empty key", options: tree.MongoDBOptions{tree.NewMongoDBOption("", "value")}, want: "cannot be empty"},
		{name: "empty required value", options: tree.MongoDBOptions{tree.NewMongoDBOption("auth_source", "")}, want: "cannot be empty"},
		{name: "duplicate", options: tree.MongoDBOptions{tree.NewMongoDBOption("hosts", "one"), tree.NewMongoDBOption("HOSTS", "two")}, want: "duplicate"},
		{name: "unsupported", options: tree.MongoDBOptions{tree.NewMongoDBOption("password", "secret")}, want: "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mongoDBOptionsToMap(t.Context(), tc.options)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestMongoDBConnectionOptionDefaultsAndFailures(t *testing.T) {
	parameters := config.MongoDBParameters{AllowLoopback: true, AllowedHostSuffixes: []string{"mongo.example"}}
	base := map[string]string{
		"hosts": "127.0.0.1:27017", "credential_secret_ref": "secret://env/MONGO", "tls_mode": "disabled",
	}
	connection, err := mongoDBConnectionFromOptions(t.Context(), 9, "source", base, nil, parameters)
	require.NoError(t, err)
	require.Equal(t, "admin", connection.AuthSource)
	require.Equal(t, "SCRAM-SHA-256", connection.AuthMechanism)
	require.Equal(t, "secondaryPreferred", connection.ReadPreference)
	require.Equal(t, "majority", connection.ReadConcern)
	require.Equal(t, mongodb.DiscoverySeeds, connection.DiscoveryMode)
	require.Equal(t, uint64(1), connection.Version)

	for _, value := range []string{"not-a-number", "-1"} {
		invalid := cloneStringMap(base)
		invalid["max_staleness_seconds"] = value
		_, err := mongoDBConnectionFromOptions(t.Context(), 9, "source", invalid, nil, parameters)
		require.ErrorContains(t, err, "must be non-negative")
	}

	updated, err := mongoDBConnectionFromOptions(t.Context(), 9, "source", map[string]string{
		"read_preference": "primary", "max_staleness_seconds": "0",
	}, &connection, parameters)
	require.NoError(t, err)
	require.Zero(t, updated.MaxStalenessSeconds)

	invalidEndpoint := cloneStringMap(base)
	invalidEndpoint["hosts"] = "blocked.example:27017"
	_, err = mongoDBConnectionFromOptions(t.Context(), 9, "source", invalidEndpoint, nil, parameters)
	require.ErrorContains(t, err, "allowlist")
}

type mongoDBFeatureSession struct {
	service   string
	accountID uint32
}

func (s mongoDBFeatureSession) GetService() string   { return s.service }
func (s mongoDBFeatureSession) GetAccountId() uint32 { return s.accountID }

func TestMongoDBFeatureGateAndRuntimeConfiguration(t *testing.T) {
	require.ErrorContains(t, ensureMongoDBFeatureEnabledForSession(t.Context(), nil), "requires a session")

	missingService := "mongodb-missing-" + t.Name()
	require.ErrorContains(t, ensureMongoDBFeatureEnabledForSession(t.Context(), mongoDBFeatureSession{service: missingService}), "configuration is unavailable")

	service := "mongodb-gate-" + t.Name()
	InitServerLevelVars(service)
	parameters := config.MongoDBParameters{}
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{MongoDB: parameters}})
	require.ErrorContains(t, ensureMongoDBFeatureEnabledForSession(t.Context(), mongoDBFeatureSession{service: service, accountID: 7}), "are disabled")

	defaultService := "mongodb-default-gate-" + t.Name()
	InitServerLevelVars(defaultService)
	defaults := config.DefaultMongoDBParameters()
	setPu(defaultService, &config.ParameterUnit{SV: &config.FrontendParameters{MongoDB: defaults}})
	for _, accountID := range []uint32{0, 7, 8} {
		require.NoError(t, ensureMongoDBFeatureEnabledForSession(
			t.Context(), mongoDBFeatureSession{service: defaultService, accountID: accountID}))
	}
	require.False(t, defaults.EnablePerAccount)

	parameters.Enable = true
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{MongoDB: parameters}})
	require.NoError(t, ensureMongoDBFeatureEnabledForSession(t.Context(), mongoDBFeatureSession{service: service, accountID: 7}))

	parameters.EnablePerAccount = true
	parameters.AllowedAccounts = []uint32{7}
	parameters.AllowLoopback = true
	parameters.AllowedHostSuffixes = []string{"mongo.example"}
	parameters.AllowedCIDRs = []string{"10.0.0.0/8"}
	setPu(service, &config.ParameterUnit{SV: &config.FrontendParameters{MongoDB: parameters}})
	require.NoError(t, ensureMongoDBFeatureEnabledForSession(t.Context(), mongoDBFeatureSession{service: service, accountID: 0}))
	require.NoError(t, ensureMongoDBFeatureEnabledForSession(t.Context(), mongoDBFeatureSession{service: service, accountID: 7}))
	require.ErrorContains(t, ensureMongoDBFeatureEnabledForSession(t.Context(), mongoDBFeatureSession{service: service, accountID: 8}), "this account")
	require.Equal(t, parameters, mongoDBParametersForService(service))

	runtimeConfig := mongoDBRuntimeConfig(parameters)
	require.True(t, runtimeConfig.Enable)
	require.True(t, runtimeConfig.EnablePerAccount)
	require.True(t, runtimeConfig.AllowLoopback)
	require.Contains(t, runtimeConfig.AllowedAccounts, uint32(7))
	require.Equal(t, []string{"mongo.example"}, runtimeConfig.AllowedHostSuffixes)
	require.Equal(t, []string{"10.0.0.0/8"}, runtimeConfig.AllowedCIDRs)
	require.Equal(t, config.MongoDBParameters{}, mongoDBParametersForService(missingService))
}

type frontendMongoDBClientFactory struct{}

func (frontendMongoDBClientFactory) Connect(context.Context, mongodb.Connection, mongodb.Credentials, mongodb.RuntimeConfig) (mongodb.Client, error) {
	return nil, nil
}

type retirementFrontendMongoDBClientFactory struct {
	client *retirementFrontendMongoDBClient
}

func (f *retirementFrontendMongoDBClientFactory) Connect(
	context.Context, mongodb.Connection, mongodb.Credentials, mongodb.RuntimeConfig,
) (mongodb.Client, error) {
	f.client = &retirementFrontendMongoDBClient{}
	return f.client, nil
}

type retirementFrontendMongoDBClient struct {
	disconnects int
}

func (*retirementFrontendMongoDBClient) Collection(string, string) mongodb.Collection { return nil }
func (*retirementFrontendMongoDBClient) Ping(context.Context) error                   { return nil }
func (c *retirementFrontendMongoDBClient) Disconnect(context.Context) error {
	c.disconnects++
	return nil
}

func TestFinishTxnAndRetireMongoDBAccounts(t *testing.T) {
	for _, tc := range []struct {
		name        string
		txnErr      error
		wantSQL     string
		wantRetired int
	}{
		{name: "commit", wantSQL: "commit;", wantRetired: 1},
		{name: "rollback", txnErr: errors.New("restore failed"), wantSQL: "rollback;"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := "mongodb-restore-retirement-" + tc.name
			rt := moruntime.DefaultRuntime()
			moruntime.SetupServiceBasedRuntime(service, rt)
			factory := &retirementFrontendMongoDBClientFactory{}
			pool := mongodb.NewClientPool(factory)
			t.Cleanup(func() { require.NoError(t, pool.Close(context.Background())) })
			rt.SetGlobalVariables(mongodb.RuntimeDependenciesKey, &mongodb.RuntimeDependencies{Pool: pool})

			lease, err := pool.Acquire(t.Context(), mongodb.Connection{
				AccountID: 7, ConnectionID: 9, Version: 1,
			}, mongodb.Credentials{}, mongodb.RuntimeConfig{})
			require.NoError(t, err)
			require.NoError(t, lease.Release(t.Context()))

			bh := &backgroundExecTest{}
			bh.init()
			err = finishTxnAndRetireMongoDBAccounts(t.Context(), bh, service, []uint32{7}, tc.txnErr)
			if tc.txnErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.txnErr)
			}
			require.Equal(t, []string{tc.wantSQL}, bh.executedSQLs)
			require.Equal(t, tc.wantRetired, factory.client.disconnects)
		})
	}
}

func TestMongoDBRuntimeDependencyLookupAndRetirement(t *testing.T) {
	missingService := fmt.Sprintf("mongodb-no-runtime-%s-%d", t.Name(), mongoDBTestServiceSequence.Add(1))
	require.NotPanics(t, func() { _ = mongoDBRuntimeDependencies(missingService) })
	service := "mongodb-runtime-" + t.Name()
	moruntime.SetupServiceBasedRuntime(service, moruntime.DefaultRuntime())
	moruntime.ServiceRuntime(service).SetGlobalVariables(mongodb.RuntimeDependenciesKey, "wrong-type")
	require.Nil(t, mongoDBRuntimeDependencies(service))

	dependencies := &mongodb.RuntimeDependencies{Pool: mongodb.NewClientPool(frontendMongoDBClientFactory{})}
	moruntime.ServiceRuntime(service).SetGlobalVariables(mongodb.RuntimeDependenciesKey, dependencies)
	require.Same(t, dependencies, mongoDBRuntimeDependencies(service))
	retireMongoDBClients(t.Context(), service, mongodb.ClientRetirement{AccountID: 7, ConnectionID: 12, VersionExclusive: 3})
	retireMongoDBClients(t.Context(), service, mongodb.ClientRetirement{AccountID: 7, ConnectionID: 12})
	retireMongoDBClients(t.Context(), service, mongodb.ClientRetirement{AccountID: 7})
	retireMongoDBClients(t.Context(), missingService, mongodb.ClientRetirement{AccountID: 7, ConnectionID: 12})
}

func TestMarkMongoDBAccountForRetirement(t *testing.T) {
	require.NotPanics(t, func() { markMongoDBAccountForRetirement(nil, 7) })

	accountIDs := []uint32{7}
	markMongoDBAccountForRetirement(&accountIDs, 7)
	markMongoDBAccountForRetirement(&accountIDs, 9)
	markMongoDBAccountForRetirement(&accountIDs, 9)
	require.Equal(t, []uint32{7, 9}, accountIDs)
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
