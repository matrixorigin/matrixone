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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

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

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
