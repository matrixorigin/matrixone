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
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCatalogMappingResolverValidatesSnapshot(t *testing.T) {
	ctx := t.Context()
	_, err := (CatalogMappingResolver{}).ResolveMongoDBMapping(ctx, 7, 11, 3)
	require.ErrorContains(t, err, "no SQL executor")

	wantErr := errors.New("catalog unavailable")
	resolver := CatalogMappingResolver{Executor: executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, wantErr
	})}
	_, err = resolver.ResolveMongoDBMapping(ctx, 7, 11, 3)
	require.ErrorIs(t, err, wantErr)

	for _, tc := range []struct {
		name    string
		result  func(*testing.T) executor.Result
		wantErr string
	}{
		{name: "not found", result: func(*testing.T) executor.Result { return executor.Result{} }, wantErr: "does not exist"},
		{name: "short batch", result: newShortCatalogResult, wantErr: "does not exist"},
		{name: "invalid columns", result: func(t *testing.T) executor.Result {
			return newMappingCatalogResult(t, 7, 11, 3, "not-json")
		}, wantErr: "column mapping is invalid"},
		{name: "stale tenant", result: func(t *testing.T) executor.Result {
			return newMappingCatalogResult(t, 8, 11, 3, `[]`)
		}, wantErr: "stale or belongs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := CatalogMappingResolver{Executor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				require.Contains(t, sql, "mapping_id = 11")
				return tc.result(t), nil
			})}
			_, err := resolver.ResolveMongoDBMapping(t.Context(), 7, 11, 3)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	resolver = CatalogMappingResolver{Executor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		require.Contains(t, sql, "version = 3")
		return newMappingCatalogResult(t, 7, 11, 3, `[{"name":"device_id","path":"meta.device_id","type_id":25,"not_nullable":true}]`), nil
	})}
	mapping, err := resolver.ResolveMongoDBMapping(ctx, 7, 11, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(101), mapping.DatabaseID)
	require.Equal(t, uint64(102), mapping.TableID)
	require.Equal(t, uint64(12), mapping.ConnectionID)
	require.Equal(t, "telemetry", mapping.Database)
	require.Equal(t, "events", mapping.Collection)
	require.Equal(t, "strict", mapping.Conversion)
	require.Equal(t, int32(1), mapping.MaxParallelism)
	require.Equal(t, "device_id", mapping.Columns[0].Name)
	require.True(t, mapping.Columns[0].NotNullable)
}

func TestCatalogConnectionResolverValidatesGeneration(t *testing.T) {
	ctx := t.Context()
	_, err := (CatalogConnectionResolver{}).ResolveMongoDBConnection(ctx, 7, 12, 3)
	require.ErrorContains(t, err, "no SQL executor")

	wantErr := errors.New("catalog unavailable")
	resolver := CatalogConnectionResolver{Executor: executor.NewMemExecutor(func(string) (executor.Result, error) {
		return executor.Result{}, wantErr
	})}
	_, err = resolver.ResolveMongoDBConnection(ctx, 7, 12, 3)
	require.ErrorIs(t, err, wantErr)

	for _, tc := range []struct {
		name    string
		result  func(*testing.T) executor.Result
		wantErr string
	}{
		{name: "not found", result: func(*testing.T) executor.Result { return executor.Result{} }, wantErr: "does not exist"},
		{name: "short batch", result: newShortCatalogResult, wantErr: "does not exist"},
		{name: "wrong tenant", result: func(t *testing.T) executor.Result {
			return newConnectionCatalogResult(t, 8, 12, 3, false)
		}, wantErr: "stale, disabled"},
		{name: "disabled", result: func(t *testing.T) executor.Result {
			return newConnectionCatalogResult(t, 7, 12, 3, true)
		}, wantErr: "stale, disabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := CatalogConnectionResolver{Executor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				require.Contains(t, sql, "connection_id = 12")
				return tc.result(t), nil
			})}
			_, err := resolver.ResolveMongoDBConnection(t.Context(), 7, 12, 3)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	resolver = CatalogConnectionResolver{Executor: executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		require.Contains(t, sql, "version = 3")
		return newConnectionCatalogResult(t, 7, 12, 3, false), nil
	})}
	connection, err := resolver.ResolveMongoDBConnection(ctx, 7, 12, 3)
	require.NoError(t, err)
	require.Equal(t, "source", connection.Name)
	require.Equal(t, "127.0.0.1:27017", connection.Hosts)
	require.Equal(t, "SCRAM-SHA-256", connection.AuthMechanism)
	require.Equal(t, int64(120), connection.MaxStalenessSeconds)
	require.False(t, connection.Disabled)
}

func TestCatalogSQLGeneratorsCoverLifecycle(t *testing.T) {
	connection := Connection{
		AccountID: 7, ConnectionID: 12, Name: `source\'one`, DiscoveryMode: DiscoverySeeds,
		Hosts: "mongo:27017", CredentialSecretRef: "secret://env/MO_MONGODB_SOURCE",
	}
	mapping := TableMapping{
		AccountID: 7, DatabaseID: 101, TableID: 102, ConnectionID: 12,
		Database: "telemetry", Collection: "events", Columns: []ColumnMapping{{Name: "device_id", Path: "meta.device_id", NotNullable: true}},
	}
	queries := []string{
		GetConnectionByNameSQL(7, connection.Name),
		GetConnectionByNameForUpdateSQL(7, connection.Name),
		GetConnectionByIDSQL(7, 12, 3),
		GetMappingByTableIDSQL(7, 102),
		GetMappingByIDSQL(7, 11, 3),
		InsertTableMappingSQL(mapping),
		DeleteTableMappingSQL(7, 101, 102),
		InsertConnectionSQL(connection),
		UpdateConnectionSQL(connection),
		DisableConnectionSQL(7, connection.Name),
		EnableConnectionSQL(7, connection.Name),
		DeleteConnectionSQL(7, 12),
		ConnectionDependencyCountSQL(7, 12),
	}
	for _, query := range queries {
		require.NotEmpty(t, query)
		require.NotContains(t, query, `source\'one`)
	}
	require.True(t, strings.HasSuffix(queries[1], " for update"))
	require.Contains(t, queries[5], "max_parallelism")
	require.Contains(t, queries[5], `"not_nullable":true`)
	require.Contains(t, queries[7], "SCRAM-SHA-256")
	require.Equal(t, uint64(1), defaultVersion(0))
	require.Equal(t, uint64(4), defaultVersion(4))
}

func newShortCatalogResult(t *testing.T) executor.Result {
	t.Helper()
	mem := executor.NewMemResult([]types.Type{types.T_uint32.ToType()}, mpool.MustNewZero())
	mem.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(mem, 0, []uint32{7}))
	return mem.GetResult()
}

func newMappingCatalogResult(t *testing.T, accountID uint32, mappingID, version uint64, columns string) executor.Result {
	t.Helper()
	mem := executor.NewMemResult([]types.Type{
		types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_int32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, mpool.MustNewZero())
	mem.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(mem, 0, []uint32{accountID}))
	for column, value := range map[int]uint64{1: 101, 2: 102, 3: mappingID, 4: 12, 13: version} {
		require.NoError(t, executor.AppendFixedRows(mem, column, []uint64{value}))
	}
	for column, value := range map[int]string{5: "telemetry", 6: "events", 7: "explicit", 8: "strict", 9: "", 11: columns, 12: "{}"} {
		require.NoError(t, executor.AppendStringRows(mem, column, []string{value}))
	}
	require.NoError(t, executor.AppendFixedRows(mem, 10, []int32{1}))
	return mem.GetResult()
}

func newConnectionCatalogResult(t *testing.T, accountID uint32, connectionID, version uint64, disabled bool) executor.Result {
	t.Helper()
	columnTypes := []types.Type{types.T_uint32.ToType(), types.T_uint64.ToType()}
	for range 12 {
		columnTypes = append(columnTypes, types.T_varchar.ToType())
	}
	columnTypes = append(columnTypes, types.T_int64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType())
	mem := executor.NewMemResult(columnTypes, mpool.MustNewZero())
	mem.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(mem, 0, []uint32{accountID}))
	require.NoError(t, executor.AppendFixedRows(mem, 1, []uint64{connectionID}))
	values := []string{"source", "seeds", "127.0.0.1:27017", "", "rs0", "admin", "SCRAM-SHA-256", "secret://env/MO_MONGODB_SOURCE", "disabled", "", "secondaryPreferred", "majority"}
	for index, value := range values {
		require.NoError(t, executor.AppendStringRows(mem, index+2, []string{value}))
	}
	require.NoError(t, executor.AppendFixedRows(mem, 14, []int64{120}))
	require.NoError(t, executor.AppendStringRows(mem, 15, []string{"{}"}))
	require.NoError(t, executor.AppendFixedRows(mem, 16, []uint64{version}))
	flag := uint64(0)
	if disabled {
		flag = 1
	}
	require.NoError(t, executor.AppendFixedRows(mem, 17, []uint64{flag}))
	return mem.GetResult()
}

func TestNullableCatalogString(t *testing.T) {
	require.Empty(t, nullableString(nil, 0))
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp))
	require.Empty(t, nullableString(vec, 0))
	vec.Free(mp)
}
