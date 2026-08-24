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
	"context"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type CatalogConnectionResolver struct {
	Executor executor.SQLExecutor
}

type CatalogMappingResolver struct {
	Executor executor.SQLExecutor
}

func (resolver CatalogMappingResolver) ResolveMongoDBMapping(
	ctx context.Context,
	accountID uint32,
	mappingID uint64,
	version uint64,
) (TableMapping, error) {
	if resolver.Executor == nil {
		return TableMapping{}, moerr.NewInternalError(ctx, "MongoDB catalog mapping resolver has no SQL executor")
	}
	result, err := resolver.Executor.Exec(
		ctx,
		GetMappingByIDSQL(accountID, mappingID, version),
		executor.Options{}.
			WithAccountID(accountID).
			WithStatementOption(executor.StatementOption{}.WithDisableLog()),
	)
	if err != nil {
		return TableMapping{}, err
	}
	defer result.Close()
	for _, bat := range result.Batches {
		if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) < 14 {
			continue
		}
		row := 0
		mapping := TableMapping{
			AccountID:      vector.GetFixedAtWithTypeCheck[uint32](bat.Vecs[0], row),
			DatabaseID:     vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[1], row),
			TableID:        vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[2], row),
			MappingID:      vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[3], row),
			ConnectionID:   vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[4], row),
			Database:       nullableString(bat.Vecs[5], row),
			Collection:     nullableString(bat.Vecs[6], row),
			SchemaMode:     nullableString(bat.Vecs[7], row),
			Conversion:     nullableString(bat.Vecs[8], row),
			SplitKey:       nullableString(bat.Vecs[9], row),
			MaxParallelism: vector.GetFixedAtWithTypeCheck[int32](bat.Vecs[10], row),
			OptionsJSON:    nullableString(bat.Vecs[12], row),
			Version:        vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[13], row),
		}
		if err := json.Unmarshal(bat.Vecs[11].GetBytesAt(row), &mapping.Columns); err != nil {
			return TableMapping{}, moerr.NewInternalError(ctx, "MongoDB catalog column mapping is invalid")
		}
		if mapping.AccountID != accountID || mapping.MappingID != mappingID || mapping.Version != version {
			return TableMapping{}, moerr.NewInvalidInput(ctx, "MongoDB table mapping is stale or belongs to another account")
		}
		return mapping, nil
	}
	return TableMapping{}, moerr.NewInvalidInput(ctx, "MongoDB table mapping does not exist at the planned version")
}

func (resolver CatalogConnectionResolver) ResolveMongoDBConnection(
	ctx context.Context,
	accountID uint32,
	connectionID uint64,
	version uint64,
) (Connection, error) {
	if resolver.Executor == nil {
		return Connection{}, moerr.NewInternalError(ctx, "MongoDB catalog resolver has no SQL executor")
	}
	result, err := resolver.Executor.Exec(
		ctx,
		GetConnectionByIDSQL(accountID, connectionID, version),
		executor.Options{}.
			WithAccountID(accountID).
			WithStatementOption(executor.StatementOption{}.WithDisableLog()),
	)
	if err != nil {
		return Connection{}, err
	}
	defer result.Close()
	for _, bat := range result.Batches {
		if bat == nil || bat.RowCount() == 0 || len(bat.Vecs) < 18 {
			continue
		}
		row := 0
		connection := Connection{
			AccountID:           vector.GetFixedAtWithTypeCheck[uint32](bat.Vecs[0], row),
			ConnectionID:        vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[1], row),
			Name:                nullableString(bat.Vecs[2], row),
			DiscoveryMode:       nullableString(bat.Vecs[3], row),
			Hosts:               nullableString(bat.Vecs[4], row),
			SRVHost:             nullableString(bat.Vecs[5], row),
			ReplicaSet:          nullableString(bat.Vecs[6], row),
			AuthSource:          nullableString(bat.Vecs[7], row),
			AuthMechanism:       nullableString(bat.Vecs[8], row),
			CredentialSecretRef: nullableString(bat.Vecs[9], row),
			TLSMode:             nullableString(bat.Vecs[10], row),
			TLSCASecretRef:      nullableString(bat.Vecs[11], row),
			ReadPreference:      nullableString(bat.Vecs[12], row),
			ReadConcern:         nullableString(bat.Vecs[13], row),
			MaxStalenessSeconds: vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[14], row),
			OptionsJSON:         nullableString(bat.Vecs[15], row),
			Version:             vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[16], row),
			Disabled:            vector.GetFixedAtWithTypeCheck[uint64](bat.Vecs[17], row) != 0,
		}
		if connection.AccountID != accountID || connection.ConnectionID != connectionID || connection.Version != version || connection.Disabled {
			return Connection{}, moerr.NewInvalidInput(ctx, "MongoDB connection is stale, disabled, or belongs to another account")
		}
		return connection, nil
	}
	return Connection{}, moerr.NewInvalidInput(ctx, "MongoDB connection does not exist at the planned version")
}

func nullableString(vec *vector.Vector, row int) string {
	if vec == nil || vec.IsNull(uint64(row)) {
		return ""
	}
	return string(vec.GetBytesAt(row))
}
