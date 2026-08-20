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

package plan

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/stretchr/testify/require"
)

func TestMongoDBTableSurfaceFailsClosedWithoutRuntimeConfig(t *testing.T) {
	require.Error(t, ensureMongoDBTableSurfaceEnabled(context.Background()))
}

func TestMongoDBTableSurfaceDefaultsToAllAccounts(t *testing.T) {
	parameters := config.DefaultMongoDBParameters()
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, &config.ParameterUnit{
		SV: &config.FrontendParameters{MongoDB: parameters},
	})
	for _, accountID := range []uint32{0, 7, 8} {
		require.NoError(t, ensureMongoDBTableSurfaceEnabled(defines.AttachAccountId(ctx, accountID)))
	}
}

func TestMongoDBTableDefinitionRequiresTypedDiscriminator(t *testing.T) {
	mapping := sqlmongodb.TableMapping{
		Connection: "source", Database: "db", Collection: "events",
		Columns: []sqlmongodb.ColumnMapping{{
			Name: "value", Path: "value", TypeID: int32(types.T_int64), Conversion: sqlmongodb.ConversionStrict,
		}},
	}
	tableDef := &pb.TableDef{
		TableType: catalog.SystemExternalRel,
		Createsql: sqlmongodb.BuildCreateSQLEnvelope(mapping),
	}
	found, err := IsMongoDBTableDef(t.Context(), tableDef)
	require.NoError(t, err)
	require.False(t, found, "a v2 text marker alone is not trusted catalog metadata")

	tableDef.FeatureFlag = features.MongoDBExternal
	found, err = IsMongoDBTableDef(t.Context(), tableDef)
	require.NoError(t, err)
	require.True(t, found)

	// Existing v1 tables predate FeatureFlag and remain readable, while their
	// envelope is still required to occupy the complete leading catalog value.
	tableDef.FeatureFlag = 0
	tableDef.Createsql = strings.Replace(tableDef.Createsql, "version=2; kind=mongodb_table;", "version=1;", 1)
	found, err = IsMongoDBTableDef(t.Context(), tableDef)
	require.NoError(t, err)
	require.True(t, found)
}

func TestMongoScanDeepCopyAndCredentialFreeProto(t *testing.T) {
	original := &pb.Node{ExternScan: &pb.ExternScan{
		Type: int32(pb.ExternType_MONGODB_TB),
		MongodbScan: &pb.MongoScan{
			TableId: 99, MappingId: 1, MappingVersion: 4, ConnectionId: 2, ConnectionVersion: 3,
			Database: "telemetry", Collection: "events", ProjectedPaths: []string{"meta.device_id"},
			Columns:         []*pb.MongoColumnMapping{{Name: "device_id", Path: "meta.device_id"}},
			PushedPredicate: &pb.MongoPredicate{Op: pb.MongoPredicateOp_MONGO_PREDICATE_EQUAL, Path: "meta.device_id", ValueBson: []byte{1, 2, 3}},
		},
	}}
	copied := DeepCopyNode(original)
	require.Equal(t, original.ExternScan, copied.ExternScan)
	copied.ExternScan.MongodbScan.Columns[0].Path = "changed"
	copied.ExternScan.MongodbScan.ProjectedPaths[0] = "changed"
	copied.ExternScan.MongodbScan.PushedPredicate.ValueBson[0] = 9
	require.Equal(t, "meta.device_id", original.ExternScan.MongodbScan.Columns[0].Path)
	require.Equal(t, "meta.device_id", original.ExternScan.MongodbScan.ProjectedPaths[0])
	require.Equal(t, byte(1), original.ExternScan.MongodbScan.PushedPredicate.ValueBson[0])

	payload, err := proto.Marshal(original)
	require.NoError(t, err)
	for _, forbidden := range [][]byte{[]byte("mongodb://"), []byte("username"), []byte("password"), []byte("secret://")} {
		require.False(t, bytes.Contains(bytes.ToLower(payload), forbidden))
	}
	decoded := new(pb.Node)
	require.NoError(t, proto.Unmarshal(payload, decoded))
	require.Equal(t, original, decoded)
}
