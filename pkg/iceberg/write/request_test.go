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

package write

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestBuildAppendRequestUnifiesSQLCTASSinkAndPublish(t *testing.T) {
	base := AppendRequestSpec{
		Namespace:            api.Namespace{"gold"},
		Table:                "orders",
		TableLocation:        "s3://warehouse/gold/orders",
		BaseSchemaID:         3,
		BaseSpecID:           7,
		BaseSchema:           baseSchema(3),
		BaseSpec:             baseSpec(7),
		KnownPartitionSpecs:  []api.PartitionSpec{baseSpec(6)},
		WriterOwnerAccountID: 9,
		IdempotencyKey:       "idem-1",
		DataFiles: []api.DataFile{{
			FilePath:        "s3://warehouse/gold/orders/data/part-1.parquet",
			RecordCount:     10,
			FileSizeInBytes: 100,
			FileFormat:      "parquet",
		}},
		SourceQueryID: "query-1",
		SourceBatch:   "batch-42",
	}
	sqlReq, err := BuildAppendRequest(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, "main", sqlReq.TargetRef)
	require.Equal(t, "branch", sqlReq.TargetRefType)
	require.Equal(t, string(AppendSourceSQLInsert), sqlReq.Summary["source-kind"])
	require.Equal(t, 3, sqlReq.BaseSchema.SchemaID)
	require.Equal(t, 7, sqlReq.BaseSpec.SpecID)
	require.Len(t, sqlReq.KnownPartitionSpecs, 1)
	require.Equal(t, 6, sqlReq.KnownPartitionSpecs[0].SpecID)
	require.Equal(t, uint32(9), sqlReq.WriterOwnerAccountID)

	ctasReq, err := BuildExistingMappingCTASAppendRequest(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, string(AppendSourceCTAS), ctasReq.Summary["source-kind"])

	sinkReq, err := BuildSinkAppendRequest(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, string(AppendSourceSink), sinkReq.Summary["source-kind"])

	publishReq, err := BuildMOIPublishAppendRequest(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, string(AppendSourceMOIPublish), publishReq.Summary["source-kind"])
}

func TestBuildAppendRequestNormalizesAndValidatesTargetRef(t *testing.T) {
	base := AppendRequestSpec{
		Namespace:            api.Namespace{"gold"},
		Table:                "orders",
		TargetRef:            "branch:publish",
		WriterOwnerAccountID: 9,
		IdempotencyKey:       "idem-1",
	}
	req, err := BuildAppendRequest(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, "publish", req.TargetRef)
	require.Equal(t, "branch", req.TargetRefType)

	base.TargetRef = "release"
	base.TargetRefType = "tag"
	base.CatalogCapabilities = api.CatalogCapabilities{BranchTag: true}
	_, err = BuildAppendRequest(context.Background(), base)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tag refs are read-only")

	base.AllowTagMove = true
	req, err = BuildAppendRequest(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, "release", req.TargetRef)
	require.Equal(t, "tag", req.TargetRefType)
}

func TestBuildAppendRequestRejectsCTASWithoutExistingMappingOrCreateCapability(t *testing.T) {
	_, err := BuildAppendRequest(context.Background(), AppendRequestSpec{
		Namespace:      api.Namespace{"gold"},
		Table:          "orders",
		IdempotencyKey: "idem-1",
		SourceKind:     AppendSourceCTAS,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
}
