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

package maintenance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestRewriteDataFilesPlannerBuildsCatalogCommitAction(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	req := rewriteDataFilesRequest("ref=main,target_file_size=268435456,idempotency_key=ignored-control")
	req.IdempotencyKey = "idem-1"
	plan, err := (RewriteDataFilesPlanner{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Loader:  MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, plan.Attempt)
	require.Equal(t, "main", plan.Attempt.TargetRef)
	require.Equal(t, int64(4), plan.Attempt.BaseSnapshotID)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "main", SnapshotID: 4}}, plan.Attempt.Requirements)
	require.Len(t, plan.Attempt.Updates, 2)
	require.Equal(t, "rewrite-data-files", plan.Attempt.Updates[0].Type)
	require.Equal(t, "s3://warehouse/orders/metadata/snap-4.avro", plan.Attempt.Updates[0].FilePath)
	require.Equal(t, "4", plan.Attempt.Updates[0].Payload["snapshot_id"])
	require.Equal(t, "268435456", plan.Attempt.Updates[0].Payload["target_file_size"])
	require.NotContains(t, plan.Attempt.Updates[0].Payload, "idempotency_key")
	require.Equal(t, "set-snapshot-summary", plan.Attempt.Updates[1].Type)
	require.Equal(t, string(OperationRewriteDataFiles), plan.Attempt.Summary["operation"])
}

func TestRewriteDataFilesPlannerUsesBranchRefSnapshot(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs["compact"] = api.SnapshotRef{SnapshotID: 3, Type: "branch"}
	plan, err := (RewriteDataFilesPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), rewriteDataFilesRequest("ref=compact"))
	require.NoError(t, err)
	require.Equal(t, "compact", plan.Attempt.TargetRef)
	require.Equal(t, int64(3), plan.Attempt.BaseSnapshotID)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "compact", SnapshotID: 3}}, plan.Attempt.Requirements)
}

func rewriteDataFilesRequest(options string) Request {
	parsed, err := ParseProcedureCall(ProcedureRewriteDataFiles, "ksa_gold.sales.orders", options)
	if err != nil {
		panic(err)
	}
	return Request{
		AccountID:      7,
		CatalogID:      42,
		Namespace:      parsed.TargetID.Namespace,
		Table:          parsed.TargetID.Table,
		TargetRef:      TargetRef(parsed.Options),
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      parsed.Operation,
		Options:        parsed.Options,
	}
}
