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

func TestRewriteManifestsPlannerBuildsCatalogCommitAction(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	req := rewriteManifestsRequest("ref=main")
	plan, err := (RewriteManifestsPlanner{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Loader:  MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, plan.Attempt)
	require.Equal(t, "main", plan.Attempt.TargetRef)
	require.Equal(t, "idem-1", plan.Attempt.IdempotencyKey)
	require.Equal(t, int64(4), plan.Attempt.BaseSnapshotID)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "main", SnapshotID: 4}}, plan.Attempt.Requirements)
	require.Len(t, plan.Attempt.Updates, 2)
	require.Equal(t, "rewrite-manifests", plan.Attempt.Updates[0].Type)
	require.Equal(t, "s3://warehouse/orders/metadata/snap-4.avro", plan.Attempt.Updates[0].FilePath)
	require.Equal(t, "4", plan.Attempt.Updates[0].Payload["snapshot_id"])
	require.Equal(t, "set-snapshot-summary", plan.Attempt.Updates[1].Type)
	require.Equal(t, string(OperationRewriteManifests), plan.Attempt.Summary["operation"])
}

func TestRewriteManifestsPlannerUsesBranchRefSnapshot(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs["dev"] = api.SnapshotRef{SnapshotID: 3, Type: "branch"}
	req := rewriteManifestsRequest("ref=dev")
	plan, err := (RewriteManifestsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "dev", plan.Attempt.TargetRef)
	require.Equal(t, int64(3), plan.Attempt.BaseSnapshotID)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "dev", SnapshotID: 3}}, plan.Attempt.Requirements)
}

func TestRewriteManifestsPlannerRejectsMissingTargetRef(t *testing.T) {
	current := int64(4)
	_, err := (RewriteManifestsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) {
			return expireMetadata(current), nil
		}),
	}).BuildMaintenanceCommit(context.Background(), rewriteManifestsRequest("ref=dev"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "target ref is not present")
}

func rewriteManifestsRequest(options string) Request {
	parsed, err := ParseProcedureCall(ProcedureRewriteManifests, "ksa_gold.sales.orders", options)
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
