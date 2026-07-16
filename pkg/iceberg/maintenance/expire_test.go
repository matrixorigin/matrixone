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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergmetadata "github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestExpireSnapshotsPlannerBuildsCommitPlan(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	req := expireRequest("older_than=2026-01-04 00:00:00,retain_last=3")
	plan, err := (ExpireSnapshotsPlanner{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Loader:  MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:     func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, plan.Attempt)
	require.Equal(t, uint64(1), plan.RemovedFileCount)
	require.Empty(t, plan.OrphanPaths)
	require.Equal(t, []string{"s3://warehouse/orders/metadata/snap-1.avro"}, plan.PostCommitOrphans)
	require.Equal(t, "main", plan.Attempt.TargetRef)
	require.Equal(t, "idem-1", plan.Attempt.IdempotencyKey)
	require.Equal(t, int64(4), plan.Attempt.BaseSnapshotID)
	require.Equal(t, []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "main", SnapshotID: 4}}, plan.Attempt.Requirements)
	require.Len(t, plan.Attempt.Updates, 1)
	require.Equal(t, "remove-snapshots", plan.Attempt.Updates[0].Type)
	require.Equal(t, []int64{1}, plan.Attempt.Updates[0].SnapshotIDs)
	require.Equal(t, "1", plan.Attempt.Summary["expired-snapshots"])
	require.Equal(t, "3", plan.Attempt.Summary["retain-last"])
}

func TestExpireSnapshotsPlannerProtectsRefsAndRetainLast(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs["audit"] = api.SnapshotRef{SnapshotID: 3, Type: "branch", MinSnapshotsToKeep: 2}
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-05 00:00:00,retain_last=1"))
	require.NoError(t, err)
	require.Len(t, plan.Attempt.Updates, 1)
	require.Equal(t, "remove-snapshots", plan.Attempt.Updates[0].Type)
	require.Equal(t, []int64{1}, plan.Attempt.Updates[0].SnapshotIDs, "snapshot 3 and its parent are protected by ref lineage, current/newest are protected by retain/current")
}

func TestExpireSnapshotsPlannerHonorsBranchSnapshotAge(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs["audit"] = api.SnapshotRef{
		SnapshotID:         3,
		Type:               "branch",
		MinSnapshotsToKeep: 1,
		MaxSnapshotAgeMS:   3 * 24 * 60 * 60 * 1000,
	}
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:    func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-05 00:00:00,retain_last=1"))
	require.NoError(t, err)
	require.Len(t, plan.Attempt.Updates, 1)
	require.Equal(t, "remove-snapshots", plan.Attempt.Updates[0].Type)
	require.Equal(t, []int64{1}, plan.Attempt.Updates[0].SnapshotIDs)
}

func TestExpireSnapshotsPlannerRemovesExpiredRefBeforeSnapshots(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs["old-tag"] = api.SnapshotRef{
		SnapshotID:  1,
		Type:        "tag",
		MaxRefAgeMS: 24 * 60 * 60 * 1000,
	}
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:    func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-05 00:00:00,retain_last=1"))
	require.NoError(t, err)
	require.Len(t, plan.Attempt.Updates, 2)
	require.Equal(t, api.CommitUpdate{Type: "remove-snapshot-ref", Ref: "old-tag"}, plan.Attempt.Updates[0])
	require.Equal(t, "remove-snapshots", plan.Attempt.Updates[1].Type)
	require.Equal(t, []int64{1, 2, 3}, plan.Attempt.Updates[1].SnapshotIDs)
	require.Equal(t, "1", plan.Attempt.Summary["expired-refs"])
}

func TestExpireSnapshotsPlannerNeverExpiresMainRef(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	main := meta.Refs["main"]
	main.MaxRefAgeMS = 1
	meta.Refs["main"] = main
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:    func() time.Time { return time.Date(2027, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2027-01-05 00:00:00,retain_last=1"))
	require.NoError(t, err)
	for _, update := range plan.Attempt.Updates {
		require.False(t, update.Type == "remove-snapshot-ref" && update.Ref == "main")
	}
}

func TestExpireSnapshotsPlannerAppliesRetainLastToImplicitMain(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs = nil
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(context.Context, Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:    func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-05 00:00:00,retain_last=3"))
	require.NoError(t, err)
	require.Equal(t, []int64{1}, plan.Attempt.Updates[0].SnapshotIDs)
}

func TestExpireSnapshotsPlannerAgeArithmeticDoesNotOverflow(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Refs["long-lived"] = api.SnapshotRef{SnapshotID: 1, Type: "tag", MaxRefAgeMS: math.MaxInt64}
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(context.Context, Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:    func() time.Time { return time.Date(2027, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2027-01-05 00:00:00,retain_last=1"))
	require.NoError(t, err)
	for _, update := range plan.Attempt.Updates {
		require.False(t, update.Type == "remove-snapshot-ref" && update.Ref == "long-lived")
	}
}

func TestExpireSnapshotsPlannerEnumeratesExpiredSnapshotFiles(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	manifestPath := "s3://warehouse/orders/metadata/manifest-1.avro"
	dataPath := "s3://warehouse/orders/data/part-1.parquet"
	manifestBytes, err := icebergmetadata.EncodeManifest([]api.ManifestEntry{{
		Status:         api.ManifestEntryAdded,
		SnapshotID:     1,
		SequenceNumber: 0,
		DataFile: api.DataFile{
			Content:         api.DataFileContentData,
			FilePath:        dataPath,
			FileFormat:      "parquet",
			RecordCount:     1,
			FileSizeInBytes: 10,
			SpecID:          0,
			Partition:       map[string]any{"region": "ksa"},
		},
	}}, icebergmetadata.ManifestWriteOptions{
		FormatVersion: 2,
		Schema:        meta.Schemas[0],
		PartitionSpec: meta.PartitionSpecs[0],
		Content:       api.ManifestContentData,
	})
	require.NoError(t, err)
	manifestListBytes, err := icebergmetadata.EncodeManifestList([]api.ManifestFile{{
		Path:            manifestPath,
		Length:          int64(len(manifestBytes)),
		PartitionSpecID: 0,
		Content:         api.ManifestContentData,
	}}, icebergmetadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 1})
	require.NoError(t, err)
	reader := expireObjectReader{objects: map[string][]byte{
		meta.Snapshots[0].ManifestList: manifestListBytes,
		manifestPath:                   manifestBytes,
	}}

	plan, err := (ExpireSnapshotsPlanner{
		Loader:       MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		ObjectReader: reader,
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-04 00:00:00,retain_last=3"))
	require.NoError(t, err)
	require.Equal(t, uint64(3), plan.RemovedFileCount)
	require.ElementsMatch(t, []string{meta.Snapshots[0].ManifestList, manifestPath, dataPath}, plan.PostCommitOrphans)
}

func TestExpireSnapshotsPlannerHonorsTableRetentionProperties(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	meta.Properties[tablePropertyMinSnapshotsToKeep] = "2"
	meta.Properties[tablePropertyMaxSnapshotAgeMS] = "172800000"
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
		Now:    func() time.Time { return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) },
	}).BuildMaintenanceCommit(context.Background(), expireRequest(""))
	require.NoError(t, err)
	require.Equal(t, "2", plan.Attempt.Summary["retain-last"])
	require.Equal(t, []int64{1, 2}, plan.Attempt.Updates[0].SnapshotIDs)

	_, err = (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-05 00:00:00,retain_last=1"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "lower than table retention policy")
}

func TestExpireSnapshotsPlannerReturnsNoOpWhenNoEligibleSnapshots(t *testing.T) {
	current := int64(4)
	meta := expireMetadata(current)
	plan, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) { return meta, nil }),
	}).BuildMaintenanceCommit(context.Background(), expireRequest("older_than=2026-01-05 00:00:00,retain_last=4"))
	require.NoError(t, err)
	require.True(t, plan.NoOp)
	require.Equal(t, current, plan.NoOpSnapshotID)
	require.Zero(t, plan.RemovedFileCount)
}

func TestExpireSnapshotsPlannerRequiresOlderThanOrPolicy(t *testing.T) {
	current := int64(4)
	_, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) {
			meta := expireMetadata(current)
			meta.Properties = nil
			return meta, nil
		}),
	}).BuildMaintenanceCommit(context.Background(), expireRequest(""))
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires older_than")
}

func TestExpireSnapshotsPlannerRejectsInvalidSnapshotBefore(t *testing.T) {
	current := int64(4)
	req := expireRequest("older_than=2026-01-05 00:00:00")
	req.SnapshotBefore = "not-a-snapshot"
	_, err := (ExpireSnapshotsPlanner{
		Loader: MaintenanceTableMetadataLoaderFunc(func(ctx context.Context, req Request) (*api.TableMetadata, error) {
			return expireMetadata(current), nil
		}),
	}).BuildMaintenanceCommit(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot_before")
}

func TestExpireSnapshotsPlannerBoundsManifestListReads(t *testing.T) {
	const manifestList = "s3://warehouse/orders/metadata/snap-1.avro"
	_, err := (ExpireSnapshotsPlanner{
		ObjectReader:      expireObjectReader{objects: map[string][]byte{manifestList: []byte("oversized")}},
		PlanningMaxMemory: 1,
	}).expiredSnapshotFiles(context.Background(), []api.Snapshot{{SnapshotID: 1, ManifestList: manifestList}})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))
}

func TestExpireSnapshotsPlannerBoundsIndividualManifestReads(t *testing.T) {
	const (
		manifestList = "s3://warehouse/orders/metadata/snap-1.avro"
		manifestPath = "s3://warehouse/orders/metadata/m0.avro"
	)
	manifestListBytes, err := icebergmetadata.EncodeManifestList([]api.ManifestFile{{
		Path:            manifestPath,
		PartitionSpecID: 0,
		Content:         api.ManifestContentData,
	}}, icebergmetadata.ManifestListWriteOptions{FormatVersion: 2, SnapshotID: 1})
	require.NoError(t, err)
	listWeight := icebergmetadata.ManifestListMemoryWeight(len(manifestListBytes), []api.ManifestFile{{
		Path:            manifestPath,
		PartitionSpecID: 0,
		Content:         api.ManifestContentData,
	}})

	_, err = (ExpireSnapshotsPlanner{
		ObjectReader: expireObjectReader{objects: map[string][]byte{
			manifestList: manifestListBytes,
			manifestPath: []byte("oversized-manifest"),
		}},
		PlanningMaxMemory: listWeight + int64(len("oversized-manifest")) - 1,
	}).expiredSnapshotFiles(context.Background(), []api.Snapshot{{SnapshotID: 1, ManifestList: manifestList}})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrPlanningLimitExceeded))
}

func expireRequest(options string) Request {
	parsed, err := ParseProcedureCall(ProcedureExpireSnapshots, "ksa_gold.sales.orders", options)
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

func expireMetadata(currentID int64) *api.TableMetadata {
	parent1 := int64(1)
	parent2 := int64(2)
	parent3 := int64(3)
	return &api.TableMetadata{
		FormatVersion:   2,
		Location:        "s3://warehouse/orders",
		CurrentSchemaID: 0,
		Schemas: []api.Schema{{SchemaID: 0, Fields: []api.SchemaField{
			{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}},
			{ID: 2, Name: "region", Type: api.IcebergType{Kind: api.TypeString}},
		}}},
		DefaultSpecID: 0,
		PartitionSpecs: []api.PartitionSpec{{SpecID: 0, Fields: []api.PartitionField{{
			SourceID: 2, FieldID: 1000, Name: "region", Transform: "identity",
		}}}},
		CurrentSnapshotID: &currentID,
		Refs: map[string]api.SnapshotRef{
			"main": {SnapshotID: currentID, Type: "branch"},
		},
		Properties: map[string]string{},
		Snapshots: []api.Snapshot{
			{SnapshotID: 1, TimestampMS: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), ManifestList: "s3://warehouse/orders/metadata/snap-1.avro"},
			{SnapshotID: 2, ParentSnapshotID: &parent1, TimestampMS: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli(), ManifestList: "s3://warehouse/orders/metadata/snap-2.avro"},
			{SnapshotID: 3, ParentSnapshotID: &parent2, TimestampMS: time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC).UnixMilli(), ManifestList: "s3://warehouse/orders/metadata/snap-3.avro"},
			{SnapshotID: 4, ParentSnapshotID: &parent3, TimestampMS: time.Date(2026, 1, 4, 0, 0, 0, 0, time.UTC).UnixMilli(), ManifestList: "s3://warehouse/orders/metadata/snap-4.avro"},
		},
	}
}

type expireObjectReader struct {
	objects map[string][]byte
}

func (r expireObjectReader) Read(ctx context.Context, location string, offset, length int64) ([]byte, error) {
	data := r.objects[location]
	if data == nil {
		return nil, api.NewError(api.ErrObjectIO, "missing expire test object", nil)
	}
	return append([]byte(nil), data...), nil
}
