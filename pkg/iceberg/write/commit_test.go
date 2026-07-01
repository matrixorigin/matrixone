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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestAppendBuilderBuildsRequirementsUpdatesAndSummary(t *testing.T) {
	req := appendRequest()
	attempt, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "idem-1", attempt.IdempotencyKey)
	require.Equal(t, int64(100), attempt.BaseSnapshotID)
	require.Len(t, attempt.Requirements, 4)
	require.Equal(t, "assert-ref-snapshot-id", attempt.Requirements[0].Type)
	require.Equal(t, int64(100), attempt.Requirements[0].SnapshotID)
	require.Equal(t, "assert-current-schema-id", attempt.Requirements[2].Type)
	require.Equal(t, 3, attempt.Requirements[2].SchemaID)
	require.Len(t, attempt.Updates, 2)
	require.Equal(t, "add-data-file", attempt.Updates[0].Type)
	require.NotNil(t, attempt.Updates[0].DataFile)
	require.Equal(t, "set-snapshot-summary", attempt.Updates[1].Type)
	require.Equal(t, "append", attempt.Summary["operation"])
	require.Equal(t, "2", attempt.Summary["added-records"])
}

func TestValidateAppendPreflightRunsBeforeDataFilesExist(t *testing.T) {
	req := appendRequest()
	req.DataFiles = nil
	require.NoError(t, ValidateAppendPreflight(req))

	req.WriterOwnerAccountID = 0
	err := ValidateAppendPreflight(req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
	require.Contains(t, err.Error(), "single-writer owner")
}

func TestAppendBuilderUsesRefNotExistsForEmptyTable(t *testing.T) {
	req := appendRequest()
	req.BaseSnapshotID = 0
	attempt, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "assert-ref-not-exists", attempt.Requirements[0].Type)
	require.Zero(t, attempt.Requirements[0].SnapshotID)
}

func TestAppendBuilderAssertsSchemaAndSpecIDZero(t *testing.T) {
	req := appendRequest()
	req.BaseSchemaID = 0
	req.BaseSpecID = 0
	req.BaseSchema = baseSchema(0)
	req.BaseSpec = baseSpec(0)
	req.DataFiles[0].SpecID = 0
	attempt, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 0, schemaRequirement(t, api.CommitRequest{Requirements: attempt.Requirements}).SchemaID)
	require.Equal(t, 0, specRequirement(t, api.CommitRequest{Requirements: attempt.Requirements}).SpecID)
}

func TestAppendBuilderNormalizesBranchRefAndRejectsReadOnlyRefs(t *testing.T) {
	req := appendRequest()
	req.TargetRef = "branch:publish"
	attempt, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "publish", attempt.TargetRef)
	require.Equal(t, "publish", attempt.Requirements[0].Ref)

	req = appendRequest()
	req.TargetRef = "release"
	req.TargetRefType = "tag"
	req.CatalogCapabilities = api.CatalogCapabilities{BranchTag: true}
	_, err = (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
	require.Contains(t, err.Error(), "tag refs are read-only")

	req.AllowTagMove = true
	attempt, err = (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "release", attempt.Requirements[0].Ref)

	req = appendRequest()
	req.TargetRef = "hash:abc123"
	_, err = (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only")

	req = appendRequest()
	req.TargetRef = "snapshot:99"
	_, err = (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only")
}

func TestAppendBuilderRejectsMissingWriterOwner(t *testing.T) {
	req := appendRequest()
	req.WriterOwnerAccountID = 0
	_, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
	require.Contains(t, err.Error(), "single-writer owner")
}

func TestAppendBuilderRejectsMismatchedWriterOwner(t *testing.T) {
	req := appendRequest()
	req.WriterOwnerAccountID = 8
	_, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
	require.Contains(t, err.Error(), "writer owner does not match")
}

func TestAppendBuilderRejectsIncompatibleWriteMetadata(t *testing.T) {
	req := appendRequest()
	req.DataFiles[0].Partition = nil
	_, err := (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrMetadataInvalid))
	require.Contains(t, err.Error(), "partition tuple")

	req = appendRequest()
	req.DataFiles[0].NullValueCounts[1] = 1
	_, err = (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrMetadataInvalid))
	require.Contains(t, err.Error(), "required column contains nulls")

	req = appendRequest()
	req.DataFiles[0].SpecID = 99
	_, err = (AppendBuilder{}).BuildAppend(context.Background(), req)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrMetadataInvalid))
	require.Contains(t, err.Error(), "unknown partition spec")
}

func TestAppendWorkflowRetriesCompatibleConflictAndRunsSuccessHooks(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{
		{err: api.NewError(api.ErrCommitConflict, "snapshot changed", nil)},
		{result: &api.CommitResult{SnapshotID: 101, MetadataLocationHash: "hash-101", CommitID: "commit-101"}},
	}}
	cache := &fakeCacheInvalidator{}
	audit := &fakeAuditRecorder{}
	workflow := AppendWorkflow{
		Committer: committer,
		ReloadBase: func(ctx context.Context, req api.AppendRequest) (BaseState, error) {
			return BaseState{SnapshotID: 101, SchemaID: req.BaseSchemaID, SpecID: req.BaseSpecID}, nil
		},
		CacheInvalidator: cache,
		AuditRecorder:    audit,
		MaxConflictRetry: 1,
	}
	result, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(101), result.SnapshotID)
	require.Len(t, committer.requests, 2)
	require.Equal(t, int64(101), committer.requests[1].Requirements[0].SnapshotID)
	require.Equal(t, 1, cache.calls)
	require.Len(t, audit.audits, 1)
	require.Equal(t, int64(2), audit.audits[0].RowCount)
}

func TestAppendWorkflowTreatsPostCommitHooksAsBestEffort(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{
		{result: &api.CommitResult{SnapshotID: 101, MetadataLocationHash: "hash-101", CommitID: "commit-101"}},
	}}
	cache := &fakeCacheInvalidator{err: api.NewError(api.ErrCatalogUnavailable, "remote invalidation failed", nil)}
	audit := &fakeAuditRecorder{err: api.NewError(api.ErrObjectIO, "audit sink failed", nil)}
	workflow := AppendWorkflow{
		Committer:        committer,
		CacheInvalidator: cache,
		AuditRecorder:    audit,
	}
	result, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(101), result.SnapshotID)
	require.Equal(t, 1, cache.calls)
	require.Len(t, audit.audits, 1)
}

func TestAppendWorkflowReportsWriteMetricsWhenCatalogSupportsIt(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{
		{result: &api.CommitResult{SnapshotID: 101, MetadataLocationHash: "hash-101", CommitID: "commit-101"}},
	}}
	metrics := &fakeMetricsReporter{}
	req := appendRequest()
	req.CatalogCapabilities = api.CatalogCapabilities{MetricsReport: true}
	workflow := AppendWorkflow{
		Committer:       committer,
		MetricsReporter: metrics,
	}
	result, err := workflow.CommitAppend(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int64(101), result.SnapshotID)
	require.Len(t, metrics.reports, 1)
	report := metrics.reports[0]
	require.Equal(t, api.MetricsReportWrite, report.Kind)
	require.Equal(t, "orders", report.Table)
	require.Equal(t, "main", report.Ref)
	require.Equal(t, int64(2), report.Rows)
	require.Equal(t, 1, report.Files)
	require.Equal(t, "commit-101", report.CommitID)
}

func TestAppendWorkflowSkipsWriteMetricsWithoutCapability(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{
		{result: &api.CommitResult{SnapshotID: 101, MetadataLocationHash: "hash-101", CommitID: "commit-101"}},
	}}
	metrics := &fakeMetricsReporter{}
	workflow := AppendWorkflow{
		Committer:       committer,
		MetricsReporter: metrics,
	}
	_, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Empty(t, metrics.reports)
}

func TestAppendWorkflowRetriesAddOptionalColumnWhenPolicyAllows(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{
		{err: api.NewError(api.ErrCommitConflict, "schema changed", nil)},
		{result: &api.CommitResult{SnapshotID: 101, MetadataLocationHash: "hash-101", CommitID: "commit-101"}},
	}}
	workflow := AppendWorkflow{
		Committer: committer,
		ReloadBase: func(ctx context.Context, req api.AppendRequest) (BaseState, error) {
			newSchema := req.BaseSchema
			newSchema.SchemaID = req.BaseSchemaID + 1
			newSchema.Fields = append(newSchema.Fields, api.SchemaField{
				ID: 2, Name: "note", Type: api.IcebergType{Kind: api.TypeString}, Required: false,
			})
			return BaseState{SnapshotID: 101, SchemaID: newSchema.SchemaID, SpecID: req.BaseSpecID, Schema: newSchema}, nil
		},
		RetryPolicy:      CompatibleRetryPolicy{AllowAddOptionalColumns: true},
		MaxConflictRetry: 1,
	}
	result, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(101), result.SnapshotID)
	require.Len(t, committer.requests, 2)
	require.Equal(t, 4, schemaRequirement(t, committer.requests[1]).SchemaID)
}

func TestAppendWorkflowRejectsRequiredColumnEvenWhenPolicyAllowsOptionalAdds(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{{err: api.NewError(api.ErrCommitConflict, "schema changed", nil)}}}
	orphan := &fakeOrphanRecorder{}
	workflow := AppendWorkflow{
		Committer: committer,
		ReloadBase: func(ctx context.Context, req api.AppendRequest) (BaseState, error) {
			newSchema := req.BaseSchema
			newSchema.SchemaID = req.BaseSchemaID + 1
			newSchema.Fields = append(newSchema.Fields, api.SchemaField{
				ID: 2, Name: "must_have", Type: api.IcebergType{Kind: api.TypeString}, Required: true,
			})
			return BaseState{SnapshotID: 101, SchemaID: newSchema.SchemaID, SpecID: req.BaseSpecID, Schema: newSchema}, nil
		},
		RetryPolicy:      CompatibleRetryPolicy{AllowAddOptionalColumns: true},
		OrphanRecorder:   orphan,
		MaxConflictRetry: 1,
	}
	_, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrCommitConflict))
	require.Len(t, committer.requests, 1)
	require.Len(t, orphan.candidates, 1)
}

func TestAppendWorkflowRetriesCompatiblePartitionSpecEvolution(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{
		{err: api.NewError(api.ErrCommitConflict, "partition spec changed", nil)},
		{result: &api.CommitResult{SnapshotID: 101, MetadataLocationHash: "hash-101", CommitID: "commit-101"}},
	}}
	workflow := AppendWorkflow{
		Committer: committer,
		ReloadBase: func(ctx context.Context, req api.AppendRequest) (BaseState, error) {
			return BaseState{
				SnapshotID: 101,
				SchemaID:   req.BaseSchemaID,
				SpecID:     req.BaseSpecID + 1,
				Spec:       api.PartitionSpec{SpecID: req.BaseSpecID + 1, Fields: []api.PartitionField{{SourceID: 1, FieldID: 1001, Name: "id_day", Transform: "day"}}},
				KnownSpecs: []api.PartitionSpec{req.BaseSpec},
			}, nil
		},
		RetryPolicy:      CompatibleRetryPolicy{AllowPartitionSpecEvolution: true},
		MaxConflictRetry: 1,
	}
	result, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(101), result.SnapshotID)
	require.Len(t, committer.requests, 2)
	require.Equal(t, 8, specRequirement(t, committer.requests[1]).SpecID)
	require.NotNil(t, committer.requests[1].Updates[0].DataFile)
	require.Equal(t, 7, committer.requests[1].Updates[0].DataFile.SpecID)
}

func TestAppendWorkflowStopsOnIncompatibleConflictAndRecordsOrphans(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{{err: api.NewError(api.ErrCommitConflict, "schema changed", nil)}}}
	orphan := &fakeOrphanRecorder{}
	cleaner := &fakeOrphanCleaner{}
	audit := &fakeAuditRecorder{}
	workflow := AppendWorkflow{
		Committer: committer,
		ReloadBase: func(ctx context.Context, req api.AppendRequest) (BaseState, error) {
			return BaseState{SnapshotID: 101, SchemaID: req.BaseSchemaID + 1, SpecID: req.BaseSpecID}, nil
		},
		OrphanRecorder:   orphan,
		OrphanCleaner:    cleaner,
		AuditRecorder:    audit,
		MaxConflictRetry: 1,
		Now:              func() time.Time { return time.Unix(10, 0).UTC() },
		OrphanTTL:        time.Hour,
	}
	_, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrCommitConflict))
	require.Len(t, committer.requests, 1)
	require.Len(t, orphan.candidates, 1)
	require.Equal(t, "pending", orphan.candidates[0].CleanupStatus)
	require.Equal(t, time.Unix(10, 0).UTC().Add(time.Hour), orphan.candidates[0].ExpireAt)
	require.Equal(t, api.PathHash("s3://warehouse/sales/orders"), orphan.candidates[0].TableLocationHash)
	require.Empty(t, cleaner.cleaned)
	require.Len(t, audit.audits, 1)
	require.Equal(t, "failed", audit.audits[0].Status)
	require.Equal(t, string(api.ErrCommitConflict), audit.audits[0].ErrorCategory)
}

func TestAppendWorkflowImmediateCleanupIsExplicitOptIn(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{{err: api.NewError(api.ErrCommitConflict, "schema changed", nil)}}}
	orphan := &fakeOrphanRecorder{}
	cleaner := &fakeOrphanCleaner{}
	workflow := AppendWorkflow{
		Committer: committer,
		ReloadBase: func(ctx context.Context, req api.AppendRequest) (BaseState, error) {
			return BaseState{SnapshotID: 101, SchemaID: req.BaseSchemaID + 1, SpecID: req.BaseSpecID}, nil
		},
		OrphanRecorder:         orphan,
		OrphanCleaner:          cleaner,
		ImmediateOrphanCleanup: true,
		MaxConflictRetry:       1,
	}
	_, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.Error(t, err)
	require.Len(t, orphan.candidates, 1)
	require.Len(t, cleaner.cleaned, 1)
}

func TestAppendWorkflowVerifiesUnknownCommit(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{{result: &api.CommitResult{Unknown: true}}}}
	verifier := &fakeVerifier{verified: &api.CommitResult{SnapshotID: 200, CommitID: "commit-200"}}
	orphan := &fakeOrphanRecorder{}
	cleaner := &fakeOrphanCleaner{}
	audit := &fakeAuditRecorder{}
	workflow := AppendWorkflow{
		Committer:              committer,
		Verifier:               verifier,
		OrphanRecorder:         orphan,
		OrphanCleaner:          cleaner,
		ImmediateOrphanCleanup: true,
		AuditRecorder:          audit,
	}
	result, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(200), result.SnapshotID)
	require.True(t, result.Verified)
	require.Equal(t, 1, verifier.calls)
	require.Len(t, committer.requests, 1)
	require.Empty(t, orphan.candidates)
	require.Empty(t, cleaner.cleaned)
	require.Len(t, audit.audits, 1)
	require.Equal(t, "committed", audit.audits[0].Status)
	require.Equal(t, "commit-200", audit.audits[0].CommitID)
}

func TestMetadataCacheInvalidatorInvalidatesLocalAndIgnoresRemoteFailure(t *testing.T) {
	cache := &fakeTableCache{}
	remote := RemoteCacheInvalidatorFunc(func(ctx context.Context, req api.AppendRequest, result api.CommitResult) error {
		return api.NewError(api.ErrCatalogUnavailable, "remote CN did not acknowledge invalidation", nil)
	})
	err := MetadataCacheInvalidator{Cache: cache, Remote: remote}.InvalidateIcebergTable(context.Background(), appendRequest(), api.CommitResult{SnapshotID: 200})
	require.NoError(t, err)
	require.Equal(t, uint32(7), cache.accountID)
	require.Equal(t, uint64(42), cache.catalogID)
	require.Equal(t, "sales", cache.namespace)
	require.Equal(t, "orders", cache.table)
}

func TestAppendWorkflowUnknownUnverifiedRecordsOrphans(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{{err: api.NewError(api.ErrCommitUnknown, "timeout after submit", nil)}}}
	orphan := &fakeOrphanRecorder{}
	cleaner := &fakeOrphanCleaner{}
	audit := &fakeAuditRecorder{}
	workflow := AppendWorkflow{
		Committer:              committer,
		Verifier:               &fakeVerifier{},
		OrphanRecorder:         orphan,
		OrphanCleaner:          cleaner,
		ImmediateOrphanCleanup: true,
		AuditRecorder:          audit,
	}
	_, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrCommitUnknown))
	require.Len(t, committer.requests, 1)
	require.Len(t, orphan.candidates, 1)
	require.False(t, strings.Contains(orphan.candidates[0].FilePathRedacted, "warehouse"))
	require.Empty(t, cleaner.cleaned, "unknown commit results must remain record-only until committed metadata verification proves the file is unreferenced")
	require.Len(t, audit.audits, 1)
	require.Equal(t, "unknown", audit.audits[0].Status)
	require.Equal(t, string(api.ErrCommitUnknown), audit.audits[0].ErrorCategory)
}

func TestAppendWorkflowUnknownVerifyErrorDoesNotCleanImmediately(t *testing.T) {
	committer := &fakeCommitter{results: []commitOutcome{{result: &api.CommitResult{Unknown: true}}}}
	orphan := &fakeOrphanRecorder{}
	cleaner := &fakeOrphanCleaner{}
	audit := &fakeAuditRecorder{}
	workflow := AppendWorkflow{
		Committer:              committer,
		Verifier:               &fakeVerifier{err: api.NewError(api.ErrMetadataIOTimeout, "metadata reload timed out", nil)},
		OrphanRecorder:         orphan,
		OrphanCleaner:          cleaner,
		ImmediateOrphanCleanup: true,
		AuditRecorder:          audit,
	}
	_, err := workflow.CommitAppend(context.Background(), appendRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrMetadataIOTimeout))
	require.Len(t, committer.requests, 1)
	require.Len(t, orphan.candidates, 1)
	require.Empty(t, cleaner.cleaned, "verification failures after an unknown commit may mask a successful catalog commit")
	require.Len(t, audit.audits, 1)
	require.Equal(t, "unknown", audit.audits[0].Status)
	require.Equal(t, string(api.ErrMetadataIOTimeout), audit.audits[0].ErrorCategory)
}

func appendRequest() api.AppendRequest {
	return api.AppendRequest{
		CatalogRequest:       api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Namespace:            api.Namespace{"sales"},
		Table:                "orders",
		TableLocation:        "s3://warehouse/sales/orders",
		TargetRef:            "main",
		TableUUID:            "table-uuid",
		BaseSnapshotID:       100,
		BaseSchemaID:         3,
		BaseSpecID:           7,
		BaseSchema:           baseSchema(3),
		BaseSpec:             baseSpec(7),
		WriterOwnerAccountID: 7,
		IdempotencyKey:       "idem-1",
		SourceBatch:          "batch-1",
		DataFiles: []api.DataFile{{
			FilePath:         "s3://warehouse/sales/orders/data/part-1.parquet",
			FilePathHash:     "file-hash",
			FilePathRedacted: api.RedactPath("s3://warehouse/sales/orders/data/part-1.parquet"),
			FileFormat:       "parquet",
			Partition:        map[string]any{"id": int64(1)},
			RecordCount:      2,
			FileSizeInBytes:  10,
			ValueCounts:      map[int]int64{1: 2},
			NullValueCounts:  map[int]int64{1: 0},
			SpecID:           7,
		}},
		PublishAuditHint: api.PublishAuditHint{
			JobID:       "job-1",
			SourceDB:    "gold",
			SourceTable: "orders",
		},
	}
}

func baseSchema(id int) api.Schema {
	return api.Schema{
		SchemaID: id,
		Fields: []api.SchemaField{{
			ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}, Required: true,
		}},
		IdentifierFieldIDs: []int{1},
	}
}

func baseSpec(id int) api.PartitionSpec {
	return api.PartitionSpec{SpecID: id, Fields: []api.PartitionField{{
		SourceID: 1, FieldID: 1000, Name: "id", Transform: "identity",
	}}}
}

func schemaRequirement(t *testing.T, req api.CommitRequest) api.CommitRequirement {
	t.Helper()
	for _, requirement := range req.Requirements {
		if requirement.Type == "assert-current-schema-id" {
			return requirement
		}
	}
	t.Fatalf("missing schema requirement: %+v", req.Requirements)
	return api.CommitRequirement{}
}

func specRequirement(t *testing.T, req api.CommitRequest) api.CommitRequirement {
	t.Helper()
	for _, requirement := range req.Requirements {
		if requirement.Type == "assert-default-spec-id" {
			return requirement
		}
	}
	t.Fatalf("missing spec requirement: %+v", req.Requirements)
	return api.CommitRequirement{}
}

type commitOutcome struct {
	result *api.CommitResult
	err    error
}

type fakeCommitter struct {
	results  []commitOutcome
	requests []api.CommitRequest
}

func (c *fakeCommitter) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	c.requests = append(c.requests, req)
	if len(c.results) == 0 {
		return &api.CommitResult{SnapshotID: 1}, nil
	}
	out := c.results[0]
	c.results = c.results[1:]
	return out.result, out.err
}

type fakeVerifier struct {
	calls    int
	verified *api.CommitResult
	err      error
}

func (v *fakeVerifier) VerifyCommit(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, result *api.CommitResult) (*api.CommitResult, bool, error) {
	v.calls++
	if v.err != nil {
		return nil, false, v.err
	}
	if v.verified == nil {
		return nil, false, nil
	}
	return v.verified, true, nil
}

type fakeOrphanRecorder struct {
	candidates []OrphanCandidate
}

func (r *fakeOrphanRecorder) RecordOrphans(ctx context.Context, candidates []OrphanCandidate) error {
	r.candidates = append(r.candidates, candidates...)
	return nil
}

type fakeOrphanCleaner struct {
	cleaned []OrphanCandidate
}

func (c *fakeOrphanCleaner) CleanupOrphan(ctx context.Context, candidate OrphanCandidate) error {
	c.cleaned = append(c.cleaned, candidate)
	return nil
}

type fakeAuditRecorder struct {
	audits []PublishAudit
	err    error
}

func (r *fakeAuditRecorder) RecordPublish(ctx context.Context, audit PublishAudit) error {
	r.audits = append(r.audits, audit)
	return r.err
}

type fakeCacheInvalidator struct {
	calls int
	err   error
}

func (i *fakeCacheInvalidator) InvalidateIcebergTable(ctx context.Context, req api.AppendRequest, result api.CommitResult) error {
	i.calls++
	return i.err
}

type fakeMetricsReporter struct {
	reports []api.MetricsReportRequest
	err     error
}

func (r *fakeMetricsReporter) ReportMetrics(ctx context.Context, req api.MetricsReportRequest) error {
	r.reports = append(r.reports, req)
	return r.err
}

type fakeTableCache struct {
	accountID uint32
	catalogID uint64
	namespace string
	table     string
}

func (c *fakeTableCache) InvalidateTable(accountID uint32, catalogID uint64, namespace, table string) int {
	c.accountID = accountID
	c.catalogID = catalogID
	c.namespace = namespace
	c.table = table
	return 1
}
