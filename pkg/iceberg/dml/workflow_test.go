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

package dml

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

var _ ManifestObjectWriter = icebergio.ProviderObjectWriter{}

func TestCommitWorkflowWritesManifestsAndCommits(t *testing.T) {
	stream, err := (NativePlanner{}).PlanUpdate(context.Background(), UpdateRequest{
		Base: testBase(),
		Mode: TableModeMergeOnRead,
		Targets: []UpdateTarget{{
			DeleteTarget: DeleteTarget{
				DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
				MatchedRows:        2,
				EqualityFieldIDs:   []int{1},
				PredicateStable:    true,
				EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
			},
		}},
		AppendedDataFile: []api.DataFile{dataFile("s3://warehouse/orders/replacement-1.parquet")},
	})
	require.NoError(t, err)
	stream.Base.CatalogCapabilities = api.CatalogCapabilities{MetricsReport: true}

	writer := &fakeManifestObjectWriter{}
	committer := &fakeDMLCommitter{result: &api.CommitResult{SnapshotID: 99, CommitID: "commit-99", MetadataLocationHash: "meta-hash"}}
	audit := &fakeDMLAuditRecorder{}
	metrics := &fakeDMLMetricsReporter{}
	result, err := (CommitWorkflow{
		ManifestWriter:  writer,
		Committer:       committer,
		AuditRecorder:   audit,
		MetricsReporter: metrics,
	}).CommitDML(context.Background(), CommitWorkflowRequest{
		Catalog:            api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Stream:             *stream,
		SnapshotID:         99,
		SequenceNumber:     12,
		TimestampMS:        123456,
		DataManifestPath:   "s3://warehouse/orders/metadata/data-99.avro",
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		TableLocation:      "s3://warehouse/orders",
	})
	require.NoError(t, err)
	require.Equal(t, int64(99), result.SnapshotID)
	require.Equal(t, []string{
		"s3://warehouse/orders/metadata/data-99.avro",
		"s3://warehouse/orders/metadata/delete-99.avro",
		"s3://warehouse/orders/metadata/snap-99.avro",
	}, writer.paths)
	require.Len(t, committer.requests, 1)
	req := committer.requests[0]
	require.Equal(t, api.Namespace{"sales"}, req.Namespace)
	require.Equal(t, "orders", req.Table)
	require.Equal(t, "main", req.TargetRef)
	require.Equal(t, "idem-1", req.IdempotencyKey)
	require.Equal(t, []string{"add-snapshot", "set-snapshot-ref"}, commitUpdateTypes(req.Updates))
	require.NotNil(t, req.Updates[0].Snapshot)
	require.Equal(t, int64(99), req.Updates[0].Snapshot.SnapshotID)
	require.Equal(t, int64(123456), req.Updates[0].Snapshot.TimestampMS)
	require.Equal(t, "update", req.Updates[0].Snapshot.Summary["operation"])
	require.Len(t, audit.audits, 1)
	require.Equal(t, "committed", audit.audits[0].Status)
	require.Equal(t, "update", audit.audits[0].SourceBatch)
	require.Equal(t, int64(11), audit.audits[0].RowCount)
	require.Equal(t, 2, audit.audits[0].FileCount)
	require.Equal(t, "commit-99", audit.audits[0].CommitID)
	require.Equal(t, "meta-hash", audit.audits[0].MetadataLocationHash)
	require.Len(t, metrics.reports, 1)
	require.Equal(t, api.MetricsReportWrite, metrics.reports[0].Kind)
	require.Equal(t, int64(99), metrics.reports[0].SnapshotID)
	require.Equal(t, "commit-99", metrics.reports[0].CommitID)
	require.Equal(t, "meta-hash", metrics.reports[0].MetadataLocationHash)
	require.Equal(t, int64(11), metrics.reports[0].Rows)
	require.Equal(t, 2, metrics.reports[0].Files)
	require.Equal(t, "update", metrics.reports[0].Extra["operation"])
	require.Equal(t, "idem-1", metrics.reports[0].Extra["idempotency_key"])
	require.Equal(t, "main", metrics.reports[0].Extra["target_ref"])
	require.NotEmpty(t, metrics.reports[0].Extra["data_manifest"])
	require.NotEmpty(t, metrics.reports[0].Extra["delete_manifest"])
	require.NotEmpty(t, metrics.reports[0].Extra["manifest_list"])
	require.NotContains(t, metrics.reports[0].Extra["data_manifest"], "warehouse")
	require.NotContains(t, metrics.reports[0].Extra["delete_manifest"], "warehouse")
	require.NotContains(t, metrics.reports[0].Extra["manifest_list"], "warehouse")
}

func TestCommitWorkflowRecordsOrphansOnCommitFailure(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        2,
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
		}},
	})
	require.NoError(t, err)
	now := time.Unix(1000, 0).UTC()
	recorder := &fakeDMLOrphanRecorder{}
	audit := &fakeDMLAuditRecorder{}
	_, err = (CommitWorkflow{
		ManifestWriter: &fakeManifestObjectWriter{},
		Committer:      &fakeDMLCommitter{err: api.NewError(api.ErrCommitConflict, "conflict", nil)},
		OrphanRecorder: recorder,
		AuditRecorder:  audit,
		Now:            func() time.Time { return now },
		OrphanTTL:      time.Hour,
	}).CommitDML(context.Background(), CommitWorkflowRequest{
		Catalog:            api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Stream:             *stream,
		SnapshotID:         99,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		TableLocation:      "s3://warehouse/orders",
	})
	require.Error(t, err)
	require.Len(t, recorder.candidates, 3)
	paths := make([]string, 0, len(recorder.candidates))
	for _, candidate := range recorder.candidates {
		require.Equal(t, uint32(7), candidate.AccountID)
		require.Equal(t, uint64(42), candidate.CatalogID)
		require.Equal(t, "idem-1", candidate.JobID)
		require.Equal(t, "pending", candidate.CleanupStatus)
		require.Equal(t, now.Add(time.Hour), candidate.ExpireAt)
		require.NotContains(t, candidate.FilePathRedacted, "warehouse")
		paths = append(paths, candidate.FilePath)
	}
	require.ElementsMatch(t, []string{
		"s3://warehouse/orders/delete-1.parquet",
		"s3://warehouse/orders/metadata/delete-99.avro",
		"s3://warehouse/orders/metadata/snap-99.avro",
	}, paths)
	require.Len(t, audit.audits, 1)
	require.Equal(t, "failed", audit.audits[0].Status)
	require.Equal(t, string(api.ErrCommitConflict), audit.audits[0].ErrorCategory)
	require.Equal(t, "delete", audit.audits[0].SourceBatch)
	require.Equal(t, int64(1), audit.audits[0].RowCount)
}

func TestCommitWorkflowRequiresWriterAndCommitter(t *testing.T) {
	stream, err := (NativePlanner{}).PlanOverwrite(context.Background(), OverwriteRequest{
		Base:             testBase(),
		ReplacementFiles: []api.DataFile{dataFile("s3://warehouse/orders/new.parquet")},
	})
	require.NoError(t, err)
	_, err = (CommitWorkflow{}).CommitDML(context.Background(), CommitWorkflowRequest{Stream: *stream})
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest writer")

	_, err = (CommitWorkflow{ManifestWriter: &fakeManifestObjectWriter{}}).CommitDML(context.Background(), CommitWorkflowRequest{Stream: *stream})
	require.Error(t, err)
	require.Contains(t, err.Error(), "committer")
}

func TestBuildManifestCommitAttemptRequiresPositiveSnapshotAndSequence(t *testing.T) {
	intent, err := BuildCommitIntent(ActionStream{
		Operation: OperationOverwrite,
		Base:      testBase(),
		Actions: []Action{{
			Kind: ActionAppendData,
			File: dataFile("s3://warehouse/orders/new.parquet"),
		}},
	})
	require.NoError(t, err)
	_, err = BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:           *intent,
		SnapshotID:       -1,
		SequenceNumber:   12,
		DataManifestPath: "s3://warehouse/orders/metadata/data.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/snap.avro",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot and sequence numbers")

	_, err = BuildManifestCommitAttempt(context.Background(), ManifestMaterializeRequest{
		Intent:           *intent,
		SnapshotID:       99,
		SequenceNumber:   -1,
		DataManifestPath: "s3://warehouse/orders/metadata/data.avro",
		ManifestListPath: "s3://warehouse/orders/metadata/snap.avro",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot and sequence numbers")
}

func TestCommitWorkflowTreatsAuditAndMetricsAsBestEffort(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        2,
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
		}},
	})
	require.NoError(t, err)
	stream.Base.CatalogCapabilities = api.CatalogCapabilities{MetricsReport: true}
	result, err := (CommitWorkflow{
		ManifestWriter:  &fakeManifestObjectWriter{},
		Committer:       &fakeDMLCommitter{result: &api.CommitResult{SnapshotID: 99, CommitID: "commit-99"}},
		AuditRecorder:   &fakeDMLAuditRecorder{err: api.NewError(api.ErrInternal, "audit down", nil)},
		MetricsReporter: &fakeDMLMetricsReporter{err: api.NewError(api.ErrInternal, "metrics down", nil)},
	}).CommitDML(context.Background(), CommitWorkflowRequest{
		Catalog:            api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Stream:             *stream,
		SnapshotID:         99,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		TableLocation:      "s3://warehouse/orders",
	})
	require.NoError(t, err)
	require.Equal(t, int64(99), result.SnapshotID)
}

func TestDMLActionCountsIncludeRemovedAndRewrittenDataFiles(t *testing.T) {
	stream := ActionStream{
		Operation: OperationOverwrite,
		Base:      testBase(),
		Actions: []Action{
			{
				Kind:         ActionDeleteDataFile,
				ReplacedFile: dataFile("s3://warehouse/orders/delete-old.parquet"),
			},
			{
				Kind:         ActionRewriteDataFile,
				ReplacedFile: dataFile("s3://warehouse/orders/rewrite-old.parquet"),
				ReplacementFiles: []api.DataFile{
					dataFile("s3://warehouse/orders/rewrite-new-1.parquet"),
					dataFile("s3://warehouse/orders/rewrite-new-2.parquet"),
				},
			},
			{
				Kind: ActionAppendData,
				File: dataFile("s3://warehouse/orders/append-new.parquet"),
			},
		},
	}
	require.Equal(t, int64(50), dmlActionRecordCount(stream))
	require.Equal(t, 5, dmlActionFileCount(stream))
}

func TestCommitWorkflowVerifierResolvesUnknownCommit(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        2,
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
		}},
	})
	require.NoError(t, err)
	verifier := &fakeDMLVerifier{verified: &api.CommitResult{SnapshotID: 101, CommitID: "verified-101"}}
	orphan := &fakeDMLOrphanRecorder{}
	audit := &fakeDMLAuditRecorder{}
	result, err := (CommitWorkflow{
		ManifestWriter: &fakeManifestObjectWriter{},
		Committer:      &fakeDMLCommitter{err: api.NewError(api.ErrCommitUnknown, "timeout after submit", nil)},
		Verifier:       verifier,
		OrphanRecorder: orphan,
		AuditRecorder:  audit,
	}).CommitDML(context.Background(), CommitWorkflowRequest{
		Catalog:            api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Stream:             *stream,
		SnapshotID:         99,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		TableLocation:      "s3://warehouse/orders",
	})
	require.NoError(t, err)
	require.Equal(t, int64(101), result.SnapshotID)
	require.True(t, result.Verified)
	require.Equal(t, 1, verifier.calls)
	require.Empty(t, orphan.candidates)
	require.Len(t, audit.audits, 1)
	require.Equal(t, "committed", audit.audits[0].Status)
	require.Equal(t, "verified-101", audit.audits[0].CommitID)
	require.Equal(t, int64(1), audit.audits[0].RowCount)
	require.Equal(t, 1, audit.audits[0].FileCount)
}

func TestCommitWorkflowAuditsUnverifiedUnknownCommit(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        2,
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
		}},
	})
	require.NoError(t, err)
	orphan := &fakeDMLOrphanRecorder{}
	audit := &fakeDMLAuditRecorder{}
	_, err = (CommitWorkflow{
		ManifestWriter: &fakeManifestObjectWriter{},
		Committer:      &fakeDMLCommitter{err: api.NewError(api.ErrCommitUnknown, "timeout after submit", nil)},
		Verifier:       &fakeDMLVerifier{},
		OrphanRecorder: orphan,
		AuditRecorder:  audit,
	}).CommitDML(context.Background(), CommitWorkflowRequest{
		Catalog:            api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Stream:             *stream,
		SnapshotID:         99,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		TableLocation:      "s3://warehouse/orders",
	})
	require.Error(t, err)
	require.Len(t, orphan.candidates, 3)
	require.Len(t, audit.audits, 1)
	require.Equal(t, "unknown", audit.audits[0].Status)
	require.Equal(t, string(api.ErrCommitUnknown), audit.audits[0].ErrorCategory)
	require.Equal(t, "delete", audit.audits[0].SourceBatch)
	require.Equal(t, int64(1), audit.audits[0].RowCount)
	require.Equal(t, 1, audit.audits[0].FileCount)
}

func TestCommitWorkflowVerifierFailureDoesNotReverseCommittedResult(t *testing.T) {
	stream, err := (NativePlanner{}).PlanDelete(context.Background(), DeleteRequest{
		Base: testBase(),
		Targets: []DeleteTarget{{
			DataFile:           dataFile("s3://warehouse/orders/data-1.parquet"),
			MatchedRows:        2,
			EqualityFieldIDs:   []int{1},
			PredicateStable:    true,
			EqualityDeleteFile: deleteFile("s3://warehouse/orders/delete-1.parquet"),
		}},
	})
	require.NoError(t, err)
	audit := &fakeDMLAuditRecorder{}
	result, err := (CommitWorkflow{
		ManifestWriter: &fakeManifestObjectWriter{},
		Committer:      &fakeDMLCommitter{result: &api.CommitResult{SnapshotID: 99, CommitID: "commit-99", Verified: true}},
		Verifier:       &fakeDMLVerifier{err: api.NewError(api.ErrCatalogUnavailable, "catalog reload failed", nil)},
		AuditRecorder:  audit,
	}).CommitDML(context.Background(), CommitWorkflowRequest{
		Catalog:            api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Stream:             *stream,
		SnapshotID:         99,
		SequenceNumber:     12,
		DeleteManifestPath: "s3://warehouse/orders/metadata/delete-99.avro",
		ManifestListPath:   "s3://warehouse/orders/metadata/snap-99.avro",
		TableLocation:      "s3://warehouse/orders",
	})
	require.NoError(t, err)
	require.Equal(t, int64(99), result.SnapshotID)
	require.Equal(t, "commit-99", result.CommitID)
	require.False(t, result.Verified)
	require.Len(t, audit.audits, 1)
	require.Equal(t, "commit-99", audit.audits[0].CommitID)
}

type fakeManifestObjectWriter struct {
	paths []string
	err   error
}

func (w *fakeManifestObjectWriter) WriteManifestObject(ctx context.Context, location string, payload []byte) error {
	w.paths = append(w.paths, location)
	if w.err != nil {
		return w.err
	}
	if len(payload) == 0 {
		return api.NewError(api.ErrMetadataInvalid, "empty payload", nil)
	}
	return nil
}

type fakeDMLCommitter struct {
	requests []api.CommitRequest
	result   *api.CommitResult
	err      error
}

func (c *fakeDMLCommitter) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}
	if c.result == nil {
		return &api.CommitResult{SnapshotID: 1}, nil
	}
	return c.result, nil
}

type fakeDMLVerifier struct {
	calls    int
	verified *api.CommitResult
	err      error
}

func (v *fakeDMLVerifier) VerifyDMLCommit(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error) {
	v.calls++
	if v.err != nil {
		return nil, false, v.err
	}
	if v.verified == nil {
		return nil, false, nil
	}
	return v.verified, true, nil
}

type fakeDMLOrphanRecorder struct {
	candidates []icebergwrite.OrphanCandidate
}

func (r *fakeDMLOrphanRecorder) RecordOrphans(ctx context.Context, candidates []icebergwrite.OrphanCandidate) error {
	r.candidates = append(r.candidates, candidates...)
	return nil
}

type fakeDMLAuditRecorder struct {
	audits []icebergwrite.PublishAudit
	err    error
}

func (r *fakeDMLAuditRecorder) RecordPublish(ctx context.Context, audit icebergwrite.PublishAudit) error {
	r.audits = append(r.audits, audit)
	return r.err
}

type fakeDMLMetricsReporter struct {
	reports []api.MetricsReportRequest
	err     error
}

func (r *fakeDMLMetricsReporter) ReportMetrics(ctx context.Context, req api.MetricsReportRequest) error {
	r.reports = append(r.reports, req)
	return r.err
}
