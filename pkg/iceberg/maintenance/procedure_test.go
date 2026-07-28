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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

func TestParseProcedureCalls(t *testing.T) {
	call, err := ParseProcedureCall(ProcedureRewriteDataFiles, "ksa_gold.sales.orders", "ref=main,target_file_size=268435456")
	require.NoError(t, err)
	require.Equal(t, OperationRewriteDataFiles, call.Operation)
	require.Equal(t, TargetIdentifier{Catalog: "ksa_gold", Namespace: "sales", Table: "orders"}, call.TargetID)
	require.Equal(t, "main", TargetRef(call.Options))
	size, ok, err := UintOption(call.Options, "target_file_size")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(268435456), size)

	call, err = ParseProcedureCall(ProcedureRewriteManifests, "ksa_gold.sales.orders", "ref=audit")
	require.NoError(t, err)
	require.Equal(t, OperationRewriteManifests, call.Operation)
	require.Equal(t, "audit", TargetRef(call.Options))

	call, err = ParseProcedureCall(ProcedureExpireSnapshots, "ksa_gold.sales.orders", "older_than=2026-01-01 00:00:00,retain_last=3")
	require.NoError(t, err)
	require.Equal(t, OperationExpireSnapshots, call.Operation)
	require.Equal(t, "3", call.Options["retain_last"])
}

func TestParseTargetIdentifier(t *testing.T) {
	target, err := ParseTargetIdentifier("ksa_gold.sales.orders")
	require.NoError(t, err)
	require.Equal(t, TargetIdentifier{Catalog: "ksa_gold", Namespace: "sales", Table: "orders"}, target)

	target, err = ParseTargetIdentifier("ksa_gold.prod.sales.orders")
	require.NoError(t, err)
	require.Equal(t, TargetIdentifier{Catalog: "ksa_gold", Namespace: "prod.sales", Table: "orders"}, target)

	_, err = ParseTargetIdentifier("sales.orders")
	require.Error(t, err)
	require.Contains(t, err.Error(), "catalog.namespace.table")

	_, err = ParseTargetIdentifier("ksa_gold..orders")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty identifier")
}

func TestDispatcherRecordsJobStateAndVerifiesUnknownResult(t *testing.T) {
	store := &fakeJobRecorder{}
	dispatcher := Dispatcher{
		Runners: map[Operation]Runner{
			OperationRewriteDataFiles: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
				return Result{SnapshotAfter: "101", RewrittenFileCount: 2, Unknown: true}, nil
			}),
		},
		Recorder: store,
		Verifier: UnknownVerifierFunc(func(ctx context.Context, req Request, result Result) (Result, bool, error) {
			result.Verified = true
			result.Unknown = false
			return result, true, nil
		}),
		Now: func() time.Time { return time.Unix(100, 0) },
	}
	result, err := dispatcher.Dispatch(context.Background(), Request{
		AccountID:      1,
		CatalogID:      2,
		Namespace:      "sales",
		Table:          "orders",
		TargetRef:      "main",
		SnapshotBefore: "100",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteDataFiles,
	})
	require.NoError(t, err)
	require.True(t, result.Verified)
	require.Len(t, store.inserted, 1)
	require.Equal(t, StatusPending, store.inserted[0].Status)
	require.Equal(t, []string{StatusRunning, StatusCommitted}, store.statuses)
	require.Equal(t, uint64(2), store.rewritten[len(store.rewritten)-1])
}

func TestDispatcherLeavesUnknownWhenVerifierCannotConfirm(t *testing.T) {
	store := &fakeJobRecorder{}
	dispatcher := Dispatcher{
		Runners: map[Operation]Runner{
			OperationExpireSnapshots: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
				return Result{Unknown: true}, nil
			}),
		},
		Recorder: store,
		Verifier: UnknownVerifierFunc(func(ctx context.Context, req Request, result Result) (Result, bool, error) {
			return result, false, nil
		}),
	}
	_, err := dispatcher.Dispatch(context.Background(), Request{
		AccountID:      1,
		CatalogID:      2,
		Namespace:      "sales",
		Table:          "orders",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationExpireSnapshots,
	})
	require.NoError(t, err)
	require.Equal(t, []string{StatusRunning, StatusUnknown}, store.statuses)
}

func TestDispatcherPreservesRunnerErrorCategory(t *testing.T) {
	store := &fakeJobRecorder{}
	dispatcher := Dispatcher{
		Runners: map[Operation]Runner{
			OperationRewriteManifests: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
				return Result{}, api.NewError(api.ErrResidencyDenied, "blocked by policy", nil)
			}),
		},
		Recorder: store,
	}
	_, err := dispatcher.Dispatch(context.Background(), Request{
		AccountID:      1,
		CatalogID:      2,
		Namespace:      "sales",
		Table:          "orders",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteManifests,
	})
	require.Error(t, err)
	require.Equal(t, []string{StatusRunning, StatusFailed}, store.statuses)
	require.Equal(t, []string{"", string(api.ErrResidencyDenied)}, store.categories)
}

func TestDispatcherRejectsTagWriteByDefault(t *testing.T) {
	dispatcher := Dispatcher{
		Runners: map[Operation]Runner{
			OperationRewriteManifests: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
				return Result{}, nil
			}),
		},
	}
	_, err := dispatcher.Dispatch(context.Background(), Request{
		AccountID:      1,
		CatalogID:      2,
		Namespace:      "sales",
		Table:          "orders",
		TargetRef:      "tag:release",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteManifests,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
}

func TestDispatcherAllowsExplicitTagMoveAndNormalizesRef(t *testing.T) {
	var runnerRef string
	dispatcher := Dispatcher{
		Runners: map[Operation]Runner{
			OperationRewriteManifests: RunnerFunc(func(ctx context.Context, req Request) (Result, error) {
				runnerRef = req.TargetRef
				return Result{SnapshotAfter: "101"}, nil
			}),
		},
	}
	_, err := dispatcher.Dispatch(context.Background(), Request{
		AccountID:           1,
		CatalogID:           2,
		Namespace:           "sales",
		Table:               "orders",
		TargetRef:           "tag:release",
		CatalogCapabilities: api.CatalogCapabilities{BranchTag: true},
		AllowTagMove:        true,
		JobID:               "job-1",
		IdempotencyKey:      "idem-1",
		Operation:           OperationRewriteManifests,
	})
	require.NoError(t, err)
	require.Equal(t, "release", runnerRef)
}

func TestMarkAndSweepUsesTTLAndCleanerReferenceChecks(t *testing.T) {
	now := time.Unix(200, 0)
	store := &fakeOrphanStore{
		candidates: []write.OrphanCandidate{
			{FilePath: "s3://warehouse/t/job-1/a.parquet", ExpireAt: now.Add(-time.Second)},
			{FilePath: "s3://warehouse/t/job-1/b.parquet", ExpireAt: now.Add(time.Hour)},
		},
	}
	cleaner := &fakeCleaner{}
	result, err := (MarkAndSweep{
		Store:   store,
		Cleaner: cleaner,
		Now:     func() time.Time { return now },
		Limit:   10,
	}).Sweep(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, result.Scanned)
	require.Equal(t, 1, result.Deleted)
	require.Equal(t, []string{"s3://warehouse/t/job-1/a.parquet"}, cleaner.cleaned)
	require.Equal(t, []string{"s3://warehouse/t/job-1/a.parquet"}, store.deleted)
}

func TestMarkAndSweepKeepsCleanupFailureRetryable(t *testing.T) {
	now := time.Unix(200, 0)
	store := &fakeOrphanStore{candidates: []write.OrphanCandidate{{FilePath: "s3://warehouse/t/job-1/a.parquet", ExpireAt: now.Add(-time.Second)}}}
	result, err := (MarkAndSweep{
		Store: store,
		Cleaner: &fakeCleaner{
			err: api.NewError(api.ErrOrphanCleanupFailed, "referenced", nil),
		},
		Now: func() time.Time { return now },
	}).Sweep(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, result.Failed)
	require.Equal(t, []string{string(api.ErrOrphanCleanupFailed)}, store.retryCategories)
	require.Empty(t, store.failedCategories)
}

type fakeJobRecorder struct {
	inserted   []model.MaintenanceJob
	statuses   []string
	categories []string
	rewritten  []uint64
}

func (r *fakeJobRecorder) InsertMaintenanceJob(ctx context.Context, job model.MaintenanceJob) error {
	r.inserted = append(r.inserted, job)
	return nil
}

func (r *fakeJobRecorder) UpdateMaintenanceJobStatus(ctx context.Context, accountID uint32, jobID, status, errorCategory string, snapshotAfter string, rewrittenFileCount, removedFileCount uint64, expectedVersion uint64) error {
	r.statuses = append(r.statuses, status)
	r.categories = append(r.categories, errorCategory)
	r.rewritten = append(r.rewritten, rewrittenFileCount)
	return nil
}

type fakeOrphanStore struct {
	candidates       []write.OrphanCandidate
	deleted          []string
	retryCategories  []string
	failedCategories []string
}

func (s *fakeOrphanStore) ListExpiredOrphans(ctx context.Context, now time.Time, limit int) ([]write.OrphanCandidate, error) {
	return append([]write.OrphanCandidate(nil), s.candidates...), nil
}

func (s *fakeOrphanStore) MarkOrphanDeleted(ctx context.Context, candidate write.OrphanCandidate) error {
	s.deleted = append(s.deleted, candidate.FilePath)
	return nil
}

func (s *fakeOrphanStore) MarkOrphanRetry(ctx context.Context, candidate write.OrphanCandidate, category string) error {
	s.retryCategories = append(s.retryCategories, category)
	return nil
}

func (s *fakeOrphanStore) MarkOrphanFailed(ctx context.Context, candidate write.OrphanCandidate, category string) error {
	s.failedCategories = append(s.failedCategories, category)
	return nil
}

type fakeCleaner struct {
	cleaned []string
	err     error
}

func (c *fakeCleaner) CleanupOrphan(ctx context.Context, candidate write.OrphanCandidate) error {
	if c.err != nil {
		return c.err
	}
	c.cleaned = append(c.cleaned, candidate.FilePath)
	return nil
}
