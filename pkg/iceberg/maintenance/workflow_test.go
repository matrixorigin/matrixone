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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

func TestCommitRunnerWritesObjectsAndCommits(t *testing.T) {
	writer := &fakeMaintenanceObjectWriter{}
	committer := &fakeMaintenanceCommitter{result: &api.CommitResult{SnapshotID: 101, CommitID: "commit-101", Verified: true}}
	result, err := (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: maintenanceCommitPlan()},
		ObjectWriter: writer,
		Committer:    committer,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.NoError(t, err)
	require.Equal(t, Result{SnapshotAfter: "101", RewrittenFileCount: 2, RemovedFileCount: 1, CommitID: "commit-101", Verified: true}, result)
	require.Equal(t, []string{"s3://warehouse/orders/metadata/rewrite-manifest.avro"}, writer.paths)
	require.Len(t, committer.requests, 1)
	req := committer.requests[0]
	require.Equal(t, api.Namespace{"prod", "sales"}, req.Namespace)
	require.Equal(t, "orders", req.Table)
	require.Equal(t, "dev", req.TargetRef)
	require.Equal(t, "idem-1", req.IdempotencyKey)
	require.Equal(t, []string{"rewrite-manifests", "set-snapshot-summary"}, commitUpdateTypes(req.Updates))
	require.Equal(t, "rewrite_manifests", req.Summary["operation"])
}

func TestCommitRunnerReturnsNoOpWithoutCommit(t *testing.T) {
	committer := &fakeMaintenanceCommitter{result: &api.CommitResult{SnapshotID: 101, CommitID: "should-not-commit"}}
	result, err := (CommitRunner{
		Planner: fakeMaintenancePlanner{plan: &CommitPlan{
			NoOp:           true,
			NoOpSnapshotID: 44,
		}},
		Committer: committer,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.NoError(t, err)
	require.Equal(t, Result{SnapshotAfter: "44", Verified: true}, result)
	require.Empty(t, committer.requests)
}

func TestCommitRunnerDurablyRecordsPostCommitCleanupBeforeMutation(t *testing.T) {
	plan := maintenanceCommitPlan()
	plan.PostCommitOrphans = []string{"s3://warehouse/orders/metadata/expired-manifest.avro"}
	recorder := &fakeMaintenanceOrphanRecorder{}
	writer := &fakeMaintenanceObjectWriter{}
	committer := &fakeMaintenanceCommitter{result: &api.CommitResult{SnapshotID: 101}}

	_, err := (CommitRunner{
		Planner:        fakeMaintenancePlanner{plan: plan},
		ObjectWriter:   writer,
		Committer:      committer,
		OrphanRecorder: recorder,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.NoError(t, err)
	require.Len(t, recorder.candidates, 1)
	require.Equal(t, plan.PostCommitOrphans[0], recorder.candidates[0].FilePath)
	require.Len(t, writer.paths, 1)
	require.Len(t, committer.requests, 1)
}

func TestCommitRunnerStopsBeforeMutationWhenPostCommitCleanupCannotBeRecorded(t *testing.T) {
	plan := maintenanceCommitPlan()
	plan.PostCommitOrphans = []string{"s3://warehouse/orders/metadata/expired-manifest.avro"}
	sentinel := api.NewError(api.ErrCatalogUnavailable, "recorder unavailable", nil)
	recorder := &fakeMaintenanceOrphanRecorder{err: sentinel}
	writer := &fakeMaintenanceObjectWriter{}
	committer := &fakeMaintenanceCommitter{}

	_, err := (CommitRunner{
		Planner:        fakeMaintenancePlanner{plan: plan},
		ObjectWriter:   writer,
		Committer:      committer,
		OrphanRecorder: recorder,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.ErrorIs(t, err, sentinel)
	require.Contains(t, err.Error(), string(api.ErrOrphanCleanupFailed))
	require.Empty(t, writer.paths)
	require.Empty(t, committer.requests)

	_, err = (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: plan},
		ObjectWriter: writer,
		Committer:    committer,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, errors.Unwrap(err).Error(), "requires an orphan recorder")
	require.Empty(t, writer.paths)
	require.Empty(t, committer.requests)
}

func TestCommitRunnerUsesSuccessVerifier(t *testing.T) {
	var verifierReq Request
	var verifierPlan *CommitPlan
	verifier := CommitResultVerifierFunc(func(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
		verifierReq = req
		verifierPlan = plan
		result.SnapshotID = 202
		result.CommitID = "verified-commit"
		return result, true, nil
	})
	result, err := (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: maintenanceCommitPlan()},
		ObjectWriter: &fakeMaintenanceObjectWriter{},
		Committer:    &fakeMaintenanceCommitter{result: &api.CommitResult{SnapshotID: 101, CommitID: "commit-101"}},
		Verifier:     verifier,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.NoError(t, err)
	require.Equal(t, "202", result.SnapshotAfter)
	require.Equal(t, "verified-commit", result.CommitID)
	require.True(t, result.Verified)
	require.Equal(t, OperationRewriteManifests, verifierReq.Operation)
	require.NotNil(t, verifierPlan)
	require.Equal(t, "idem-1", verifierPlan.Attempt.IdempotencyKey)
}

func TestCommitRunnerTreatsVerifierFailureAsUnverifiedSuccess(t *testing.T) {
	result, err := (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: maintenanceCommitPlan()},
		ObjectWriter: &fakeMaintenanceObjectWriter{},
		Committer:    &fakeMaintenanceCommitter{result: &api.CommitResult{SnapshotID: 101, CommitID: "commit-101", Verified: true}},
		Verifier: CommitResultVerifierFunc(func(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
			return api.CommitResult{}, false, api.NewError(api.ErrCatalogUnavailable, "catalog metadata not visible yet", nil)
		}),
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.NoError(t, err)
	require.Equal(t, "101", result.SnapshotAfter)
	require.Equal(t, "commit-101", result.CommitID)
	require.False(t, result.Verified)
}

func TestCommitRunnerReturnsUnknownAndRecordsOrphans(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	recorder := &fakeMaintenanceOrphanRecorder{}
	result, err := (CommitRunner{
		Planner:        fakeMaintenancePlanner{plan: maintenanceCommitPlan()},
		ObjectWriter:   &fakeMaintenanceObjectWriter{},
		Committer:      &fakeMaintenanceCommitter{result: &api.CommitResult{Unknown: true, SnapshotID: 102, CommitID: "commit-unknown"}},
		OrphanRecorder: recorder,
		Now:            func() time.Time { return now },
		OrphanTTL:      time.Hour,
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrCommitUnknown))
	require.True(t, result.Unknown)
	require.Equal(t, "102", result.SnapshotAfter)
	require.Equal(t, "commit-unknown", result.CommitID)
	require.Len(t, recorder.candidates, 2)
	paths := make([]string, 0, len(recorder.candidates))
	for _, candidate := range recorder.candidates {
		require.Equal(t, uint32(7), candidate.AccountID)
		require.Equal(t, uint64(42), candidate.CatalogID)
		require.Equal(t, "job-1", candidate.JobID)
		require.Equal(t, now.Add(time.Hour), candidate.ExpireAt)
		require.NotContains(t, candidate.FilePathRedacted, "warehouse")
		paths = append(paths, candidate.FilePath)
	}
	require.ElementsMatch(t, []string{
		"s3://warehouse/orders/metadata/rewrite-manifest.avro",
		"s3://warehouse/orders/data/rewrite-output.parquet",
	}, paths)
}

func TestCommitRunnerVerifiesUnknownOutcomeFromAttempt(t *testing.T) {
	var verifierResult api.CommitResult
	result, err := (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: maintenanceCommitPlan()},
		ObjectWriter: &fakeMaintenanceObjectWriter{},
		Committer: &fakeMaintenanceCommitter{
			err: api.NewError(api.ErrCommitUnknown, "response lost", nil),
		},
		Verifier: CommitResultVerifierFunc(func(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
			verifierResult = result
			result.SnapshotID = plan.Attempt.BaseSnapshotID
			return result, true, nil
		}),
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.NoError(t, err)
	require.True(t, verifierResult.Unknown)
	require.Equal(t, "100", result.SnapshotAfter)
	require.True(t, result.Verified)
	require.False(t, result.Unknown)
}

func TestCommitRunnerRequiresInjectedDependencies(t *testing.T) {
	_, err := (CommitRunner{}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a planner")

	_, err = (CommitRunner{Planner: fakeMaintenancePlanner{plan: maintenanceCommitPlan()}}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a committer")

	_, err = (CommitRunner{
		Planner:   fakeMaintenancePlanner{plan: maintenanceCommitPlan()},
		Committer: &fakeMaintenanceCommitter{},
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires an object writer")
}

func TestCommitRunnerRejectsPlannerTargetRefMismatch(t *testing.T) {
	plan := maintenanceCommitPlan()
	plan.Attempt.TargetRef = "other"
	_, err := (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: plan},
		ObjectWriter: &fakeMaintenanceObjectWriter{},
		Committer:    &fakeMaintenanceCommitter{},
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "target ref does not match")
}

func TestCommitRunnerRejectsPlannerRequirementRefMismatch(t *testing.T) {
	plan := maintenanceCommitPlan()
	plan.Attempt.Requirements[0].Ref = "other"
	_, err := (CommitRunner{
		Planner:      fakeMaintenancePlanner{plan: plan},
		ObjectWriter: &fakeMaintenanceObjectWriter{},
		Committer:    &fakeMaintenanceCommitter{},
	}).RunMaintenance(context.Background(), maintenanceRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "ref requirement does not match")
}

func maintenanceRequest() Request {
	return Request{
		AccountID:      7,
		CatalogID:      42,
		Namespace:      "prod.sales",
		Table:          "orders",
		TargetRef:      "dev",
		JobID:          "job-1",
		IdempotencyKey: "idem-1",
		Operation:      OperationRewriteManifests,
	}
}

func maintenanceCommitPlan() *CommitPlan {
	return &CommitPlan{
		Catalog: api.CatalogRequest{Catalog: model.Catalog{AccountID: 7, CatalogID: 42}},
		Attempt: &api.CommitAttempt{
			Requirements: []api.CommitRequirement{{Type: "assert-ref-snapshot-id", Ref: "dev", SnapshotID: 100}},
			Updates: []api.CommitUpdate{
				{Type: "rewrite-manifests", FilePath: "s3://warehouse/orders/metadata/rewrite-manifest.avro"},
				{Type: "set-snapshot-summary", Payload: map[string]string{"operation": "rewrite_manifests"}},
			},
			Summary:        map[string]string{"operation": "rewrite_manifests"},
			IdempotencyKey: "idem-1",
			BaseSnapshotID: 100,
			TargetRef:      "dev",
		},
		Objects: []ObjectWrite{{
			Location: "s3://warehouse/orders/metadata/rewrite-manifest.avro",
			Payload:  []byte("manifest"),
		}},
		OrphanPaths:        []string{"s3://warehouse/orders/data/rewrite-output.parquet"},
		RewrittenFileCount: 2,
		RemovedFileCount:   1,
	}
}

type fakeMaintenancePlanner struct {
	plan *CommitPlan
	err  error
}

func (p fakeMaintenancePlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.plan, nil
}

type fakeMaintenanceObjectWriter struct {
	paths []string
	err   error
}

func (w *fakeMaintenanceObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	w.paths = append(w.paths, location)
	return w.err
}

type fakeMaintenanceCommitter struct {
	requests []api.CommitRequest
	result   *api.CommitResult
	err      error
}

func (c *fakeMaintenanceCommitter) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}
	if c.result == nil {
		return &api.CommitResult{SnapshotID: 1}, nil
	}
	return c.result, nil
}

type fakeMaintenanceOrphanRecorder struct {
	candidates []write.OrphanCandidate
	err        error
}

func (r *fakeMaintenanceOrphanRecorder) RecordOrphans(ctx context.Context, candidates []write.OrphanCandidate) error {
	if r.err != nil {
		return r.err
	}
	r.candidates = append(r.candidates, candidates...)
	return nil
}

func commitUpdateTypes(updates []api.CommitUpdate) []string {
	out := make([]string, 0, len(updates))
	for _, update := range updates {
		out = append(out, update.Type)
	}
	return out
}
