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
	stderrors "errors"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type ObjectWrite struct {
	Location string
	Payload  []byte
}

type ObjectWriter interface {
	WriteObject(ctx context.Context, location string, payload []byte) error
}

type ObjectWriterFunc func(ctx context.Context, location string, payload []byte) error

func (f ObjectWriterFunc) WriteObject(ctx context.Context, location string, payload []byte) error {
	return f(ctx, location, payload)
}

type CommitPlanner interface {
	BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error)
}

type CommitPlannerFunc func(ctx context.Context, req Request) (*CommitPlan, error)

func (f CommitPlannerFunc) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	return f(ctx, req)
}

type CommitResultVerifier interface {
	VerifyCommittedMaintenance(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error)
}

type CommitResultVerifierFunc func(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error)

func (f CommitResultVerifierFunc) VerifyCommittedMaintenance(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) (api.CommitResult, bool, error) {
	return f(ctx, req, plan, result)
}

type CommitPlan struct {
	Catalog            api.CatalogRequest
	Attempt            *api.CommitAttempt
	Objects            []ObjectWrite
	OrphanPaths        []string
	PostCommitOrphans  []string
	RewrittenFileCount uint64
	RemovedFileCount   uint64
	NoOp               bool
	NoOpSnapshotID     int64
}

type CommitRunner struct {
	Planner        CommitPlanner
	ObjectWriter   ObjectWriter
	Committer      api.Committer
	Verifier       CommitResultVerifier
	OrphanRecorder write.OrphanRecorder
	Now            func() time.Time
	OrphanTTL      time.Duration
}

func (r CommitRunner) RunMaintenance(ctx context.Context, req Request) (Result, error) {
	if r.Planner == nil {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit runner requires a planner", map[string]string{"operation": string(req.Operation)})
	}
	if r.Committer == nil {
		return Result{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit runner requires a committer", map[string]string{"operation": string(req.Operation)})
	}
	plan, err := r.Planner.BuildMaintenanceCommit(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if plan != nil && plan.NoOp {
		return Result{
			SnapshotAfter:      snapshotIDString(&api.CommitResult{SnapshotID: plan.NoOpSnapshotID}),
			RewrittenFileCount: plan.RewrittenFileCount,
			RemovedFileCount:   plan.RemovedFileCount,
			Verified:           true,
		}, nil
	}
	if err := validateCommitPlan(req, plan); err != nil {
		recoveryCtx, cancel := write.NewRecoveryContext(ctx)
		defer cancel()
		return Result{}, stderrors.Join(err, r.recordOrphans(recoveryCtx, req, plan))
	}
	if len(plan.Objects) > 0 && r.ObjectWriter == nil {
		recoveryCtx, cancel := write.NewRecoveryContext(ctx)
		defer cancel()
		primaryErr := api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit runner requires an object writer", map[string]string{"operation": string(req.Operation)})
		return Result{}, stderrors.Join(primaryErr, r.recordOrphans(recoveryCtx, req, plan))
	}
	// Persist cleanup intent before the external catalog mutation. Once
	// expire_snapshots commits, the removed manifests may no longer be
	// discoverable from current metadata, so a post-commit recorder outage
	// cannot be repaired. Candidates are TTL-delayed and the sweeper rechecks
	// committed metadata, which makes this ordering safe when the commit fails.
	if err := r.recordPostCommitOrphans(ctx, req, plan); err != nil {
		return Result{}, api.WrapError(api.ErrOrphanCleanupFailed, "Iceberg maintenance could not durably schedule post-commit cleanup", map[string]string{
			"operation": string(req.Operation),
			"table":     req.Table,
		}, err)
	}
	for _, object := range plan.Objects {
		if err := r.ObjectWriter.WriteObject(ctx, object.Location, object.Payload); err != nil {
			recoveryCtx, cancel := write.NewRecoveryContext(ctx)
			defer cancel()
			return Result{}, stderrors.Join(err, r.recordOrphans(recoveryCtx, req, plan))
		}
	}
	result, err := r.Committer.CommitTable(ctx, maintenanceCommitRequest(req, plan))
	if isCommitUnknown(err, result) {
		return r.resolveUnknownCommit(ctx, req, plan, result)
	}
	if err != nil {
		recoveryCtx, cancel := write.NewRecoveryContext(ctx)
		defer cancel()
		return Result{}, stderrors.Join(err, r.recordOrphans(recoveryCtx, req, plan))
	}
	if result == nil {
		return r.resolveUnknownCommit(ctx, req, plan, nil)
	}
	recoveryCtx, cancel := write.NewRecoveryContext(ctx)
	defer cancel()
	committed := r.verifyCommitted(recoveryCtx, req, plan, *result)
	return Result{
		SnapshotAfter:      strconv.FormatInt(committed.SnapshotID, 10),
		RewrittenFileCount: plan.RewrittenFileCount,
		RemovedFileCount:   plan.RemovedFileCount,
		CommitID:           committed.CommitID,
		Verified:           committed.Verified,
	}, nil
}

func (r CommitRunner) resolveUnknownCommit(ctx context.Context, req Request, plan *CommitPlan, result *api.CommitResult) (Result, error) {
	recoveryCtx, cancel := write.NewRecoveryContext(ctx)
	defer cancel()
	candidate := api.CommitResult{Unknown: true}
	if result != nil {
		candidate = *result
	}
	if r.Verifier != nil {
		verified, ok, err := r.Verifier.VerifyCommittedMaintenance(recoveryCtx, req, plan, candidate)
		if err == nil && ok {
			verified.Verified = true
			verified.Unknown = false
			return maintenanceResult(plan, verified), nil
		}
		if err != nil {
			recoveryErr := r.recordOrphans(recoveryCtx, req, plan)
			unknownErr := api.WrapError(api.ErrCommitUnknown, "Iceberg maintenance commit outcome could not be verified", map[string]string{
				"operation": string(req.Operation),
				"table":     req.Table,
			}, err)
			return maintenanceResult(plan, candidate), stderrors.Join(unknownErr, recoveryErr)
		}
	}
	recoveryErr := r.recordOrphans(recoveryCtx, req, plan)
	unknownErr := api.NewError(api.ErrCommitUnknown, "Iceberg maintenance commit outcome is unknown and could not be verified", map[string]string{
		"operation": string(req.Operation),
		"table":     req.Table,
	})
	return maintenanceResult(plan, candidate), stderrors.Join(unknownErr, recoveryErr)
}

func maintenanceResult(plan *CommitPlan, result api.CommitResult) Result {
	return Result{
		SnapshotAfter:      snapshotIDString(&result),
		RewrittenFileCount: plan.RewrittenFileCount,
		RemovedFileCount:   plan.RemovedFileCount,
		CommitID:           result.CommitID,
		Verified:           result.Verified,
		Unknown:            result.Unknown,
	}
}

func (r CommitRunner) verifyCommitted(ctx context.Context, req Request, plan *CommitPlan, result api.CommitResult) api.CommitResult {
	if r.Verifier == nil {
		return result
	}
	verified, ok, err := r.Verifier.VerifyCommittedMaintenance(ctx, req, plan, result)
	if err != nil {
		logutil.Warn("Iceberg maintenance commit verification failed after commit",
			zap.Uint32("account-id", req.AccountID),
			zap.Uint64("catalog-id", req.CatalogID),
			zap.String("namespace", req.Namespace),
			zap.String("table", req.Table),
			zap.String("operation", string(req.Operation)),
			zap.Int64("snapshot-id", result.SnapshotID),
			zap.String("commit-id", result.CommitID),
			zap.Error(err))
		result.Verified = false
		return result
	}
	if !ok {
		result.Verified = false
		return result
	}
	verified.Verified = true
	return verified
}

func validateCommitPlan(req Request, plan *CommitPlan) error {
	if plan == nil || plan.Attempt == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit planner returned an empty plan", map[string]string{"operation": string(req.Operation)})
	}
	if strings.TrimSpace(plan.Attempt.IdempotencyKey) == "" && strings.TrimSpace(req.IdempotencyKey) == "" {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit plan requires an idempotency key", map[string]string{"operation": string(req.Operation)})
	}
	if len(plan.Attempt.Updates) == 0 {
		return api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance commit plan requires table updates", map[string]string{"operation": string(req.Operation)})
	}
	expectedRef := strings.TrimSpace(req.TargetRef)
	if expectedRef == "" {
		expectedRef = "main"
	}
	planRef := strings.TrimSpace(plan.Attempt.TargetRef)
	if planRef != "" && planRef != expectedRef {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit plan target ref does not match the authorized request ref", map[string]string{
			"operation": string(req.Operation),
			"ref":       planRef,
		})
	}
	for _, requirement := range plan.Attempt.Requirements {
		switch requirement.Type {
		case "assert-ref-snapshot-id", "assert-ref-not-exists":
			if strings.TrimSpace(requirement.Ref) != "" && strings.TrimSpace(requirement.Ref) != expectedRef {
				return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance commit plan ref requirement does not match the authorized request ref", map[string]string{
					"operation": string(req.Operation),
					"ref":       requirement.Ref,
				})
			}
		}
	}
	for _, object := range plan.Objects {
		if strings.TrimSpace(object.Location) == "" || len(object.Payload) == 0 {
			return api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance object write is invalid", map[string]string{"operation": string(req.Operation)})
		}
	}
	return nil
}

func maintenanceCommitRequest(req Request, plan *CommitPlan) api.CommitRequest {
	return api.CommitRequest{
		CatalogRequest: plan.Catalog,
		Namespace:      namespaceFromString(req.Namespace),
		Table:          req.Table,
		TargetRef:      firstNonEmptyString(req.TargetRef, plan.Attempt.TargetRef, "main"),
		Requirements:   append([]api.CommitRequirement(nil), plan.Attempt.Requirements...),
		Updates:        append([]api.CommitUpdate(nil), plan.Attempt.Updates...),
		IdempotencyKey: firstNonEmptyString(plan.Attempt.IdempotencyKey, req.IdempotencyKey),
		Summary:        cloneStringMap(plan.Attempt.Summary),
	}
}

func namespaceFromString(namespace string) api.Namespace {
	parts := strings.Split(namespace, ".")
	out := make(api.Namespace, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			out = append(out, strings.TrimSpace(part))
		}
	}
	return out
}

func (r CommitRunner) recordOrphans(ctx context.Context, req Request, plan *CommitPlan) error {
	if r.OrphanRecorder == nil || plan == nil {
		return nil
	}
	paths := make([]string, 0, len(plan.Objects)+len(plan.OrphanPaths))
	for _, object := range plan.Objects {
		paths = append(paths, object.Location)
	}
	paths = append(paths, plan.OrphanPaths...)
	return r.recordOrphanPaths(ctx, req, paths)
}

func (r CommitRunner) recordPostCommitOrphans(ctx context.Context, req Request, plan *CommitPlan) error {
	if plan == nil || len(plan.PostCommitOrphans) == 0 {
		return nil
	}
	if r.OrphanRecorder == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance post-commit cleanup requires an orphan recorder", map[string]string{
			"operation": string(req.Operation),
			"table":     req.Table,
		})
	}
	return r.recordOrphanPaths(ctx, req, plan.PostCommitOrphans)
}

func (r CommitRunner) recordOrphanPaths(ctx context.Context, req Request, paths []string) error {
	if r.OrphanRecorder == nil {
		return nil
	}
	if len(paths) == 0 {
		return nil
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	ttl := r.OrphanTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	candidates := make([]write.OrphanCandidate, 0, len(paths))
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		candidates = append(candidates, write.OrphanCandidate{
			AccountID:         req.AccountID,
			CatalogID:         req.CatalogID,
			JobID:             firstNonEmptyString(req.JobID, req.IdempotencyKey),
			Namespace:         req.Namespace,
			TableName:         req.Table,
			TableLocationHash: api.PathHash(req.Namespace + "." + req.Table),
			FilePath:          path,
			FilePathHash:      api.PathHash(path),
			FilePathRedacted:  api.RedactPath(path),
			WrittenAt:         now,
			ExpireAt:          now.Add(ttl),
			CleanupStatus:     "pending",
		})
	}
	if len(candidates) == 0 {
		return nil
	}
	return r.OrphanRecorder.RecordOrphans(ctx, candidates)
}

func isCommitUnknown(err error, result *api.CommitResult) bool {
	if result != nil && result.Unknown {
		return true
	}
	var icebergErr *api.IcebergError
	return stderrors.As(err, &icebergErr) && icebergErr.Code == api.ErrCommitUnknown
}

func snapshotIDString(result *api.CommitResult) string {
	if result == nil || result.SnapshotID == 0 {
		return ""
	}
	return strconv.FormatInt(result.SnapshotID, 10)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
