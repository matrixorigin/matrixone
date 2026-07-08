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
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergref "github.com/matrixorigin/matrixone/pkg/iceberg/ref"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type Operation string

const (
	OperationRewriteDataFiles Operation = "rewrite_data_files"
	OperationRewriteManifests Operation = "rewrite_manifests"
	OperationExpireSnapshots  Operation = "expire_snapshots"
)

const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCommitted = "committed"
	StatusUnknown   = "unknown"
	StatusFailed    = "failed"

	ProcedureRewriteDataFiles = "iceberg_rewrite_data_files"
	ProcedureRewriteManifests = "iceberg_rewrite_manifests"
	ProcedureExpireSnapshots  = "iceberg_expire_snapshots"
)

type ParsedCall struct {
	Operation Operation
	Target    string
	TargetID  TargetIdentifier
	Options   map[string]string
}

type TargetIdentifier struct {
	Catalog   string
	Namespace string
	Table     string
}

type Request struct {
	AccountID           uint32
	RoleID              uint64
	UserID              uint64
	CatalogID           uint64
	Catalog             model.Catalog
	ExternalPrincipal   string
	ResidencyPolicies   []model.ResidencyPolicy
	Namespace           string
	Table               string
	TargetRef           string
	TargetRefType       string
	AllowTagMove        bool
	CatalogCapabilities api.CatalogCapabilities
	SnapshotBefore      string
	JobID               string
	IdempotencyKey      string
	Operation           Operation
	Options             map[string]string
}

type Result struct {
	SnapshotAfter      string
	RewrittenFileCount uint64
	RemovedFileCount   uint64
	CommitID           string
	Unknown            bool
	Verified           bool
}

type Runner interface {
	RunMaintenance(ctx context.Context, req Request) (Result, error)
}

type RunnerFunc func(ctx context.Context, req Request) (Result, error)

func (f RunnerFunc) RunMaintenance(ctx context.Context, req Request) (Result, error) {
	return f(ctx, req)
}

type UnknownVerifier interface {
	VerifyMaintenance(ctx context.Context, req Request, result Result) (Result, bool, error)
}

type UnknownVerifierFunc func(ctx context.Context, req Request, result Result) (Result, bool, error)

func (f UnknownVerifierFunc) VerifyMaintenance(ctx context.Context, req Request, result Result) (Result, bool, error) {
	return f(ctx, req, result)
}

type JobRecorder interface {
	InsertMaintenanceJob(ctx context.Context, job model.MaintenanceJob) error
	UpdateMaintenanceJobStatus(ctx context.Context, accountID uint32, jobID, status, errorCategory string, snapshotAfter string, rewrittenFileCount, removedFileCount uint64, expectedVersion uint64) error
}

type Dispatcher struct {
	Runners  map[Operation]Runner
	Recorder JobRecorder
	Verifier UnknownVerifier
	Now      func() time.Time
}

func ParseProcedureCall(name, target, options string) (ParsedCall, error) {
	operation, err := OperationForProcedure(name)
	if err != nil {
		return ParsedCall{}, err
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return ParsedCall{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance procedure requires a target table", map[string]string{"procedure": name})
	}
	targetID, err := ParseTargetIdentifier(target)
	if err != nil {
		return ParsedCall{}, err
	}
	return ParsedCall{Operation: operation, Target: target, TargetID: targetID, Options: parseOptions(options)}, nil
}

func OperationForProcedure(name string) (Operation, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case ProcedureRewriteDataFiles:
		return OperationRewriteDataFiles, nil
	case ProcedureRewriteManifests:
		return OperationRewriteManifests, nil
	case ProcedureExpireSnapshots:
		return OperationExpireSnapshots, nil
	default:
		return "", api.NewError(api.ErrUnsupportedFeature, "Iceberg maintenance procedure is unsupported", map[string]string{"procedure": name})
	}
}

func ParseTargetIdentifier(target string) (TargetIdentifier, error) {
	target = strings.TrimSpace(target)
	parts := strings.Split(target, ".")
	if len(parts) < 3 {
		return TargetIdentifier{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance target must be catalog.namespace.table", map[string]string{
			"target": target,
		})
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return TargetIdentifier{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance target contains an empty identifier part", map[string]string{
				"target": target,
			})
		}
	}
	normalized := make([]string, len(parts))
	for i, part := range parts {
		normalized[i] = strings.TrimSpace(part)
	}
	return TargetIdentifier{
		Catalog:   normalized[0],
		Namespace: strings.Join(normalized[1:len(normalized)-1], "."),
		Table:     normalized[len(normalized)-1],
	}, nil
}

func (d Dispatcher) Dispatch(ctx context.Context, req Request) (Result, error) {
	normalized, err := NormalizeRequestRef(req)
	if err != nil {
		return Result{}, err
	}
	req = normalized
	if err := validateRequest(req); err != nil {
		return Result{}, err
	}
	runner := d.Runners[req.Operation]
	if runner == nil {
		return Result{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg maintenance runner is not configured", map[string]string{"operation": string(req.Operation)})
	}
	now := time.Now()
	if d.Now != nil {
		now = d.Now()
	}
	job := NewJob(req, now)
	if d.Recorder != nil {
		if err := d.Recorder.InsertMaintenanceJob(ctx, job); err != nil {
			return Result{}, err
		}
	}
	if err := d.transition(ctx, job, StatusRunning, "", Result{}); err != nil {
		return Result{}, err
	}
	job.Status = StatusRunning
	job.Version++
	result, err := runner.RunMaintenance(ctx, req)
	if err != nil {
		_ = d.transition(ctx, job, StatusFailed, errorCategory(err), result)
		return Result{}, err
	}
	status := StatusCommitted
	if result.Unknown {
		status = StatusUnknown
		if d.Verifier != nil {
			verified, ok, verifyErr := d.Verifier.VerifyMaintenance(ctx, req, result)
			if verifyErr != nil {
				_ = d.transition(ctx, job, StatusUnknown, errorCategory(verifyErr), result)
				return Result{}, verifyErr
			}
			if ok {
				verified.Verified = true
				result = verified
				status = StatusCommitted
			}
		}
	}
	if err := d.transition(ctx, job, status, "", result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (d Dispatcher) transition(ctx context.Context, job model.MaintenanceJob, status, category string, result Result) error {
	if d.Recorder == nil {
		return nil
	}
	return d.Recorder.UpdateMaintenanceJobStatus(
		ctx,
		job.AccountID,
		job.JobID,
		status,
		category,
		result.SnapshotAfter,
		result.RewrittenFileCount,
		result.RemovedFileCount,
		job.Version,
	)
}

func errorCategory(err error) string {
	if err == nil {
		return ""
	}
	var icebergErr *api.IcebergError
	if stderrors.As(err, &icebergErr) && icebergErr.Code != "" {
		return string(icebergErr.Code)
	}
	return string(api.ErrInternal)
}

func NewJob(req Request, now time.Time) model.MaintenanceJob {
	jobID := strings.TrimSpace(req.JobID)
	if jobID == "" {
		jobID = req.IdempotencyKey
	}
	ref := strings.TrimSpace(req.TargetRef)
	if ref == "" {
		ref = model.DefaultRefMain
	}
	return model.MaintenanceJob{
		JobID:           jobID,
		AccountID:       req.AccountID,
		CatalogID:       req.CatalogID,
		Namespace:       req.Namespace,
		TableName:       req.Table,
		Operation:       string(req.Operation),
		TargetRef:       ref,
		SnapshotBefore:  req.SnapshotBefore,
		Status:          StatusPending,
		CreatedAt:       now,
		UpdatedAt:       now,
		StatusUpdatedAt: now,
		Version:         1,
	}
}

type OrphanStore interface {
	ListExpiredOrphans(ctx context.Context, now time.Time, limit int) ([]write.OrphanCandidate, error)
	MarkOrphanDeleted(ctx context.Context, candidate write.OrphanCandidate) error
	MarkOrphanFailed(ctx context.Context, candidate write.OrphanCandidate, category string) error
}

type MarkAndSweep struct {
	Store   OrphanStore
	Cleaner write.OrphanCleaner
	Now     func() time.Time
	Limit   int
}

type SweepResult struct {
	Scanned int
	Deleted int
	Failed  int
}

func (s MarkAndSweep) Sweep(ctx context.Context) (SweepResult, error) {
	if s.Store == nil {
		return SweepResult{}, api.NewError(api.ErrConfigInvalid, "Iceberg mark-and-sweep requires an orphan store", nil)
	}
	if s.Cleaner == nil {
		return SweepResult{}, api.NewError(api.ErrConfigInvalid, "Iceberg mark-and-sweep requires an orphan cleaner", nil)
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	limit := s.Limit
	if limit <= 0 {
		limit = 100
	}
	candidates, err := s.Store.ListExpiredOrphans(ctx, now, limit)
	if err != nil {
		return SweepResult{}, err
	}
	result := SweepResult{Scanned: len(candidates)}
	for _, candidate := range candidates {
		if candidate.ExpireAt.After(now) {
			continue
		}
		if err := s.Cleaner.CleanupOrphan(ctx, candidate); err != nil {
			result.Failed++
			_ = s.Store.MarkOrphanFailed(ctx, candidate, string(api.ErrOrphanCleanupFailed))
			continue
		}
		result.Deleted++
		if err := s.Store.MarkOrphanDeleted(ctx, candidate); err != nil {
			return result, err
		}
	}
	return result, nil
}

func validateRequest(req Request) error {
	if req.CatalogID == 0 {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance requires catalog id", nil)
	}
	if req.Catalog.CatalogID != 0 && (req.Catalog.AccountID != req.AccountID || req.Catalog.CatalogID != req.CatalogID) {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance resolved catalog does not match request ids", map[string]string{
			"catalog": req.Catalog.Name,
		})
	}
	if strings.TrimSpace(req.Namespace) == "" || strings.TrimSpace(req.Table) == "" {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance requires namespace and table", nil)
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" && strings.TrimSpace(req.JobID) == "" {
		return api.NewError(api.ErrConfigInvalid, "Iceberg maintenance requires an idempotency key or job id", map[string]string{"table": req.Table})
	}
	switch req.Operation {
	case OperationRewriteDataFiles, OperationRewriteManifests, OperationExpireSnapshots:
		return nil
	default:
		return api.NewError(api.ErrUnsupportedFeature, "Iceberg maintenance operation is unsupported", map[string]string{"operation": string(req.Operation)})
	}
}

func NormalizeRequestRef(req Request) (Request, error) {
	raw := strings.TrimSpace(req.TargetRef)
	if raw == "" {
		raw = model.DefaultRefMain
	}
	spec, err := icebergref.ParseNessieRef(raw, nil)
	if err != nil {
		return Request{}, err
	}
	if refType := strings.ToLower(strings.TrimSpace(req.TargetRefType)); refType != "" {
		spec.Type = icebergref.Type(refType)
		if strings.TrimSpace(spec.Name) == "" {
			spec.Name = raw
		}
	}
	if err := icebergref.ValidateWrite(spec, req.CatalogCapabilities, req.AllowTagMove); err != nil {
		return Request{}, err
	}
	req.TargetRef = spec.Name
	req.TargetRefType = string(spec.Type)
	return req, nil
}

func parseOptions(options string) map[string]string {
	out := make(map[string]string)
	for _, item := range strings.Split(options, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, "=", 2)
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		value := ""
		if len(parts) == 2 {
			value = strings.TrimSpace(parts[1])
		}
		out[key] = strings.Trim(value, `'"`)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func TargetRef(options map[string]string) string {
	ref := strings.TrimSpace(options["ref"])
	if ref == "" {
		return model.DefaultRefMain
	}
	return ref
}

func UintOption(options map[string]string, key string) (uint64, bool, error) {
	raw := strings.TrimSpace(options[key])
	if raw == "" {
		return 0, false, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false, api.WrapError(api.ErrConfigInvalid, "Iceberg maintenance option must be unsigned integer", map[string]string{"option": key}, err)
	}
	return value, true, nil
}
