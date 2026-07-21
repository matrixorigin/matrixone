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
	stderrors "errors"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type ManifestObjectWriter interface {
	WriteManifestObject(ctx context.Context, location string, payload []byte) error
}

type ManifestObjectWriterFunc func(ctx context.Context, location string, payload []byte) error

func (f ManifestObjectWriterFunc) WriteManifestObject(ctx context.Context, location string, payload []byte) error {
	return f(ctx, location, payload)
}

type CommitWorkflow struct {
	ManifestWriter   ManifestObjectWriter
	Committer        api.Committer
	Verifier         CommitVerifier
	OrphanRecorder   icebergwrite.OrphanRecorder
	AuditRecorder    icebergwrite.AuditRecorder
	CacheInvalidator icebergwrite.CacheInvalidator
	MetricsReporter  api.MetricsReporter
	Now              func() time.Time
	OrphanTTL        time.Duration
}

type CommitVerifier interface {
	VerifyDMLCommit(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error)
}

type CommitVerifierFunc func(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error)

func (f CommitVerifierFunc) VerifyDMLCommit(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error) {
	return f(ctx, req, materialized, result)
}

type CommitWorkflowRequest struct {
	Catalog            api.CatalogRequest
	Stream             ActionStream
	FormatVersion      int
	Schema             api.Schema
	PartitionSpecs     []api.PartitionSpec
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	DataManifestPath   string
	DeleteManifestPath string
	ManifestListPath   string
	PreservedManifests []api.ManifestFile
	PreservedSources   []PreservedManifestSource
	TableLocation      string
	MaxMemoryBytes     int64
	InitialMemoryBytes int64
}

func (w CommitWorkflow) CommitDML(ctx context.Context, req CommitWorkflowRequest) (*api.CommitResult, error) {
	if w.ManifestWriter == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML commit workflow requires a manifest writer", nil)
	}
	if w.Committer == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML commit workflow requires a committer", nil)
	}
	intent, err := BuildCommitIntent(req.Stream)
	if err != nil {
		recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
		defer cancel()
		recordErr := w.recordOrphans(recoveryCtx, req, nil)
		w.onFailure(recoveryCtx, req, "failed", dmlErrorCategory(err))
		return nil, stderrors.Join(err, recordErr)
	}
	materialized, err := BuildManifestCommitAttempt(ctx, ManifestMaterializeRequest{
		Intent:             *intent,
		FormatVersion:      req.FormatVersion,
		Schema:             req.Schema,
		PartitionSpecs:     append([]api.PartitionSpec(nil), req.PartitionSpecs...),
		SnapshotID:         req.SnapshotID,
		SequenceNumber:     req.SequenceNumber,
		TimestampMS:        req.TimestampMS,
		DataManifestPath:   req.DataManifestPath,
		DeleteManifestPath: req.DeleteManifestPath,
		ManifestListPath:   req.ManifestListPath,
		PreservedManifests: append([]api.ManifestFile(nil), req.PreservedManifests...),
		PreservedSources:   clonePreservedManifestSources(req.PreservedSources),
		MaxMemoryBytes:     req.MaxMemoryBytes,
		InitialMemoryBytes: req.InitialMemoryBytes,
	})
	if err != nil {
		recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
		defer cancel()
		recordErr := w.recordOrphans(recoveryCtx, req, nil)
		w.onFailure(recoveryCtx, req, "failed", dmlErrorCategory(err))
		return nil, stderrors.Join(err, recordErr)
	}
	if err := w.writeManifests(ctx, materialized); err != nil {
		recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
		defer cancel()
		recordErr := w.recordOrphans(recoveryCtx, req, materialized)
		w.onFailure(recoveryCtx, req, "failed", dmlErrorCategory(err))
		return nil, stderrors.Join(err, recordErr)
	}
	result, err := w.Committer.CommitTable(ctx, dmlCommitRequest(req.Catalog, req.Stream.Base, materialized.Attempt))
	if err != nil {
		if isDMLCommitUnknown(err, result) {
			recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
			defer cancel()
			verified, ok, verifyErr := w.verifyUnknown(recoveryCtx, req, materialized, result)
			if verifyErr != nil {
				recordErr := w.recordOrphans(recoveryCtx, req, materialized)
				w.onFailure(recoveryCtx, req, "unknown", dmlErrorCategory(verifyErr))
				return nil, stderrors.Join(verifyErr, recordErr)
			}
			if ok {
				w.onSuccess(recoveryCtx, req, intent, materialized, *verified)
				return verified, nil
			}
			recordErr := w.recordOrphans(recoveryCtx, req, materialized)
			unknownErr := api.NewError(api.ErrCommitUnknown, "Iceberg DML commit result is unknown and could not be verified", map[string]string{"table": req.Stream.Base.Table})
			w.onFailure(recoveryCtx, req, "unknown", dmlErrorCategory(unknownErr))
			return nil, stderrors.Join(unknownErr, recordErr)
		}
		recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
		defer cancel()
		recordErr := w.recordOrphans(recoveryCtx, req, materialized)
		w.onFailure(recoveryCtx, req, "failed", dmlErrorCategory(err))
		return nil, stderrors.Join(err, recordErr)
	}
	if result == nil || result.Unknown {
		recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
		defer cancel()
		verified, ok, verifyErr := w.verifyUnknown(recoveryCtx, req, materialized, result)
		if verifyErr != nil {
			recordErr := w.recordOrphans(recoveryCtx, req, materialized)
			w.onFailure(recoveryCtx, req, "unknown", dmlErrorCategory(verifyErr))
			return nil, stderrors.Join(verifyErr, recordErr)
		}
		if ok {
			w.onSuccess(recoveryCtx, req, intent, materialized, *verified)
			return verified, nil
		}
		recordErr := w.recordOrphans(recoveryCtx, req, materialized)
		unknownErr := api.NewError(api.ErrCommitUnknown, "Iceberg DML commit result is unknown and could not be verified", map[string]string{"table": req.Stream.Base.Table})
		w.onFailure(recoveryCtx, req, "unknown", dmlErrorCategory(unknownErr))
		return nil, stderrors.Join(unknownErr, recordErr)
	}
	recoveryCtx, cancel := icebergwrite.NewRecoveryContext(ctx)
	defer cancel()
	committed := w.verifyCommitted(recoveryCtx, req, materialized, *result)
	w.onSuccess(recoveryCtx, req, intent, materialized, committed)
	return &committed, nil
}

func (w CommitWorkflow) verifyUnknown(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error) {
	if w.Verifier == nil {
		return nil, false, nil
	}
	verified, ok, err := w.Verifier.VerifyDMLCommit(ctx, req, materialized, result)
	if verified != nil {
		verified.Verified = ok
	}
	return verified, ok, err
}

func (w CommitWorkflow) verifyCommitted(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult, result api.CommitResult) api.CommitResult {
	if w.Verifier == nil {
		return result
	}
	verified, ok, err := w.Verifier.VerifyDMLCommit(ctx, req, materialized, &result)
	if err != nil {
		logDMLHookWarning("Iceberg DML commit verification failed after commit", req, result, err)
		result.Verified = false
		return result
	}
	if !ok || verified == nil {
		result.Verified = false
		return result
	}
	verified.Verified = true
	return *verified
}

func isDMLCommitUnknown(err error, result *api.CommitResult) bool {
	if result != nil && result.Unknown {
		return true
	}
	var iceErr *api.IcebergError
	return stderrors.As(err, &iceErr) && iceErr.Code == api.ErrCommitUnknown
}

func (w CommitWorkflow) onSuccess(ctx context.Context, req CommitWorkflowRequest, intent *CommitIntent, materialized *ManifestMaterializeResult, result api.CommitResult) {
	if w.CacheInvalidator != nil {
		if err := w.CacheInvalidator.InvalidateIcebergTable(ctx, dmlCacheInvalidationRequest(req), result); err != nil {
			logDMLHookWarning("Iceberg DML cache invalidation failed after commit", req, result, err)
		}
	}
	if w.AuditRecorder != nil {
		if err := w.AuditRecorder.RecordPublish(ctx, dmlPublishAudit(req, result, "committed", "")); err != nil {
			logDMLHookWarning("Iceberg DML audit record failed after commit", req, result, err)
		}
	}
	if w.MetricsReporter != nil && req.Stream.Base.CatalogCapabilities.MetricsReport {
		if err := w.MetricsReporter.ReportMetrics(ctx, dmlMetricsReport(req, intent, materialized, result)); err != nil {
			logDMLHookWarning("Iceberg DML metrics report failed after commit", req, result, err)
		}
	}
}

func dmlCacheInvalidationRequest(req CommitWorkflowRequest) api.AppendRequest {
	return api.AppendRequest{
		CatalogRequest: req.Catalog,
		Namespace:      append(api.Namespace(nil), req.Stream.Base.Namespace...),
		Table:          req.Stream.Base.Table,
		TargetRef:      req.Stream.Base.TargetRef,
	}
}

func (w CommitWorkflow) onFailure(ctx context.Context, req CommitWorkflowRequest, status, category string) {
	if w.AuditRecorder == nil {
		return
	}
	result := api.CommitResult{}
	if err := w.AuditRecorder.RecordPublish(ctx, dmlPublishAudit(req, result, status, category)); err != nil {
		logDMLHookWarning("Iceberg DML audit record failed after failed commit", req, result, err)
		return
	}
}

func (w CommitWorkflow) writeManifests(ctx context.Context, result *ManifestMaterializeResult) error {
	if result == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest materialization result is empty", nil)
	}
	for _, rewritten := range result.RewrittenPreservedManifests {
		if strings.TrimSpace(rewritten.Manifest.Path) == "" {
			return api.NewError(api.ErrConfigInvalid, "Iceberg DML rewritten preserved manifest path is empty", nil)
		}
		if err := w.ManifestWriter.WriteManifestObject(ctx, rewritten.Manifest.Path, rewritten.ManifestBytes); err != nil {
			return err
		}
	}
	for _, manifest := range result.DataManifests {
		if err := w.ManifestWriter.WriteManifestObject(ctx, manifest.Manifest.Path, manifest.ManifestBytes); err != nil {
			return err
		}
	}
	for _, manifest := range result.DeleteManifests {
		if err := w.ManifestWriter.WriteManifestObject(ctx, manifest.Manifest.Path, manifest.ManifestBytes); err != nil {
			return err
		}
	}
	if strings.TrimSpace(result.AttemptManifestListPath()) == "" {
		return api.NewError(api.ErrConfigInvalid, "Iceberg DML manifest list path is empty", nil)
	}
	return w.ManifestWriter.WriteManifestObject(ctx, result.AttemptManifestListPath(), result.ManifestListBytes)
}

func (w CommitWorkflow) recordOrphans(ctx context.Context, req CommitWorkflowRequest, materialized *ManifestMaterializeResult) error {
	if w.OrphanRecorder == nil {
		return nil
	}
	now := time.Now()
	if w.Now != nil {
		now = w.Now()
	}
	ttl := w.OrphanTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	paths := dmlOrphanPaths(req.Stream.Actions)
	if materialized != nil {
		paths = append(paths, dmlManifestPaths(materialized)...)
	}
	if len(paths) == 0 {
		return nil
	}
	candidates := make([]icebergwrite.OrphanCandidate, 0, len(paths))
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		candidates = append(candidates, icebergwrite.OrphanCandidate{
			AccountID:         req.Catalog.Catalog.AccountID,
			CatalogID:         req.Catalog.Catalog.CatalogID,
			JobID:             firstNonEmpty(req.Stream.Base.StatementID, req.Stream.Base.IdempotencyKey),
			Namespace:         strings.Join(req.Stream.Base.Namespace, "."),
			TableName:         req.Stream.Base.Table,
			TableLocationHash: api.PathHash(firstNonEmpty(req.TableLocation, req.Stream.Base.Table)),
			FilePath:          path,
			FilePathHash:      api.PathHash(path),
			FilePathRedacted:  api.RedactPath(path),
			WrittenAt:         now,
			ExpireAt:          now.Add(ttl),
			CleanupStatus:     "pending",
		})
	}
	return w.OrphanRecorder.RecordOrphans(ctx, candidates)
}

func dmlCommitRequest(catalog api.CatalogRequest, base CommitBase, attempt *api.CommitAttempt) api.CommitRequest {
	return api.CommitRequest{
		CatalogRequest: catalog,
		Namespace:      base.Namespace,
		Table:          base.Table,
		TargetRef:      firstNonEmpty(attempt.TargetRef, base.TargetRef, "main"),
		Requirements:   append([]api.CommitRequirement(nil), attempt.Requirements...),
		Updates:        append([]api.CommitUpdate(nil), attempt.Updates...),
		IdempotencyKey: firstNonEmpty(attempt.IdempotencyKey, base.IdempotencyKey),
		Summary:        cloneStringMap(attempt.Summary),
	}
}

func dmlPublishAudit(req CommitWorkflowRequest, result api.CommitResult, status, category string) icebergwrite.PublishAudit {
	base := req.Stream.Base
	return icebergwrite.PublishAudit{
		JobID:                firstNonEmpty(base.StatementID, base.IdempotencyKey),
		AccountID:            req.Catalog.Catalog.AccountID,
		TargetCatalogID:      req.Catalog.Catalog.CatalogID,
		TargetNamespace:      strings.Join(base.Namespace, "."),
		TargetTable:          base.Table,
		SourceBatch:          string(req.Stream.Operation),
		SnapshotID:           result.SnapshotID,
		MetadataLocationHash: result.MetadataLocationHash,
		CommitID:             result.CommitID,
		RowCount:             dmlActionRecordCount(req.Stream),
		FileCount:            dmlActionFileCount(req.Stream),
		Status:               status,
		ErrorCategory:        category,
	}
}

func dmlMetricsReport(req CommitWorkflowRequest, intent *CommitIntent, materialized *ManifestMaterializeResult, result api.CommitResult) api.MetricsReportRequest {
	extra := BuildAuditProfile(req.Stream, intent)
	if materialized != nil {
		if materialized.DataManifest != nil {
			extra["data_manifest"] = api.RedactPath(materialized.DataManifest.Path)
		}
		if materialized.DeleteManifest != nil {
			extra["delete_manifest"] = api.RedactPath(materialized.DeleteManifest.Path)
		}
		if path := materialized.AttemptManifestListPath(); path != "" {
			extra["manifest_list"] = api.RedactPath(path)
		}
	}
	base := req.Stream.Base
	return api.MetricsReportRequest{
		CatalogRequest:       req.Catalog,
		Namespace:            append(api.Namespace(nil), base.Namespace...),
		Table:                base.Table,
		Ref:                  firstNonEmpty(base.TargetRef, "main"),
		SnapshotID:           result.SnapshotID,
		StatementID:          base.StatementID,
		Kind:                 api.MetricsReportWrite,
		CommitID:             result.CommitID,
		MetadataLocationHash: result.MetadataLocationHash,
		Rows:                 dmlActionRecordCount(req.Stream),
		Files:                dmlActionFileCount(req.Stream),
		Extra:                extra,
	}
}

func dmlErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	var icebergErr *api.IcebergError
	if stderrors.As(err, &icebergErr) && icebergErr.Code != "" {
		return string(icebergErr.Code)
	}
	return string(api.ErrInternal)
}

func dmlActionRecordCount(stream ActionStream) int64 {
	var rows int64
	for _, action := range stream.Actions {
		switch action.Kind {
		case ActionAppendData:
			rows = saturatingDMLMetricAdd(rows, positiveRecordCount(action.File))
		case ActionAddEqualityDelete, ActionAddPositionDelete:
			rows = saturatingDMLMetricAdd(rows, positiveRecordCount(action.DeleteFile))
		case ActionDeleteDataFile:
			rows = saturatingDMLMetricAdd(rows, positiveRecordCount(action.ReplacedFile))
		case ActionRewriteDataFile:
			rows = saturatingDMLMetricAdd(rows, positiveRecordCount(action.ReplacedFile))
			for _, file := range action.ReplacementFiles {
				rows = saturatingDMLMetricAdd(rows, positiveRecordCount(file))
			}
		}
	}
	return rows
}

func dmlActionFileCount(stream ActionStream) int {
	var files int
	for _, action := range stream.Actions {
		switch action.Kind {
		case ActionAppendData:
			files += presentDataFileCount(action.File)
		case ActionAddEqualityDelete, ActionAddPositionDelete:
			files += presentDataFileCount(action.DeleteFile)
		case ActionDeleteDataFile:
			files += presentDataFileCount(action.ReplacedFile)
		case ActionRewriteDataFile:
			files += presentDataFileCount(action.ReplacedFile)
			for _, file := range action.ReplacementFiles {
				files += presentDataFileCount(file)
			}
		}
	}
	return files
}

func presentDataFileCount(file api.DataFile) int {
	if strings.TrimSpace(file.FilePath) != "" {
		return 1
	}
	return 0
}

func positiveRecordCount(file api.DataFile) int64 {
	if file.RecordCount <= 0 {
		return 0
	}
	return file.RecordCount
}

func saturatingDMLMetricAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func logDMLHookWarning(message string, req CommitWorkflowRequest, result api.CommitResult, err error) {
	logutil.Warn(message,
		zap.Uint32("account-id", req.Catalog.Catalog.AccountID),
		zap.Uint64("catalog-id", req.Catalog.Catalog.CatalogID),
		zap.String("namespace", strings.Join(req.Stream.Base.Namespace, ".")),
		zap.String("table", req.Stream.Base.Table),
		zap.String("operation", string(req.Stream.Operation)),
		zap.Int64("snapshot-id", result.SnapshotID),
		zap.String("commit-id", result.CommitID),
		zap.Error(err))
}

func (r *ManifestMaterializeResult) AttemptManifestListPath() string {
	if r == nil || r.Attempt == nil {
		return ""
	}
	for _, update := range r.Attempt.Updates {
		if update.Type == "set-manifest-list" {
			return update.FilePath
		}
		if update.Type == "add-snapshot" && update.Snapshot != nil {
			return update.Snapshot.ManifestList
		}
	}
	return ""
}

func dmlOrphanPaths(actions []Action) []string {
	paths := make([]string, 0, len(actions))
	for _, action := range actions {
		switch action.Kind {
		case ActionAppendData:
			paths = append(paths, action.File.FilePath)
		case ActionAddEqualityDelete, ActionAddPositionDelete:
			paths = append(paths, action.DeleteFile.FilePath)
		case ActionRewriteDataFile:
			for _, file := range action.ReplacementFiles {
				paths = append(paths, file.FilePath)
			}
		}
	}
	return paths
}

func dmlManifestPaths(result *ManifestMaterializeResult) []string {
	paths := make([]string, 0, 3)
	if result == nil {
		return paths
	}
	for _, rewritten := range result.RewrittenPreservedManifests {
		if rewritten.Manifest.Path != "" {
			paths = append(paths, rewritten.Manifest.Path)
		}
	}
	for _, manifest := range result.DataManifests {
		paths = append(paths, manifest.Manifest.Path)
	}
	for _, manifest := range result.DeleteManifests {
		paths = append(paths, manifest.Manifest.Path)
	}
	if path := result.AttemptManifestListPath(); path != "" {
		paths = append(paths, path)
	}
	return paths
}

func clonePreservedManifestSources(in []PreservedManifestSource) []PreservedManifestSource {
	if len(in) == 0 {
		return nil
	}
	out := make([]PreservedManifestSource, len(in))
	for i := range in {
		out[i].Manifest = in[i].Manifest
		out[i].Entries = append([]api.ManifestEntry(nil), in[i].Entries...)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
