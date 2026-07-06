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
	stderrors "errors"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type AppendBuilder struct{}

func (AppendBuilder) BuildAppend(ctx context.Context, req api.AppendRequest) (*api.CommitAttempt, error) {
	if strings.TrimSpace(req.Table) == "" || len(req.Namespace) == 0 {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append requires namespace and table", nil)
	}
	if len(req.DataFiles) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg append requires at least one data file", nil)
	}
	if strings.TrimSpace(req.TargetRef) == "" {
		req.TargetRef = "main"
	}
	var err error
	req, err = NormalizeAppendTargetRef(req)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append requires an idempotency key", nil)
	}
	if err := ValidateAppendRequest(req); err != nil {
		return nil, err
	}
	requirements := []api.CommitRequirement{refSnapshotRequirement(req.TargetRef, req.BaseSnapshotID)}
	if req.TableUUID != "" {
		requirements = append(requirements, api.CommitRequirement{Type: "assert-table-uuid", TableUUID: req.TableUUID})
	}
	requirements = append(requirements, api.CommitRequirement{Type: "assert-current-schema-id", SchemaID: req.BaseSchemaID})
	requirements = append(requirements, api.CommitRequirement{Type: "assert-default-spec-id", SpecID: req.BaseSpecID})
	if req.BaseSortOrderID != 0 {
		requirements = append(requirements, api.CommitRequirement{Type: "assert-default-sort-order-id", SortOrderID: req.BaseSortOrderID})
	}
	dataFiles := cloneDataFiles(req.DataFiles)
	updates := make([]api.CommitUpdate, 0, len(dataFiles)+1)
	for i := range dataFiles {
		file := dataFiles[i]
		updates = append(updates, api.CommitUpdate{
			Type:     "add-data-file",
			FilePath: file.FilePath,
			DataFile: &file,
			Payload: map[string]string{
				"record_count":       strconv.FormatInt(file.RecordCount, 10),
				"file_size_in_bytes": strconv.FormatInt(file.FileSizeInBytes, 10),
				"file_format":        file.FileFormat,
			},
		})
	}
	summary := cloneStringMap(req.Summary)
	if summary == nil {
		summary = make(map[string]string)
	}
	summary["operation"] = "append"
	summary["engine"] = "matrixone"
	summary["idempotency-key"] = req.IdempotencyKey
	if req.SourceBatch != "" {
		summary["source-batch"] = req.SourceBatch
	}
	if req.SourceQueryID != "" {
		summary["source-query-id"] = req.SourceQueryID
	}
	if req.WriterID != "" {
		summary["writer-id"] = req.WriterID
	}
	summary["added-data-files"] = strconv.Itoa(len(dataFiles))
	summary["added-records"] = strconv.FormatInt(totalRecords(dataFiles), 10)
	updates = append(updates, api.CommitUpdate{Type: "set-snapshot-summary", Payload: cloneStringMap(summary)})
	return &api.CommitAttempt{
		Requirements:   requirements,
		Updates:        updates,
		DataFiles:      dataFiles,
		Summary:        summary,
		IdempotencyKey: req.IdempotencyKey,
		BaseSnapshotID: req.BaseSnapshotID,
		TargetRef:      req.TargetRef,
		TargetRefType:  req.TargetRefType,
	}, nil
}

type AppendWorkflow struct {
	Builder                api.WriteBuilder
	Committer              api.Committer
	Verifier               CommitVerifier
	ReloadBase             ReloadBaseFunc
	RetryPolicy            CompatibleRetryPolicy
	OrphanRecorder         OrphanRecorder
	OrphanCleaner          OrphanCleaner
	ImmediateOrphanCleanup bool
	AuditRecorder          AuditRecorder
	CacheInvalidator       CacheInvalidator
	MetricsReporter        api.MetricsReporter
	MaxConflictRetry       int
	Now                    func() time.Time
	OrphanTTL              time.Duration
}

type CompatibleRetryPolicy struct {
	AllowAddOptionalColumns     bool
	AllowPartitionSpecEvolution bool
	AllowSortOrderIDChange      bool
}

type CommitVerifier interface {
	VerifyCommit(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, result *api.CommitResult) (*api.CommitResult, bool, error)
}

type ReloadBaseFunc func(context.Context, api.AppendRequest) (BaseState, error)

type BaseState struct {
	SnapshotID   int64
	SchemaID     int
	SpecID       int
	SortOrderID  int
	MetadataHash string
	Schema       api.Schema
	Spec         api.PartitionSpec
	KnownSpecs   []api.PartitionSpec
}

type OrphanRecorder interface {
	RecordOrphans(ctx context.Context, candidates []OrphanCandidate) error
}

type OrphanCleaner interface {
	CleanupOrphan(ctx context.Context, candidate OrphanCandidate) error
}

type OrphanCandidate struct {
	AccountID         uint32
	CatalogID         uint64
	JobID             string
	Namespace         string
	TableName         string
	TableLocationHash string
	FilePath          string
	FilePathHash      string
	FilePathRedacted  string
	WrittenAt         time.Time
	ExpireAt          time.Time
	CleanupStatus     string
	Version           uint64
}

type AuditRecorder interface {
	RecordPublish(ctx context.Context, audit PublishAudit) error
}

type PublishAudit struct {
	JobID                string
	AccountID            uint32
	TargetCatalogID      uint64
	TargetNamespace      string
	TargetTable          string
	SourceDB             string
	SourceTable          string
	SourceBatch          string
	WatermarkStart       string
	WatermarkEnd         string
	BusinessWindow       string
	SnapshotID           int64
	MetadataLocationHash string
	CommitID             string
	RowCount             int64
	FileCount            int
	Status               string
	ErrorCategory        string
}

type CacheInvalidator interface {
	InvalidateIcebergTable(ctx context.Context, req api.AppendRequest, result api.CommitResult) error
}

func (w AppendWorkflow) CommitAppend(ctx context.Context, req api.AppendRequest) (*api.CommitResult, error) {
	if w.Builder == nil {
		w.Builder = AppendBuilder{}
	}
	if w.Committer == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg append workflow requires a committer", nil)
	}
	attempt, err := w.Builder.BuildAppend(ctx, req)
	if err != nil {
		return nil, err
	}
	maxRetries := w.MaxConflictRetry
	if maxRetries < 0 {
		maxRetries = 0
	}
	for retry := 0; ; retry++ {
		result, err := w.Committer.CommitTable(ctx, commitRequest(req, attempt))
		if err == nil && result != nil && !result.Unknown {
			w.onSuccess(ctx, req, attempt, *result)
			return result, nil
		}
		if isUnknownResult(err, result) {
			verified, ok, verifyErr := w.verifyUnknown(ctx, req, attempt, result)
			if verifyErr != nil {
				_ = w.recordOrphans(ctx, req, attempt, false)
				w.onFailure(ctx, req, attempt, "unknown", appendErrorCategory(verifyErr))
				return nil, verifyErr
			}
			if ok {
				w.onSuccess(ctx, req, attempt, *verified)
				return verified, nil
			}
			_ = w.recordOrphans(ctx, req, attempt, false)
			unknownErr := api.NewError(api.ErrCommitUnknown, "Iceberg append commit result is unknown and could not be verified", map[string]string{"table": req.Table})
			w.onFailure(ctx, req, attempt, "unknown", appendErrorCategory(unknownErr))
			return nil, unknownErr
		}
		if isConflict(err) && retry < maxRetries {
			base, reloadErr := w.reloadBase(ctx, req)
			if reloadErr != nil {
				_ = w.recordOrphans(ctx, req, attempt, true)
				w.onFailure(ctx, req, attempt, "failed", appendErrorCategory(reloadErr))
				return nil, reloadErr
			}
			if compatibleRetry(req, base, w.RetryPolicy) {
				req = rebaseAppendRequest(req, base)
				attempt, err = w.Builder.BuildAppend(ctx, req)
				if err != nil {
					_ = w.recordOrphans(ctx, req, attempt, true)
					w.onFailure(ctx, req, attempt, "failed", appendErrorCategory(err))
					return nil, err
				}
				continue
			}
		}
		if err != nil {
			_ = w.recordOrphans(ctx, req, attempt, true)
			w.onFailure(ctx, req, attempt, "failed", appendErrorCategory(err))
			return nil, err
		}
		_ = w.recordOrphans(ctx, req, attempt, false)
		unknownErr := api.NewError(api.ErrCommitUnknown, "Iceberg append commit did not return a result", map[string]string{"table": req.Table})
		w.onFailure(ctx, req, attempt, "unknown", appendErrorCategory(unknownErr))
		return nil, unknownErr
	}
}

func (w AppendWorkflow) reloadBase(ctx context.Context, req api.AppendRequest) (BaseState, error) {
	if w.ReloadBase == nil {
		return BaseState{}, api.NewError(api.ErrCommitConflict, "Iceberg append commit conflict requires metadata reload", map[string]string{"table": req.Table})
	}
	return w.ReloadBase(ctx, req)
}

func (w AppendWorkflow) verifyUnknown(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, result *api.CommitResult) (*api.CommitResult, bool, error) {
	if w.Verifier == nil {
		return nil, false, nil
	}
	verified, ok, err := w.Verifier.VerifyCommit(ctx, req, attempt, result)
	if verified != nil {
		verified.Verified = ok
	}
	return verified, ok, err
}

func (w AppendWorkflow) onSuccess(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, result api.CommitResult) {
	if w.CacheInvalidator != nil {
		if err := w.CacheInvalidator.InvalidateIcebergTable(ctx, req, result); err != nil {
			logAppendHookWarning("Iceberg append cache invalidation failed after commit", req, result, err)
		}
	}
	if w.AuditRecorder != nil {
		if err := w.AuditRecorder.RecordPublish(ctx, publishAudit(req, attempt, result, "committed", "")); err != nil {
			logAppendHookWarning("Iceberg append audit record failed after commit", req, result, err)
		}
	}
	if w.MetricsReporter != nil && req.CatalogCapabilities.MetricsReport {
		if err := w.MetricsReporter.ReportMetrics(ctx, appendMetricsReport(req, attempt, result)); err != nil {
			logAppendHookWarning("Iceberg append metrics report failed after commit", req, result, err)
		}
	}
}

func (w AppendWorkflow) onFailure(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, status, category string) {
	if w.AuditRecorder == nil {
		return
	}
	if err := w.AuditRecorder.RecordPublish(ctx, publishAudit(req, attempt, api.CommitResult{}, status, category)); err != nil {
		logAppendHookWarning("Iceberg append audit record failed after commit failure", req, api.CommitResult{}, err)
	}
}

func appendMetricsReport(req api.AppendRequest, attempt *api.CommitAttempt, result api.CommitResult) api.MetricsReportRequest {
	var rows int64
	var files int
	var summary map[string]string
	if attempt != nil {
		rows = totalRecords(attempt.DataFiles)
		files = len(attempt.DataFiles)
		summary = cloneStringMap(attempt.Summary)
	}
	return api.MetricsReportRequest{
		CatalogRequest:       req.CatalogRequest,
		Namespace:            append(api.Namespace(nil), req.Namespace...),
		Table:                req.Table,
		Ref:                  firstNonEmpty(req.TargetRef, "main"),
		SnapshotID:           result.SnapshotID,
		QueryID:              req.SourceQueryID,
		StatementID:          req.StatementID,
		Kind:                 api.MetricsReportWrite,
		CommitID:             result.CommitID,
		MetadataLocationHash: result.MetadataLocationHash,
		Rows:                 rows,
		Files:                files,
		Extra:                summary,
	}
}

func logAppendHookWarning(message string, req api.AppendRequest, result api.CommitResult, err error) {
	logutil.Warn(message,
		zap.Uint32("account-id", req.Catalog.AccountID),
		zap.Uint64("catalog-id", req.Catalog.CatalogID),
		zap.String("namespace", strings.Join(req.Namespace, ".")),
		zap.String("table", req.Table),
		zap.Int64("snapshot-id", result.SnapshotID),
		zap.String("commit-id", result.CommitID),
		zap.Error(err))
}

func (w AppendWorkflow) recordOrphans(ctx context.Context, req api.AppendRequest, attempt *api.CommitAttempt, allowImmediateCleanup bool) error {
	if w.OrphanRecorder == nil || attempt == nil || len(attempt.DataFiles) == 0 {
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
	candidates := make([]OrphanCandidate, 0, len(attempt.DataFiles))
	for _, file := range attempt.DataFiles {
		candidates = append(candidates, OrphanCandidate{
			AccountID:         req.Catalog.AccountID,
			CatalogID:         req.Catalog.CatalogID,
			JobID:             firstNonEmpty(req.PublishAuditHint.JobID, req.StatementID, req.IdempotencyKey),
			Namespace:         strings.Join(req.Namespace, "."),
			TableName:         req.Table,
			TableLocationHash: api.PathHash(firstNonEmpty(req.TableLocation, req.Table)),
			FilePath:          file.FilePath,
			FilePathHash:      firstNonEmpty(file.FilePathHash, api.PathHash(file.FilePath)),
			FilePathRedacted:  firstNonEmpty(file.FilePathRedacted, api.RedactPath(file.FilePath)),
			WrittenAt:         now,
			ExpireAt:          now.Add(ttl),
			CleanupStatus:     "pending",
		})
	}
	if err := w.OrphanRecorder.RecordOrphans(ctx, candidates); err != nil {
		return err
	}
	if w.OrphanCleaner == nil || !w.ImmediateOrphanCleanup || !allowImmediateCleanup {
		return nil
	}
	for _, candidate := range candidates {
		if err := w.OrphanCleaner.CleanupOrphan(ctx, candidate); err != nil {
			return err
		}
	}
	return nil
}

func refSnapshotRequirement(ref string, snapshotID int64) api.CommitRequirement {
	if snapshotID == 0 {
		return api.CommitRequirement{Type: "assert-ref-snapshot-id", Ref: ref}
	}
	return api.CommitRequirement{
		Type:       "assert-ref-snapshot-id",
		Ref:        ref,
		SnapshotID: snapshotID,
	}
}

func commitRequest(req api.AppendRequest, attempt *api.CommitAttempt) api.CommitRequest {
	return api.CommitRequest{
		CatalogRequest: req.CatalogRequest,
		Namespace:      req.Namespace,
		Table:          req.Table,
		TargetRef:      firstNonEmpty(attempt.TargetRef, req.TargetRef, "main"),
		Requirements:   append([]api.CommitRequirement(nil), attempt.Requirements...),
		Updates:        append([]api.CommitUpdate(nil), attempt.Updates...),
		IdempotencyKey: firstNonEmpty(attempt.IdempotencyKey, req.IdempotencyKey),
		Summary:        cloneStringMap(attempt.Summary),
	}
}

func compatibleRetry(req api.AppendRequest, base BaseState, policy CompatibleRetryPolicy) bool {
	if req.BaseSchemaID != base.SchemaID && !compatibleSchemaEvolution(req, base, policy) {
		return false
	}
	if req.BaseSpecID != base.SpecID && !compatibleSpecEvolution(req, base, policy) {
		return false
	}
	if req.BaseSortOrderID != base.SortOrderID && !policy.AllowSortOrderIDChange {
		return false
	}
	return true
}

func rebaseAppendRequest(req api.AppendRequest, base BaseState) api.AppendRequest {
	req.BaseSnapshotID = base.SnapshotID
	req.BaseSchemaID = base.SchemaID
	req.BaseSpecID = base.SpecID
	req.BaseSortOrderID = base.SortOrderID
	if len(base.Schema.Fields) > 0 {
		req.BaseSchema = base.Schema
	}
	if len(base.Spec.Fields) > 0 || base.Spec.SpecID != 0 {
		req.KnownPartitionSpecs = mergeKnownPartitionSpecs(req.BaseSpec, base.KnownSpecs)
		req.BaseSpec = base.Spec
	}
	return req
}

func mergeKnownPartitionSpecs(spec api.PartitionSpec, specs []api.PartitionSpec) []api.PartitionSpec {
	out := make([]api.PartitionSpec, 0, len(specs)+1)
	seen := make(map[int]struct{}, len(specs)+1)
	if len(spec.Fields) > 0 || spec.SpecID != 0 {
		out = append(out, clonePartitionSpec(spec))
		seen[spec.SpecID] = struct{}{}
	}
	for _, candidate := range specs {
		if _, exists := seen[candidate.SpecID]; exists {
			continue
		}
		out = append(out, clonePartitionSpec(candidate))
		seen[candidate.SpecID] = struct{}{}
	}
	return out
}

func compatibleSchemaEvolution(req api.AppendRequest, base BaseState, policy CompatibleRetryPolicy) bool {
	if !policy.AllowAddOptionalColumns {
		return false
	}
	if len(req.BaseSchema.Fields) == 0 || len(base.Schema.Fields) == 0 {
		return false
	}
	return schemaAllowsOnlyOptionalAdds(req.BaseSchema, base.Schema)
}

func schemaAllowsOnlyOptionalAdds(oldSchema, newSchema api.Schema) bool {
	oldByID := make(map[int]api.SchemaField, len(oldSchema.Fields))
	for _, field := range oldSchema.Fields {
		if field.ID == 0 {
			return false
		}
		oldByID[field.ID] = field
	}
	for _, field := range newSchema.Fields {
		old, exists := oldByID[field.ID]
		if !exists {
			if field.Required {
				return false
			}
			continue
		}
		if old.Name != field.Name || old.Required != field.Required || old.Type.String() != field.Type.String() {
			return false
		}
		delete(oldByID, field.ID)
	}
	return len(oldByID) == 0 && sameIntSet(oldSchema.IdentifierFieldIDs, newSchema.IdentifierFieldIDs)
}

func compatibleSpecEvolution(req api.AppendRequest, base BaseState, policy CompatibleRetryPolicy) bool {
	if !policy.AllowPartitionSpecEvolution {
		return false
	}
	if !allDataFilesUseSpec(req.DataFiles, req.BaseSpecID) {
		return false
	}
	if len(req.BaseSpec.Fields) == 0 && req.BaseSpec.SpecID == 0 {
		return false
	}
	for _, spec := range append([]api.PartitionSpec{base.Spec}, base.KnownSpecs...) {
		if spec.SpecID == req.BaseSpecID && partitionSpecsEqual(req.BaseSpec, spec) {
			return true
		}
	}
	return false
}

func allDataFilesUseSpec(files []api.DataFile, specID int) bool {
	for _, file := range files {
		if file.SpecID != 0 && file.SpecID != specID {
			return false
		}
	}
	return true
}

func partitionSpecsEqual(a, b api.PartitionSpec) bool {
	if a.SpecID != b.SpecID || len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

func sameIntSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[int]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}
	return true
}

func isConflict(err error) bool {
	var icebergErr *api.IcebergError
	return stderrors.As(err, &icebergErr) && icebergErr.Code == api.ErrCommitConflict
}

func isUnknownResult(err error, result *api.CommitResult) bool {
	if result != nil && result.Unknown {
		return true
	}
	var icebergErr *api.IcebergError
	return stderrors.As(err, &icebergErr) && icebergErr.Code == api.ErrCommitUnknown
}

func publishAudit(req api.AppendRequest, attempt *api.CommitAttempt, result api.CommitResult, status, category string) PublishAudit {
	hint := req.PublishAuditHint
	var rowCount int64
	var fileCount int
	if attempt != nil {
		rowCount = totalRecords(attempt.DataFiles)
		fileCount = len(attempt.DataFiles)
	}
	return PublishAudit{
		JobID:                firstNonEmpty(hint.JobID, req.StatementID, req.IdempotencyKey),
		AccountID:            req.Catalog.AccountID,
		TargetCatalogID:      req.Catalog.CatalogID,
		TargetNamespace:      strings.Join(req.Namespace, "."),
		TargetTable:          req.Table,
		SourceDB:             hint.SourceDB,
		SourceTable:          hint.SourceTable,
		SourceBatch:          firstNonEmpty(hint.SourceBatch, req.SourceBatch),
		WatermarkStart:       hint.WatermarkStart,
		WatermarkEnd:         hint.WatermarkEnd,
		BusinessWindow:       hint.BusinessWindow,
		SnapshotID:           result.SnapshotID,
		MetadataLocationHash: result.MetadataLocationHash,
		CommitID:             result.CommitID,
		RowCount:             rowCount,
		FileCount:            fileCount,
		Status:               status,
		ErrorCategory:        category,
	}
}

func appendErrorCategory(err error) string {
	if err == nil {
		return ""
	}
	var icebergErr *api.IcebergError
	if stderrors.As(err, &icebergErr) {
		return string(icebergErr.Code)
	}
	return string(api.ErrInternal)
}

func totalRecords(files []api.DataFile) int64 {
	var total int64
	for _, file := range files {
		total += file.RecordCount
	}
	return total
}

func cloneDataFiles(in []api.DataFile) []api.DataFile {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.DataFile(nil), in...)
	for i := range out {
		out[i].Partition = cloneAnyMap(in[i].Partition)
		out[i].PartitionFieldIDs = cloneStringIntMap(in[i].PartitionFieldIDs)
		out[i].ColumnSizes = cloneInt64Map(in[i].ColumnSizes)
		out[i].ValueCounts = cloneInt64Map(in[i].ValueCounts)
		out[i].NullValueCounts = cloneInt64Map(in[i].NullValueCounts)
		out[i].NaNValueCounts = cloneInt64Map(in[i].NaNValueCounts)
		out[i].LowerBounds = cloneBytesMap(in[i].LowerBounds)
		out[i].UpperBounds = cloneBytesMap(in[i].UpperBounds)
		out[i].SplitOffsets = append([]int64(nil), in[i].SplitOffsets...)
	}
	return out
}

func clonePartitionSpecs(in []api.PartitionSpec) []api.PartitionSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.PartitionSpec, len(in))
	for i := range in {
		out[i] = clonePartitionSpec(in[i])
	}
	return out
}

func clonePartitionSpec(in api.PartitionSpec) api.PartitionSpec {
	out := in
	out.Fields = append([]api.PartitionField(nil), in.Fields...)
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneInt64Map(in map[int]int64) map[int]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneBytesMap(in map[int][]byte) map[int][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int][]byte, len(in))
	for k, v := range in {
		out[k] = append([]byte(nil), v...)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
