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

package iceberg

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type DMLActionExecutor struct {
	Workflow           dml.CommitWorkflow
	Catalog            api.CatalogRequest
	TableLocation      string
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	PreservedManifests []api.ManifestFile
	PreservedSources   []dml.PreservedManifestSource
}

func (e DMLActionExecutor) CommitDelete(ctx context.Context, req DMLDeleteActionStreamRequest) (DMLCommitActionStreamResult, error) {
	req.TableLocation = firstNonEmpty(req.TableLocation, e.TableLocation)
	if req.SnapshotID == 0 {
		req.SnapshotID = e.SnapshotID
	}
	if err := e.validateCommitPrerequisites(ctx, req.TableLocation, req.SnapshotID); err != nil {
		return DMLCommitActionStreamResult{}, err
	}
	tracker := newDMLMaterializedObjectTracker()
	wrapDMLDeleteRequestWriters(&req, tracker)
	stream, err := BuildDMLDeleteActionStream(ctx, req)
	if err != nil {
		_ = e.recordMaterializedOrphans(ctx, req.TableLocation, req.Base, tracker.paths())
		return DMLCommitActionStreamResult{}, err
	}
	return e.commit(ctx, req.TableLocation, req.SnapshotID, stream)
}

func (e DMLActionExecutor) CommitUpdate(ctx context.Context, req DMLUpdateActionStreamRequest) (DMLCommitActionStreamResult, error) {
	req.TableLocation = firstNonEmpty(req.TableLocation, e.TableLocation)
	if req.SnapshotID == 0 {
		req.SnapshotID = e.SnapshotID
	}
	if err := e.validateCommitPrerequisites(ctx, req.TableLocation, req.SnapshotID); err != nil {
		return DMLCommitActionStreamResult{}, err
	}
	tracker := newDMLMaterializedObjectTracker()
	wrapDMLUpdateRequestWriters(&req, tracker)
	stream, err := BuildDMLUpdateActionStream(ctx, req)
	if err != nil {
		_ = e.recordMaterializedOrphans(ctx, req.TableLocation, req.Base, tracker.paths())
		return DMLCommitActionStreamResult{}, err
	}
	return e.commit(ctx, req.TableLocation, req.SnapshotID, stream)
}

func (e DMLActionExecutor) CommitMerge(ctx context.Context, req DMLMergeActionStreamRequest) (DMLCommitActionStreamResult, error) {
	req.TableLocation = firstNonEmpty(req.TableLocation, e.TableLocation)
	if req.SnapshotID == 0 {
		req.SnapshotID = e.SnapshotID
	}
	if err := e.validateCommitPrerequisites(ctx, req.TableLocation, req.SnapshotID); err != nil {
		return DMLCommitActionStreamResult{}, err
	}
	tracker := newDMLMaterializedObjectTracker()
	wrapDMLMergeRequestWriters(&req, tracker)
	stream, err := BuildDMLMergeActionStream(ctx, req)
	if err != nil {
		_ = e.recordMaterializedOrphans(ctx, req.TableLocation, req.Base, tracker.paths())
		return DMLCommitActionStreamResult{}, err
	}
	return e.commit(ctx, req.TableLocation, req.SnapshotID, stream)
}

func (e DMLActionExecutor) CommitOverwrite(ctx context.Context, req DMLOverwriteActionStreamRequest) (DMLCommitActionStreamResult, error) {
	req.TableLocation = firstNonEmpty(req.TableLocation, e.TableLocation)
	if req.SnapshotID == 0 {
		req.SnapshotID = e.SnapshotID
	}
	if err := e.validateCommitPrerequisites(ctx, req.TableLocation, req.SnapshotID); err != nil {
		return DMLCommitActionStreamResult{}, err
	}
	tracker := newDMLMaterializedObjectTracker()
	wrapDMLOverwriteRequestWriters(&req, tracker)
	stream, err := BuildDMLOverwriteActionStream(ctx, req)
	if err != nil {
		_ = e.recordMaterializedOrphans(ctx, req.TableLocation, req.Base, tracker.paths())
		return DMLCommitActionStreamResult{}, err
	}
	return e.commit(ctx, req.TableLocation, req.SnapshotID, stream)
}

func (e DMLActionExecutor) commit(ctx context.Context, tableLocation string, snapshotID int64, stream *dml.ActionStream) (DMLCommitActionStreamResult, error) {
	if stream == nil {
		return DMLCommitActionStreamResult{}, api.ToMOErr(ctx, api.NewError(api.ErrMetadataInvalid, "Iceberg DML action executor produced an empty action stream", nil))
	}
	return CommitDMLActionStream(ctx, DMLCommitActionStreamSpec{
		Workflow:       e.Workflow,
		Catalog:        e.Catalog,
		Stream:         *stream,
		TableLocation:  strings.TrimRight(strings.TrimSpace(tableLocation), "/"),
		SnapshotID:     snapshotID,
		SequenceNumber: e.SequenceNumber,
		TimestampMS:    e.TimestampMS,
		PreservedManifests: append([]api.ManifestFile(nil),
			e.PreservedManifests...),
		PreservedSources: append([]dml.PreservedManifestSource(nil),
			e.PreservedSources...),
	})
}

func (e DMLActionExecutor) validateCommitPrerequisites(ctx context.Context, tableLocation string, snapshotID int64) error {
	if e.Workflow.ManifestWriter == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML action executor requires a manifest writer before materializing data or delete files", nil))
	}
	if e.Workflow.Committer == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML action executor requires a committer before materializing data or delete files", nil))
	}
	if strings.TrimSpace(tableLocation) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML action executor requires table location before materializing data or delete files", nil))
	}
	if snapshotID <= 0 || e.SequenceNumber <= 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML action executor requires positive snapshot and sequence numbers before materializing data or delete files", nil))
	}
	return nil
}

func (e DMLActionExecutor) recordMaterializedOrphans(ctx context.Context, tableLocation string, base dml.CommitBase, paths []string) error {
	if e.Workflow.OrphanRecorder == nil || len(paths) == 0 {
		return nil
	}
	now := time.Now()
	if e.Workflow.Now != nil {
		now = e.Workflow.Now()
	}
	ttl := e.Workflow.OrphanTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	candidates := make([]icebergwrite.OrphanCandidate, 0, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		candidates = append(candidates, icebergwrite.OrphanCandidate{
			AccountID:         e.Catalog.Catalog.AccountID,
			CatalogID:         e.Catalog.Catalog.CatalogID,
			JobID:             firstNonEmpty(base.StatementID, base.IdempotencyKey),
			Namespace:         strings.Join(base.Namespace, "."),
			TableName:         base.Table,
			TableLocationHash: api.PathHash(firstNonEmpty(tableLocation, base.Table)),
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
	return e.Workflow.OrphanRecorder.RecordOrphans(ctx, candidates)
}

type dmlMaterializedObjectTracker struct {
	seen  map[string]struct{}
	items []string
}

func newDMLMaterializedObjectTracker() *dmlMaterializedObjectTracker {
	return &dmlMaterializedObjectTracker{seen: make(map[string]struct{})}
}

func (t *dmlMaterializedObjectTracker) track(path string) {
	if t == nil {
		return
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	if _, ok := t.seen[path]; ok {
		return
	}
	t.seen[path] = struct{}{}
	t.items = append(t.items, path)
}

func (t *dmlMaterializedObjectTracker) pathsCopy() []string {
	if t == nil || len(t.items) == 0 {
		return nil
	}
	return append([]string(nil), t.items...)
}

func (t *dmlMaterializedObjectTracker) paths() []string {
	return t.pathsCopy()
}

type trackingDMLDeleteObjectWriter struct {
	inner   dml.DeleteObjectWriter
	tracker *dmlMaterializedObjectTracker
}

func (w trackingDMLDeleteObjectWriter) WriteObject(ctx context.Context, location string, payload []byte) error {
	if w.inner == nil {
		return api.NewError(api.ErrConfigInvalid, "Iceberg DML object tracker requires object writer", nil)
	}
	if err := w.inner.WriteObject(ctx, location, payload); err != nil {
		return err
	}
	w.tracker.track(location)
	return nil
}

type trackingDMLDataFileOutputFactory struct {
	inner   icebergwrite.DataFileOutputFactory
	tracker *dmlMaterializedObjectTracker
}

func (f trackingDMLDataFileOutputFactory) CreateDataFile(ctx context.Context, location string) (io.WriteCloser, error) {
	if f.inner == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg DML object tracker requires output factory", nil)
	}
	wc, err := f.inner.CreateDataFile(ctx, location)
	if err != nil {
		return nil, err
	}
	return trackingDMLDataFile{WriteCloser: wc, location: location, tracker: f.tracker}, nil
}

type trackingDMLDataFile struct {
	io.WriteCloser
	location string
	tracker  *dmlMaterializedObjectTracker
}

func (w trackingDMLDataFile) Close() error {
	if err := w.WriteCloser.Close(); err != nil {
		return err
	}
	w.tracker.track(w.location)
	return nil
}

func wrapDMLDeleteObjectWriter(writer dml.DeleteObjectWriter, tracker *dmlMaterializedObjectTracker) dml.DeleteObjectWriter {
	if writer == nil {
		return nil
	}
	return trackingDMLDeleteObjectWriter{inner: writer, tracker: tracker}
}

func wrapDMLDataFileOutputFactory(factory icebergwrite.DataFileOutputFactory, tracker *dmlMaterializedObjectTracker) icebergwrite.DataFileOutputFactory {
	if factory == nil {
		return nil
	}
	return trackingDMLDataFileOutputFactory{inner: factory, tracker: tracker}
}

func wrapDMLReplacementBatchWriters(batch *DMLReplacementDataBatch, tracker *dmlMaterializedObjectTracker) {
	if batch == nil {
		return
	}
	batch.ObjectWriter = wrapDMLDeleteObjectWriter(batch.ObjectWriter, tracker)
	batch.OutputFactory = wrapDMLDataFileOutputFactory(batch.OutputFactory, tracker)
}

func wrapDMLDeleteRequestWriters(req *DMLDeleteActionStreamRequest, tracker *dmlMaterializedObjectTracker) {
	if req == nil {
		return
	}
	req.ObjectWriter = wrapDMLDeleteObjectWriter(req.ObjectWriter, tracker)
}

func wrapDMLUpdateRequestWriters(req *DMLUpdateActionStreamRequest, tracker *dmlMaterializedObjectTracker) {
	if req == nil {
		return
	}
	wrapDMLDeleteRequestWriters(&req.DMLDeleteActionStreamRequest, tracker)
	wrapDMLReplacementBatchWriters(&req.ReplacementBatch, tracker)
	for idx := range req.ReplacementBatches {
		wrapDMLReplacementBatchWriters(&req.ReplacementBatches[idx], tracker)
	}
}

func wrapDMLMergeRequestWriters(req *DMLMergeActionStreamRequest, tracker *dmlMaterializedObjectTracker) {
	if req == nil {
		return
	}
	req.ObjectWriter = wrapDMLDeleteObjectWriter(req.ObjectWriter, tracker)
	wrapDMLReplacementBatchWriters(&req.MatchedUpdateReplacementBatch, tracker)
	for idx := range req.MatchedUpdateReplacementBatches {
		wrapDMLReplacementBatchWriters(&req.MatchedUpdateReplacementBatches[idx], tracker)
	}
	wrapDMLReplacementBatchWriters(&req.UnmatchedAppendBatch, tracker)
	for idx := range req.UnmatchedAppendBatches {
		wrapDMLReplacementBatchWriters(&req.UnmatchedAppendBatches[idx], tracker)
	}
}

func wrapDMLOverwriteRequestWriters(req *DMLOverwriteActionStreamRequest, tracker *dmlMaterializedObjectTracker) {
	if req == nil {
		return
	}
	req.ObjectWriter = wrapDMLDeleteObjectWriter(req.ObjectWriter, tracker)
	wrapDMLReplacementBatchWriters(&req.ReplacementBatch, tracker)
	for idx := range req.ReplacementBatches {
		wrapDMLReplacementBatchWriters(&req.ReplacementBatches[idx], tracker)
	}
}
