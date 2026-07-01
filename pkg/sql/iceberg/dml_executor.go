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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
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
	stream, err := BuildDMLDeleteActionStream(ctx, req)
	if err != nil {
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
	stream, err := BuildDMLUpdateActionStream(ctx, req)
	if err != nil {
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
	stream, err := BuildDMLMergeActionStream(ctx, req)
	if err != nil {
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
	stream, err := BuildDMLOverwriteActionStream(ctx, req)
	if err != nil {
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
