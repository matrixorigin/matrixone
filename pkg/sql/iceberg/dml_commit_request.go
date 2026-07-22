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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
)

type DMLCommitWorkflowRequestSpec struct {
	Catalog            api.CatalogRequest
	Stream             dml.ActionStream
	TableLocation      string
	FormatVersion      int
	Schema             api.Schema
	PartitionSpecs     []api.PartitionSpec
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	PreservedManifests []api.ManifestFile
	PreservedSources   []dml.PreservedManifestSource
	MaxMemoryBytes     int64
	InitialMemoryBytes int64
}

type DMLCommitActionStreamSpec struct {
	Workflow           dml.CommitWorkflow
	Catalog            api.CatalogRequest
	Stream             dml.ActionStream
	TableLocation      string
	FormatVersion      int
	Schema             api.Schema
	PartitionSpecs     []api.PartitionSpec
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	PreservedManifests []api.ManifestFile
	PreservedSources   []dml.PreservedManifestSource
	MaxMemoryBytes     int64
	InitialMemoryBytes int64
}

type DMLCommitActionStreamResult struct {
	CommitResult *api.CommitResult
	Request      dml.CommitWorkflowRequest
	Profile      map[string]string
}

func BuildDMLCommitWorkflowRequest(ctx context.Context, spec DMLCommitWorkflowRequestSpec) (dml.CommitWorkflowRequest, error) {
	tableLocation := strings.TrimRight(strings.TrimSpace(spec.TableLocation), "/")
	if tableLocation == "" {
		return dml.CommitWorkflowRequest{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML commit request requires table location", map[string]string{
			"table": spec.Stream.Base.Table,
		}))
	}
	if spec.SnapshotID <= 0 || spec.SequenceNumber <= 0 {
		return dml.CommitWorkflowRequest{}, api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg DML commit request requires positive snapshot and sequence numbers", map[string]string{
			"table": spec.Stream.Base.Table,
		}))
	}
	paths, err := BuildDMLManifestPaths(ctx, DMLManifestPathRequest{
		TableLocation: tableLocation,
		Stream:        spec.Stream,
		SnapshotID:    spec.SnapshotID,
	})
	if err != nil {
		return dml.CommitWorkflowRequest{}, err
	}
	return dml.CommitWorkflowRequest{
		Catalog:            spec.Catalog,
		Stream:             spec.Stream,
		FormatVersion:      spec.FormatVersion,
		Schema:             spec.Schema,
		PartitionSpecs:     append([]api.PartitionSpec(nil), spec.PartitionSpecs...),
		SnapshotID:         spec.SnapshotID,
		SequenceNumber:     spec.SequenceNumber,
		TimestampMS:        spec.TimestampMS,
		DataManifestPath:   paths.DataManifestPath,
		DeleteManifestPath: paths.DeleteManifestPath,
		ManifestListPath:   paths.ManifestListPath,
		PreservedManifests: append([]api.ManifestFile(nil), spec.PreservedManifests...),
		PreservedSources: append([]dml.PreservedManifestSource(nil),
			spec.PreservedSources...),
		TableLocation:      tableLocation,
		MaxMemoryBytes:     spec.MaxMemoryBytes,
		InitialMemoryBytes: spec.InitialMemoryBytes,
	}, nil
}

func CommitDMLActionStream(ctx context.Context, spec DMLCommitActionStreamSpec) (DMLCommitActionStreamResult, error) {
	req, err := BuildDMLCommitWorkflowRequest(ctx, DMLCommitWorkflowRequestSpec{
		Catalog:        spec.Catalog,
		Stream:         spec.Stream,
		TableLocation:  spec.TableLocation,
		FormatVersion:  spec.FormatVersion,
		Schema:         spec.Schema,
		PartitionSpecs: append([]api.PartitionSpec(nil), spec.PartitionSpecs...),
		SnapshotID:     spec.SnapshotID,
		SequenceNumber: spec.SequenceNumber,
		TimestampMS:    spec.TimestampMS,
		PreservedManifests: append([]api.ManifestFile(nil),
			spec.PreservedManifests...),
		PreservedSources: append([]dml.PreservedManifestSource(nil),
			spec.PreservedSources...),
		MaxMemoryBytes:     spec.MaxMemoryBytes,
		InitialMemoryBytes: spec.InitialMemoryBytes,
	})
	if err != nil {
		return DMLCommitActionStreamResult{}, err
	}
	intent, err := dml.BuildCommitIntent(spec.Stream)
	if err != nil {
		return DMLCommitActionStreamResult{}, api.ToMOErr(ctx, err)
	}
	commitResult, err := spec.Workflow.CommitDML(ctx, req)
	if err != nil {
		return DMLCommitActionStreamResult{}, api.ToMOErr(ctx, err)
	}
	return DMLCommitActionStreamResult{
		CommitResult: commitResult,
		Request:      req,
		Profile:      buildDMLActionStreamCommitProfile(spec.Stream, intent, req, commitResult),
	}, nil
}

func buildDMLActionStreamCommitProfile(stream dml.ActionStream, intent *dml.CommitIntent, req dml.CommitWorkflowRequest, result *api.CommitResult) map[string]string {
	profile := dml.BuildAuditProfile(stream, intent)
	if req.DataManifestPath != "" {
		profile["data_manifest"] = api.RedactPath(req.DataManifestPath)
	}
	if req.DeleteManifestPath != "" {
		profile["delete_manifest"] = api.RedactPath(req.DeleteManifestPath)
	}
	if req.ManifestListPath != "" {
		profile["manifest_list"] = api.RedactPath(req.ManifestListPath)
	}
	if result != nil {
		profile["snapshot_id"] = strconv.FormatInt(result.SnapshotID, 10)
		profile["commit_id"] = result.CommitID
		profile["metadata_location_hash"] = result.MetadataLocationHash
		profile["verified"] = strconv.FormatBool(result.Verified)
	}
	return profile
}
