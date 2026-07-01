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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

type RewriteManifestsMaterializer struct {
	Metadata     api.MetadataFacade
	ObjectReader api.ObjectReader
	PathPrefix   string
}

type RewriteManifestsMaterializeRequest struct {
	Metadata       *api.TableMetadata
	Snapshot       api.Snapshot
	JobID          string
	IdempotencyKey string
}

type RewriteManifestsMaterializeResult struct {
	Manifests              []api.ManifestFile
	ManifestListPath       string
	Objects                []ObjectWrite
	PostCommitOrphanPaths  []string
	RewrittenManifestCount uint64
}

type NativeRewriteManifestsPlanner struct {
	Catalog      api.CatalogRequest
	Loader       MaintenanceTableMetadataLoader
	Now          func() time.Time
	Materializer RewriteManifestsMaterializer
}

func (p NativeRewriteManifestsPlanner) BuildMaintenanceCommit(ctx context.Context, req Request) (*CommitPlan, error) {
	if req.Operation != OperationRewriteManifests {
		return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg native rewrite-manifests planner only supports rewrite_manifests", map[string]string{
			"operation": string(req.Operation),
		})
	}
	if p.Loader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg native rewrite-manifests planner requires a metadata loader", nil)
	}
	meta, err := p.Loader.LoadMaintenanceTableMetadata(ctx, req)
	if err != nil {
		return nil, err
	}
	snapshot, err := maintenanceTargetSnapshot(meta, req.TargetRef, req.SnapshotBefore)
	if err != nil {
		return nil, err
	}
	materialized, err := p.Materializer.Materialize(ctx, RewriteManifestsMaterializeRequest{
		Metadata:       meta,
		Snapshot:       snapshot,
		JobID:          req.JobID,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	targetRef := firstNonEmptyString(req.TargetRef, "main")
	now := maintenanceNow(p.Now)
	newSnapshotID := nextMaintenanceSnapshotID(now, meta)
	sequenceNumber := nextMaintenanceSequenceNumber(meta)
	summary := map[string]string{
		"operation":           string(OperationRewriteManifests),
		"engine":              "matrixone",
		"idempotency-key":     firstNonEmptyString(req.IdempotencyKey, req.JobID),
		"base-snapshot":       strconv.FormatInt(snapshot.SnapshotID, 10),
		"rewritten-manifests": strconv.FormatUint(materialized.RewrittenManifestCount, 10),
	}
	updates := []api.CommitUpdate{
		api.NewAddSnapshotUpdate(api.NewCommitSnapshot(
			newSnapshotID,
			snapshot.SnapshotID,
			sequenceNumber,
			meta.CurrentSchemaID,
			now.UnixMilli(),
			materialized.ManifestListPath,
			summary,
		)),
		api.NewSetSnapshotRefUpdate(targetRef, req.TargetRefType, newSnapshotID),
	}
	return &CommitPlan{
		Catalog: p.Catalog,
		Attempt: &api.CommitAttempt{
			Requirements: []api.CommitRequirement{{
				Type:       "assert-ref-snapshot-id",
				Ref:        targetRef,
				SnapshotID: snapshot.SnapshotID,
			}},
			Updates:        updates,
			ManifestFiles:  append([]api.ManifestFile(nil), materialized.Manifests...),
			Summary:        summary,
			IdempotencyKey: firstNonEmptyString(req.IdempotencyKey, req.JobID),
			BaseSnapshotID: snapshot.SnapshotID,
			TargetRef:      targetRef,
			TargetRefType:  req.TargetRefType,
		},
		Objects:            append([]ObjectWrite(nil), materialized.Objects...),
		PostCommitOrphans:  append([]string(nil), materialized.PostCommitOrphanPaths...),
		RewrittenFileCount: materialized.RewrittenManifestCount,
	}, nil
}

func (m RewriteManifestsMaterializer) Materialize(ctx context.Context, req RewriteManifestsMaterializeRequest) (*RewriteManifestsMaterializeResult, error) {
	if m.ObjectReader == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests materializer requires an object reader", nil)
	}
	if strings.TrimSpace(req.Snapshot.ManifestList) == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests materializer requires a target manifest list", map[string]string{
			"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
		})
	}
	facade := m.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	manifestListData, err := m.ObjectReader.Read(ctx, req.Snapshot.ManifestList, 0, -1)
	if err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg rewrite-manifests materializer failed to read manifest list", map[string]string{
			"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
		}, err)
	}
	manifests, err := facade.ReadManifestList(ctx, manifestListData)
	if err != nil {
		return nil, err
	}
	if len(manifests) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests materializer found an empty manifest list", map[string]string{
			"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
		})
	}
	basePath, err := m.rewriteBasePath(req)
	if err != nil {
		return nil, err
	}
	result := &RewriteManifestsMaterializeResult{
		Manifests:             make([]api.ManifestFile, 0, len(manifests)),
		Objects:               make([]ObjectWrite, 0, len(manifests)+1),
		PostCommitOrphanPaths: []string{req.Snapshot.ManifestList},
	}
	for idx, manifest := range manifests {
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg rewrite-manifests materializer found manifest without path", map[string]string{
				"manifest_list": api.RedactPath(req.Snapshot.ManifestList),
			})
		}
		manifestData, err := m.ObjectReader.Read(ctx, manifestPath, 0, -1)
		if err != nil {
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg rewrite-manifests materializer failed to read manifest", map[string]string{
				"manifest": api.RedactPath(manifestPath),
			}, err)
		}
		entries, err := facade.ReadManifest(ctx, manifestData)
		if err != nil {
			return nil, err
		}
		rewritePath := joinObjectPath(basePath, fmt.Sprintf("manifest-%05d.avro", idx))
		rewriteData, err := metadata.EncodeManifest(entries)
		if err != nil {
			return nil, err
		}
		rewrittenManifest := manifest
		rewrittenManifest.Path = rewritePath
		rewrittenManifest.Length = int64(len(rewriteData))
		rewrittenManifest.ManifestPathRedacted = api.RedactPath(rewritePath)
		rewrittenManifest.ManifestPathHash = api.PathHash(rewritePath)
		result.Manifests = append(result.Manifests, rewrittenManifest)
		result.Objects = append(result.Objects, ObjectWrite{Location: rewritePath, Payload: rewriteData})
		result.PostCommitOrphanPaths = append(result.PostCommitOrphanPaths, manifestPath)
	}
	manifestListPath := joinObjectPath(basePath, "manifest-list.avro")
	manifestListBytes, err := metadata.EncodeManifestList(result.Manifests)
	if err != nil {
		return nil, err
	}
	result.ManifestListPath = manifestListPath
	result.Objects = append(result.Objects, ObjectWrite{Location: manifestListPath, Payload: manifestListBytes})
	result.PostCommitOrphanPaths = dedupeNonEmptyStrings(result.PostCommitOrphanPaths)
	result.RewrittenManifestCount = uint64(len(result.Manifests))
	return result, nil
}

func (m RewriteManifestsMaterializer) rewriteBasePath(req RewriteManifestsMaterializeRequest) (string, error) {
	base := strings.TrimRight(strings.TrimSpace(m.PathPrefix), "/")
	if base == "" && req.Metadata != nil && strings.TrimSpace(req.Metadata.Location) != "" {
		base = joinObjectPath(req.Metadata.Location, "metadata")
	}
	if base == "" {
		base = objectDir(req.Snapshot.ManifestList)
	}
	if base == "" {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg rewrite-manifests materializer requires table location, manifest list path, or path prefix", map[string]string{
			"snapshot_id": strconv.FormatInt(req.Snapshot.SnapshotID, 10),
		})
	}
	return joinObjectPath(base, "mo-rewrite-manifests", rewriteManifestsID(req)), nil
}

func rewriteManifestsID(req RewriteManifestsMaterializeRequest) string {
	raw := firstNonEmptyString(req.IdempotencyKey, req.JobID, strconv.FormatInt(req.Snapshot.SnapshotID, 10))
	if raw == "" {
		raw = "rewrite-manifests"
	}
	return "rw-" + api.PathHash(raw)
}

func joinObjectPath(base string, parts ...string) string {
	out := strings.TrimRight(strings.TrimSpace(base), "/")
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), "/")
		if part == "" {
			continue
		}
		if out == "" {
			out = part
		} else {
			out += "/" + part
		}
	}
	return out
}

func objectDir(location string) string {
	location = strings.TrimSpace(location)
	if location == "" {
		return ""
	}
	idx := strings.LastIndex(location, "/")
	if idx <= 0 {
		return ""
	}
	return strings.TrimRight(location[:idx], "/")
}
