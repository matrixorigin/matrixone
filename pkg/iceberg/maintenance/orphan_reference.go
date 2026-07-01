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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
	"github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type MetadataReferenceChecker struct {
	Loader       MaintenanceTableMetadataLoader
	Metadata     api.MetadataFacade
	ObjectReader api.ObjectReader
}

func (c MetadataReferenceChecker) IsReferenced(ctx context.Context, candidate write.OrphanCandidate) (bool, error) {
	if c.Loader == nil {
		return false, api.NewError(api.ErrConfigInvalid, "Iceberg orphan reference checker requires a metadata loader", nil)
	}
	if c.ObjectReader == nil {
		return false, api.NewError(api.ErrConfigInvalid, "Iceberg orphan reference checker requires an object reader", nil)
	}
	facade := c.Metadata
	if facade == nil {
		facade = metadata.NativeFacade{}
	}
	path := strings.TrimSpace(candidate.FilePath)
	if path == "" {
		return false, api.NewError(api.ErrConfigInvalid, "Iceberg orphan reference checker requires a file path", nil)
	}
	if strings.TrimSpace(candidate.Namespace) == "" || strings.TrimSpace(candidate.TableName) == "" {
		return false, api.NewError(api.ErrConfigInvalid, "Iceberg orphan reference checker requires namespace and table", map[string]string{
			"location": api.RedactPath(path),
		})
	}
	meta, err := c.Loader.LoadMaintenanceTableMetadata(ctx, Request{
		AccountID: candidate.AccountID,
		CatalogID: candidate.CatalogID,
		Namespace: candidate.Namespace,
		Table:     candidate.TableName,
	})
	if err != nil {
		return false, err
	}
	return c.isPathReferencedByMetadata(ctx, facade, meta, path)
}

func (c MetadataReferenceChecker) isPathReferencedByMetadata(ctx context.Context, facade api.MetadataFacade, meta *api.TableMetadata, path string) (bool, error) {
	if meta == nil {
		return false, api.NewError(api.ErrMetadataInvalid, "Iceberg orphan reference checker requires table metadata", nil)
	}
	seenManifestLists := make(map[string]struct{})
	for _, snapshot := range meta.Snapshots {
		manifestList := strings.TrimSpace(snapshot.ManifestList)
		if manifestList == "" {
			continue
		}
		if manifestList == path {
			return true, nil
		}
		if _, ok := seenManifestLists[manifestList]; ok {
			continue
		}
		seenManifestLists[manifestList] = struct{}{}
		referenced, err := c.isPathReferencedByManifestList(ctx, facade, manifestList, path)
		if err != nil || referenced {
			return referenced, err
		}
	}
	return false, nil
}

func (c MetadataReferenceChecker) isPathReferencedByManifestList(ctx context.Context, facade api.MetadataFacade, manifestListPath, candidatePath string) (bool, error) {
	data, err := c.ObjectReader.Read(ctx, manifestListPath, 0, -1)
	if err != nil {
		return false, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg orphan reference checker failed to read manifest list", map[string]string{
			"manifest_list": api.RedactPath(manifestListPath),
		}, err)
	}
	manifests, err := facade.ReadManifestList(ctx, data)
	if err != nil {
		return false, err
	}
	seenManifests := make(map[string]struct{})
	for _, manifest := range manifests {
		manifestPath := strings.TrimSpace(manifest.Path)
		if manifestPath == "" {
			continue
		}
		if manifestPath == candidatePath {
			return true, nil
		}
		if _, ok := seenManifests[manifestPath]; ok {
			continue
		}
		seenManifests[manifestPath] = struct{}{}
		referenced, err := c.isPathReferencedByManifest(ctx, facade, manifestPath, candidatePath)
		if err != nil || referenced {
			return referenced, err
		}
	}
	return false, nil
}

func (c MetadataReferenceChecker) isPathReferencedByManifest(ctx context.Context, facade api.MetadataFacade, manifestPath, candidatePath string) (bool, error) {
	data, err := c.ObjectReader.Read(ctx, manifestPath, 0, -1)
	if err != nil {
		return false, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg orphan reference checker failed to read manifest", map[string]string{
			"manifest": api.RedactPath(manifestPath),
		}, err)
	}
	entries, err := facade.ReadManifest(ctx, data)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if strings.TrimSpace(entry.DataFile.FilePath) == candidatePath || strings.TrimSpace(entry.DataFile.ReferencedDataFile) == candidatePath {
			return true, nil
		}
	}
	return false, nil
}

var _ write.OrphanReferenceChecker = MetadataReferenceChecker{}
