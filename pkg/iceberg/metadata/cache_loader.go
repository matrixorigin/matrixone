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

package metadata

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type CachedTableMetadataLoader struct {
	Catalog          api.CatalogClient
	Metadata         api.MetadataFacade
	ObjectReader     api.ObjectReader
	Cache            *Cache
	CredentialHash   string
	ExternalRef      string
	SnapshotSelector api.SnapshotSelector
}

type LoadedTableMetadata struct {
	LoadTable         *api.LoadTableResponse
	Metadata          *api.TableMetadata
	CacheHit          bool
	Revalidated       bool
	MetadataBytes     int64
	MetadataCacheKey  CacheKey
	LocationCacheKey  CacheKey
	StorageCredential []api.StorageCredential
}

type CachedManifestReader struct {
	Metadata       api.MetadataFacade
	ObjectReader   api.ObjectReader
	Cache          *Cache
	BaseKey        CacheKey
	CredentialHash string
}

type CachedManifestListRead struct {
	Manifests []api.ManifestFile
	CacheHit  bool
	SizeBytes int64
}

type CachedManifestRead struct {
	Entries   []api.ManifestEntry
	CacheHit  bool
	SizeBytes int64
}

func (l CachedTableMetadataLoader) Load(ctx context.Context, req api.LoadTableRequest) (*LoadedTableMetadata, error) {
	if l.Catalog == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg cached metadata loader requires catalog client", nil)
	}
	if l.Metadata == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg cached metadata loader requires metadata facade", nil)
	}
	locationKey := l.locationCacheKey(req)
	if cached, ok := l.Cache.Get(locationKey); ok && cached.Metadata != nil {
		return &LoadedTableMetadata{
			Metadata:         cached.Metadata,
			CacheHit:         true,
			MetadataBytes:    cached.SizeBytes,
			LocationCacheKey: locationKey,
			MetadataCacheKey: l.metadataCacheKey(req, cached.MetadataLocation),
		}, nil
	}

	stale, hasStale := l.Cache.GetStaleForRevalidation(locationKey)
	loadReq := req
	if hasStale {
		loadReq.IfNoneMatch = stale.ETag
	}
	resp, err := l.Catalog.LoadTable(ctx, loadReq)
	if err != nil {
		return nil, err
	}
	if resp.NotModified {
		if stale.Metadata == nil {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg metadata cache revalidation succeeded but cached metadata is missing", nil)
		}
		l.Cache.Refresh(locationKey, stale.ETag)
		metadataKey := l.metadataCacheKey(req, stale.MetadataLocation)
		l.Cache.Refresh(metadataKey, stale.ETag)
		return &LoadedTableMetadata{
			LoadTable:         resp,
			Metadata:          stale.Metadata,
			CacheHit:          true,
			Revalidated:       true,
			MetadataBytes:     stale.SizeBytes,
			LocationCacheKey:  locationKey,
			MetadataCacheKey:  metadataKey,
			StorageCredential: cloneStorageCredentials(resp.StorageCredentials),
		}, nil
	}

	metadataLocation := strings.TrimSpace(resp.MetadataLocation)
	if metadataLocation == "" {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg REST load table response did not include metadata location", nil)
	}
	metadataJSON := append([]byte(nil), resp.MetadataJSON...)
	if len(metadataJSON) == 0 {
		if l.ObjectReader == nil {
			return nil, api.NewError(api.ErrConfigInvalid, "Iceberg metadata location requires object reader", map[string]string{
				"metadata_location": api.RedactPath(metadataLocation),
			})
		}
		metadataJSON, err = l.ObjectReader.Read(ctx, metadataLocation, 0, -1)
		if err != nil {
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg metadata JSON read failed", map[string]string{
				"metadata_location": api.RedactPath(metadataLocation),
			}, err)
		}
	}
	meta, err := l.Metadata.ParseTableMetadata(ctx, metadataJSON, metadataLocation)
	if err != nil {
		return nil, err
	}

	metadataKey := l.metadataCacheKey(req, metadataLocation)
	entry := CacheEntry{
		ETag:             resp.ETag,
		MetadataLocation: metadataLocation,
		MetadataJSON:     metadataJSON,
		Metadata:         meta,
		SizeBytes:        int64(len(metadataJSON)),
	}
	l.Cache.Put(locationKey, entry)
	l.Cache.Put(metadataKey, entry)
	return &LoadedTableMetadata{
		LoadTable:         cloneLoadTableResponse(resp),
		Metadata:          meta,
		MetadataBytes:     int64(len(metadataJSON)),
		LocationCacheKey:  locationKey,
		MetadataCacheKey:  metadataKey,
		StorageCredential: cloneStorageCredentials(resp.StorageCredentials),
	}, nil
}

func (r CachedManifestReader) ReadManifestList(ctx context.Context, manifestListPath string) ([]api.ManifestFile, bool, error) {
	read, err := r.ReadManifestListWithStats(ctx, manifestListPath)
	if err != nil {
		return nil, false, err
	}
	return read.Manifests, read.CacheHit, nil
}

func (r CachedManifestReader) ReadManifestListWithStats(ctx context.Context, manifestListPath string) (CachedManifestListRead, error) {
	key := r.cacheKey(CacheKindManifestList, manifestListPath)
	if cached, ok := r.Cache.Get(key); ok && len(cached.ManifestList) > 0 {
		return CachedManifestListRead{Manifests: cached.ManifestList, CacheHit: true, SizeBytes: cached.SizeBytes}, nil
	}
	if r.Metadata == nil || r.ObjectReader == nil {
		return CachedManifestListRead{}, api.NewError(api.ErrConfigInvalid, "Iceberg manifest list reader requires metadata facade and object reader", nil)
	}
	data, err := r.ObjectReader.Read(ctx, manifestListPath, 0, -1)
	if err != nil {
		return CachedManifestListRead{}, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest list read failed", map[string]string{
			"manifest_list": api.RedactPath(manifestListPath),
		}, err)
	}
	manifests, err := r.Metadata.ReadManifestList(ctx, data)
	if err != nil {
		return CachedManifestListRead{}, err
	}
	r.Cache.Put(key, CacheEntry{ManifestList: manifests, SizeBytes: int64(len(data))})
	return CachedManifestListRead{Manifests: manifests, SizeBytes: int64(len(data))}, nil
}

func (r CachedManifestReader) ReadManifest(ctx context.Context, manifestPath string) ([]api.ManifestEntry, bool, error) {
	read, err := r.ReadManifestWithStats(ctx, manifestPath)
	if err != nil {
		return nil, false, err
	}
	return read.Entries, read.CacheHit, nil
}

func (r CachedManifestReader) ReadManifestWithStats(ctx context.Context, manifestPath string) (CachedManifestRead, error) {
	key := r.cacheKey(CacheKindManifest, manifestPath)
	if cached, ok := r.Cache.Get(key); ok && len(cached.ManifestEntries) > 0 {
		return CachedManifestRead{Entries: cached.ManifestEntries, CacheHit: true, SizeBytes: cached.SizeBytes}, nil
	}
	if r.Metadata == nil || r.ObjectReader == nil {
		return CachedManifestRead{}, api.NewError(api.ErrConfigInvalid, "Iceberg manifest reader requires metadata facade and object reader", nil)
	}
	data, err := r.ObjectReader.Read(ctx, manifestPath, 0, -1)
	if err != nil {
		return CachedManifestRead{}, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest read failed", map[string]string{
			"manifest": api.RedactPath(manifestPath),
		}, err)
	}
	entries, err := r.Metadata.ReadManifest(ctx, data)
	if err != nil {
		return CachedManifestRead{}, err
	}
	r.Cache.Put(key, CacheEntry{ManifestEntries: entries, SizeBytes: int64(len(data))})
	return CachedManifestRead{Entries: entries, SizeBytes: int64(len(data))}, nil
}

func (l CachedTableMetadataLoader) locationCacheKey(req api.LoadTableRequest) CacheKey {
	key := l.baseKey(req)
	key.Kind = CacheKindMetadataLocation
	return key
}

func (l CachedTableMetadataLoader) metadataCacheKey(req api.LoadTableRequest, metadataLocation string) CacheKey {
	key := l.baseKey(req)
	key.Kind = CacheKindMetadataJSON
	key.MetadataLocationHash = api.PathHash(metadataLocation)
	return key
}

func (l CachedTableMetadataLoader) baseKey(req api.LoadTableRequest) CacheKey {
	ref := l.ExternalRef
	if ref == "" {
		ref = req.Snapshots
	}
	return CacheKey{
		AccountID:              req.Catalog.AccountID,
		CatalogID:              req.Catalog.CatalogID,
		Namespace:              api.NamespaceCacheKey(req.Namespace),
		Table:                  req.Table,
		Ref:                    ref,
		ExternalPrincipal:      req.ExternalPrincipal,
		SnapshotID:             snapshotID(l.SnapshotSelector),
		TimestampMS:            timestampMS(l.SnapshotSelector),
		CredentialIdentityHash: l.CredentialHash,
	}
}

func (r CachedManifestReader) cacheKey(kind CacheKind, path string) CacheKey {
	key := r.BaseKey
	key.Kind = kind
	key.ManifestPathHash = api.PathHash(path)
	if r.CredentialHash != "" {
		key.CredentialIdentityHash = r.CredentialHash
	}
	return key
}

func snapshotID(selector api.SnapshotSelector) int64 {
	if selector.HasSnapshotID {
		return selector.SnapshotID
	}
	return 0
}

func timestampMS(selector api.SnapshotSelector) int64 {
	if selector.HasTimestampMS {
		return selector.TimestampMS
	}
	return 0
}

func cloneLoadTableResponse(in *api.LoadTableResponse) *api.LoadTableResponse {
	if in == nil {
		return nil
	}
	out := *in
	out.MetadataJSON = append([]byte(nil), in.MetadataJSON...)
	out.Config = cloneStringMap(in.Config)
	out.StorageCredentials = cloneStorageCredentials(in.StorageCredentials)
	return &out
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

func cloneStorageCredentials(in []api.StorageCredential) []api.StorageCredential {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.StorageCredential(nil), in...)
	for i := range out {
		out[i].Config = cloneStringMap(in[i].Config)
	}
	return out
}
