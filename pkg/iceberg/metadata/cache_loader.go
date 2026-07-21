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
	stderrors "errors"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type CachedTableMetadataLoader struct {
	Catalog           api.CatalogClient
	Metadata          api.MetadataFacade
	ObjectReader      api.ObjectReader
	Cache             *Cache
	CredentialHash    string
	ExternalRef       string
	SnapshotSelector  api.SnapshotSelector
	PlanningMaxMemory int64
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
	planningMaxMemory := l.PlanningMaxMemory
	if planningMaxMemory <= 0 {
		planningMaxMemory = api.DefaultConfig().Scan.PlanningMaxMemory
	}
	if len(metadataJSON) == 0 {
		if l.ObjectReader == nil {
			return nil, api.NewError(api.ErrConfigInvalid, "Iceberg metadata location requires object reader", map[string]string{
				"metadata_location": api.RedactPath(metadataLocation),
			})
		}
		metadataJSON, err = readPlanningObject(ctx, l.ObjectReader, metadataLocation, planningMaxMemory)
		if err != nil {
			if isPlanningLimitError(err) {
				return nil, err
			}
			return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg metadata JSON read failed", map[string]string{
				"metadata_location": api.RedactPath(metadataLocation),
			}, err)
		}
	}
	// Reject obviously oversized JSON before parsing creates its decoded object
	// graph. The same estimator is used for cache/planning accounting below.
	if estimate := metadataMemoryWeight(cap(metadataJSON), nil); estimate > planningMaxMemory {
		return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg table metadata exceeded the planning memory limit", map[string]string{
			"actual_bytes": strconv.FormatInt(estimate, 10),
			"limit_bytes":  strconv.FormatInt(planningMaxMemory, 10),
		})
	}
	meta, err := l.Metadata.ParseTableMetadata(ctx, metadataJSON, metadataLocation)
	if err != nil {
		return nil, err
	}

	metadataKey := l.metadataCacheKey(req, metadataLocation)
	memoryBytes := metadataMemoryWeight(cap(metadataJSON), meta)
	if memoryBytes > planningMaxMemory {
		return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg decoded table metadata exceeded the planning memory limit", map[string]string{
			"actual_bytes": strconv.FormatInt(memoryBytes, 10),
			"limit_bytes":  strconv.FormatInt(planningMaxMemory, 10),
		})
	}
	entry := CacheEntry{
		ETag:             resp.ETag,
		MetadataLocation: metadataLocation,
		MetadataJSON:     metadataJSON,
		Metadata:         meta,
		SizeBytes:        memoryBytes,
	}
	l.Cache.Put(locationKey, entry)
	l.Cache.Put(metadataKey, entry)
	return &LoadedTableMetadata{
		LoadTable:         cloneLoadTableResponse(resp),
		Metadata:          meta,
		MetadataBytes:     memoryBytes,
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
	return r.ReadManifestListWithLimits(ctx, manifestListPath, 0, 0)
}

func (r CachedManifestReader) ReadManifestListWithLimits(ctx context.Context, manifestListPath string, maxBytes int64, maxRecords int) (CachedManifestListRead, error) {
	key := r.cacheKey(CacheKindManifestList, manifestListPath)
	if cached, ok := r.Cache.Get(key); ok && len(cached.ManifestList) > 0 {
		if err := validatePlanningReadLimits("manifest_list", cached.SizeBytes, len(cached.ManifestList), maxBytes, maxRecords); err != nil {
			return CachedManifestListRead{}, err
		}
		return CachedManifestListRead{Manifests: cached.ManifestList, CacheHit: true, SizeBytes: cached.SizeBytes}, nil
	}
	if r.Metadata == nil || r.ObjectReader == nil {
		return CachedManifestListRead{}, api.NewError(api.ErrConfigInvalid, "Iceberg manifest list reader requires metadata facade and object reader", nil)
	}
	data, err := readPlanningObject(ctx, r.ObjectReader, manifestListPath, maxBytes)
	if err != nil {
		if isPlanningLimitError(err) {
			return CachedManifestListRead{}, err
		}
		return CachedManifestListRead{}, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest list read failed", map[string]string{
			"manifest_list": api.RedactPath(manifestListPath),
		}, err)
	}
	var manifests []api.ManifestFile
	if bounded, ok := r.Metadata.(interface {
		ReadManifestListWithLimits(context.Context, []byte, int, int64) ([]api.ManifestFile, error)
	}); ok && maxRecords > 0 && maxBytes > 0 {
		manifests, err = bounded.ReadManifestListWithLimits(ctx, data, maxRecords, maxBytes)
	} else if bounded, ok := r.Metadata.(interface {
		ReadManifestListBounded(context.Context, []byte, int) ([]api.ManifestFile, error)
	}); ok && maxRecords > 0 {
		manifests, err = bounded.ReadManifestListBounded(ctx, data, maxRecords)
	} else {
		// Third-party metadata adapters can opt into the bounded decoder above.
		// The post-check still bounds retained state, while the native production
		// facade enforces the limit during Avro decoding.
		manifests, err = r.Metadata.ReadManifestList(ctx, data)
	}
	if err != nil {
		return CachedManifestListRead{}, err
	}
	memoryBytes := ManifestListMemoryWeight(cap(data), manifests)
	if err := validatePlanningReadLimits("manifest_list", memoryBytes, len(manifests), maxBytes, maxRecords); err != nil {
		return CachedManifestListRead{}, err
	}
	r.Cache.Put(key, CacheEntry{ManifestList: manifests, SizeBytes: memoryBytes})
	return CachedManifestListRead{Manifests: manifests, SizeBytes: memoryBytes}, nil
}

func (r CachedManifestReader) ReadManifest(ctx context.Context, manifestPath string) ([]api.ManifestEntry, bool, error) {
	read, err := r.ReadManifestWithStats(ctx, manifestPath)
	if err != nil {
		return nil, false, err
	}
	return read.Entries, read.CacheHit, nil
}

func (r CachedManifestReader) ReadManifestWithStats(ctx context.Context, manifestPath string) (CachedManifestRead, error) {
	return r.ReadManifestWithLimits(ctx, manifestPath, 0, 0)
}

func (r CachedManifestReader) ReadManifestWithLimits(ctx context.Context, manifestPath string, maxBytes int64, maxRecords int) (CachedManifestRead, error) {
	key := r.cacheKey(CacheKindManifest, manifestPath)
	if cached, ok := r.Cache.Get(key); ok && len(cached.ManifestEntries) > 0 {
		if err := validatePlanningReadLimits("manifest", cached.SizeBytes, len(cached.ManifestEntries), maxBytes, maxRecords); err != nil {
			return CachedManifestRead{}, err
		}
		return CachedManifestRead{Entries: cached.ManifestEntries, CacheHit: true, SizeBytes: cached.SizeBytes}, nil
	}
	if r.Metadata == nil || r.ObjectReader == nil {
		return CachedManifestRead{}, api.NewError(api.ErrConfigInvalid, "Iceberg manifest reader requires metadata facade and object reader", nil)
	}
	data, err := readPlanningObject(ctx, r.ObjectReader, manifestPath, maxBytes)
	if err != nil {
		if isPlanningLimitError(err) {
			return CachedManifestRead{}, err
		}
		return CachedManifestRead{}, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest read failed", map[string]string{
			"manifest": api.RedactPath(manifestPath),
		}, err)
	}
	var entries []api.ManifestEntry
	if bounded, ok := r.Metadata.(interface {
		ReadManifestWithLimits(context.Context, []byte, int, int64) ([]api.ManifestEntry, error)
	}); ok && maxRecords > 0 && maxBytes > 0 {
		entries, err = bounded.ReadManifestWithLimits(ctx, data, maxRecords, maxBytes)
	} else if bounded, ok := r.Metadata.(interface {
		ReadManifestBounded(context.Context, []byte, int) ([]api.ManifestEntry, error)
	}); ok && maxRecords > 0 {
		entries, err = bounded.ReadManifestBounded(ctx, data, maxRecords)
	} else {
		entries, err = r.Metadata.ReadManifest(ctx, data)
	}
	if err != nil {
		return CachedManifestRead{}, err
	}
	memoryBytes := ManifestEntriesMemoryWeight(cap(data), entries)
	if err := validatePlanningReadLimits("manifest", memoryBytes, len(entries), maxBytes, maxRecords); err != nil {
		return CachedManifestRead{}, err
	}
	r.Cache.Put(key, CacheEntry{ManifestEntries: entries, SizeBytes: memoryBytes})
	return CachedManifestRead{Entries: entries, SizeBytes: memoryBytes}, nil
}

func isPlanningLimitError(err error) bool {
	var icebergErr *api.IcebergError
	return stderrors.As(err, &icebergErr) && icebergErr.Code == api.ErrPlanningLimitExceeded
}

func readPlanningObject(ctx context.Context, reader api.ObjectReader, location string, maxBytes int64) ([]byte, error) {
	if maxBytes > 0 {
		if bounded, ok := reader.(interface {
			ReadBounded(context.Context, string, int64) ([]byte, error)
		}); ok {
			return bounded.ReadBounded(ctx, location, maxBytes)
		}
	}
	// Custom/testing adapters may not expose streaming. Keep the post-check for
	// compatibility; production ProviderObjectReader implements ReadBounded so
	// unknown-length metadata is capped before materialization.
	data, err := reader.Read(ctx, location, 0, -1)
	if err != nil {
		return nil, err
	}
	if maxBytes > 0 && int64(cap(data)) > maxBytes {
		return nil, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg metadata object exceeds the planning memory limit", map[string]string{
			"path":        api.RedactPath(location),
			"bytes":       strconv.FormatInt(int64(cap(data)), 10),
			"limit_bytes": strconv.FormatInt(maxBytes, 10),
		})
	}
	return data, nil
}

func validatePlanningReadLimits(kind string, sizeBytes int64, records int, maxBytes int64, maxRecords int) error {
	if maxBytes > 0 && sizeBytes > maxBytes {
		return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg cached metadata object exceeds the planning memory limit", map[string]string{
			"kind":        kind,
			"bytes":       strconv.FormatInt(sizeBytes, 10),
			"limit_bytes": strconv.FormatInt(maxBytes, 10),
		})
	}
	if maxRecords > 0 && records > maxRecords {
		return api.NewError(api.ErrPlanningLimitExceeded, "Iceberg metadata record count exceeds the planning limit", map[string]string{
			"kind":    kind,
			"records": strconv.Itoa(records),
			"limit":   strconv.Itoa(maxRecords),
		})
	}
	return nil
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
		CatalogPrefixHash:      api.PathHash(req.Prefix),
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
