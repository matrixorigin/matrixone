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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestCachedTableMetadataLoaderFreshAndETagRevalidation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	cache := NewCacheWithClock(time.Minute, 1<<20, func() time.Time { return now })
	calls := 0
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			calls++
			switch calls {
			case 1:
				if req.IfNoneMatch != "" {
					t.Fatalf("initial load must not send If-None-Match, got %q", req.IfNoneMatch)
				}
				return &api.LoadTableResponse{
					MetadataLocation:   "s3://warehouse/sales/orders/metadata/v2.metadata.json",
					MetadataJSON:       []byte(sampleMetadataJSON),
					ETag:               "etag-1",
					StorageCredentials: []api.StorageCredential{{Prefix: "s3://warehouse/sales"}},
				}, nil
			case 2:
				if req.IfNoneMatch != "etag-1" {
					t.Fatalf("revalidation should use cached etag, got %q", req.IfNoneMatch)
				}
				return &api.LoadTableResponse{NotModified: true, ETag: "etag-1"}, nil
			default:
				t.Fatalf("unexpected catalog load call %d", calls)
			}
			return nil, nil
		},
	}
	loader := CachedTableMetadataLoader{
		Catalog:        client,
		Metadata:       NativeFacade{},
		Cache:          cache,
		CredentialHash: "cred-a",
		ExternalRef:    "main",
	}
	req := cacheLoadTableRequest()
	first, err := loader.Load(ctx, req)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	if first.CacheHit || first.Revalidated || first.Metadata.CurrentSnapshotID == nil || *first.Metadata.CurrentSnapshotID != 22 {
		t.Fatalf("unexpected first load result: %+v", first)
	}
	if first.StorageCredential[0].Prefix != "s3://warehouse/sales" {
		t.Fatalf("storage credentials should be carried through: %+v", first.StorageCredential)
	}

	second, err := loader.Load(ctx, req)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if !second.CacheHit || second.Revalidated || calls != 1 {
		t.Fatalf("fresh cache should avoid catalog load, hit=%v revalidated=%v calls=%d", second.CacheHit, second.Revalidated, calls)
	}

	now = now.Add(time.Minute)
	third, err := loader.Load(ctx, req)
	if err != nil {
		t.Fatalf("third load: %v", err)
	}
	if !third.CacheHit || !third.Revalidated || calls != 2 {
		t.Fatalf("expired cache should revalidate, hit=%v revalidated=%v calls=%d", third.CacheHit, third.Revalidated, calls)
	}
	if _, ok := cache.Get(third.LocationCacheKey); !ok {
		t.Fatalf("location cache should be fresh after 304 revalidation")
	}
}

func TestCachedTableMetadataLoaderReadsMetadataLocation(t *testing.T) {
	ctx := context.Background()
	reader := &fakeObjectReader{data: map[string][]byte{
		"s3://warehouse/sales/orders/metadata/v2.metadata.json": []byte(sampleMetadataJSON),
	}}
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{
				MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.metadata.json",
				ETag:             "etag-1",
			}, nil
		},
	}
	loader := CachedTableMetadataLoader{
		Catalog:      client,
		Metadata:     NativeFacade{},
		ObjectReader: reader,
		Cache:        NewCache(time.Minute, 1<<20),
		ExternalRef:  "main",
	}
	loaded, err := loader.Load(ctx, cacheLoadTableRequest())
	if err != nil {
		t.Fatalf("load metadata via object reader: %v", err)
	}
	if loaded.Metadata.Location != "s3://warehouse/sales/orders" || reader.calls != 1 {
		t.Fatalf("unexpected object metadata load result metadata=%+v calls=%d", loaded.Metadata, reader.calls)
	}
}

func TestCachedTableMetadataLoaderSnapshotlessRevalidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		optIn    bool
		etag     string
		selector api.SnapshotSelector
		metadata string
		refresh  bool
	}{
		{name: "conditional empty", optIn: true, etag: "empty-1", refresh: true},
		{name: "empty without ETag", optIn: true, refresh: true},
		{name: "legacy caller", etag: "empty-1"},
		{name: "explicit snapshot", optIn: true, selector: api.SnapshotSelector{SnapshotID: 22, HasSnapshotID: true}},
		{name: "explicit timestamp", optIn: true, selector: api.SnapshotSelector{TimestampMS: 1710000200000, HasTimestampMS: true}},
		{name: "other ref", optIn: true, selector: api.SnapshotSelector{RefName: "audit"}},
		{name: "populated", optIn: true, metadata: sampleMetadataJSON},
		{name: "dangling ref", optIn: true, metadata: strings.Replace(emptySnapshotMetadataJSON, `"refs": {}`, `"refs": {"main":{"snapshot-id":22,"type":"branch"}}`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := tc.metadata
			if payload == "" {
				payload = emptySnapshotMetadataJSON
			}
			calls := 0
			client := &catalog.MockClient{LoadTableFunc: func(_ context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
				calls++
				if calls == 2 && req.IfNoneMatch != tc.etag {
					t.Fatalf("conditional ETag: got %q, want %q", req.IfNoneMatch, tc.etag)
				}
				if calls == 2 && tc.etag != "" {
					return &api.LoadTableResponse{NotModified: true, ETag: tc.etag}, nil
				}
				return &api.LoadTableResponse{MetadataLocation: "s3://warehouse/empty/metadata.json", MetadataJSON: []byte(payload), ETag: tc.etag}, nil
			}}
			loader := CachedTableMetadataLoader{Catalog: client, Metadata: NativeFacade{}, Cache: NewCache(time.Hour, 1<<20), SnapshotSelector: tc.selector, RevalidateSnapshotless: tc.optIn}
			for range 2 {
				loaded, err := loader.Load(context.Background(), cacheLoadTableRequest())
				if err != nil {
					t.Fatal(err)
				}
				if calls == 2 && tc.etag != "" && (!loaded.CacheHit || !loaded.Revalidated) {
					t.Fatalf("304 must reuse cached empty metadata: %+v", loaded)
				}
			}
			wantCalls := 1
			if tc.refresh {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("catalog calls: got %d, want %d", calls, wantCalls)
			}
		})
	}
}

func TestLocalScanPlannerSnapshotlessRevalidationFailure(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint("canceled=", canceled), func(t *testing.T) {
			fixture := newPlannerFixtureWithMetadata(t, 0, emptySnapshotMetadataJSON)
			planner := fixture.planner()
			req := api.ScanPlanRequest{CatalogRequest: cacheLoadTableRequest().CatalogRequest, Namespace: api.Namespace{"sales"}, Table: "empty_orders"}
			if _, err := planner.PlanScan(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture.client.LoadTableFunc = func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
				if canceled {
					cancel()
					return nil, context.Canceled
				}
				return nil, api.NewError(api.ErrTableNotFound, "table dropped", nil)
			}
			plan, err := planner.PlanScan(ctx, req)
			code := api.ErrTableNotFound
			if canceled {
				code = api.ErrPlanningTimeout
			}
			assertIcebergCode(t, err, code)
			if plan != nil || fixture.reader.totalCalls() != 0 {
				t.Fatalf("failed refresh must not return a stale empty plan: %+v", plan)
			}
		})
	}
}

func TestCachedManifestReaderCachesByCredential(t *testing.T) {
	ctx := context.Background()
	reader := &fakeObjectReader{data: map[string][]byte{
		"s3://warehouse/sales/orders/metadata/snap-22.avro": []byte("manifest-list"),
	}}
	cache := NewCache(time.Minute, 1<<20)
	base := CacheKey{AccountID: 1, CatalogID: 2, Namespace: "sales", Table: "orders", Ref: "main", ExternalPrincipal: "principal", MetadataLocationHash: "meta"}
	manifestReader := CachedManifestReader{
		Metadata:       fakeMetadataFacade{},
		ObjectReader:   reader,
		Cache:          cache,
		BaseKey:        base,
		CredentialHash: "cred-a",
	}
	manifestPath := "s3://warehouse/sales/orders/metadata/snap-22.avro"
	first, hit, err := manifestReader.ReadManifestList(ctx, manifestPath)
	if err != nil {
		t.Fatalf("read manifest list: %v", err)
	}
	if hit || len(first) != 1 || reader.calls != 1 {
		t.Fatalf("first manifest read should miss cache, hit=%v manifests=%+v calls=%d", hit, first, reader.calls)
	}
	second, hit, err := manifestReader.ReadManifestList(ctx, manifestPath)
	if err != nil {
		t.Fatalf("read cached manifest list: %v", err)
	}
	if !hit || len(second) != 1 || reader.calls != 1 {
		t.Fatalf("second manifest read should hit cache, hit=%v manifests=%+v calls=%d", hit, second, reader.calls)
	}
	manifestReader.CredentialHash = "cred-b"
	if _, hit, err := manifestReader.ReadManifestList(ctx, manifestPath); err != nil || hit || reader.calls != 2 {
		t.Fatalf("different credential hash should miss cache, hit=%v calls=%d err=%v", hit, reader.calls, err)
	}
}

func TestCachedManifestReaderEnforcesPlanningLimitsBeforeRetention(t *testing.T) {
	entries := []api.ManifestEntry{
		{Status: api.ManifestEntryAdded, SnapshotID: 1, DataFile: api.DataFile{Content: api.DataFileContentData, FilePath: "a.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 1, SpecID: 7}},
		{Status: api.ManifestEntryAdded, SnapshotID: 1, DataFile: api.DataFile{Content: api.DataFileContentData, FilePath: "b.parquet", FileFormat: "parquet", RecordCount: 1, FileSizeInBytes: 1, SpecID: 7}},
	}
	data, err := EncodeManifest(entries, manifestTestOptions(api.ManifestContentData, api.SchemaField{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}))
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	const path = "s3://warehouse/t/metadata/m.avro"
	reader := CachedManifestReader{
		Metadata:     NativeFacade{},
		ObjectReader: &fakeObjectReader{data: map[string][]byte{path: data}},
		Cache:        NewCache(time.Minute, 1<<20),
	}

	if _, err := reader.ReadManifestWithLimits(context.Background(), path, int64(len(data)-1), 10); err == nil {
		t.Fatal("expected encoded-byte planning limit")
	} else {
		assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
	}
	if _, err := reader.ReadManifestWithLimits(context.Background(), path, int64(len(data)), 1); err == nil {
		t.Fatal("expected record-count planning limit")
	} else {
		assertIcebergCode(t, err, api.ErrPlanningLimitExceeded)
	}
}

func cacheLoadTableRequest() api.LoadTableRequest {
	return api.LoadTableRequest{
		CatalogRequest: api.CatalogRequest{
			Catalog: model.Catalog{
				AccountID: 1,
				CatalogID: 2,
				Name:      "test",
				Type:      "rest",
				URI:       "https://catalog.example.com",
			},
			ExternalPrincipal: "principal",
		},
		Namespace: api.Namespace{"sales"},
		Table:     "orders",
	}
}

type fakeObjectReader struct {
	data  map[string][]byte
	calls int
}

func (r *fakeObjectReader) Read(ctx context.Context, location string, offset, length int64) ([]byte, error) {
	r.calls++
	data := r.data[location]
	if offset > 0 {
		data = data[offset:]
	}
	if length >= 0 && int(length) < len(data) {
		data = data[:length]
	}
	if data == nil {
		return nil, api.NewError(api.ErrObjectIO, "missing fake object", map[string]string{"location": location})
	}
	return append([]byte(nil), data...), nil
}

func TestCachedLoaderRedactsMissingMetadataLocationReader(t *testing.T) {
	ctx := context.Background()
	client := &catalog.MockClient{
		LoadTableFunc: func(ctx context.Context, req api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{MetadataLocation: "s3://warehouse/sales/orders/metadata/v2.metadata.json"}, nil
		},
	}
	loader := CachedTableMetadataLoader{Catalog: client, Metadata: NativeFacade{}, Cache: NewCache(time.Minute, 1<<20)}
	_, err := loader.Load(ctx, cacheLoadTableRequest())
	if err == nil || strings.Contains(err.Error(), "warehouse") {
		t.Fatalf("missing reader error should redact metadata location, got %v", err)
	}
}

func TestCachedLoaderBoundsExternalMetadataJSON(t *testing.T) {
	const location = "s3://warehouse/sales/orders/metadata/v2.metadata.json"
	client := &catalog.MockClient{
		LoadTableFunc: func(context.Context, api.LoadTableRequest) (*api.LoadTableResponse, error) {
			return &api.LoadTableResponse{MetadataLocation: location}, nil
		},
	}
	loader := CachedTableMetadataLoader{
		Catalog:           client,
		Metadata:          NativeFacade{},
		ObjectReader:      &fakeObjectReader{data: map[string][]byte{location: []byte("oversized")}},
		Cache:             NewCache(time.Minute, 1<<20),
		PlanningMaxMemory: 1,
	}
	_, err := loader.Load(context.Background(), cacheLoadTableRequest())
	if err == nil || !strings.Contains(err.Error(), string(api.ErrPlanningLimitExceeded)) {
		t.Fatalf("expected bounded metadata read error, got %v", err)
	}
}
