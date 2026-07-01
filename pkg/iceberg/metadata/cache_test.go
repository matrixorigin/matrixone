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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestCacheTTLAndClone(t *testing.T) {
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	cache := NewCacheWithClock(time.Minute, func() time.Time { return now })
	key := CacheKey{
		Kind:                   CacheKindMetadataJSON,
		AccountID:              1,
		CatalogID:              2,
		Namespace:              "sales",
		Table:                  "orders",
		Ref:                    "main",
		ExternalPrincipal:      "role/a",
		MetadataLocationHash:   "meta",
		CredentialIdentityHash: "cred",
	}
	cache.Put(key, CacheEntry{
		ETag:         "etag-1",
		MetadataJSON: []byte(`{"format-version":2}`),
		Metadata: &api.TableMetadata{
			FormatVersion: 2,
			Properties:    map[string]string{"owner": "mo"},
		},
	})
	got, ok := cache.Get(key)
	if !ok {
		t.Fatalf("cache miss before ttl")
	}
	got.MetadataJSON[0] = '['
	got.Metadata.Properties["owner"] = "mutated"
	gotAgain, ok := cache.Get(key)
	if !ok {
		t.Fatalf("cache miss after clone mutation")
	}
	if string(gotAgain.MetadataJSON) != `{"format-version":2}` || gotAgain.Metadata.Properties["owner"] != "mo" {
		t.Fatalf("cache entry was mutated through clone: %+v", gotAgain)
	}
	now = now.Add(time.Minute)
	if _, ok := cache.Get(key); ok {
		t.Fatalf("cache entry should expire at ttl boundary")
	}
}

func TestCacheKeyIncludesPrincipalAndCredential(t *testing.T) {
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	cache := NewCacheWithClock(time.Minute, func() time.Time { return now })
	keyA := CacheKey{Kind: CacheKindManifest, AccountID: 1, CatalogID: 2, Namespace: "sales", Table: "orders", Ref: "main", ExternalPrincipal: "a", MetadataLocationHash: "m", ManifestPathHash: "p", CredentialIdentityHash: "cred-a"}
	keyB := keyA
	keyB.ExternalPrincipal = "b"
	keyB.CredentialIdentityHash = "cred-b"
	cache.Put(keyA, CacheEntry{ManifestEntries: []api.ManifestEntry{{SnapshotID: 1}}})
	cache.Put(keyB, CacheEntry{ManifestEntries: []api.ManifestEntry{{SnapshotID: 2}}})
	gotA, ok := cache.Get(keyA)
	if !ok || gotA.ManifestEntries[0].SnapshotID != 1 {
		t.Fatalf("unexpected cache A: ok=%v entry=%+v", ok, gotA)
	}
	gotB, ok := cache.Get(keyB)
	if !ok || gotB.ManifestEntries[0].SnapshotID != 2 {
		t.Fatalf("unexpected cache B: ok=%v entry=%+v", ok, gotB)
	}
}

func TestCacheKeyIsolationDimensions(t *testing.T) {
	cache := NewCache(time.Minute)
	base := CacheKey{
		Kind:                   CacheKindManifest,
		AccountID:              1,
		CatalogID:              2,
		Namespace:              "sales",
		Table:                  "orders",
		Ref:                    "main",
		ExternalPrincipal:      "principal-a",
		SnapshotID:             22,
		TimestampMS:            1767225600000,
		MetadataLocationHash:   "metadata-a",
		ManifestPathHash:       "manifest-a",
		CredentialIdentityHash: "credential-a",
	}
	cache.Put(base, CacheEntry{ManifestEntries: []api.ManifestEntry{{SnapshotID: 22}}})
	variants := []struct {
		name   string
		mutate func(*CacheKey)
	}{
		{name: "kind", mutate: func(key *CacheKey) { key.Kind = CacheKindManifestList }},
		{name: "account", mutate: func(key *CacheKey) { key.AccountID = 9 }},
		{name: "catalog", mutate: func(key *CacheKey) { key.CatalogID = 9 }},
		{name: "namespace", mutate: func(key *CacheKey) { key.Namespace = "finance" }},
		{name: "table", mutate: func(key *CacheKey) { key.Table = "customers" }},
		{name: "ref", mutate: func(key *CacheKey) { key.Ref = "audit" }},
		{name: "principal", mutate: func(key *CacheKey) { key.ExternalPrincipal = "principal-b" }},
		{name: "snapshot", mutate: func(key *CacheKey) { key.SnapshotID = 23 }},
		{name: "timestamp", mutate: func(key *CacheKey) { key.TimestampMS = 1767225601000 }},
		{name: "metadata_location_hash", mutate: func(key *CacheKey) { key.MetadataLocationHash = "metadata-b" }},
		{name: "manifest_path_hash", mutate: func(key *CacheKey) { key.ManifestPathHash = "manifest-b" }},
		{name: "credential", mutate: func(key *CacheKey) { key.CredentialIdentityHash = "credential-b" }},
	}
	for _, variant := range variants {
		key := base
		variant.mutate(&key)
		if _, ok := cache.Get(key); ok {
			t.Fatalf("cache key dimension %s should isolate entries", variant.name)
		}
	}
	got, ok := cache.Get(base)
	if !ok || len(got.ManifestEntries) != 1 || got.ManifestEntries[0].SnapshotID != 22 {
		t.Fatalf("base cache entry should remain available, ok=%v entry=%+v", ok, got)
	}
}

func TestCacheInvalidateTable(t *testing.T) {
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	cache := NewCacheWithClock(time.Minute, func() time.Time { return now })
	key := CacheKey{Kind: CacheKindManifestList, AccountID: 1, CatalogID: 2, Namespace: "sales", Table: "orders", Ref: "main"}
	other := key
	other.Table = "customers"
	cache.Put(key, CacheEntry{ManifestList: []api.ManifestFile{{Path: "p1"}}})
	cache.Put(other, CacheEntry{ManifestList: []api.ManifestFile{{Path: "p2"}}})
	if removed := cache.InvalidateTable(1, 2, "sales", "orders"); removed != 1 {
		t.Fatalf("removed=%d, want 1", removed)
	}
	if _, ok := cache.Get(key); ok {
		t.Fatalf("orders entry should be invalidated")
	}
	if _, ok := cache.Get(other); !ok {
		t.Fatalf("customers entry should remain")
	}
}

func TestCacheETagRevalidation(t *testing.T) {
	now := time.Date(2026, 6, 17, 10, 0, 0, 0, time.UTC)
	cache := NewCacheWithClock(time.Minute, func() time.Time { return now })
	key := CacheKey{Kind: CacheKindMetadataLocation, AccountID: 1, CatalogID: 2, Namespace: "sales", Table: "orders", Ref: "main"}
	cache.Put(key, CacheEntry{ETag: "etag-1", MetadataLocation: "s3://warehouse/t/metadata/v1.json"})
	now = now.Add(time.Minute)
	if _, ok := cache.Get(key); ok {
		t.Fatalf("expired entry should not be a fresh hit")
	}
	stale, ok := cache.GetStaleForRevalidation(key)
	if !ok || stale.ETag != "etag-1" {
		t.Fatalf("expected stale entry for revalidation, ok=%v entry=%+v", ok, stale)
	}
	if !cache.Refresh(key, "etag-1") {
		t.Fatalf("expected refresh to accept matching etag")
	}
	if fresh, ok := cache.Get(key); !ok || fresh.MetadataLocation == "" {
		t.Fatalf("expected fresh entry after refresh, ok=%v entry=%+v", ok, fresh)
	}
	now = now.Add(time.Minute)
	if cache.Refresh(key, "other-etag") {
		t.Fatalf("refresh must reject mismatched etag")
	}
}
