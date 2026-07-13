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
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type CacheKind string

const (
	CacheKindMetadataLocation CacheKind = "metadata_location"
	CacheKindMetadataJSON     CacheKind = "metadata_json"
	CacheKindManifestList     CacheKind = "manifest_list"
	CacheKindManifest         CacheKind = "manifest"
)

type CacheKey struct {
	Kind                   CacheKind
	AccountID              uint32
	CatalogID              uint64
	Namespace              string
	Table                  string
	Ref                    string
	ExternalPrincipal      string
	SnapshotID             int64
	TimestampMS            int64
	MetadataLocationHash   string
	ManifestPathHash       string
	CredentialIdentityHash string
}

type CacheEntry struct {
	ETag             string
	MetadataLocation string
	MetadataJSON     []byte
	Metadata         *api.TableMetadata
	ManifestList     []api.ManifestFile
	ManifestEntries  []api.ManifestEntry
	SizeBytes        int64 // Positive serialized payload size used as the cache weight.
	StoredAt         time.Time
	ExpiresAt        time.Time
}

type Cache struct {
	mu        sync.Mutex
	ttl       time.Duration
	maxBytes  int64
	usedBytes int64
	now       func() time.Time
	entries   map[CacheKey]*list.Element
	lru       list.List
}

type cacheItem struct {
	key    CacheKey
	entry  CacheEntry
	weight int64
}

func NewCache(ttl time.Duration, maxBytes int64) *Cache {
	return &Cache{
		ttl:      ttl,
		maxBytes: maxBytes,
		now:      time.Now,
		entries:  make(map[CacheKey]*list.Element),
	}
}

func NewCacheWithClock(ttl time.Duration, maxBytes int64, now func() time.Time) *Cache {
	cache := NewCache(ttl, maxBytes)
	if now != nil {
		cache.now = now
	}
	return cache
}

func (c *Cache) Get(key CacheKey) (CacheEntry, bool) {
	if c == nil || c.ttl <= 0 || c.maxBytes <= 0 {
		return CacheEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return CacheEntry{}, false
	}
	entry := element.Value.(*cacheItem).entry
	if !entry.ExpiresAt.IsZero() && !c.now().Before(entry.ExpiresAt) {
		if entry.ETag == "" {
			c.removeElement(element)
		}
		return CacheEntry{}, false
	}
	c.lru.MoveToFront(element)
	return cloneCacheEntry(entry), true
}

func (c *Cache) GetStaleForRevalidation(key CacheKey) (CacheEntry, bool) {
	if c == nil || c.ttl <= 0 || c.maxBytes <= 0 {
		return CacheEntry{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return CacheEntry{}, false
	}
	entry := element.Value.(*cacheItem).entry
	if entry.ETag == "" {
		return CacheEntry{}, false
	}
	if entry.ExpiresAt.IsZero() || c.now().Before(entry.ExpiresAt) {
		return CacheEntry{}, false
	}
	c.lru.MoveToFront(element)
	return cloneCacheEntry(entry), true
}

func (c *Cache) Refresh(key CacheKey, etag string) bool {
	if c == nil || c.ttl <= 0 || c.maxBytes <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return false
	}
	item := element.Value.(*cacheItem)
	entry := item.entry
	if entry.ETag == "" || entry.ETag != etag {
		return false
	}
	now := c.now()
	entry.StoredAt = now
	entry.ExpiresAt = now.Add(c.ttl)
	item.entry = entry
	c.lru.MoveToFront(element)
	return true
}

func (c *Cache) Put(key CacheKey, entry CacheEntry) {
	if c == nil || c.ttl <= 0 || c.maxBytes <= 0 {
		return
	}
	weight := entry.SizeBytes
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[key]; ok {
		c.removeElement(existing)
	}
	// SizeBytes is the cache weight supplied by the loader from the serialized
	// metadata or manifest. Refuse missing and oversized weights so neither can
	// bypass the configured bound.
	if weight <= 0 || weight > c.maxBytes {
		return
	}
	for c.usedBytes > c.maxBytes-weight {
		c.removeElement(c.lru.Back())
	}
	now := c.now()
	entry.StoredAt = now
	entry.ExpiresAt = now.Add(c.ttl)
	item := &cacheItem{key: key, entry: cloneCacheEntry(entry), weight: weight}
	c.entries[key] = c.lru.PushFront(item)
	c.usedBytes += weight
}

func (c *Cache) Invalidate(key CacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		c.removeElement(element)
	}
}

func (c *Cache) InvalidateTable(accountID uint32, catalogID uint64, namespace, table string) int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := 0
	for key, element := range c.entries {
		if key.AccountID == accountID && key.CatalogID == catalogID && key.Namespace == namespace && key.Table == table {
			c.removeElement(element)
			removed++
		}
	}
	return removed
}

func (c *Cache) InvalidateIcebergCache(_ context.Context, req api.CacheInvalidationRequest) (int, error) {
	return c.InvalidateTable(req.AccountID, req.CatalogID, req.Namespace, req.Table), nil
}

func (c *Cache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *Cache) removeElement(element *list.Element) {
	if element == nil {
		return
	}
	item := element.Value.(*cacheItem)
	delete(c.entries, item.key)
	c.lru.Remove(element)
	c.usedBytes -= item.weight
}

func cloneCacheEntry(in CacheEntry) CacheEntry {
	out := in
	out.MetadataJSON = append([]byte(nil), in.MetadataJSON...)
	out.Metadata = cloneTableMetadata(in.Metadata)
	out.ManifestList = cloneManifestFiles(in.ManifestList)
	out.ManifestEntries = cloneManifestEntries(in.ManifestEntries)
	return out
}

func cloneTableMetadata(in *api.TableMetadata) *api.TableMetadata {
	if in == nil {
		return nil
	}
	out := *in
	out.Schemas = append([]api.Schema(nil), in.Schemas...)
	out.PartitionSpecs = append([]api.PartitionSpec(nil), in.PartitionSpecs...)
	out.Snapshots = append([]api.Snapshot(nil), in.Snapshots...)
	out.SnapshotLog = append([]api.SnapshotLogEntry(nil), in.SnapshotLog...)
	out.MetadataLog = append([]api.MetadataLogEntry(nil), in.MetadataLog...)
	out.RawJSON = append([]byte(nil), in.RawJSON...)
	if in.CurrentSnapshotID != nil {
		current := *in.CurrentSnapshotID
		out.CurrentSnapshotID = &current
	}
	if len(in.Properties) > 0 {
		out.Properties = make(map[string]string, len(in.Properties))
		for k, v := range in.Properties {
			out.Properties[k] = v
		}
	}
	if len(in.Refs) > 0 {
		out.Refs = make(map[string]api.SnapshotRef, len(in.Refs))
		for k, v := range in.Refs {
			out.Refs[k] = v
		}
	}
	for i := range out.Schemas {
		out.Schemas[i].Fields = cloneSchemaFields(in.Schemas[i].Fields)
		out.Schemas[i].IdentifierFieldIDs = append([]int(nil), in.Schemas[i].IdentifierFieldIDs...)
	}
	for i := range out.PartitionSpecs {
		out.PartitionSpecs[i].Fields = append([]api.PartitionField(nil), in.PartitionSpecs[i].Fields...)
	}
	return &out
}

func cloneSchemaFields(in []api.SchemaField) []api.SchemaField {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.SchemaField(nil), in...)
	for i := range out {
		out[i].InitialDefault = append([]byte(nil), in[i].InitialDefault...)
		out[i].WriteDefault = append([]byte(nil), in[i].WriteDefault...)
		out[i].Type = cloneIcebergType(in[i].Type)
	}
	return out
}

func cloneIcebergType(in api.IcebergType) api.IcebergType {
	out := in
	out.Fields = cloneSchemaFields(in.Fields)
	if in.Element != nil {
		element := cloneIcebergType(*in.Element)
		out.Element = &element
	}
	if in.Key != nil {
		key := cloneIcebergType(*in.Key)
		out.Key = &key
	}
	if in.Value != nil {
		value := cloneIcebergType(*in.Value)
		out.Value = &value
	}
	return out
}

func cloneManifestFiles(in []api.ManifestFile) []api.ManifestFile {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.ManifestFile(nil), in...)
	for i := range out {
		out[i].Partitions = append([]api.PartitionFieldSummary(nil), in[i].Partitions...)
		out[i].KeyMetadata = append([]byte(nil), in[i].KeyMetadata...)
		if in[i].FirstRowID != nil {
			first := *in[i].FirstRowID
			out[i].FirstRowID = &first
		}
		for j := range out[i].Partitions {
			out[i].Partitions[j].LowerBound = append([]byte(nil), in[i].Partitions[j].LowerBound...)
			out[i].Partitions[j].UpperBound = append([]byte(nil), in[i].Partitions[j].UpperBound...)
		}
	}
	return out
}

func cloneManifestEntries(in []api.ManifestEntry) []api.ManifestEntry {
	if len(in) == 0 {
		return nil
	}
	out := append([]api.ManifestEntry(nil), in...)
	for i := range out {
		out[i].DataFile = cloneDataFile(in[i].DataFile)
	}
	return out
}

func cloneDataFile(in api.DataFile) api.DataFile {
	out := in
	out.Partition = cloneAnyMap(in.Partition)
	out.PartitionFieldIDs = cloneStringIntMap(in.PartitionFieldIDs)
	out.ColumnSizes = cloneInt64Map(in.ColumnSizes)
	out.ValueCounts = cloneInt64Map(in.ValueCounts)
	out.NullValueCounts = cloneInt64Map(in.NullValueCounts)
	out.NaNValueCounts = cloneInt64Map(in.NaNValueCounts)
	out.LowerBounds = cloneBytesMap(in.LowerBounds)
	out.UpperBounds = cloneBytesMap(in.UpperBounds)
	out.SplitOffsets = append([]int64(nil), in.SplitOffsets...)
	out.EqualityIDs = append([]int(nil), in.EqualityIDs...)
	out.KeyMetadata = append([]byte(nil), in.KeyMetadata...)
	out.EncryptionKeyMetadata = append([]byte(nil), in.EncryptionKeyMetadata...)
	if in.FirstRowID != nil {
		first := *in.FirstRowID
		out.FirstRowID = &first
	}
	return out
}

func cloneStringIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for k, v := range in {
		out[k] = v
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
