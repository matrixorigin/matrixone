// Copyright 2024 Matrix Origin
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

package external

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const (
	hivePartitionCacheVersion         = "mo-hive-part-cache-v1"
	defaultHivePartitionCacheEntries  = 1024
	defaultHivePartitionCacheMaxBytes = 64 << 20
)

type HivePartitionListCacheEntry struct {
	Entries  []fileservice.DirEntry
	ExpireAt time.Time
	SizeHint int64
	lastUsed time.Time
}

type hivePartitionInflight struct {
	done    chan struct{}
	entries []fileservice.DirEntry
	err     error
}

type hivePartitionListCache struct {
	mu       sync.Mutex
	entries  map[string]*HivePartitionListCacheEntry
	inflight map[string]*hivePartitionInflight
	bytes    int64
}

var globalHivePartitionListCache = &hivePartitionListCache{
	entries:  make(map[string]*HivePartitionListCacheEntry),
	inflight: make(map[string]*hivePartitionInflight),
}

func ResetHivePartitionListCacheForTest() {
	globalHivePartitionListCache.mu.Lock()
	defer globalHivePartitionListCache.mu.Unlock()
	globalHivePartitionListCache.entries = make(map[string]*HivePartitionListCacheEntry)
	globalHivePartitionListCache.inflight = make(map[string]*hivePartitionInflight)
	globalHivePartitionListCache.bytes = 0
}

func BuildHivePartitionListCacheKey(param *tree.ExternParam, accountID uint32, basePath, prefix string) string {
	return BuildHivePartitionListCacheKeyPrefix(param, accountID, basePath) +
		"\x1f" + "prefix=" + normalizeExternalPath(prefix)
}

func BuildHivePartitionListCacheKeyPrefix(param *tree.ExternParam, accountID uint32, basePath string) string {
	var parts []string
	parts = append(parts,
		hivePartitionCacheVersion,
		fmt.Sprintf("account=%d", accountID),
		fmt.Sprintf("scan=%d", param.ScanType),
		"base="+normalizeExternalPath(basePath),
	)
	if param.ScanType == tree.S3 && param.S3Param != nil {
		s3 := param.S3Param
		parts = append(parts,
			"endpoint="+strings.TrimRight(s3.Endpoint, "/"),
			"bucket="+s3.Bucket,
			"provider="+strings.ToLower(s3.Provider),
			"role="+s3.RoleArn,
			"external_id="+s3.ExternalId,
			"access_key_hash="+hashHivePartitionAccessKeyID(s3.APIKey),
		)
	}
	return strings.Join(parts, "\x1f")
}

func hashHivePartitionAccessKeyID(accessKeyID string) string {
	sum := sha256.Sum256([]byte(hivePartitionCacheVersion + "\x00" + accessKeyID))
	return hex.EncodeToString(sum[:])
}

func listHivePartitionDir(
	ctx context.Context,
	listDir ListDirFunc,
	prefix string,
	options DiscoverOptions,
	result *PartitionDiscoveryResult,
) ([]fileservice.DirEntry, error) {
	if options.CacheTTL <= 0 || options.CacheKeyPrefix == "" {
		return collectHivePartitionListEntries(ctx, listDir, prefix, options, result)
	}

	key := options.CacheKeyPrefix + "\x1f" + "prefix=" + normalizeExternalPath(prefix)
	if entries, ok := globalHivePartitionListCache.get(key); ok {
		recordHivePartitionCacheHit(result, options)
		recordHivePartitionDirectPrefixHit(result, options)
		return entries, nil
	}

	leader, flight, entries, ok := globalHivePartitionListCache.getOrStartInflight(key)
	if ok {
		recordHivePartitionCacheHit(result, options)
		recordHivePartitionDirectPrefixHit(result, options)
		return entries, nil
	}
	recordHivePartitionCacheMiss(result, options)
	recordHivePartitionDirectPrefixMiss(result, options)

	if !leader {
		select {
		case <-flight.done:
			if entries, ok := globalHivePartitionListCache.get(key); ok {
				return entries, nil
			}
			return cloneDirEntries(flight.entries), flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	entries, err := collectHivePartitionListEntries(ctx, listDir, prefix, options, result)
	if err == nil {
		globalHivePartitionListCache.set(key, entries, options.CacheTTL, options.CacheMaxEntries, options.CacheMaxBytes)
	}
	globalHivePartitionListCache.finishInflight(key, flight, entries, err)
	return entries, err
}

func collectHivePartitionListEntries(
	ctx context.Context,
	listDir ListDirFunc,
	prefix string,
	options DiscoverOptions,
	result *PartitionDiscoveryResult,
) ([]fileservice.DirEntry, error) {
	if err := addDiscoveryListCall(result, options); err != nil {
		return nil, err
	}
	if options.listSemaphore != nil {
		select {
		case options.listSemaphore <- struct{}{}:
			defer func() { <-options.listSemaphore }()
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
	entries := make([]fileservice.DirEntry, 0)
	for entry, err := range listDir(ctx, prefix) {
		if err != nil {
			return nil, err
		}
		if entry == nil {
			continue
		}
		entries = append(entries, *entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	return entries, nil
}

func recordHivePartitionCacheHit(result *PartitionDiscoveryResult, options DiscoverOptions) {
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.CacheHits++
}

func recordHivePartitionCacheMiss(result *PartitionDiscoveryResult, options DiscoverOptions) {
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.CacheMisses++
}

func recordHivePartitionDirectPrefixHit(result *PartitionDiscoveryResult, options DiscoverOptions) {
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.DirectPrefixHits++
}

func recordHivePartitionDirectPrefixMiss(result *PartitionDiscoveryResult, options DiscoverOptions) {
	if options.statsMu != nil {
		options.statsMu.Lock()
		defer options.statsMu.Unlock()
	}
	result.DirectPrefixMisses++
}

func (c *hivePartitionListCache) get(key string) ([]fileservice.DirEntry, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if now.After(entry.ExpireAt) {
		c.bytes -= entry.SizeHint
		delete(c.entries, key)
		return nil, false
	}
	entry.lastUsed = now
	return cloneDirEntries(entry.Entries), true
}

func (c *hivePartitionListCache) set(key string, entries []fileservice.DirEntry, ttl time.Duration, maxEntries int, maxBytes int64) {
	if maxEntries <= 0 {
		maxEntries = defaultHivePartitionCacheEntries
	}
	if maxBytes <= 0 {
		maxBytes = defaultHivePartitionCacheMaxBytes
	}
	now := time.Now()
	sizeHint := estimateDirEntriesSize(entries)

	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.entries[key]; ok {
		c.bytes -= old.SizeHint
	}
	c.entries[key] = &HivePartitionListCacheEntry{
		Entries:  cloneDirEntries(entries),
		ExpireAt: now.Add(ttl),
		SizeHint: sizeHint,
		lastUsed: now,
	}
	c.bytes += sizeHint
	c.evictLocked(maxEntries, maxBytes)
}

func (c *hivePartitionListCache) getOrStartInflight(key string) (bool, *hivePartitionInflight, []fileservice.DirEntry, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		if now.After(entry.ExpireAt) {
			c.bytes -= entry.SizeHint
			delete(c.entries, key)
		} else {
			entry.lastUsed = now
			return false, nil, cloneDirEntries(entry.Entries), true
		}
	}
	if flight, ok := c.inflight[key]; ok {
		return false, flight, nil, false
	}
	flight := &hivePartitionInflight{done: make(chan struct{})}
	c.inflight[key] = flight
	return true, flight, nil, false
}

func (c *hivePartitionListCache) finishInflight(key string, flight *hivePartitionInflight, entries []fileservice.DirEntry, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	flight.entries = cloneDirEntries(entries)
	flight.err = err
	delete(c.inflight, key)
	close(flight.done)
}

func (c *hivePartitionListCache) evictLocked(maxEntries int, maxBytes int64) {
	for len(c.entries) > maxEntries || c.bytes > maxBytes {
		var victimKey string
		var victim *HivePartitionListCacheEntry
		for key, entry := range c.entries {
			if victim == nil || entry.lastUsed.Before(victim.lastUsed) {
				victimKey = key
				victim = entry
			}
		}
		if victim == nil {
			return
		}
		c.bytes -= victim.SizeHint
		delete(c.entries, victimKey)
	}
}

func cloneDirEntries(entries []fileservice.DirEntry) []fileservice.DirEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]fileservice.DirEntry, len(entries))
	copy(out, entries)
	return out
}

func estimateDirEntriesSize(entries []fileservice.DirEntry) int64 {
	var size int64
	for i := range entries {
		size += int64(len(entries[i].Name)) + 64
	}
	return size
}
