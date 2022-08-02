// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"context"
	"sync/atomic"
)

type CacheFS struct {
	upstream FileService

	memLRU *LRU
	stats  *lruStats
}

type lruStats struct {
	Read     int64
	CacheHit int64
}

var _ FileService = new(CacheFS)

func NewCacheFS(
	upstream FileService,
	memoryCapacity int,
) (
	*CacheFS,
	error,
) {

	fs := &CacheFS{
		upstream: upstream,
		memLRU:   NewLRU(memoryCapacity),
	}

	return fs, nil
}

func (c *CacheFS) Delete(ctx context.Context, filePath string) error {
	return c.upstream.Delete(ctx, filePath)
}

func (c *CacheFS) List(ctx context.Context, dirPath string) ([]DirEntry, error) {
	return c.upstream.List(ctx, dirPath)
}

func (c *CacheFS) Read(ctx context.Context, vector *IOVector) error {

	numHit := 0
	defer func() {
		if c.stats != nil {
			atomic.AddInt64(&c.stats.Read, int64(len(vector.Entries)))
			atomic.AddInt64(&c.stats.CacheHit, int64(numHit))
		}
	}()

	noObject := true
	for i, entry := range vector.Entries {
		if entry.ToObject == nil {
			continue
		}
		noObject = false

		// read from cache
		key := CacheKey{
			Path:   vector.FilePath,
			Offset: entry.Offset,
			Size:   entry.Size,
		}
		obj, ok := c.memLRU.Get(key)
		if ok {
			vector.Entries[i].Object = obj
			vector.Entries[i].ignore = true
			numHit++
		}
	}

	if err := c.upstream.Read(ctx, vector); err != nil {
		return err
	}

	if noObject {
		return nil
	}

	for i, entry := range vector.Entries {
		vector.Entries[i].ignore = false

		// set cache
		if entry.Object != nil {
			key := CacheKey{
				Path:   vector.FilePath,
				Offset: entry.Offset,
				Size:   entry.Size,
			}
			c.memLRU.Set(key, entry.Object, entry.ObjectSize)
		}
	}

	return nil
}

func (c *CacheFS) Write(ctx context.Context, vector IOVector) error {
	return c.upstream.Write(ctx, vector)
}
