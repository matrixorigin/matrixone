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
	"io"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type CacheConfig struct {
	MemoryCapacity       *toml.ByteSize `toml:"memory-capacity"`
	DiskPath             *string        `toml:"disk-path"`
	DiskCapacity         *toml.ByteSize `toml:"disk-capacity"`
	DiskMinEvictInterval *toml.Duration `toml:"disk-min-evict-interval"`
	DiskEvictTarget      *float64       `toml:"disk-evict-target"`

	enableDiskCacheForLocalFS bool // for testing only
}

func (c *CacheConfig) setDefaults() {
	if c.MemoryCapacity == nil {
		size := toml.ByteSize(512 << 20)
		c.MemoryCapacity = &size
	}
	if c.DiskCapacity == nil {
		size := toml.ByteSize(8 << 30)
		c.DiskCapacity = &size
	}
	if c.DiskMinEvictInterval == nil {
		c.DiskMinEvictInterval = &toml.Duration{
			Duration: time.Minute * 7,
		}
	}
	if c.DiskEvictTarget == nil {
		target := 0.8
		c.DiskEvictTarget = &target
	}
}

var DisabledCacheConfig = CacheConfig{
	MemoryCapacity: ptrTo[toml.ByteSize](DisableCacheCapacity),
	DiskCapacity:   ptrTo[toml.ByteSize](DisableCacheCapacity),
}

const DisableCacheCapacity = 1

// var DefaultCacheDataAllocator = RCBytesPool
var DefaultCacheDataAllocator = new(bytesAllocator)

// VectorCache caches IOVector
type IOVectorCache interface {
	Read(
		ctx context.Context,
		vector *IOVector,
	) error
	Update(
		ctx context.Context,
		vector *IOVector,
		async bool,
	) error
	Flush()
}

type CacheKey struct {
	Path   string
	Offset int64
	Size   int64
}

// DataCache caches IOEntry.CachedData
type DataCache interface {
	Set(ctx context.Context, key CacheKey, value CacheData, preloading bool)
	Get(ctx context.Context, key CacheKey, preloading bool) (value CacheData, ok bool)
	Flush()
	Capacity() int64
	Used() int64
	Available() int64
}

// FileContentCache caches contents of files
type FileContentCache interface {
	GetFileContent(
		ctx context.Context,
		path string,
		offset int64,
	) (
		r io.ReadCloser,
		err error,
	)

	SetFileContent(
		ctx context.Context,
		path string,
		readFunc func(ctx context.Context, vec *IOVector) error,
	) (
		err error,
	)
}
