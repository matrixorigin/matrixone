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
)

// var DefaultCacheDataAllocator = RCBytesPool
var DefaultCacheDataAllocator = new(bytesAllocator)

// Cache is the interface for reading IOEntry
type Cache interface {
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
	Set(ctx context.Context, key CacheKey, value CacheData)
	Get(ctx context.Context, key CacheKey) (value CacheData, ok bool)
	Flush()
	Capacity() int64
	Used() int64
	Available() int64
}

// FileCache caches whole files
type FileCache interface {
	GetFile(
		ctx context.Context,
		path string,
	) (
		r io.ReadSeekCloser,
		err error,
	)

	SetFile(
		ctx context.Context,
		path string,
	) (
		w io.WriteCloser,
		err error,
	)
}
