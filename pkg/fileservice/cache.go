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

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

type CacheConfig struct {
	MemoryCapacity toml.ByteSize `toml:"memory-capacity"`
	DiskPath       string        `toml:"disk-path"`
	DiskCapacity   toml.ByteSize `toml:"disk-capacity"`
}

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

type IOVectorCacheKey struct {
	Path   string
	Offset int64
	Size   int64
}

// ObjectCache caches IOEntry.Object
type ObjectCache interface {
	Set(key any, value any, size int64, preloading bool)
	Get(key any, preloading bool) (value any, size int64, ok bool)
	Flush()
	Size() int64
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
