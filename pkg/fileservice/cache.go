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

	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

//TODO in-memory bytes cache

type CacheKey struct {
	Path   string
	Offset int64
	Size   int64
}

type CacheConfig struct {
	MemoryCapacity toml.ByteSize `toml:"memory-capacity"`
	DiskPath       string        `toml:"disk-path"`
	DiskCapacity   toml.ByteSize `toml:"disk-capacity"`
}

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

type ObjectCache interface {
	Set(key any, value any, size int64, preloading bool)
	Get(key any, preloading bool) (value any, size int64, ok bool)
	Flush()
	Size() int64
}
