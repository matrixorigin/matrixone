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

package memorycache

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache/lrucache"
	cache "github.com/matrixorigin/matrixone/pkg/pb/query"
)

var EnableTracing = false

type CacheData interface {
	Bytes() []byte
	Slice(length int) CacheData
	Release()
}

type Cache struct {
	size atomic.Int64
	l    *lrucache.LRU[cache.CacheKey, *Data]
}

// RCBytes represents a reference counting data from cache
// it is immutable, caller should not modify it and call Release after use
type RCBytes struct {
	d *Data
	// the total size in the cache
	size *atomic.Int64
}

// Data is a reference counted byte buffer
type Data struct {
	size int
	buf  []byte
	// reference counta for the Data, the Data is free
	// when the reference count is 0
	ref       refcnt
	bufHandle *malloc.Handle
}
