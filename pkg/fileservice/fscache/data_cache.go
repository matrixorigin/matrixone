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

package fscache

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
)

type CacheKey = pb.CacheKey

// ErrCacheAdmissionRejected reports that a cache entry exists but a caller's
// separate retention budget or pin policy declined it. A cache coordinator may
// treat this sentinel as a miss and retry through an uncached authoritative
// read; cancellation and backend errors must remain distinct.
var ErrCacheAdmissionRejected = moerr.NewInternalErrorNoCtx("cache admission rejected")

type DataCache interface {
	EnsureNBytes(ctx context.Context, want int)
	Capacity() int64
	Used() int64
	Available() int64
	Get(context.Context, CacheKey) (Data, bool)
	Set(context.Context, CacheKey, Data) (inserted bool, err error)
	DeletePaths(context.Context, []string)
	Flush(ctx context.Context)
	Evict(ctx context.Context, done chan int64)
	EvictToTargetWithWait(ctx context.Context, target int64) int64
}

// DataCachePinAdmission runs before a cache hit retains its backing. It executes
// under the cache key's shard lock and therefore must be bounded, non-blocking,
// and must not call back into the cache. Its returned release function owns the
// admitted capacity for exactly as long as the caller owns the retained Data
// reference. The cache may also invoke release under the shard lock when an
// admission attempt reports an error, so release has the same bounded and
// non-reentrant requirements.
type DataCachePinAdmission func(capacity int64) (release func(), err error)

// DataCacheWithPinAdmission is the cache capability required by consumers
// that account retained cache backing against a separate statement budget.
type DataCacheWithPinAdmission interface {
	DataCache
	GetWithPinAdmission(
		context.Context,
		CacheKey,
		DataCachePinAdmission,
	) (data Data, release func(), ok bool, err error)
}
