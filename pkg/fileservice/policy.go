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

import "context"

type Policy uint64

const (
	SkipMemoryCacheReads = 1 << iota
	SkipMemoryCacheWrites
	SkipDiskCacheReads
	SkipDiskCacheWrites
	SkipFullFilePreloads
)

const (
	SkipCacheReads  = SkipMemoryCacheReads | SkipDiskCacheReads
	SkipCacheWrites = SkipMemoryCacheWrites | SkipDiskCacheWrites
	SkipDiskCache   = SkipDiskCacheReads | SkipDiskCacheWrites
	SkipMemoryCache = SkipMemoryCacheReads | SkipMemoryCacheWrites
	SkipAllCache    = SkipDiskCache | SkipMemoryCache
)

func (c Policy) Any(policies ...Policy) bool {
	for _, policy := range policies {
		if policy&c > 0 {
			return true
		}
	}
	return false
}

func (c Policy) CacheIOEntry() bool {
	// cache IOEntry if not caching full file
	return c.Any(SkipFullFilePreloads)
}

func (c Policy) CacheFullFile() bool {
	return !c.Any(SkipFullFilePreloads)
}

type contextKeyPolicy struct{}

// WithFileServicePolicy attaches a read/write cache policy to ctx.
// Callers deeper in the stack (e.g. LoadPersistedColumnData) will pick it
// up automatically, so no interface changes are required.
func WithFileServicePolicy(ctx context.Context, policy Policy) context.Context {
	return context.WithValue(ctx, contextKeyPolicy{}, policy)
}

// GetFileServicePolicy returns the Policy stored in ctx, or Policy(0) if none.
func GetFileServicePolicy(ctx context.Context) Policy {
	if p, ok := ctx.Value(contextKeyPolicy{}).(Policy); ok {
		return p
	}
	return Policy(0)
}
