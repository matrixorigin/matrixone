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
	pb "github.com/matrixorigin/matrixone/pkg/pb/query"
)

type CacheKey = pb.CacheKey

type DataCache interface {
	EnsureNBytes(n int)
	Capacity() int64
	Used() int64
	Available() int64
	Get(context.Context, CacheKey) (Data, bool)
	Set(context.Context, CacheKey, Data) error
	DeletePaths(context.Context, []string)
	Flush()
	Evict(chan int64)
}
