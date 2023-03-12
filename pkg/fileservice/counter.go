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
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"sync"
)

type Counter struct {
	sync.Mutex
	S3ListObjects   int64
	S3HeadObject    int64
	S3PutObject     int64
	S3GetObject     int64
	S3DeleteObjects int64
	S3DeleteObject  int64

	MemCacheRead  int64
	MemCacheHit   int64
	DiskCacheRead int64
	DiskCacheHit  int64
}

type ctxKeyCounters struct{}

var CtxKeyCounters = ctxKeyCounters{}

func updateCounters(ctx context.Context, fn func(*Counter)) {
	v := ctx.Value(CtxKeyCounters)
	if v == nil {
		return
	}
	counters := v.([]*Counter)
	for _, counter := range counters {
		fn(counter)
	}
}

func WithCounter(ctx context.Context, statsFName string, counter *Counter) context.Context {
	// check existed
	v := ctx.Value(CtxKeyCounters)

	metric.DefaultStatsRegistry.RegisterStats(statsFName, counter) //TODO: Need suggestions here.

	if v == nil {
		return context.WithValue(ctx, CtxKeyCounters, []*Counter{counter})
	}
	counters := v.([]*Counter)
	newCounters := make([]*Counter, len(counters), len(counters)+1)
	copy(newCounters, counters)
	newCounters = append(newCounters, counter)
	return context.WithValue(ctx, CtxKeyCounters, newCounters)
}

func (c *Counter) Collect() metric.Stats {
	stats := make(map[string]int64)

	c.Lock()
	stats["S3ListObjects"] = c.S3ListObjects
	stats["S3HeadObject"] = c.S3HeadObject
	stats["S3PutObject"] = c.S3PutObject
	stats["S3GetObject"] = c.S3GetObject
	stats["S3DeleteObjects"] = c.S3DeleteObjects
	stats["S3DeleteObject"] = c.S3DeleteObject

	stats["MemCacheRead"] = c.MemCacheRead
	stats["MemCacheHit"] = c.MemCacheHit
	stats["DiskCacheRead"] = c.DiskCacheRead
	stats["DiskCacheHit"] = c.DiskCacheHit

	c.Unlock()
	return stats
}
