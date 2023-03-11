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
)

type Counter struct {
	S3ListObjects   int64
	S3HeadObject    int64
	S3PutObject     int64
	S3GetObject     int64
	S3DeleteObjects int64
	S3DeleteObject  int64

	CacheRead     int64
	CacheHit      int64
	MemCacheRead  int64
	MemCacheHit   int64
	DiskCacheRead int64
	DiskCacheHit  int64
}

type ctxKeyCounters struct{}

var CtxKeyCounters = ctxKeyCounters{}

type Counters = map[*Counter]struct{}

func updateCounters(ctx context.Context, fn func(*Counter), extraCounters ...*Counter) {
	v := ctx.Value(CtxKeyCounters)
	var counters Counters
	if v != nil {
		counters = v.(Counters)
		for counter := range counters {
			fn(counter)
		}
	}
	for _, counter := range extraCounters {
		if counter == nil {
			continue
		}
		if counters != nil {
			if _, ok := counters[counter]; ok {
				continue
			}
		}
		fn(counter)
	}
}

func WithCounter(ctx context.Context, counter *Counter) context.Context {
	// check existed
	v := ctx.Value(CtxKeyCounters)
	if v == nil {
		return context.WithValue(ctx, CtxKeyCounters, Counters{
			counter: struct{}{},
		})
	}
	counters := v.(Counters)
	newCounters := make(Counters, len(counters)+1)
	for counter := range counters {
		newCounters[counter] = struct{}{}
	}
	newCounters[counter] = struct{}{}
	return context.WithValue(ctx, CtxKeyCounters, newCounters)
}
