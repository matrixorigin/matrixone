// Copyright 2023 Matrix Origin
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

package perfcounter

import "context"

type Counters = map[*Counter]struct{}

type ctxKeyCounters struct{}

var CtxKeyCounters = ctxKeyCounters{}

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
