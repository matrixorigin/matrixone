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

func Update(ctx context.Context, fn func(*Counter), extraCounters ...*Counter) {
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
