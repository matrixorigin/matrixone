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

import (
	"context"
)

func Update(ctx context.Context, fn func(*CounterSet), extraCounterSets ...*CounterSet) {
	var counterSets CounterSets

	// from context
	v := ctx.Value(CtxKeyCounters)
	if v != nil {
		counterSets = v.(CounterSets)
		for set := range counterSets {
			fn(set)
		}
	}

	// extra
	for _, set := range extraCounterSets {
		if set == nil {
			continue
		}
		if counterSets != nil {
			if _, ok := counterSets[set]; ok {
				continue
			}
		}
		fn(set)
	}

	// global
	if _, ok := counterSets[globalCounterSet]; !ok {
		fn(globalCounterSet)
	}

	// per table TODO
}
