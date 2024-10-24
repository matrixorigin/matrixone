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

	// Check if InternalExecutorKey is present in the context.
	// No action is taken when InternalExecutorKey is present.
	if ctx.Value(InternalExecutorKey{}) == nil {
		// If the InternalExecutorKey does not exist, it means that you are using a generic executor.
		if counter1, ok := ctx.Value(ExecPipelineMarkKey{}).(*CounterSet); ok && counter1 != nil {
			// No code here; At this stage, independent functions are used to statistically analyze S3 requests
		} else if counter2, ok := ctx.Value(CompilePlanMarkKey{}).(*CounterSet); ok && counter2 != nil {
			fn(counter2)
		} else if counter3, ok := ctx.Value(BuildPlanMarkKey{}).(*CounterSet); ok && counter3 != nil {
			fn(counter3)
		}

		// Handling the usage of S3 resources when calling a function
		if counter, ok := ctx.Value(S3RequestKey{}).(*CounterSet); ok && counter != nil {
			fn(counter)
		}
	}

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
