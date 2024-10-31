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

type CounterSets = map[*CounterSet]struct{}

type ctxKeyCounters struct{}

var CtxKeyCounters = ctxKeyCounters{}

func WithCounterSet(ctx context.Context, sets ...*CounterSet) context.Context {
	// check existed
	v := ctx.Value(CtxKeyCounters)
	if v == nil {
		v := make(CounterSets)
		for _, s := range sets {
			if s == nil {
				panic("nil counter set")
			}
			v[s] = struct{}{}
		}
		return context.WithValue(ctx, CtxKeyCounters, v)
	}

	counters := v.(CounterSets)

	allExist := true
	for _, s := range sets {
		if _, ok := counters[s]; !ok {
			allExist = false
			break
		}
	}

	// if all exist already, try not to nest context too depth
	if allExist {
		return ctx
	}

	newCounters := make(CounterSets, len(counters)+1)
	for counter := range counters {
		newCounters[counter] = struct{}{}
	}

	for _, s := range sets {
		if s == nil {
			panic("nil counter set")
		}
		newCounters[s] = struct{}{}
	}
	return context.WithValue(ctx, CtxKeyCounters, newCounters)
}

func WithCounterSetFrom(ctx context.Context, fromCtx context.Context) context.Context {
	if v := fromCtx.Value(CtxKeyCounters); v != nil {
		var sets []*CounterSet
		for set := range v.(CounterSets) {
			sets = append(sets, set)
		}
		return WithCounterSet(ctx, sets...)
	}
	return ctx
}

// ----------------------------------------------------------------------------------------------------------------------
// S3RequestKey is an empty struct used as a key for attaching S3 request counters to a context, which
// used to count the usage of S3 resources when a certain function is called
type S3RequestKey struct{}

// AttachS3RequestKey attaches a *CounterSet to the given context with an S3RequestKey.
func AttachS3RequestKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, S3RequestKey{}, counter)
}

func GetS3RequestKey(ctx context.Context) (*CounterSet, bool) {
	counter, ok := ctx.Value(S3RequestKey{}).(*CounterSet)
	return counter, ok
}

// Internal executor context key
type InternalExecutorKey struct{}

func AttachInternalExecutorKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, InternalExecutorKey{}, struct{}{})
}

// IsInternalExecutor checks if the given context contains the InternalExecutorKey
func IsInternalExecutor(ctx context.Context) bool {
	_, ok := ctx.Value(InternalExecutorKey{}).(struct{})
	return ok
}

// Generic executor context key
type GenericExecutorKey struct{}

func AttachGenericExecutorKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, GenericExecutorKey{}, struct{}{})
}

// BuildPlanMarkKey is an empty struct used as a key for attaching a build plan counter to a context.
type BuildPlanMarkKey struct{}

// AttachBuildPlanMarkKey attaches a *CounterSet to the given context with a BuildPlanMarkKey.
func AttachBuildPlanMarkKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, BuildPlanMarkKey{}, counter)
}

// CompilePlanMarkKey is an empty struct used as a key for attaching a compile plan counter to a context.
type CompilePlanMarkKey struct{}

// AttachCompilePlanMarkKey attaches a *CounterSet to the given context with a CompilePlanMarkKey.
func AttachCompilePlanMarkKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, CompilePlanMarkKey{}, counter)
}

// ExecPipelineMarkKey is an empty struct used as a key for attaching an execution pipeline counter to a context.
type ExecPipelineMarkKey struct{}

// AttachExecPipelineKey attaches a *CounterSet to the given context with an ExecPipelineMarkKey.
func AttachExecPipelineKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, ExecPipelineMarkKey{}, counter)
}
