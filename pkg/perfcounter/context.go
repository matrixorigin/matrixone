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
type S3RequestKey struct{}

func AttachS3RequestKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, S3RequestKey{}, counter)
}

func GetS3RequestKey(ctx context.Context) (*CounterSet, bool) {
	counter, ok := ctx.Value(S3RequestKey{}).(*CounterSet)
	return counter, ok
}

// 内部执行器context KEY
type InternalExecutorKey struct{}

func AttachInternalExecutorKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, InternalExecutorKey{}, struct{}{})
}

// ctx = perfcounter.AttachInternalExecutorKey(ctx)

// 通用执行器context KEY
type GenericExecutorKey struct{}

func AttachGenericExecutorKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, GenericExecutorKey{}, struct{}{})
}

type ParserMarkKey struct{}

func AttachParserMarkKey(ctx context.Context) context.Context {
	return context.WithValue(ctx, ParserMarkKey{}, struct{}{})
}

type BuildPlanMarkKey struct{}

func AttachBuildPlanMarkKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, BuildPlanMarkKey{}, counter)
}

type CompilePlanMarkKey struct{}

func AttachCompilePlanMarkKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, CompilePlanMarkKey{}, counter)
}

type ExecPipelineMarkKey struct{}

func AttachExecPipelineKey(ctx context.Context, counter *CounterSet) context.Context {
	return context.WithValue(ctx, ExecPipelineMarkKey{}, counter)
}
