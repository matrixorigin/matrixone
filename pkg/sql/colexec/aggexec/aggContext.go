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

package aggexec

import "github.com/matrixorigin/matrixone/pkg/container/types"

// todo: sca.
//type AggContext struct {
//	hasCommonContext bool
//	hasGroupContext  bool
//	commonContext    AggCommonExecContext
//	groupContext     []AggGroupExecContext
//}
//
//func (a *AggContext) setCommonContext(c AggCommonExecContext) {
//	if c == nil {
//		return
//	}
//	a.hasCommonContext = true
//	a.commonContext = c
//}
//
//func (a *AggContext) preExtend(n int) {
//	if !a.hasGroupContext {
//		return
//	}
//	if n <= cap(a.groupContext) {
//		return
//	}
//
//	oldLen := len(a.groupContext)
//	a.groupContext = append(a.groupContext, make([]AggGroupExecContext, n-cap(a.groupContext))...)
//	a.groupContext = a.groupContext[:oldLen]
//}
//
//func (a *AggContext) growsGroupContext(initGroup func() AggGroupExecContext, n int) {
//	if !a.hasGroupContext {
//		return
//	}
//	oldLen, newLen := len(a.groupContext), len(a.groupContext)+n
//	if newLen > cap(a.groupContext) {
//		a.groupContext = append(a.groupContext, make([]AggGroupExecContext, n)...)
//		for i := oldLen; i < newLen; i++ {
//			a.groupContext[i] = initGroup()
//		}
//
//	} else {
//		a.groupContext = a.groupContext[:newLen]
//		for i := oldLen; i < newLen; i++ {
//			a.groupContext[i] = initGroup()
//		}
//	}
//}
//
//func (a *AggContext) getCommonContext() AggCommonExecContext {
//	return a.commonContext
//}
//
//func (a *AggContext) getGroupContext(i int) AggGroupExecContext {
//	if a.hasGroupContext {
//		return a.groupContext[i]
//	}
//	return nil
//}

// AggCommonExecContext stores the common context for all the groups.
// like the type scale, timezone and so on.
type AggCommonExecContext interface {
	AggCanMarshal
}

// AggGroupExecContext store the content of each group individually.
// like the row-count has been filled, the sum of the values and so on.
type AggGroupExecContext interface {
	AggCanMarshal
}

/*
	prepared context structures for agg.

	EmptyContextOfSingleAggRetFixed and EmptyContextOfSingleAggRetBytes are used for aggregation
	which does not need to store any context.

	ContextWithEmptyFlagOfSingleAggRetFixed and ContextWithEmptyFlagOfSingleAggRetBytes are used for aggregation
	which only needs to store a flag to indicate whether it is empty.
*/

type EmptyContextOfSingleAggRetFixed[T types.FixedSizeTExceptStrType] struct{}

func (a EmptyContextOfSingleAggRetFixed[T]) Marshal() []byte  { return nil }
func (a EmptyContextOfSingleAggRetFixed[T]) Unmarshal([]byte) {}
func GenerateEmptyContextFromFixedToFixed[from, to types.FixedSizeTExceptStrType]() SingleAggFromFixedRetFixed[from, to] {
	return EmptyContextOfSingleAggRetFixed[to]{}
}
func GenerateEmptyContextFromVarToFixed[to types.FixedSizeTExceptStrType]() SingleAggFromVarRetFixed[to] {
	return EmptyContextOfSingleAggRetFixed[to]{}
}

type EmptyContextOfSingleAggRetBytes struct{}

func (a EmptyContextOfSingleAggRetBytes) Marshal() []byte  { return nil }
func (a EmptyContextOfSingleAggRetBytes) Unmarshal([]byte) {}
func GenerateEmptyContextFromFixedToVar[from types.FixedSizeTExceptStrType]() SingleAggFromFixedRetVar[from] {
	return EmptyContextOfSingleAggRetBytes{}
}
func GenerateEmptyContextFromVarToVar() SingleAggFromVarRetVar {
	return EmptyContextOfSingleAggRetBytes{}
}

type ContextWithEmptyFlagOfSingleAggRetFixed[T types.FixedSizeTExceptStrType] struct {
	IsEmpty bool
}

func (a *ContextWithEmptyFlagOfSingleAggRetFixed[T]) Marshal() []byte {
	return types.EncodeBool(&a.IsEmpty)
}
func (a *ContextWithEmptyFlagOfSingleAggRetFixed[T]) Unmarshal(data []byte) {
	a.IsEmpty = types.DecodeBool(data)
}
func GenerateFlagContextFromFixedToFixed[from, to types.FixedSizeTExceptStrType]() SingleAggFromFixedRetFixed[from, to] {
	return &ContextWithEmptyFlagOfSingleAggRetFixed[to]{}
}
func InitFlagContextFromFixedToFixed[from, to types.FixedSizeTExceptStrType](exec SingleAggFromFixedRetFixed[from, to], setter AggSetter[to], arg, ret types.Type) error {
	a := exec.(*ContextWithEmptyFlagOfSingleAggRetFixed[to])
	a.IsEmpty = true
	return nil
}

func GenerateFlagContextFromVarToFixed[to types.FixedSizeTExceptStrType]() SingleAggFromVarRetFixed[to] {
	return &ContextWithEmptyFlagOfSingleAggRetFixed[to]{}
}

type ContextWithEmptyFlagOfSingleAggRetBytes struct {
	IsEmpty bool
}

func (a *ContextWithEmptyFlagOfSingleAggRetBytes) Marshal() []byte {
	return types.EncodeBool(&a.IsEmpty)
}
func (a *ContextWithEmptyFlagOfSingleAggRetBytes) Unmarshal(data []byte) {
	a.IsEmpty = types.DecodeBool(data)
}
func GenerateFlagContextFromFixedToVar[from types.FixedSizeTExceptStrType]() SingleAggFromFixedRetVar[from] {
	return &ContextWithEmptyFlagOfSingleAggRetBytes{}
}
func GenerateFlagContextFromVarToVar() SingleAggFromVarRetVar {
	return &ContextWithEmptyFlagOfSingleAggRetBytes{}
}
func InitFlagContextFromVarToVar(exec SingleAggFromVarRetVar, setter AggBytesSetter, arg, ret types.Type) error {
	a := exec.(*ContextWithEmptyFlagOfSingleAggRetBytes)
	a.IsEmpty = true
	return nil
}
