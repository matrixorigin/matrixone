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

type AggContext struct {
	hasCommonContext bool
	hasGroupContext  bool
	commonContext    AggCommonExecContext
	groupContext     []AggGroupExecContext
	initGroup        AggGroupContextInit
}

func newAggContextFromImpl(
	ctxImpl aggContextImplementation,
	result types.Type,
	args ...types.Type) *AggContext {

	ctx := &AggContext{
		hasCommonContext: ctxImpl.hasCommonContext,
		hasGroupContext:  ctxImpl.hasGroupContext,
	}
	if ctx.hasGroupContext {
		ctx.initGroup = ctxImpl.generateGroupContext
	}
	if ctxImpl.hasCommonContext {
		ctx.setCommonContext(ctxImpl.generateCommonContext(result, args...))
	}
	return ctx
}

type AggGroupContextInit func(resultType types.Type, parameters ...types.Type) AggGroupExecContext
type AggCommonContextInit func(resultType types.Type, parameters ...types.Type) AggCommonExecContext

func (a *AggContext) setCommonContext(c AggCommonExecContext) {
	if !a.hasCommonContext {
		a.commonContext = c
	}
}

func (a *AggContext) preAllocate(more int) {
	if !a.hasGroupContext {
		return
	}

	n := len(a.groupContext) + more
	if n <= cap(a.groupContext) {
		return
	}

	oldLen := len(a.groupContext)
	a.groupContext = append(a.groupContext, make([]AggGroupExecContext, n-cap(a.groupContext))...)
	a.groupContext = a.groupContext[:oldLen]
}

func (a *AggContext) growsGroupContext(
	more int,
	resultType types.Type, parameters ...types.Type) {
	if !a.hasGroupContext {
		return
	}
	oldLen, newLen := len(a.groupContext), len(a.groupContext)+more
	if newLen > cap(a.groupContext) {
		a.groupContext = append(a.groupContext, make([]AggGroupExecContext, more)...)
		for i := oldLen; i < newLen; i++ {
			a.groupContext[i] = a.initGroup(resultType, parameters...)
		}

	} else {
		a.groupContext = a.groupContext[:newLen]
		for i := oldLen; i < newLen; i++ {
			a.groupContext[i] = a.initGroup(resultType, parameters...)
		}
	}
}

func (a *AggContext) getCommonContext() AggCommonExecContext {
	return a.commonContext
}

func (a *AggContext) getGroupContext(i int) AggGroupExecContext {
	if a.hasGroupContext {
		return a.groupContext[i]
	}
	return nil
}

func (a *AggContext) Marshal() ([]byte, error) {
	encoded := &EncodedAggContext{
		HasCommonContext: a.hasCommonContext,
		HasGroupContext:  a.hasGroupContext,
	}
	if a.hasCommonContext {
		encoded.CommonContext = a.commonContext.Marshal()
	}
	if a.hasGroupContext {
		encoded.GroupContext = make([][]byte, len(a.groupContext))
		for i := range a.groupContext {
			encoded.GroupContext[i] = a.groupContext[i].Marshal()
		}
	}
	return encoded.Marshal()
}

func (a *AggContext) Unmarshal(bs []byte, resultType types.Type, parameters ...types.Type) error {
	encoded := &EncodedAggContext{}
	if err := encoded.Unmarshal(bs); err != nil {
		return err
	}
	a.hasCommonContext = encoded.HasCommonContext
	a.hasGroupContext = encoded.HasGroupContext
	if a.hasCommonContext {
		a.commonContext.Unmarshal(encoded.CommonContext)
	}
	if a.hasGroupContext {
		a.groupContext = make([]AggGroupExecContext, len(encoded.GroupContext))

		for i := range encoded.GroupContext {
			a.groupContext[i] = a.initGroup(resultType, parameters...)
			a.groupContext[i].Unmarshal(encoded.GroupContext[i])
		}
	}
	return nil
}

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
