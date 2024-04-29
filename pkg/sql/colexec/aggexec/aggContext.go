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
