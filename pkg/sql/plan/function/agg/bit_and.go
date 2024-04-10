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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

var BitAndSupportedParameters = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_binary, types.T_varbinary,
	types.T_bit,
}

func BitAndReturnType(typs []types.Type) types.Type {
	if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
		return typs[0]
	}
	return types.T_uint64.ToType()
}

type aggBitAnd[T numeric] struct{}

func newAggBitAnd[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitAnd[T]{}
}

func (a aggBitAnd[T]) Marshal() []byte  { return nil }
func (a aggBitAnd[T]) Unmarshal([]byte) {}
func (a aggBitAnd[T]) Init(set aggexec.AggSetter[uint64], arg, ret types.Type) error {
	set(^uint64(0))
	return nil
}

type aggBitBinary struct {
	isEmpty bool
}

func (a *aggBitBinary) Marshal() []byte     { return types.EncodeBool(&a.isEmpty) }
func (a *aggBitBinary) Unmarshal(bs []byte) { a.isEmpty = types.DecodeBool(bs) }
func (a *aggBitBinary) Init(set aggexec.AggBytesSetter, arg types.Type, ret types.Type) error {
	a.isEmpty = true
	return nil
}

type aggBitAndBinary struct {
	aggBitBinary
}

func newAggBitAndBinary() aggexec.SingleAggFromVarRetVar {
	return &aggBitAndBinary{}
}
