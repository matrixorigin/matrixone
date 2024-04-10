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

var AnyValueSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time,
	types.T_decimal64, types.T_decimal128,
	types.T_bool,
	types.T_bit,
	types.T_varchar, types.T_char, types.T_blob, types.T_text,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
	types.T_Rowid,
}

func AnyValueReturnType(typs []types.Type) types.Type {
	return typs[0]
}

func newAggAnyValue[T types.FixedSizeTExceptStrType]() aggexec.SingleAggFromFixedRetFixed[T, T] {
	return &aggAnyValue[T]{}
}

type aggAnyValue[T types.FixedSizeTExceptStrType] struct {
	has bool
}

func (a *aggAnyValue[T]) Marshal() []byte       { return types.EncodeBool(&a.has) }
func (a *aggAnyValue[T]) Unmarshal(data []byte) { a.has = types.DecodeBool(data) }
func (a *aggAnyValue[T]) Init(setter aggexec.AggSetter[T], arg, ret types.Type) error {
	a.has = false
	return nil
}

type aggAnyBytesValue struct {
	has bool
}

func newAggAnyBytesValue() aggexec.SingleAggFromVarRetVar {
	return &aggAnyBytesValue{}
}

func (a *aggAnyBytesValue) Marshal() []byte { return types.EncodeBool(&a.has) }
func (a *aggAnyBytesValue) Unmarshal(data []byte) {
	a.has = types.DecodeBool(data)
}
func (a *aggAnyBytesValue) Init(setter aggexec.AggBytesSetter, arg types.Type, ret types.Type) error {
	a.has = false
	return nil
}
