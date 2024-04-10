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

type aggBitXor[T numeric] struct{}

func newAggBitXor[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitXor[T]{}
}

func (a aggBitXor[T]) Marshal() []byte  { return nil }
func (a aggBitXor[T]) Unmarshal([]byte) {}
func (a aggBitXor[T]) Init(set aggexec.AggSetter[uint64], arg, ret types.Type) error {
	set(0)
	return nil
}

type aggBitXorBinary struct {
	aggBitBinary
}

func newAggBitXorBinary() aggexec.SingleAggFromVarRetVar {
	return &aggBitXorBinary{}
}
