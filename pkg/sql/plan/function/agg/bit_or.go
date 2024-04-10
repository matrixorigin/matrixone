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

type aggBitOr[T numeric] struct{}

func newAggBitOr[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitOr[T]{}
}

func (a aggBitOr[T]) Marshal() []byte  { return nil }
func (a aggBitOr[T]) Unmarshal([]byte) {}
func (a aggBitOr[T]) Init(set aggexec.AggSetter[uint64], arg, ret types.Type) error {
	set(0)
	return nil
}

type aggBitOrBinary struct {
	aggBitBinary
}

func newAggBitOrBinary() aggexec.SingleAggFromVarRetVar {
	return &aggBitOrBinary{}
}
