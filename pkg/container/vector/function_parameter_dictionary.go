// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import "github.com/matrixorigin/matrixone/pkg/container/types"

// functionParameterDictionary borrows the input, exactly like ordinary parameter
// wrappers. The caller must keep its source live through function execution.
// Resolve the backing wrapper once, preserving its existing conversion rules.
// Logical NULL coordinates must never be replaced by physical value indices.
type functionParameterDictionary[T types.FixedSizeT] struct {
	source *Vector
	rows   []int64
	base   FunctionParameterWrapper[T]
}

func (p *functionParameterDictionary[T]) GetType() types.Type      { return p.base.GetType() }
func (p *functionParameterDictionary[T]) GetSourceVector() *Vector { return p.source }
func (p *functionParameterDictionary[T]) WithAnyNullValue() bool   { return p.source.HasNull() }
func (p *functionParameterDictionary[T]) GetValue(row uint64) (value T, isNull bool) {
	if p.source.IsNull(row) {
		return value, true
	}
	return p.base.GetValue(uint64(p.rows[row]))
}
func (p *functionParameterDictionary[T]) GetStrValue(row uint64) ([]byte, bool) {
	if p.source.IsNull(row) {
		return nil, true
	}
	return p.base.GetStrValue(uint64(p.rows[row]))
}
func (*functionParameterDictionary[T]) UnSafeGetAllValue() []T {
	// Kernels requiring contiguous logical values must normalize explicitly at
	// their owning execution boundary. Never expose unselected base storage.
	panic(errDictionaryPhysicalAccess)
}
