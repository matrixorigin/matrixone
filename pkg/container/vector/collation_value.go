// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import "github.com/matrixorigin/matrixone/pkg/container/types"

// ValidateCollationValues 检查执行域和非NULL值的编码，不构造key、不修改原值。
// 域只解析一次，常量只检查物理值。generic codec不调用它；SQL/持久化准入仍
// 由各自边界负责，调用方可仅对新原生域启用以保留历史行为。
func (v *Vector) ValidateCollationValues() error {
	if v == nil {
		return nil
	}
	domain, transformed, err := types.CollationDomainForType(*v.GetType())
	if err != nil {
		return err
	}
	if !transformed || v.IsConstNull() {
		return nil
	}
	length := v.Length()
	if v.IsConst() {
		length = min(length, 1)
	}
	for i := 0; i < length; i++ {
		if v.GetNulls().Contains(uint64(i)) {
			continue
		}
		if err := domain.ValidateValue(v.GetBytesAt(i)); err != nil {
			return err
		}
	}
	return nil
}
