// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nulls

// Allocated counts retained bitmap backing capacity.
func Allocated(nsp *Nulls) int {
	if nsp == nil {
		return 0
	}
	return nsp.np.Allocated()
}

// GrowthAllocation predicts complete new storage while old data remains live.
func GrowthAllocation(nsp *Nulls, size int) int64 {
	if nsp == nil {
		return int64((uint64(max(size, 0))+63)/64) * 8
	}
	return nsp.np.GrowthAllocation(size)
}
