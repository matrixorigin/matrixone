// Copyright 2021 Matrix Origin
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

package vector

// int64MinMaxNoNull scans an admitted all-valid flat column. Independent
// accumulators remove the serial min/max dependency without changing integer
// ordering. The caller admits at least 32 rows; nullable and constant columns
// keep their existing path.
func int64MinMaxNoNull(col []int64) (int64, int64) {
	lo0, hi0 := col[0], col[0]
	lo1, hi1 := col[1], col[1]
	lo2, hi2 := col[2], col[2]
	lo3, hi3 := col[3], col[3]
	col = col[4:]
	for len(col) >= 4 {
		a, b, c, d := col[0], col[1], col[2], col[3]
		lo0, hi0 = min(lo0, a), max(hi0, a)
		lo1, hi1 = min(lo1, b), max(hi1, b)
		lo2, hi2 = min(lo2, c), max(hi2, c)
		lo3, hi3 = min(lo3, d), max(hi3, d)
		col = col[4:]
	}
	lo, hi := min(lo0, lo1, lo2, lo3), max(hi0, hi1, hi2, hi3)
	for _, value := range col {
		lo, hi = min(lo, value), max(hi, value)
	}
	return lo, hi
}
