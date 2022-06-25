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

package or

var (
	SelOr func([]int64, []int64, []int64) int64
)

func init() {
	SelOr = selOr
}

func selOr(xs, ys, rs []int64) int64 {
	cnt := 0
	i, j, n, m := 0, 0, len(xs), len(ys)
	for i < n && j < m {
		switch {
		case xs[i] > ys[j]:
			rs[cnt] = ys[j]
			cnt++
			j++
		case xs[i] < ys[j]:
			rs[cnt] = xs[i]
			cnt++
			i++
		default:
			rs[cnt] = xs[i]
			cnt++
			i++
			j++
		}
	}
	for ; i < n; i++ {
		rs[cnt] = xs[i]
		cnt++
	}
	for ; j < m; j++ {
		rs[cnt] = ys[j]
		cnt++
	}
	return int64(cnt)
}
