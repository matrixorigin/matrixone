// Copyright 2022 Matrix Origin
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

package generate_series

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

func doGenerateInt(start, end, step int) ([]int, error) {
	if step == 0 {
		return nil, moerr.NewInvalidInput("step cannot be zero")
	}
	if start == end {
		return []int{start}, nil
	}
	s1 := step > 0
	s2 := end > start
	if s1 != s2 {
		return nil, moerr.NewInvalidInput("step and start/end are not compatible")
	}
	var res []int
	if step > 0 {
		for i := start; i <= end; i += step {
			res = append(res, i)
		}
	} else {
		for i := start; i >= end; i += step {
			res = append(res, i)
		}
	}
	return res, nil
}
