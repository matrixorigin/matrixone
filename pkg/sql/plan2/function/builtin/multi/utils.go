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

package multi

import "github.com/matrixorigin/matrixone/pkg/container/vector"

// Get the slice of whether the corresponding parameter is a constant from the vector slice
func GetIsConstSliceFromVectors(vs []*vector.Vector) []bool {
	isConst := make([]bool, len(vs))
	for i := 0; i < len(vs); i++ {
		isConst[i] = vs[i].IsConstant()
	}
	return isConst
}
