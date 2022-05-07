// Copyright 2021 - 2022 Matrix Origin
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

package function

import "github.com/matrixorigin/matrixone/pkg/container/types"

// todo(cms): sometimes naive
var precisionLevel = map[types.T]int{
	types.T_int8:  1,
	types.T_int16: 2,
}

func getHighestPrecisionType(typs []types.T) types.T {
	return types.T_int64
}

func SearchCastRule(fname string, args []types.T) (finalArgs []types.T, needCast bool) {
	// uint8 + uint64 , return []types.T{uint64, uint64}, true
	return nil, false
}
