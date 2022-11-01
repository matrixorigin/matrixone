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

// plan build & optimize logical plan
package plan

import "github.com/matrixorigin/matrixone/pkg/container/types"

var intCastTableForRewrite map[[2]types.T]struct{}
var uintCastTableForRewrite map[[2]types.T]struct{}
var uint2intCastTableForRewrite map[[2]types.T]struct{}

func init() {
	intCastTableForRewrite = make(map[[2]types.T]struct{})
	uintCastTableForRewrite = make(map[[2]types.T]struct{})
	uint2intCastTableForRewrite = make(map[[2]types.T]struct{})

	rule1 := []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	}
	for i := 0; i < len(rule1); i++ {
		for j := i + 1; j < len(rule1); j++ {
			intCastTableForRewrite[[2]types.T{rule1[i], rule1[j]}] = struct{}{}
		}
	}

	rule2 := []types.T{
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	}
	for i := 0; i < len(rule2); i++ {
		for j := i + 1; j < len(rule2); j++ {
			uintCastTableForRewrite[[2]types.T{rule2[i], rule2[j]}] = struct{}{}
		}
	}

	rule3 := [][]types.T{
		{types.T_uint8, types.T_int16, types.T_int32, types.T_int64},
		{types.T_uint16, types.T_int32, types.T_int64},
		{types.T_uint32, types.T_int64},
	}
	for i := 0; i < len(rule3); i++ {
		for j := 1; j < len(rule3[i]); j++ {
			uintCastTableForRewrite[[2]types.T{rule3[i][0], rule2[j]}] = struct{}{}
		}
	}
}
