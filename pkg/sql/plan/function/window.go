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

var (
	// rank() supported input type and output type.
	winRankReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}

	// row_number() supported input type and output type.
	winRowNumberReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}

	// dense_rank() supported input type and output type.
	winDenseRankReturnType = func(typs []types.Type) types.Type {
		return types.T_int64.ToType()
	}
)
