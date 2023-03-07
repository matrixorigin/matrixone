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

package types

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

func BitAnd(result, v1, v2 []byte) {
	if len(v1) != len(v2) {
		panic(moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length"))
	}

	for i := range v1 {
		result[i] = v1[i] & v2[i]
	}
}

func BitOr(result, v1, v2 []byte) {
	if len(v1) != len(v2) {
		panic(moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length"))
	}

	for i := range v1 {
		result[i] = v1[i] | v2[i]
	}
}

func BitXor(result, v1, v2 []byte) {
	if len(v1) != len(v2) {
		panic(moerr.NewInternalErrorNoCtx("Binary operands of bitwise operators must be of equal length"))
	}

	for i := range v1 {
		result[i] = v1[i] ^ v2[i]
	}
}
