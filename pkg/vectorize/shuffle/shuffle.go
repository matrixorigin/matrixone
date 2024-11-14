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

package shuffle

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func FixedLengthShuffle[T types.FixedSizeT](vs, ws []T, sels []int64) []T {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	return ws[:len(sels)]
}

func TupleShuffle(vs, ws [][]interface{}, sels []int64) [][]interface{} {
	for i, sel := range sels {
		ws[i] = vs[sel]
	}
	return ws[:len(sels)]
}
