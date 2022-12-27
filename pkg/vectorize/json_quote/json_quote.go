// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json_quote

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strconv"
)

func Single(str string) ([]byte, error) {
	bj, err := types.ParseStringToByteJson(strconv.Quote(str))
	if err != nil {
		return nil, err
	}
	return bj.Marshal()
}

func Batch(xs []string, rs [][]byte, nsp *nulls.Nulls) ([][]byte, error) {
	var err error
	for i, x := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		rs[i], err = Single(x)
		if err != nil {
			return nil, err
		}
	}
	return rs, nil
}
