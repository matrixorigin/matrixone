// Copyright 2022 Matrix Origin
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

package json_unquote

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func StringSingle(v []byte) (string, error) {
	bj, err := types.ParseSliceToByteJson(v)
	if err != nil {
		return "", err
	}
	return bj.Unquote()
}
func JsonSingle(v []byte) (string, error) {
	bj := types.DecodeJson(v)
	return bj.Unquote()
}

func StringBatch(xs [][]byte, rs []string, nsp *nulls.Nulls) ([]string, error) {
	for i, v := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		bj, err := types.ParseSliceToByteJson(v)
		if err != nil {
			return nil, err
		}
		r, err := bj.Unquote()
		if err != nil {
			return nil, err
		}
		rs[i] = r
	}
	return rs, nil
}

func JsonBatch(xs [][]byte, rs []string, nsp *nulls.Nulls) ([]string, error) {
	for i, v := range xs {
		if nsp.Contains(uint64(i)) {
			continue
		}
		bj := types.DecodeJson(v)
		r, err := bj.Unquote()
		if err != nil {
			return nil, err
		}
		rs[i] = r
	}
	return rs, nil
}
