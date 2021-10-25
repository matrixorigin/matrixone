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

package mock

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"strconv"
)

func MockBatch(types []types.Type, rows uint64) *batch.Batch {
	var attrs []string
	for idx := range types {
		attrs = append(attrs, "mock_"+strconv.Itoa(idx))
	}

	bat := batch.New(true, attrs)
	var err error
	for i, colType := range types {
		vec := vector.MockVector(colType, rows)
		bat.Vecs[i], err = vec.CopyToVector()
		if err != nil {
			panic(err)
		}
		vec.Close()
	}

	return bat
}
