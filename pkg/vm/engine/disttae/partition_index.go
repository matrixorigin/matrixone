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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type PartitionIndex struct {
	RowIDs *Map[types.Rowid, Versions[RowRef]]
}

type RowRef struct {
	Batch  *batch.Batch
	Offset int
}

func NewPartitionIndex() *PartitionIndex {
	return &PartitionIndex{
		RowIDs: new(Map[types.Rowid, Versions[RowRef]]),
	}
}
