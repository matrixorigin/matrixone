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

package engine_util

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

func NewCNTombstoneBatch(
	pkAttr string,
	pkType *types.Type,
) *batch.Batch {
	ret := batch.New(false, []string{catalog.Row_ID, pkAttr})
	ret.SetVector(objectio.TombstoneAttr_Rowid_Idx, vector.NewVec(objectio.RowidType))
	ret.SetVector(objectio.TombstoneAttr_PK_Idx, vector.NewVec(*pkType))
	return ret
}
