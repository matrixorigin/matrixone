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

package testutil

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var _ = OperatorCatchBatch

// OperatorCatchBatch return a string with format
//
//	`insert operator catch a batch, batch length is 100, [vec 0 : len is 100]`
func OperatorCatchBatch(operatorName string, bat *batch.Batch) string {
	if bat == nil {
		return ""
	}
	var rowIdIdx int = -1
	str := fmt.Sprintf("`%s` operator catch a batch, batch length is %d\n", operatorName, bat.Length())
	for i, vec := range bat.Vecs {
		str += fmt.Sprintf("[vec-%d(%s)[type is %v, %p] : len is %d]\n", i,
			bat.Attrs[i],
			vec.GetType().Oid, vec,
			vec.Length())
		if vec.GetType().Oid == types.T_Rowid {
			rowIdIdx = i
		}
	}
	if rowIdIdx != -1 {
		// output first 2 rows
		vec := bat.GetVector(int32(rowIdIdx))
		rowIds := vector.MustFixedCol[types.Rowid](vec)
		rows := 10
		for i, rowId := range rowIds {
			if i >= rows {
				break
			}
			str += fmt.Sprintf("[rowId is %s]", rowId.String())
		}
	}
	return str
}

func DebugBlockRowId(columnBatch *batch.Batch) {
	var str string
	for _, vec := range columnBatch.Vecs {
		if vec.GetType().Oid == types.T_Rowid {
			// output first 2 rows
			rowIds := vector.MustFixedCol[types.Rowid](vec)
			rows := 10
			for i, rowId := range rowIds {
				if i >= rows {
					break
				}
				str += fmt.Sprintf("[rowId is %s]\n", rowId.String())
			}
			break
		}
	}
	if str != "" {
		logutil.Infof("block read rowId is %s", str)
	}
}
