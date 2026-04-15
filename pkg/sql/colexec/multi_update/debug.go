// Copyright 2025 Matrix Origin
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

package multi_update

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

const fullTextMainTableDebugRows = 4096

func shouldLogFullTextMainTableBatch(
	updateCtx *MultiUpdateCtx,
	tableType UpdateTableType,
	rowCount int,
) bool {
	return rowCount >= fullTextMainTableDebugRows && tableType == UpdateMainTable
}

func debugBatchPKRange(
	bat *batch.Batch,
	pkName string,
) (string, string) {
	if bat == nil || bat.RowCount() == 0 {
		return "", ""
	}
	pkIdx := -1
	for i, attr := range bat.Attrs {
		if strings.EqualFold(attr, pkName) {
			pkIdx = i
			break
		}
	}
	if pkIdx < 0 {
		return "", ""
	}
	return debugVectorValue(bat.Vecs[pkIdx], 0), debugVectorValue(bat.Vecs[pkIdx], bat.RowCount()-1)
}

func debugVectorValue(vec *vector.Vector, row int) string {
	if vec == nil || row < 0 || row >= vec.Length() {
		return ""
	}
	return fmt.Sprint(vector.GetAny(vec, row, true))
}

func debugObjectRefName(ref *plan.ObjectRef) string {
	if ref == nil {
		return ""
	}
	if ref.SchemaName == "" {
		return ref.ObjName
	}
	return ref.SchemaName + "." + ref.ObjName
}

func debugBatchRowIDRange(
	bat *batch.Batch,
	rowIDIdx int,
) (string, string, int) {
	if bat == nil || rowIDIdx < 0 || rowIDIdx >= len(bat.Vecs) || bat.RowCount() == 0 {
		return "", "", 0
	}
	vec := bat.Vecs[rowIDIdx]
	if vec == nil {
		return "", "", 0
	}
	nulls := vec.GetNulls()
	nullCount := int(nulls.Count())
	first := ""
	for i := 0; i < bat.RowCount(); i++ {
		if !nulls.Contains(uint64(i)) {
			first = debugVectorValue(vec, i)
			break
		}
	}
	last := ""
	for i := bat.RowCount() - 1; i >= 0; i-- {
		if !nulls.Contains(uint64(i)) {
			last = debugVectorValue(vec, i)
			break
		}
	}
	return first, last, nullCount
}

func debugTotalBatchRows(bats []*batch.Batch) int {
	total := 0
	for _, bat := range bats {
		if bat != nil {
			total += bat.RowCount()
		}
	}
	return total
}

func debugUpdateAction(action UpdateAction) string {
	switch action {
	case UpdateWriteTable:
		return "write-table"
	case UpdateWriteS3:
		return "write-s3"
	case UpdateFlushS3Info:
		return "flush-s3-info"
	default:
		return fmt.Sprintf("unknown-%d", action)
	}
}
