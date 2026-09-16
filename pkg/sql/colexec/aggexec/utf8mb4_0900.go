// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Native 0900 aggregates retain the original user value as their result, but
// select winners with the same relation used by SQL comparison. In
// particular, 0900_ai_ci is a UCA 9.0 collation and 0900_bin is NO PAD; the
// legacy aggregate comparators have different rules and cannot be reused.
func newUTF8mb40900AIMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = collation.UCA0900AICollate
	} else {
		exec.comp = func(x, y []byte) int { return -collation.UCA0900AICollate(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newUTF8mb40900BinMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = bytes.Compare
	} else {
		exec.comp = func(x, y []byte) int { return -bytes.Compare(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}
