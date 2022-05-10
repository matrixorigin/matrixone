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

package updates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutationControllerAppend(t *testing.T) {
	mc := NewMVCCHandle(nil)

	nodeCnt := 10000
	rowsPerNode := uint32(5)
	ts := uint64(2)
	queries := make([]uint64, 0)
	queries = append(queries, ts-1)
	for i := 0; i < nodeCnt; i++ {
		txn := mockTxn()
		txn.CommitTS = ts
		node := mc.AddAppendNodeLocked(txn, rowsPerNode*(uint32(i)+1))
		node.ApplyCommit(nil)
		queries = append(queries, ts+1)
		ts += 2
	}

	st := time.Now()
	for i, qts := range queries {
		row, ok := mc.GetMaxVisibleRowLocked(qts)
		if i == 0 {
			assert.False(t, ok)
		} else {
			assert.True(t, ok)
			assert.Equal(t, uint32(i)*rowsPerNode, row)
		}
	}
	t.Logf("%s -- %d ops", time.Since(st), len(queries))
}
