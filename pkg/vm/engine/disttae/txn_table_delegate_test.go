// Copyright 2021-2024 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
)

func TestTxnTableDelegate_CollectChanges(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.CollectChanges(
			context.Background(),
			types.TS{},
			types.TS{},
			false,
			&mpool.MPool{},
		)
	})
}

func TestTxnTableDelegate_MergeObjects(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MergeObjects(
			context.Background(),
			[]objectio.ObjectStats{},
			1024,
		)
	})
}

func TestTxnTableDelegate_UpdateConstraint(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.UpdateConstraint(
			context.Background(),
			&engine.ConstraintDef{},
		)
	})
}

func TestTxnTableDelegate_TableRenameInTxn(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.TableRenameInTxn(
			context.Background(),
			[][]byte{},
		)
	})
}

func TestTxnTableDelegate_MaxAndMinValues(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "not implemented", func() {
		table.MaxAndMinValues(context.Background())
	})
}

func TestTxnTableDelegate_Write(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot write data to partition primary table", func() {
		table.Write(context.Background(), &batch.Batch{})
	})
}

func TestTxnTableDelegate_Delete(t *testing.T) {
	table := &txnTableDelegate{}
	table.combined.is = true
	table.combined.tbl = newMockCombinedTxnTable()

	assert.PanicsWithValue(t, "BUG: cannot delete data to partition primary table", func() {
		table.Delete(context.Background(), &batch.Batch{}, "")
	})
}
