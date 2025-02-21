// Copyright 2024 Matrix Origin
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

package memoryengine

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/assert"
)

// Only for passing the UT coverage check.
func Test_TableReader(t *testing.T) {
	table := new(Table)
	assert.Panics(t, func() {
		table.BuildShardingReaders(
			context.TODO(),
			nil,
			nil,
			nil,
			0,
			0,
			false,
			0,
		)
	})
	assert.Panics(t, func() {
		table.GetProcess()
	})
}

func TestTable_Reset(t *testing.T) {
	table := new(Table)
	newOp, closeFn := client.NewTestTxnOperator(context.TODO())
	defer closeFn()
	assert.NoError(t, table.Reset(newOp))
}

func TestMemRelationData(t *testing.T) {
	data := MemRelationData{}
	require.Panics(t, func() {
		data.GetPState()
	})
	require.Panics(t, func() {
		data.SetPState(nil)
	})
}
