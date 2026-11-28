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

package txnimpl

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

// A DECIMAL(p > 38) primary key is a decimal256 vector. The in-memory table
// index used to panic on it ("type not supported").
func TestSimpleTableIndexDecimal256Key(t *testing.T) {
	keys := containers.MockVector2(types.New(types.T_decimal256, 50, 0), 100, 0)
	defer containers.ReleaseVector(keys)

	idx := NewSimpleTableIndex()
	require.NoError(t, idx.BatchDedup("pk", keys))
	require.NoError(t, idx.BatchInsert("pk", keys, 0, keys.Length(), 0, false))

	dup, err := keys.Window(40, 43)
	require.NoError(t, err)
	require.Error(t, idx.BatchDedup("pk", dup), "keys already in the index are duplicates")
	err = idx.BatchInsert("pk", keys, 10, 1, 0, false)
	require.Error(t, err, "inserting a key already in the index fails")
	require.Contains(t, err.Error(), "10", "the duplicate key is named")

	fresh := containers.MockVector2(types.New(types.T_decimal256, 50, 0), 5, 1000)
	defer containers.ReleaseVector(fresh)
	require.NoError(t, idx.BatchDedup("pk", fresh))
}
