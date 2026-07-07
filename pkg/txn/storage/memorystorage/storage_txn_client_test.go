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

package memorystorage

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/stretchr/testify/require"
)

func TestStorageTxnOperatorSnapshotTS(t *testing.T) {
	orig := timestamp.Timestamp{PhysicalTime: 1, LogicalTime: 2}
	op := &StorageTxnOperator{
		meta: txn.TxnMeta{
			SnapshotTS: orig,
		},
	}

	require.Equal(t, orig, op.SnapshotTS())

	updated := timestamp.Timestamp{PhysicalTime: 3, LogicalTime: 4}
	op.SetSnapshotTS(updated)
	require.Equal(t, updated, op.SnapshotTS())
	require.Equal(t, updated, op.Txn().SnapshotTS)
}
