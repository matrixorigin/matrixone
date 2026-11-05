// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSnapshotTxnMatchesEachScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	base := mock_frontend.NewMockTxnOperator(ctrl)
	first := mock_frontend.NewMockTxnOperator(ctrl)
	second := mock_frontend.NewMockTxnOperator(ctrl)
	logical := mock_frontend.NewMockTxnOperator(ctrl)
	a := timestamp.Timestamp{PhysicalTime: 10}
	b := timestamp.Timestamp{PhysicalTime: 20}
	c := timestamp.Timestamp{PhysicalTime: 10, LogicalTime: 1}
	first.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: a}).AnyTimes()
	base.EXPECT().CloneSnapshotOp(a).Return(first).Times(1)
	base.EXPECT().CloneSnapshotOp(b).Return(second).Times(1)
	base.EXPECT().CloneSnapshotOp(c).Return(logical).Times(1)
	proc := &process.Process{Base: &process.BaseProcess{TxnOperator: base}}
	require.Same(t, first, txnOperatorForSnapshot(proc, a))
	require.Same(t, second, txnOperatorForSnapshot(proc, b))
	require.Same(t, logical, txnOperatorForSnapshot(proc, c))
	require.Same(t, first, txnOperatorForSnapshot(proc, a))
	require.Same(t, first, proc.GetCloneTxnOperator(), "do not retarget a snapshot already held by another relation")
}
