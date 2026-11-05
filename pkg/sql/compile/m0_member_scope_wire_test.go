//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// This tests the existing scope codec and receiver with explicitly constructed
// member scopes. It does not prove that the scheduler can freeze a workspace or
// construct these scopes for a real combined relation.
func TestM0MemberScopesCarrySeparateTombstones(t *testing.T) {
	ctrl := gomock.NewController(t)
	c := newMergeTopFallbackTestCompile(t)
	t.Cleanup(c.proc.Free)
	ts := timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}
	c.proc.Base.TxnClient = &m0WatermarkClient{result: ts}
	inner := mock_frontend.NewMockTxnOperator(ctrl)
	inner.EXPECT().GetWorkspace().Return(&disttae.Transaction{}).AnyTimes()
	inner.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	inner.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: ts}).AnyTimes()
	inner.EXPECT().SnapshotTS().Return(ts).AnyTimes()
	op := &m0StampedTxn{TxnOperator: inner}
	c.proc.Base.TxnOperator = op
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	c.e = eng
	eng.EXPECT().Database(gomock.Any(), "db", op).Return(db, nil).AnyTimes()

	first := types.RandomRowid()
	second := types.NewRowid(first.BorrowBlockID(), first.GetRowOffset()^1)
	rows := []types.Rowid{first, second}
	for i, name := range []string{"member_a", "member_b"} {
		id := uint64(101 + i)
		def := &plan.TableDef{TblId: id, Version: 1, Name: name, Cols: []*plan.ColDef{
			{ColId: 11, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int32)}},
		}}
		node := &plan.Node{ObjRef: &plan.ObjectRef{SchemaName: "db", ObjName: name, Obj: int64(id)}, TableDef: def, Stats: &plan.Stats{}}
		deletes := readutil.NewEmptyTombstoneData()
		require.NoError(t, deletes.AppendInMemory(rows[i]))
		deletes.SortInMemory()
		data := readutil.NewBlockListRelationData(0)
		require.NoError(t, data.AttachTombstones(deletes))
		sender := &Scope{Magic: Remote, NodeInfo: engine.Node{Mcpu: 1, CNCNT: 1, Data: data}, Proc: c.proc.NewNoContextChildProc(0), RootOp: table_scan.NewArgument(), DataSource: &Source{
			SchemaName: "db", RelationName: name, Attributes: []string{"a"}, TableDef: def, node: node, Timestamp: ts,
		}}
		t.Cleanup(sender.release)
		wire, err := encodeRemoteScope(sender, c.proc)
		require.NoError(t, err)
		received, err := decodeScope(wire, c.proc, true, nil)
		require.NoError(t, err)
		t.Cleanup(received.release)
		received.Proc.Ctx = c.proc.Ctx
		require.Equal(t, id, uint64(received.DataSource.node.ObjRef.Obj))
		rel := mock_frontend.NewMockRelation(ctrl)
		db.EXPECT().Relation(gomock.Any(), name, gomock.Any()).Return(rel, nil)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, engine.RangesParam) (engine.RelData, error) {
			return readutil.NewBlockListRelationData(0), nil
		})
		require.NoError(t, received.getRelData(c, nil))
		got := received.NodeInfo.Data.GetTombstones()
		require.NotNil(t, got)
		for j, row := range rows {
			left := got.ApplyInMemTombstones(row.BorrowBlockID(), []int64{int64(row.GetRowOffset())}, nil)
			if j == i {
				require.Empty(t, left, "member must apply its own deletion")
			} else {
				require.Len(t, left, 1, "member must not receive the other deletion")
			}
		}
	}
}
