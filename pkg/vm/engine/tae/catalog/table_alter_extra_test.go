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

package catalog

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

func TestAlterTablePreservesSchemaExtra(t *testing.T) {
	cat := MockCatalog(nil)
	defer cat.Close()
	txnMgr := txnbase.NewTxnManager(MockTxnStoreFactory(cat), MockTxnFactory(cat), types.NewMockHLCClock(1))
	txnMgr.Start(t.Context())
	defer txnMgr.Stop()
	create, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	db, err := create.CreateDatabase("db", "", "")
	require.NoError(t, err)
	schema := MockSchema(2, 0)
	schema.FromPublication = true
	schema.Extra = &api.SchemaExtra{
		NextColSeqnum: 17, DroppedAttrs: []string{"dropped"}, ColumnChanged: true, OldName: "old",
		MinOsizeQuailifed: 2, MaxObjOnerun: 3, MaxOsizeMergedObj: 4, Hints: []api.MergeHint{1},
		MinCnMergeSize: 5, BlockMaxRows: 6, ObjectMaxBlocks: 7, FeatureFlag: features.SchemaEvolved,
		IndexTables: []uint64{89, 90}, ParentTableID: 88, FromPublication: true,
		AutoIncrOffset: 11, AutoIncrEpoch: 12, Checks: []*plan.CheckDef{{Name: "chk", OriginSql: "true"}},
		DefaultCharset: uint32(types.CharsetUTF8MB4Bin), AutoIdCache: 19, KeyFormat: 1, CollationVersion: 1,
	}
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, create.Commit(t.Context()))

	tbl := rel.(*mockTableHandle).entry
	change, err := txnMgr.StartTxn(nil)
	require.NoError(t, err)
	isNew, altered, err := tbl.AlterTable(t.Context(), change, api.NewUpdateCommentReq(tbl.GetDB().GetID(), tbl.GetID(), "new comment"))
	require.NoError(t, err)
	require.True(t, isNew)
	require.Equal(t, "new comment", altered.Comment)
	want := proto.Clone(schema.Extra).(*api.SchemaExtra)
	want.ColumnChanged = false
	want.DroppedAttrs = nil
	want.OldName = ""
	require.True(t, proto.Equal(want, altered.Extra), "want %s, got %s", want, altered.Extra)
	require.Nil(t, altered.Extra.DroppedAttrs)
	require.True(t, schema.Extra.ColumnChanged)
	require.Equal(t, []string{"dropped"}, schema.Extra.DroppedAttrs)
	require.NotSame(t, schema.Extra.Checks[0], altered.Extra.Checks[0])
	require.NoError(t, change.Commit(t.Context()))
}
