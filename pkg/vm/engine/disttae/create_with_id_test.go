// Copyright 2022 Matrix Origin
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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCreateWithIDPreservesColumnSequences(t *testing.T) {
	for _, tc := range []struct {
		name       string
		next, want uint32
	}{{"retain high-water mark", 31, 31}, {"advance over largest sequence", 3, 10}} {
		t.Run(tc.name, func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			defer txn.closeWorkspaceAllocation()
			defer func() {
				for _, entry := range txn.writes {
					entry.bat.Clean(txn.workspaceMP())
				}
			}()

			op := txn.op.(*mock_frontend.MockTxnOperator)
			op.EXPECT().IsSnapOp().Return(false).AnyTimes()
			txn.tnStores = []DNStore{{}}
			txn.tableCache = &sync.Map{}
			packer := types.NewPacker()
			defer packer.Close()
			txn.engine.packerPool = fileservice.NewPool(1, func() *types.Packer { return packer }, (*types.Packer).Reset, nil)
			// The catalog index INSERT resolves its PK through the workspace.
			catalogDB := &txnDatabase{op: op, databaseId: catalog.MO_CATALOG_ID, databaseName: catalog.MO_CATALOG}
			txn.tableCache.Store(genTableKey(catalog.System_Account, catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME,
				catalog.MO_CATALOG_ID, catalog.MO_CATALOG), &txnTableDelegate{origin: &txnTable{db: catalogDB, tableId: catalog.MO_TABLES_LOGICAL_ID_INDEX_ID}})
			db := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
			ctx := defines.AttachAccountId(t.Context(), 1)
			typ := types.T_int64.ToType()
			defs := []engine.TableDef{
				&engine.AttributeDef{Attr: engine.Attribute{Name: "payload", Seqnum: 9, Type: typ}},
				&engine.AttributeDef{Attr: engine.Attribute{Name: "pk", Seqnum: 3, Type: typ, Primary: true}},
				&engine.AttributeDef{Attr: engine.Attribute{Name: "original", Seqnum: 0, Type: typ}},
			}
			extra := &api.SchemaExtra{NextColSeqnum: tc.next}
			require.NoError(t, db.createWithID(ctx, "evolved", 45, 45, false, defs, true, extra, nil))
			recreated := txn.tableOps.existAndActive(genTableKey(1, "evolved", 7, "db"))
			require.NotNil(t, recreated)
			require.Equal(t, tc.want, recreated.extraInfo.NextColSeqnum)
			require.Equal(t, 1, recreated.primaryIdx)
			require.Equal(t, 3, recreated.primarySeqnum)
			require.Equal(t, uint32(9), recreated.tableDef.Cols[0].Seqnum)
			require.Equal(t, uint32(0), recreated.tableDef.Cols[2].Seqnum)
			written := txn.writes[1].bat
			seq := vector.MustFixedColWithTypeCheck[uint16](written.Vecs[1+catalog.MO_COLUMNS_ATT_SEQNUM_IDX])
			require.Equal(t, []uint16{9, 3, 0, objectio.SEQNUM_ROWID}, seq)
			require.Equal(t, tc.want, api.MustUnmarshalTblExtra(txn.writes[0].bat.Vecs[1+catalog.MO_TABLES_EXTRA_INFO_IDX].GetBytesAt(0)).NextColSeqnum)
		})
	}
}
