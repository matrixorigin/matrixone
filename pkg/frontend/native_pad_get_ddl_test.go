// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNativePADGetDDLRejectsBeforeExportPreparation(t *testing.T) {
	for _, kind := range []string{"native_column", "native_default", "legacy"} {
		t.Run(kind, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctx := defines.AttachAccountId(t.Context(), 42)
			eng, db, rel := mock_frontend.NewMockEngine(ctrl), mock_frontend.NewMockDatabase(ctrl), mock_frontend.NewMockRelation(ctrl)
			txn := mock_frontend.NewMockTxnOperator(ctrl)
			source := &pbplan.TableDef{
				Name: "n", DbName: "db", TableType: catalog.SystemOrdinaryRel,
				DefaultCharset: uint32(types.CharsetUTF8MB4Bin),
				Cols:           []*pbplan.ColDef{{Name: "s", Typ: pbplan.Type{Id: int32(types.T_varchar), Width: 8, Charset: uint32(types.CharsetUTF8MB4Bin)}, Default: &pbplan.Default{NullAbility: true}}},
			}
			if kind == "native_column" {
				source.Cols[0].Typ.CollationVersion = 1
			} else if kind == "native_default" {
				source.CollationVersion = 1
			}
			before := proto.Clone(source)
			ownedCopy := proto.Clone(source).(*pbplan.TableDef)
			eng.EXPECT().Database(ctx, "db", txn).Return(db, nil)
			db.EXPECT().Relation(ctx, "n", nil).Return(rel, nil)
			rel.EXPECT().GetTableDef(ctx).Return(source)
			rel.EXPECT().CopyTableDef(ctx).Return(ownedCopy)
			preparations := 0
			rel.EXPECT().GetTableID(ctx).DoAndReturn(func(context.Context) uint64 {
				preparations++
				return 17
			}).AnyTimes()
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
			defer func() {
				bat.Clean(mp)
				require.Zero(t, mp.CurrNB())
			}()
			for i, oid := range []types.T{types.T_varchar, types.T_varchar, types.T_int64, types.T_varchar} {
				bat.Vecs[i] = vector.NewVec(oid.ToType())
			}
			err := visitTableDdl(ctx, "db", "n", bat, txn, eng, mp)
			if kind == "legacy" {
				require.NoError(t, err)
				require.Equal(t, 1, preparations)
				require.Equal(t, 1, bat.RowCount())
				require.Contains(t, string(bat.Vecs[3].GetBytesAt(0)), "COLLATE=utf32_bin")
			} else {
				require.ErrorContains(t, err, "versioned collations")
				require.Zero(t, preparations)
				require.Zero(t, bat.RowCount())
				for _, vec := range bat.Vecs {
					require.Zero(t, vec.Length())
				}
				require.Equal(t, before, ownedCopy)
			}
			require.Equal(t, before, source)
		})
	}
}
