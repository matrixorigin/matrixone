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

package disttae

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/stretchr/testify/require"
)

func TestInternalAutoIncrementOwnsRelationNames(t *testing.T) {
	for _, names := range []struct{ db, table string }{
		{"db", "t"}, // inline Varlena storage
		{"database_name_longer_than_inline_varlena", "table_name_longer_than_inline_varlena"},
	} {
		for _, constant := range []bool{false, true} {
			layout := "flat"
			if constant {
				layout = "const"
			}
			t.Run(names.db+"/"+layout, func(t *testing.T) {
				eng := &Engine{}
				eng.catalog.Store(cache.NewCatalog())
				op, txn := newResetTxnForTest(t, eng)
				proc := txn.proc
				proc.Ctx = context.WithValue(defines.AttachAccountId(context.Background(), 7), defines.EngineKey{}, eng)
				proc.Base.TxnOperator = op
				eng.GetLatestCatalogCache().UpdateDuration(types.TS{}, types.MaxTs())

				// Use the real engine/catalog/transaction cache, not a mock that
				// copies names and accidentally hides the ownership violation.
				func() {
					packer := types.NewPacker()
					defer packer.Close()
					bat, err := catalog.GenCreateDatabaseTuple("", 7, 1, 1, names.db, 10, "", proc.Mp(), packer)
					require.NoError(t, err)
					defer bat.Clean(proc.Mp())
					_, err = fillRandomRowidAndZeroTs(bat, proc.Mp())
					require.NoError(t, err)
					eng.GetLatestCatalogCache().InsertDatabase(bat)
				}()
				insertCatalogTableForResetTest(t, eng, txn, 7, 10, 42, names.db, names.table)
				insertCatalogTableForResetTest(t, eng, txn, 7, 10, 43, names.db, "next_table")

				newInput := func(name string) *vector.Vector {
					v := vector.NewVec(types.T_varchar.ToType())
					t.Cleanup(func() { v.Free(proc.Mp()) })
					require.NoError(t, vector.AppendBytes(v, []byte(name), false, proc.Mp()))
					if constant {
						v.ToConst()
					}
					return v
				}
				dbv, tblv := newInput(names.db), newInput(names.table)
				out, err := function.RunFunctionDirectly(proc,
					function.EncodeOverloadID(function.INTERNAL_AUTO_INCREMENT, 0),
					[]*vector.Vector{dbv, tblv}, 1)
				require.NoError(t, err)
				defer out.Free(proc.Mp())
				// Tables without AUTO_INCREMENT also pass through the name/cache
				// path, returning SQL NULL rather than bypassing relation lookup.
				require.True(t, out.IsNull(0))
				key := genTableKey(7, names.table, 10, names.db)
				before, ok := txn.tableCache.Load(key)
				require.True(t, ok)

				// Deterministically simulate producer-buffer reuse. Keep storage
				// allocated: the test must diagnose corruption, not dereference
				// unmapped memory or wait for an allocator-dependent crash.
				copy(dbv.GetBytesAt(0), strings.Repeat("x", len(names.db)))
				copy(tblv.GetBytesAt(0), strings.Repeat("y", len(names.table)))
				txn.tableCache.Range(func(k, v any) bool {
					require.Equal(t, key, k, "cached names must survive source Vector reuse")
					require.Equal(t, names.db, v.(*txnTableDelegate).origin.db.databaseName)
					return true
				})
				after, ok := txn.tableCache.Load(key)
				require.True(t, ok)
				require.Same(t, before, after)

				db, err := eng.Database(proc.Ctx, names.db, op)
				require.NoError(t, err)
				rel, err := db.Relation(proc.Ctx, names.table, nil)
				require.NoError(t, err)
				require.Same(t, before, rel, "reuse must not create a second cached relation")
				next, err := db.Relation(proc.Ctx, "next_table", nil)
				require.NoError(t, err)
				require.Equal(t, uint64(43), next.GetTableID(proc.Ctx))
			})
		}
	}
}
