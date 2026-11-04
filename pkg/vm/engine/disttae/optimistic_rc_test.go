// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/stretchr/testify/require"
)

func TestOptimisticRCWriteDependencies(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		t.Run(map[bool]string{false: "data", true: "tombstone"}[tombstone], func(t *testing.T) {
			tbl, eng, fs, keys := newPersistedPKCheckTable(t, types.T_int64, tombstone)
			mp := tbl.proc.Load().Mp()
			keys.Attrs = []string{"pk"}
			for _, typ := range []int{INSERT, DELETE} {
				e := Entry{typ: typ, tableId: tbl.tableId, bat: keys}
				if typ == DELETE {
					e.bat = &batch.Batch{Attrs: []string{"rowid", "pk"}, Vecs: []*vector.Vector{nil, keys.Vecs[0]}}
				}
				require.Error(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0)))
				require.NoError(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)))
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				require.ErrorIs(t, validateOptimisticWriteEntry(ctx, tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0)), context.Canceled)
			}
			// A CN spill has no commit-TS column. Validate its PK column one block
			// at a time against the same committed history as the in-memory entry.
			writer := ioutil.ConstructWriter(0, []uint16{0}, 0, true, false, fs)
			_, err := writer.WriteBatch(keys)
			require.NoError(t, err)
			_, _, err = writer.Sync(t.Context())
			require.NoError(t, err)
			metadata := batch.NewWithSize(2)
			defer metadata.Clean(mp)
			metadata.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			metadata.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
			stats := writer.GetObjectStats()
			require.NoError(t, vector.AppendBytes(metadata.Vecs[1], stats.Marshal(), false, mp))
			metadata.SetRowCount(1)
			e := Entry{typ: INSERT, tableId: tbl.tableId, bat: metadata, fileName: "spill"}
			err = validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(10, 0), types.BuildTS(50, 0))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict))
			require.NoError(t, validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(50, 0), types.BuildTS(60, 0)))

			part := eng.GetOrCreateLatestPart(t.Context(), uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId)
			state, done := part.MutateState()
			state.UpdateDuration(types.BuildTS(70, 0), types.MaxTs())
			done()
			err = validateOptimisticWriteEntry(t.Context(), tbl, e, types.BuildTS(60, 0), types.BuildTS(80, 0))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "truncated dependency history must fail closed")
		})
	}
}
