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

package iscp

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/stretchr/testify/require"
)

// newTestCuvsIgnoredTableDef is a cuvs index on a vecf32(2) column with the given quantization
// and limit, and an ignored table.
func newTestCuvsIgnoredTableDef(quantization string, maxBadRecords int) *plan.TableDef {
	td := newTestCuvsTableDef("pk", "v", 2)
	params := fmt.Sprintf(`{"op_type":"vector_l2_ops","quantization":"%s","max_bad_records":"%d"}`, quantization, maxBadRecords)
	for _, idx := range td.Indexes {
		idx.IndexAlgoParams = params
	}
	td.Indexes = append(td.Indexes, &plan.IndexDef{
		IndexName:          "cuvs_idx",
		TableExist:         true,
		IndexAlgo:          catalog.MoIndexCagraAlgo.ToString(),
		IndexAlgoTableType: catalog.IndexIgnored_TblType,
		IndexTableName:     "ignored_tbl",
		Parts:              []string{"v"},
		IndexAlgoParams:    params,
	})
	return td
}

// decodeCuvsRecords returns the op and key of each event record a writer produced.
func decodeCuvsRecords(t *testing.T, w *CuvsCdcWriter, out []byte) []string {
	var got []string
	for len(out) > 0 {
		rec, n, ok := cuvscdc.DecodeEventRecord(out, w.vecBytesPer, w.includeBytesPer)
		require.True(t, ok)
		got = append(got, fmt.Sprintf("%d:%d", rec.Op, rec.Pkid))
		out = out[n:]
	}
	return got
}

func TestCuvsWriterIgnoresUnstorableVectors(t *testing.T) {
	ctx := context.Background()
	td := newTestCuvsIgnoredTableDef("float16", 10)
	w, err := NewCuvsCdcWriter("cagra", "test_db", "test_tbl", "cuvs_idx", td, td.Indexes)
	require.NoError(t, err)
	require.True(t, w.toFloat16)
	ignored, err := newIgnoredRows("test_db", td, td.Indexes)
	require.NoError(t, err)
	require.NotNil(t, ignored)
	// the table holds rows 4 and 5
	ignored.stored = true
	var rw IndexSqlWriter = w
	rw.(ignoredRowWriter).setIgnored(ignored)
	ignored.setTs(types.BuildTS(7, 0))

	require.NoError(t, w.Insert(ctx, []any{int64(1), []float32{1, 1}}))
	// float16 cannot hold 100000
	require.NoError(t, w.Insert(ctx, []any{int64(2), []float32{100000, 1}}))
	// an update to a value that cannot be stored removes the indexed version
	require.NoError(t, w.Upsert(ctx, []any{int64(3), []float32{1, 70000}}))
	// an L2 distance of a vector this large overflows float32
	require.NoError(t, w.Insert(ctx, []any{int64(6), []float32{1e20, 1}}))
	// an ignored row that is updated to a storable value or deleted leaves the table
	require.NoError(t, w.Upsert(ctx, []any{int64(4), []float32{2, 2}}))
	require.NoError(t, w.Delete(ctx, []any{int64(5)}))

	out, err := w.ToSql()
	require.NoError(t, err)
	require.Equal(t, []string{
		fmt.Sprintf("%d:1", cuvscdc.CdcOpInsert),
		fmt.Sprintf("%d:3", cuvscdc.CdcOpDelete),
		fmt.Sprintf("%d:4", cuvscdc.CdcOpUpsert),
		fmt.Sprintf("%d:5", cuvscdc.CdcOpDelete),
	}, decodeCuvsRecords(t, w, out))

	sqls, _ := captureIgnoredSql(t)
	require.NoError(t, ignored.flush(ctx, "", nil, "cuvs_idx"))
	require.Equal(t, []string{
		"DELETE FROM `test_db`.`ignored_tbl` WHERE `__mo_index_pk` IN (1,4,5)",
		fmt.Sprintf("REPLACE INTO `test_db`.`ignored_tbl` (`__mo_index_pk`, `__mo_index_code`, `__mo_index_reason`, `__mo_index_ts`) VALUES (2, %d, NULL, 7), (3, %d, NULL, 7), (6, %d, NULL, 7)",
			moerr.ErrIndexVectorOutOfRange, moerr.ErrIndexVectorOutOfRange, moerr.ErrIndexVectorOutOfRange),
		"SELECT COUNT(*) FROM `test_db`.`ignored_tbl`",
	}, *sqls)
}

// Without float16 storage a vecf32 element above the float16 range is stored; a vecf16 base
// is never narrowed.
func TestCuvsWriterFloat16OnlyNarrowsVecf32(t *testing.T) {
	ctx := context.Background()
	for _, q := range []string{"", "float32", "int8", "uint8"} {
		td := newTestCuvsIgnoredTableDef(q, 0)
		w, err := NewCuvsCdcWriter("ivfpq", "db", "tbl", "cuvs_idx", td, td.Indexes)
		require.NoError(t, err, q)
		require.False(t, w.toFloat16, q)
		ignored, err := newIgnoredRows("db", td, td.Indexes)
		require.NoError(t, err)
		w.setIgnored(ignored)
		require.NoError(t, w.Insert(ctx, []any{int64(1), []float32{100000, 1}}), q)
		require.Zero(t, ignored.count, q)
	}

	td := newTestCuvsIgnoredTableDef("float16", 0)
	td.Cols[1].Typ.Id = int32(types.T_array_float16)
	w, err := NewCuvsCdcWriter("ivfpq", "db", "tbl", "cuvs_idx", td, td.Indexes)
	require.NoError(t, err)
	require.False(t, w.toFloat16)
	ignored, err := newIgnoredRows("db", td, td.Indexes)
	require.NoError(t, err)
	w.setIgnored(ignored)
	require.NoError(t, w.Insert(ctx, []any{int64(1), []types.Float16{types.Float16FromFloat32(65504), 0}}))
	require.Zero(t, ignored.count)
}

// An index without an ignored table keeps writing every vector.
func TestCuvsWriterWithoutIgnoredTable(t *testing.T) {
	ctx := context.Background()
	td := newTestCuvsTableDef("pk", "v", 2)
	td.Indexes[0].IndexAlgoParams = `{"quantization":"float16"}`
	w, err := NewCuvsCdcWriter("cagra", "db", "tbl", "cuvs_idx", td, td.Indexes)
	require.NoError(t, err)
	ignored, err := newIgnoredRows("db", td, td.Indexes)
	require.NoError(t, err)
	require.Nil(t, ignored)
	require.NoError(t, w.Insert(ctx, []any{int64(2), []float32{100000, 1}}))
	out, err := w.ToSql()
	require.NoError(t, err)
	require.Equal(t, []string{fmt.Sprintf("%d:2", cuvscdc.CdcOpInsert)}, decodeCuvsRecords(t, w, out))
}
