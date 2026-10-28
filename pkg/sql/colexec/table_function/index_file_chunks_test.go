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

package table_function

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type indexFileRow struct {
	indexID string
	chunkID int64
	data    []byte
	tag     int64
}

var indexFileChunksCall = regexp.MustCompile(`index_file_chunks\('([0-9a-f]+)', (-?\d+), (-?\d+)\)`)

func newIndexFileChunksTF() *TableFunction {
	vc := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	i64 := types.T_int64.ToType()
	blob := types.T_blob.ToType()
	rets := []*plan.ColDef{
		{Name: catalog.IndexStorage_TblCol_Index_Id, Typ: plan.Type{Id: int32(vc.Oid), Width: vc.Width}},
		{Name: catalog.IndexStorage_TblCol_Chunk_Id, Typ: plan.Type{Id: int32(i64.Oid)}},
		{Name: catalog.IndexStorage_TblCol_Data, Typ: plan.Type{Id: int32(blob.Oid)}},
		{Name: catalog.IndexStorage_TblCol_Tag, Typ: plan.Type{Id: int32(i64.Oid)}},
	}
	tf := &TableFunction{
		Attrs:        []string{catalog.IndexStorage_TblCol_Index_Id, catalog.IndexStorage_TblCol_Chunk_Id, catalog.IndexStorage_TblCol_Data, catalog.IndexStorage_TblCol_Tag},
		Rets:         rets,
		FuncName:     indexfile.FuncName,
		OperatorBase: vm.OperatorBase{OperatorInfo: vm.OperatorInfo{}},
	}
	tf.ctr.retSchema = []types.Type{vc, i64, blob, i64}
	return tf
}

// runIndexFileChunks runs index_file_chunks(token, start, count) and returns its rows.
func runIndexFileChunks(proc *process.Process, token string, start, count int64) ([]indexFileRow, error) {
	tf := newIndexFileChunksTF()
	tf.Args = []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: token}}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: start}}}},
		{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: count}}}},
	}
	st, err := indexFileChunksPrepare(proc, tf)
	if err != nil {
		return nil, err
	}
	defer func() {
		st.free(tf, proc, false, nil)
		tf.ctr.cleanExecutors()
	}()
	for i := range tf.ctr.executorsForArgs {
		if tf.ctr.argVecs[i], err = tf.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil); err != nil {
			return nil, err
		}
	}
	if err := st.start(tf, proc, 0, nil); err != nil {
		return nil, err
	}
	var rows []indexFileRow
	for {
		res, err := st.call(tf, proc)
		if err != nil {
			return nil, err
		}
		if res.Batch.IsDone() {
			return rows, nil
		}
		bat := res.Batch
		if bat.RowCount() > indexFileChunksBatchRows {
			return nil, fmt.Errorf("batch of %d rows", bat.RowCount())
		}
		for i := 0; i < bat.RowCount(); i++ {
			rows = append(rows, indexFileRow{
				indexID: bat.Vecs[0].GetStringAt(i),
				chunkID: vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[1], i),
				data:    append([]byte(nil), bat.Vecs[2].GetBytesAt(i)...),
				tag:     vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[3], i),
			})
		}
	}
}

func indexFileTestEnv(t *testing.T, txnID string) (*indexfile.Env, *process.Process) {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte(txnID)}).AnyTimes()
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = op
	return &indexfile.Env{Proc: proc, Txn: op}, proc
}

func indexFileTestData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/indexfile.ChunkSize)
	}
	return b
}

func TestIndexFileChunksWriteRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		size  int
		first int64
		tag   int64
	}{
		{"base file over several statements", (indexfile.ChunksPerStatement+1)*indexfile.ChunkSize + 77, 0, 0},
		{"tail frame", 2*indexfile.ChunkSize + 5, 40, 1},
		{"one exact chunk", indexfile.ChunkSize, 0, 0},
		{"empty", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, proc := indexFileTestEnv(t, "txn-1")
			data := indexFileTestData(tc.size)
			ref := indexfile.Ref{DB: "db", Table: "idx", IndexID: "idx:1:0", Tag: tc.tag, FirstChunk: tc.first}

			var statements int
			assembled := make([]byte, 0, len(data))
			next := tc.first
			size, sum, err := indexfile.Write(context.Background(), env, ref, bytes.NewReader(data), func(sql string) error {
				statements++
				require.Contains(t, sql, "INSERT INTO `db`.`idx` (index_id, chunk_id, data, tag) SELECT index_id, chunk_id, data, tag FROM index_file_chunks(")
				m := indexFileChunksCall.FindStringSubmatch(sql)
				require.NotNil(t, m, sql)
				start, _ := strconv.ParseInt(m[2], 10, 64)
				count, _ := strconv.ParseInt(m[3], 10, 64)
				require.LessOrEqual(t, count, int64(indexfile.ChunksPerStatement))
				rows, err := runIndexFileChunks(proc, m[1], start, count)
				if err != nil {
					return err
				}
				require.Len(t, rows, int(count))
				for _, r := range rows {
					require.Equal(t, ref.IndexID, r.indexID)
					require.Equal(t, tc.tag, r.tag)
					require.Equal(t, next, r.chunkID)
					next++
					assembled = append(assembled, r.data...)
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, int64(len(data)), size)
			want := md5.Sum(data)
			require.Equal(t, hex.EncodeToString(want[:]), sum)
			require.Equal(t, data, assembled)
			require.Equal(t, (len(data)+indexfile.ChunkSize-1)/indexfile.ChunkSize, int(next-tc.first))
			require.Equal(t, (int(next-tc.first)+indexfile.ChunksPerStatement-1)/indexfile.ChunksPerStatement, statements)
		})
	}
}

func TestIndexFileChunksRejectsMisuse(t *testing.T) {
	env, proc := indexFileTestEnv(t, "txn-1")
	_, otherProc := indexFileTestEnv(t, "txn-2")
	data := indexFileTestData(3*indexfile.ChunkSize + 1)
	ref := indexfile.Ref{DB: "db", Table: "idx", IndexID: "i", FirstChunk: 10}

	var token string
	_, _, err := indexfile.Write(context.Background(), env, ref, bytes.NewReader(data), func(sql string) error {
		token = indexFileChunksCall.FindStringSubmatch(sql)[1]
		for _, bad := range [][2]int64{{9, 1}, {10, 5}, {13, 2}, {10, 0}, {10, -1}} {
			_, err := runIndexFileChunks(proc, token, bad[0], bad[1])
			require.Error(t, err, "chunks %v", bad)
		}
		_, err := runIndexFileChunks(otherProc, token, 10, 1)
		require.ErrorContains(t, err, "unknown source", "another transaction cannot read the source")
		_, err = runIndexFileChunks(proc, "00", 10, 1)
		require.ErrorContains(t, err, "unknown source")
		return nil
	})
	require.NoError(t, err)
	_, err = runIndexFileChunks(proc, token, 10, 1)
	require.ErrorContains(t, err, "unknown source", "the source is gone after Write returns")

	boom := fmt.Errorf("insert failed")
	_, _, err = indexfile.Write(context.Background(), env, ref, bytes.NewReader(data), func(string) error { return boom })
	require.ErrorIs(t, err, boom)
}

func indexFileChunksArgs(token *string, start, count any) []*plan.Expr {
	str := func(s *string) *plan.Expr {
		if s == nil {
			return &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
		}
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 64}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: *s}}}}
	}
	num := func(v any) *plan.Expr {
		switch x := v.(type) {
		case int32:
			return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: x}}}}
		case int64:
			return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: x}}}}
		case string:
			return &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 8}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: x}}}}
		}
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	}
	return []*plan.Expr{str(token), num(start), num(count)}
}

// drainIndexFileChunks runs the operator until it ends and returns the chunk ids.
func drainIndexFileChunks(t *testing.T, tf *TableFunction, proc *process.Process) ([]int64, error) {
	var ids []int64
	for {
		res, err := tf.Call(proc)
		if err != nil {
			return ids, err
		}
		if res.Batch == nil || res.Batch.IsDone() {
			return ids, nil
		}
		require.LessOrEqual(t, res.Batch.RowCount(), indexFileChunksBatchRows)
		ids = append(ids, vector.MustFixedColNoTypeCheck[int64](res.Batch.Vecs[1])...)
	}
}

func TestIndexFileChunksOperatorLifecycle(t *testing.T) {
	env, proc := indexFileTestEnv(t, "txn-1")
	data := indexFileTestData(100*indexfile.ChunkSize + 3)
	ref := indexfile.Ref{DB: "db", Table: "idx", IndexID: "i", FirstChunk: 5}
	_, _, err := indexfile.Write(context.Background(), env, ref, bytes.NewReader(data), func(sql string) error {
		token := indexFileChunksCall.FindStringSubmatch(sql)[1]

		tf := newIndexFileChunksTF()
		tf.Args = indexFileChunksArgs(&token, int32(10), int64(70))
		require.NoError(t, tf.Prepare(proc))
		ids, err := drainIndexFileChunks(t, tf, proc)
		require.NoError(t, err)
		require.Len(t, ids, 70)
		require.Equal(t, int64(10), ids[0])
		require.Equal(t, int64(79), ids[69])

		tf.Reset(proc, false, nil)
		ids, err = drainIndexFileChunks(t, tf, proc)
		require.NoError(t, err)
		require.Len(t, ids, 70, "a reset operator runs again")
		tf.Free(proc, false, nil)

		for name, args := range map[string][]*plan.Expr{
			"null token":      indexFileChunksArgs(nil, int64(5), int64(1)),
			"null start":      indexFileChunksArgs(&token, nil, int64(1)),
			"string count":    indexFileChunksArgs(&token, int64(5), "1"),
			"huge count":      indexFileChunksArgs(&token, int64(6), int64(math.MaxInt64)),
			"count past end":  indexFileChunksArgs(&token, int64(100), int64(10)),
			"wrong arguments": indexFileChunksArgs(&token, int64(5), int64(1))[:2],
		} {
			tf := newIndexFileChunksTF()
			tf.Args = args
			require.NoError(t, tf.Prepare(proc), name)
			_, err := drainIndexFileChunks(t, tf, proc)
			require.Error(t, err, name)
			tf.Free(proc, false, nil)
		}

		withInput := newIndexFileChunksTF()
		withInput.Args = indexFileChunksArgs(&token, int64(5), int64(1))
		withInput.AppendChild(newIndexFileChunksTF())
		st, err := indexFileChunksPrepare(proc, withInput)
		require.NoError(t, err)
		require.ErrorContains(t, st.start(withInput, proc, 0, nil), "cannot take an input")
		withInput.ctr.cleanExecutors()
		return nil
	})
	require.NoError(t, err)
}
