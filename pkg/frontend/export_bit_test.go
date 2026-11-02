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

package frontend

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestResultSetCSVBitEncoding(t *testing.T) {
	for _, width := range []uint32{1, 7, 8, 9, 15, 16, 31, 32, 63, 64} {
		for _, value := range []uint64{0, 1, (uint64(1) << width) - 1} {
			t.Run(fmt.Sprintf("bit%d/%d", width, value), func(t *testing.T) {
				// LOAD DATA consumes BIT fields as big-endian raw bytes, not
				// decimal digits. Build the oracle independently of the exporter.
				var encoded [8]byte
				binary.BigEndian.PutUint64(encoded[:], value)
				expected := append([]byte(nil), encoded[8-int((width+7)/8):]...)
				expected = append(expected, '\n')
				require.Equal(t, expected, exportBitResultForTest(t, width, value))
			})
		}
	}
	t.Run("null", func(t *testing.T) {
		require.Equal(t, []byte("\\N\n"), exportBitResultForTest(t, 8, nil))
	})
}

func TestSavedQueryResultBitExport(t *testing.T) {
	ioutil.RunPipelineTest(func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.proc.Base.SessionInfo.Account = sysAccountName
		ses.proc.Base.SessionInfo.Role = moAdminRoleName
		require.NoError(t, ses.SetSessionSysVar(ctx, "save_query_result", int8(1)))
		proc := testutil.NewProcess(t)
		proc.Base.FileService = getPu("").FileService
		proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc}
		id := uuid.New()
		ses.SetStmtId(id)
		ses.tStmt = &motrace.StatementInfo{StatementID: id}
		ses.rs = &plan.ResultColDef{ResultCols: []*plan.ColDef{
			{Name: "b", Typ: plan.Type{Id: int32(types.T_bit), Width: 8}},
			{Name: "wide", Typ: plan.Type{Id: int32(types.T_bit), Width: 64}},
		}}
		statements, err := parsers.Parse(ctx, dialect.MYSQL, "select b, wide from t", 1)
		require.NoError(t, err)
		ses.ast, ses.p = statements[0], &plan.Plan{}
		require.True(t, canSaveQueryResult(ctx, ses))
		data := batch.NewWithSize(2)
		defer data.Clean(proc.Mp())
		for col, width := range []int32{8, 64} {
			data.Vecs[col] = vector.NewVec(types.New(types.T_bit, width, 0))
			for _, value := range []uint64{0, 1, 65, 255} {
				require.NoError(t, vector.AppendFixed(data.Vecs[col], value, false, proc.Mp()))
			}
			require.NoError(t, vector.AppendFixed(data.Vecs[col], uint64(0), true, proc.Mp()))
		}
		require.NoError(t, vector.SetFixedAtNoTypeCheck[uint64](data.Vecs[1], 3, ^uint64(0)))
		data.SetRowCount(5)
		require.NoError(t, saveBatch(ctx, ses, data))
		require.NoError(t, saveMeta(ctx, ses))
		path := fileservice.JoinPath(defines.SharedFileServiceName, "/bits.csv")
		ep := &tree.ExportParam{
			Outfile: true, QueryId: id.String(), FilePath: path,
			Fields: &tree.Fields{
				Terminated: &tree.Terminated{Value: ","},
				EnclosedBy: &tree.EnclosedBy{},
			},
			Lines: &tree.Lines{TerminatedBy: &tree.Terminated{Value: "\n"}},
		}
		require.NoError(t, doDumpQueryResult(ctx, ses, ep))
		var reader io.ReadCloser
		require.NoError(t, getPu("").FileService.Read(ctx, &fileservice.IOVector{
			FilePath: path,
			Entries:  []fileservice.IOEntry{{Offset: 0, Size: -1, ReadCloserForRead: &reader}},
		}))
		defer reader.Close()
		got, err := io.ReadAll(reader)
		require.NoError(t, err)
		expected := make([]byte, 0, 50)
		for row, small := range []byte{0, 1, 65, 255} {
			wide := uint64(small)
			if row == 3 {
				wide = ^uint64(0)
			}
			var encoded [8]byte
			binary.BigEndian.PutUint64(encoded[:], wide)
			expected = append(expected, small, ',')
			expected = append(expected, encoded[:]...)
			expected = append(expected, '\n')
		}
		expected = append(expected, []byte("\\N,\\N\n")...)
		require.Equal(t, expected, got)
	})
}

func exportBitResultForTest(t *testing.T, width uint32, value any) []byte {
	t.Helper()
	column := &MysqlColumn{}
	column.SetColumnType(defines.MYSQL_TYPE_BIT)
	column.SetLength(width)
	mrs := &MysqlResultSet{}
	mrs.AddColumn(column)
	mrs.AddRow([]any{value})
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	type readResult struct {
		data []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(reader)
		done <- readResult{data, err}
	}()
	ep := &ExportConfig{
		ctx: context.Background(), mrs: mrs, AsyncWriter: writer,
		userConfig: &tree.ExportParam{Fields: &tree.Fields{EnclosedBy: &tree.EnclosedBy{}}},
		Symbol:     [][]byte{[]byte("\n")}, ColumnFlag: []bool{false},
	}
	err := exportDataFromResultSetToCSVFile(ep)
	require.NoError(t, writer.CloseWithError(err))
	result := <-done
	require.NoError(t, err)
	require.NoError(t, result.err)
	require.Equal(t, uint64(1), ep.Rows)
	return result.data
}
