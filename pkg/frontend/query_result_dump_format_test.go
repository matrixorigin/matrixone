// Copyright 2021-2026 Matrix Origin
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

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
)

// setupDumpTestProc wires the transaction compiler context the same way the
// production dump path expects, mirroring Test_saveQueryResultMeta: MODUMP
// resolves saved result files through the session's TxnCompilerContext.
func setupDumpTestProc(t *testing.T, ses *Session) *process.Process {
	proc := testutil.NewProcess(t)
	proc.Base.FileService = getPu("").FileService
	proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.TODO(),
		proc:   proc,
	}
	return proc
}

// saveIntResultForDump saves a 3-column int8 result (rows 0,1,2 per column)
// as a query result of the session and returns its query id.
func saveIntResultForDump(t *testing.T, ctx context.Context, ses *Session, proc *process.Process) string {
	typs := []types.Type{
		types.T_int8.ToType(),
		types.T_int8.ToType(),
		types.T_int8.ToType(),
	}
	colDefs := make([]*plan.ColDef, len(typs))
	for i, ty := range typs {
		colDefs[i] = &plan.ColDef{
			Name: fmt.Sprintf("a_%d", i),
			Typ:  plan.Type{Id: int32(ty.Oid), Scale: ty.Scale, Width: ty.Width},
		}
	}
	ses.rs = &plan.ResultColDef{ResultCols: colDefs}
	queryID := uuid.NullUUID{}.UUID.String()
	ses.tStmt = &motrace.StatementInfo{StatementID: uuid.NullUUID{}.UUID}
	asts, err := parsers.Parse(ctx, dialect.MYSQL, "select a,b,c from t", 1)
	require.NoError(t, err)
	ses.ast = asts[0]
	ses.p = &plan.Plan{}
	require.True(t, canSaveQueryResult(ctx, ses))
	// saveBatch writes synchronously and does not take ownership: the test
	// must release the batch's mpool allocations (see query_result_test.go).
	bat := newBatch(typs, 3, proc)
	defer bat.Clean(proc.Mp())
	require.NoError(t, saveBatch(ctx, ses, bat))
	require.NoError(t, saveMeta(ctx, ses))
	return queryID
}

func dumpModumpToPath(t *testing.T, ctx context.Context, ses *Session, queryID, path string) error {
	ep := &tree.ExportParam{
		Outfile:  true,
		QueryId:  queryID,
		FilePath: path,
		Fields: &tree.Fields{
			Terminated: &tree.Terminated{Value: ","},
			EnclosedBy: &tree.EnclosedBy{Value: '"'},
		},
		Lines:      &tree.Lines{TerminatedBy: &tree.Terminated{Value: "\n"}},
		Header:     false,
		ForceQuote: nil,
	}
	return doDumpQueryResult(ctx, ses, ep)
}

func readSharedExportFile(t *testing.T, ctx context.Context, path string) []byte {
	fs := getPu("").FileService
	var r io.ReadCloser
	err := fs.Read(ctx, &fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset:            0,
			Size:              -1,
			ReadCloserForRead: &r,
		}},
	})
	require.NoError(t, err)
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	return content
}

// TestDoDumpQueryResultHonorsFileSuffix covers issue #30570: MODUMP
// QUERY_RESULT must honor the output file suffix (.jsonl/.jsonline/.ndjson as
// JSON Lines, .parquet as Parquet) instead of always writing CSV, while
// unsuffixed and .csv outputs keep the legacy CSV bytes.
func TestDoDumpQueryResultHonorsFileSuffix(t *testing.T) {
	ioutil.RunPipelineTest(func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.proc.Base.SessionInfo.Account = sysAccountName
		ses.proc.Base.SessionInfo.Role = moAdminRoleName
		require.NoError(t, ses.SetSessionSysVar(context.TODO(), "save_query_result", int8(1)))
		ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, TenantID: sysAccountID})

		ctx := context.Background()
		proc := setupDumpTestProc(t, ses)
		queryID := saveIntResultForDump(t, ctx, ses, proc)

		expectedCSV := "0,0,0\n1,1,1\n2,2,2\n"

		t.Run("jsonl suffix writes JSON Lines", func(t *testing.T) {
			path := fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_a.jsonl")
			require.NoError(t, dumpModumpToPath(t, ctx, ses, queryID, path))
			content := readSharedExportFile(t, ctx, path)
			lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
			require.Len(t, lines, 3)
			for i, line := range lines {
				var row map[string]interface{}
				require.NoError(t, json.Unmarshal([]byte(line), &row), "line %d must be a JSON object", i)
				require.Equal(t, map[string]interface{}{
					"a_0": float64(i), "a_1": float64(i), "a_2": float64(i),
				}, row)
			}
		})

		t.Run("ndjson alias writes JSON Lines", func(t *testing.T) {
			path := fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_b.ndjson")
			require.NoError(t, dumpModumpToPath(t, ctx, ses, queryID, path))
			content := readSharedExportFile(t, ctx, path)
			var row map[string]interface{}
			require.NoError(t, json.Unmarshal([]byte(strings.SplitN(string(content), "\n", 2)[0]), &row))
			require.Equal(t, float64(0), row["a_0"])
		})

		t.Run("jsonline alias writes JSON Lines", func(t *testing.T) {
			path := fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_c.jsonline")
			require.NoError(t, dumpModumpToPath(t, ctx, ses, queryID, path))
			content := readSharedExportFile(t, ctx, path)
			var row map[string]interface{}
			require.NoError(t, json.Unmarshal([]byte(strings.SplitN(string(content), "\n", 2)[0]), &row))
			require.Equal(t, float64(0), row["a_2"])
		})

		t.Run("parquet suffix decodes to the dumped rowset", func(t *testing.T) {
			path := fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_d.parquet")
			require.NoError(t, dumpModumpToPath(t, ctx, ses, queryID, path))
			content := readSharedExportFile(t, ctx, path)
			file, err := parquet.OpenFile(bytes.NewReader(content), int64(len(content)))
			require.NoError(t, err, "dumped .parquet must be readable by a real Parquet reader")
			colPaths := file.Schema().Columns()
			require.Len(t, colPaths, 3)
			require.Equal(t, "a_0", colPaths[0][0])
			require.Equal(t, "a_1", colPaths[1][0])
			require.Equal(t, "a_2", colPaths[2][0])
			rows, err := parquet.Read[struct {
				A0 int32 `parquet:"a_0,optional"`
				A1 int32 `parquet:"a_1,optional"`
				A2 int32 `parquet:"a_2,optional"`
			}](bytes.NewReader(content), int64(len(content)))
			require.NoError(t, err)
			require.Len(t, rows, 3)
			for i, row := range rows {
				require.Equal(t, int32(i), row.A0)
				require.Equal(t, int32(i), row.A1)
				require.Equal(t, int32(i), row.A2)
			}
		})

		t.Run("csv suffix keeps legacy CSV bytes", func(t *testing.T) {
			path := fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_e.csv")
			require.NoError(t, dumpModumpToPath(t, ctx, ses, queryID, path))
			require.Equal(t, expectedCSV, string(readSharedExportFile(t, ctx, path)))
		})

		t.Run("unsuffixed path keeps legacy CSV bytes", func(t *testing.T) {
			path := fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_f")
			require.NoError(t, dumpModumpToPath(t, ctx, ses, queryID, path))
			require.Equal(t, expectedCSV, string(readSharedExportFile(t, ctx, path)))
		})
		// All dump paths must leave the read pool balanced: every product-path
		// allocation (read batches, export workers) is freed by the time
		// doDumpQueryResult returns; the test-owned result batch is released in
		// saveIntResultForDump.
		require.Zero(t, proc.Mp().Stats().NumCurrBytes.Load(),
			"dump paths must not leak mpool memory")
	})
}

// TestDoDumpQueryResultJsonlineRejectsInvalidUTF8: the jsonline path must
// surface the existing conversion error as the statement error instead of
// writing partial output silently, and the session must stay usable for the
// next dump.
func TestDoDumpQueryResultJsonlineRejectsInvalidUTF8(t *testing.T) {
	ioutil.RunPipelineTest(func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ses := newTestSession(t, ctrl)
		defer ses.Close()
		ses.proc.Base.SessionInfo.Account = sysAccountName
		ses.proc.Base.SessionInfo.Role = moAdminRoleName
		require.NoError(t, ses.SetSessionSysVar(context.TODO(), "save_query_result", int8(1)))
		ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, TenantID: sysAccountID})

		ctx := context.Background()
		proc := setupDumpTestProc(t, ses)

		ses.rs = &plan.ResultColDef{ResultCols: []*plan.ColDef{{
			Name: "v",
			Typ:  plan.Type{Id: int32(types.T_varchar.ToType().Oid)},
		}}}
		tid := uuid.NullUUID{}.UUID
		ses.tStmt = &motrace.StatementInfo{StatementID: tid}
		queryID := tid.String()
		asts, err := parsers.Parse(ctx, dialect.MYSQL, "select v from t", 1)
		assert.NoError(t, err)
		ses.ast = asts[0]
		ses.p = &plan.Plan{}
		require.True(t, canSaveQueryResult(ctx, ses))

		bat := batch.NewWithSize(1)
		vec, err := proc.AllocVectorOfRows(types.T_varchar.ToType(), 0, nil)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(vec, []byte("valid"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(vec, []byte{'x', 0xff, 'y'}, false, proc.Mp()))
		bat.Vecs[0] = vec
		bat.SetRowCount(2)
		require.NoError(t, saveBatch(ctx, ses, bat))
		// saveBatch does not take ownership; release the test-owned batch now
		// that it is persisted, so the pool-balance probe below is exact.
		bat.Clean(proc.Mp())
		require.NoError(t, saveMeta(ctx, ses))

		err = dumpModumpToPath(t, ctx, ses, queryID,
			fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_bad.jsonl"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid UTF-8")

		// The same session must still dump the saved int result afterwards.
		intID := saveIntResultForDump(t, ctx, ses, proc)
		require.NoError(t, dumpModumpToPath(t, ctx, ses, intID,
			fileservice.JoinPath(defines.SharedFileServiceName, "/dump_fmt_after_error.csv")))
		// The failed jsonline dump and the succeeding retry must also leave
		// the pool balanced: the owned batch is released above and product
		// paths free their allocations in deferred release/finish.
		require.Zero(t, proc.Mp().Stats().NumCurrBytes.Load(),
			"failed and retry dumps must not leak mpool memory")
	})
}
