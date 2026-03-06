// Copyright 2025 Matrix Origin
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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestDataBranchOutputConfigAndQualifiedTableName(t *testing.T) {
	cfg := newDiffCSVUserConfig()
	require.NotNil(t, cfg)
	require.True(t, cfg.Outfile)
	require.NotNil(t, cfg.Fields)
	require.NotNil(t, cfg.Lines)
	require.Equal(t, tree.DefaultFieldsTerminated, cfg.Fields.Terminated.Value)
	require.Equal(t, tree.DefaultFieldsEnclosedBy[0], cfg.Fields.EnclosedBy.Value)
	require.Equal(t, tree.DefaultFieldsEscapedBy[0], cfg.Fields.EscapedBy.Value)
	require.Equal(t, "\n", cfg.Lines.TerminatedBy.Value)
	require.False(t, cfg.Header)

	require.Equal(t, "db.t", qualifiedTableName("db", "t"))
}

func TestDataBranchOutputMakeFileName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableName().Return("t1").AnyTimes()
	tarRel.EXPECT().GetTableName().Return("t2").AnyTimes()

	tblStuff := tableStuff{
		baseRel: baseRel,
		tarRel:  tarRel,
	}

	got := makeFileName(nil, nil, tblStuff)
	require.Regexp(t, regexp.MustCompile(`^diff_t2_t1_\d{8}_\d{6}$`), got)

	got = makeFileName(
		&tree.AtTimeStamp{SnapshotName: "sp1"},
		&tree.AtTimeStamp{SnapshotName: "sp2"},
		tblStuff,
	)
	require.Regexp(t, regexp.MustCompile(`^diff_t2_sp2_t1_sp1_\d{8}_\d{6}$`), got)
}

func TestDataBranchOutputBuildOutputSchema(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ses.SetMysqlResultSet(&MysqlResultSet{})

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableName().Return("t1").AnyTimes()
	tarRel.EXPECT().GetTableName().Return("t2").AnyTimes()

	tblStuff := tableStuff{
		baseRel: baseRel,
		tarRel:  tarRel,
	}
	tblStuff.def.colNames = []string{"id", "name"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.visibleIdxes = []int{0, 1}

	target := tree.NewTableName(tree.Identifier("t2"), tree.ObjectNamePrefix{}, nil)
	base := tree.NewTableName(tree.Identifier("t1"), tree.ObjectNamePrefix{}, nil)

	t.Run("default output", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   nil,
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))

		mrs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(4), mrs.GetColumnCount())
		col0, err := mrs.GetColumn(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, "diff t2 against t1", col0.Name())
		col1, err := mrs.GetColumn(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, "flag", col1.Name())
	})

	t.Run("summary output", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   &tree.DiffOutputOpt{Summary: true},
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))

		mrs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(3), mrs.GetColumnCount())
		col0, err := mrs.GetColumn(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, "metric", col0.Name())
		col1, err := mrs.GetColumn(ctx, 1)
		require.NoError(t, err)
		require.Contains(t, col1.Name(), "t2")
		col2, err := mrs.GetColumn(ctx, 2)
		require.NoError(t, err)
		require.Contains(t, col2.Name(), "t1")
	})

	t.Run("count output", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   &tree.DiffOutputOpt{Count: true},
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))

		mrs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(1), mrs.GetColumnCount())
		col0, err := mrs.GetColumn(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, "COUNT(*)", col0.Name())
	})

	t.Run("file output", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   &tree.DiffOutputOpt{DirPath: "/tmp"},
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))

		mrs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(2), mrs.GetColumnCount())
		col0, err := mrs.GetColumn(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, "FILE SAVED TO", col0.Name())
		col1, err := mrs.GetColumn(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, "HINT", col1.Name())
	})

	t.Run("unsupported output", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   &tree.DiffOutputOpt{},
		}
		err := buildOutputSchema(ctx, ses, stmt, tblStuff)
		require.Error(t, err)
	})
}

func TestDataBranchOutputWriteRowValues(t *testing.T) {
	tblStuff := tableStuff{}
	tblStuff.def.colNames = []string{"id", "name"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.visibleIdxes = []int{0, 1}
	tblStuff.def.pkColIdxes = []int{0}
	tblStuff.def.pkColIdx = 0

	row := []any{int64(7), "alice"}

	insertBuf := &bytes.Buffer{}
	require.NoError(t, writeInsertRowValues(nil, tblStuff, row, insertBuf))
	require.Equal(t, "(7,'alice')", insertBuf.String())

	deleteBuf := &bytes.Buffer{}
	require.NoError(t, writeDeleteRowValues(nil, tblStuff, row, deleteBuf))
	require.Equal(t, "7", deleteBuf.String())

	tblStuff.def.pkColIdxes = []int{0, 1}
	deleteTupleBuf := &bytes.Buffer{}
	require.NoError(t, writeDeleteRowValues(nil, tblStuff, row, deleteTupleBuf))
	require.Equal(t, "(7,'alice')", deleteTupleBuf.String())

	alwaysTupleBuf := &bytes.Buffer{}
	require.NoError(t, writeDeleteRowValuesAsTuple(nil, tblStuff, row, alwaysTupleBuf))
	require.Equal(t, "(7,'alice')", alwaysTupleBuf.String())
}

func TestDataBranchOutputWriteDeleteRowSQLFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "t1",
	}).AnyTimes()

	tblStuff := tableStuff{
		baseRel: baseRel,
	}
	tblStuff.def.colNames = []string{"id", "name"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.visibleIdxes = []int{0, 1}

	row := []any{int64(9), nil}
	buf := &bytes.Buffer{}
	require.NoError(t, writeDeleteRowSQLFull(context.Background(), nil, tblStuff, row, buf))
	require.Equal(t, "delete from db1.t1 where id = 9 and name is null limit 1;\n", buf.String())
}

func TestDataBranchOutputExecSQLStatementsWithWriteFile(t *testing.T) {
	var out bytes.Buffer
	writeFile := func(b []byte) error {
		_, err := out.Write(b)
		return err
	}

	err := execSQLStatements(
		context.Background(),
		nil,
		nil,
		writeFile,
		[]string{"select 1", "", "insert into t values (1)"},
	)
	require.NoError(t, err)
	require.Equal(t, "select 1;\ninsert into t values (1);\n", out.String())
}

func TestDataBranchOutputInitAndDropPKTablesWithWriteFile(t *testing.T) {
	pkInfo := &pkBatchInfo{
		dbName:       "db1",
		baseTable:    "base_t",
		deleteTable:  "__mo_diff_del_x",
		insertTable:  "__mo_diff_ins_x",
		pkNames:      []string{"id"},
		visibleNames: []string{"id", "name"},
	}

	var out bytes.Buffer
	writeFile := func(b []byte) error {
		_, err := out.Write(b)
		return err
	}

	require.NoError(t, initPKTables(context.Background(), nil, nil, pkInfo, writeFile))
	require.NoError(t, dropPKTables(context.Background(), nil, nil, pkInfo, writeFile))

	got := out.String()
	require.Contains(t, got, "drop table if exists db1.__mo_diff_del_x;\n")
	require.Contains(t, got, "drop table if exists db1.__mo_diff_ins_x;\n")
	require.Contains(t, got, "create table db1.__mo_diff_del_x as select id from db1.base_t where 1=0;\n")
	require.Contains(t, got, "create table db1.__mo_diff_ins_x as select id,name from db1.base_t where 1=0;\n")
}

func TestDataBranchOutputFlushSqlValuesWithWriteFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "t1",
	}).AnyTimes()

	tblStuff := tableStuff{
		baseRel: baseRel,
	}
	tblStuff.def.colNames = []string{"id", "name"}
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0, 1}

	pkInfo := &pkBatchInfo{
		dbName:       "db1",
		baseTable:    "t1",
		deleteTable:  "__mo_diff_del_x",
		insertTable:  "__mo_diff_ins_x",
		pkNames:      []string{"id", "name"},
		visibleNames: []string{"id", "name"},
	}

	var out bytes.Buffer
	writeFile := func(b []byte) error {
		_, err := out.Write(b)
		return err
	}

	require.NoError(t, flushSqlValues(
		context.Background(),
		nil,
		nil,
		tblStuff,
		bytes.NewBufferString("delete from db1.t1 where id = 1 limit 1;\n"),
		true,
		true,
		nil,
		writeFile,
	))

	require.NoError(t, flushSqlValues(
		context.Background(),
		nil,
		nil,
		tblStuff,
		bytes.NewBufferString("(1,'a')"),
		true,
		false,
		pkInfo,
		writeFile,
	))

	require.NoError(t, flushSqlValues(
		context.Background(),
		nil,
		nil,
		tblStuff,
		bytes.NewBufferString("(2,'b')"),
		false,
		false,
		nil,
		writeFile,
	))

	got := out.String()
	require.Contains(t, got, "delete from db1.t1 where id = 1 limit 1;\n")
	require.Contains(t, got, "insert into db1.__mo_diff_del_x values (1,'a');\n")
	require.Contains(t, got, "delete from db1.t1 where (id,name) in (select id,name from db1.__mo_diff_del_x);\n")
	require.Contains(t, got, "insert into db1.t1 values (2,'b');\n")
}

func TestDataBranchOutputTryFlushDeletesOrInserts(t *testing.T) {
	pkInfo := &pkBatchInfo{
		dbName:       "db1",
		baseTable:    "t1",
		deleteTable:  "__mo_diff_del_x",
		insertTable:  "__mo_diff_ins_x",
		pkNames:      []string{"id"},
		visibleNames: []string{"id", "name"},
	}

	t.Run("force flush both buffers", func(t *testing.T) {
		var out bytes.Buffer
		writeFile := func(b []byte) error {
			_, err := out.Write(b)
			return err
		}

		deleteCnt := 1
		insertCnt := 1
		deleteBuf := bytes.NewBufferString("(1)")
		insertBuf := bytes.NewBufferString("(1,'a')")

		err := tryFlushDeletesOrInserts(
			context.Background(),
			nil,
			nil,
			tableStuff{},
			"",
			0,
			0,
			false,
			pkInfo,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			writeFile,
		)
		require.NoError(t, err)
		require.Equal(t, 0, deleteCnt)
		require.Equal(t, 0, insertCnt)
		require.Equal(t, 0, deleteBuf.Len())
		require.Equal(t, 0, insertBuf.Len())
		require.Contains(t, out.String(), "delete from db1.t1 where id in (select id from db1.__mo_diff_del_x);\n")
		require.Contains(t, out.String(), "insert into db1.t1 (id,name) select id,name from db1.__mo_diff_ins_x;\n")
	})

	t.Run("do nothing when thresholds are not reached", func(t *testing.T) {
		var called int
		writeFile := func([]byte) error {
			called++
			return nil
		}

		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}

		err := tryFlushDeletesOrInserts(
			context.Background(),
			nil,
			nil,
			tableStuff{},
			diffDelete,
			1,
			1,
			false,
			pkInfo,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			writeFile,
		)
		require.NoError(t, err)
		require.Equal(t, 0, called)
	})

	t.Run("flush delete before insert on insert threshold", func(t *testing.T) {
		var out bytes.Buffer
		writeFile := func(b []byte) error {
			_, err := out.Write(b)
			return err
		}

		deleteCnt := 1
		insertCnt := maxSqlBatchCnt - 1
		deleteBuf := bytes.NewBufferString("(7)")
		insertBuf := bytes.NewBufferString("(8,'x')")

		err := tryFlushDeletesOrInserts(
			context.Background(),
			nil,
			nil,
			tableStuff{},
			diffInsert,
			1,
			1,
			false,
			pkInfo,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			writeFile,
		)
		require.NoError(t, err)

		got := out.String()
		deletePos := strings.Index(got, "insert into db1.__mo_diff_del_x")
		insertPos := strings.Index(got, "insert into db1.__mo_diff_ins_x")
		require.NotEqual(t, -1, deletePos)
		require.NotEqual(t, -1, insertPos)
		require.Less(t, deletePos, insertPos)
	})
}

func TestDataBranchOutputNewPKBatchInfo(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "t1",
	}).AnyTimes()
	baseRel.EXPECT().GetTableName().Return("t1").AnyTimes()

	tblStuff := tableStuff{
		baseRel: baseRel,
	}
	tblStuff.def.colNames = []string{"id", "name", "age"}
	tblStuff.def.pkColIdxes = []int{0, 2}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.pkKind = normalKind

	info := newPKBatchInfo(context.Background(), &Session{}, tblStuff)
	require.NotNil(t, info)
	require.Equal(t, "db1", info.dbName)
	require.Equal(t, "t1", info.baseTable)
	require.Equal(t, []string{"id", "age"}, info.pkNames)
	require.Equal(t, []string{"id", "name", "age"}, info.visibleNames)
	require.True(t, strings.HasPrefix(info.deleteTable, "__mo_diff_del_"))
	require.True(t, strings.HasPrefix(info.insertTable, "__mo_diff_ins_"))

	tblStuff.def.pkKind = fakeKind
	require.Nil(t, newPKBatchInfo(context.Background(), &Session{}, tblStuff))
}

func TestDataBranchOutputAppenderAppendRowAndFlushAll(t *testing.T) {
	pkInfo := &pkBatchInfo{
		dbName:       "db1",
		baseTable:    "t1",
		deleteTable:  "__mo_diff_del_x",
		insertTable:  "__mo_diff_ins_x",
		pkNames:      []string{"id"},
		visibleNames: []string{"id", "name"},
	}

	t.Run("append delete in full-row mode", func(t *testing.T) {
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}

		appender := sqlValuesAppender{
			ctx:             context.Background(),
			deleteByFullRow: true,
			pkInfo:          pkInfo,
			deleteCnt:       &deleteCnt,
			deleteBuf:       deleteBuf,
			insertCnt:       &insertCnt,
			insertBuf:       insertBuf,
		}

		err := appender.appendRow(diffDelete, []byte("delete from db1.t1 where id = 1 limit 1;\n"))
		require.NoError(t, err)
		require.Equal(t, 1, deleteCnt)
		require.Equal(t, "delete from db1.t1 where id = 1 limit 1;\n", deleteBuf.String())
	})

	t.Run("append insert with comma", func(t *testing.T) {
		deleteCnt := 0
		insertCnt := 1
		deleteBuf := &bytes.Buffer{}
		insertBuf := bytes.NewBufferString("(1,'a')")

		appender := sqlValuesAppender{
			ctx:             context.Background(),
			deleteByFullRow: false,
			pkInfo:          pkInfo,
			deleteCnt:       &deleteCnt,
			deleteBuf:       deleteBuf,
			insertCnt:       &insertCnt,
			insertBuf:       insertBuf,
		}

		err := appender.appendRow(diffInsert, []byte("(2,'b')"))
		require.NoError(t, err)
		require.Equal(t, 2, insertCnt)
		require.Equal(t, "(1,'a'),(2,'b')", insertBuf.String())
	})

	t.Run("flush all buffers", func(t *testing.T) {
		var out bytes.Buffer
		writeFile := func(b []byte) error {
			_, err := out.Write(b)
			return err
		}

		deleteCnt := 1
		insertCnt := 1
		deleteBuf := bytes.NewBufferString("(1)")
		insertBuf := bytes.NewBufferString("(1,'a')")
		appender := sqlValuesAppender{
			ctx:             context.Background(),
			deleteByFullRow: false,
			pkInfo:          pkInfo,
			deleteCnt:       &deleteCnt,
			deleteBuf:       deleteBuf,
			insertCnt:       &insertCnt,
			insertBuf:       insertBuf,
			writeFile:       writeFile,
		}

		require.NoError(t, appender.flushAll())
		require.Equal(t, 0, deleteCnt)
		require.Equal(t, 0, insertCnt)
		require.Equal(t, 0, deleteBuf.Len())
		require.Equal(t, 0, insertBuf.Len())
		require.Contains(t, out.String(), "insert into db1.__mo_diff_del_x values (1);")
		require.Contains(t, out.String(), "insert into db1.__mo_diff_ins_x values (1,'a');")
	})
}

func TestDataBranchOutputNewSingleWriteAppenderNilWorker(t *testing.T) {
	_, _, err := newSingleWriteAppender(context.Background(), nil, nil, "unused", nil)
	require.Error(t, err)
}

func TestDataBranchOutputNewSingleWriteAppenderSuccess(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "diff.sql")

	etlFS, targetPath, err := fileservice.GetForETL(ctx, nil, filePath)
	require.NoError(t, err)

	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()

	called := false
	writeFile, release, err := newSingleWriteAppender(ctx, pool, etlFS, targetPath, func() {
		called = true
	})
	require.NoError(t, err)

	require.NoError(t, writeFile([]byte("BEGIN;\n")))
	require.NoError(t, writeFile([]byte("COMMIT;\n")))
	release()
	require.False(t, called)

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, "BEGIN;\nCOMMIT;\n", string(content))
}

type failingWriteFS struct {
	fileservice.FileService
}

func (fs *failingWriteFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	return moerr.NewInternalErrorNoCtx("mock write failure")
}

func TestDataBranchOutputNewSingleWriteAppenderWriteFail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "diff.sql")

	etlFS, _, err := fileservice.GetForETL(ctx, nil, filePath)
	require.NoError(t, err)

	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()

	called := false
	writeFile, release, err := newSingleWriteAppender(
		ctx,
		pool,
		&failingWriteFS{FileService: etlFS},
		"diff.sql",
		func() { called = true },
	)
	require.NoError(t, err)

	_ = writeFile([]byte("SOME SQL;\n"))
	release()
	require.True(t, called)
}

func TestDataBranchOutputNewSingleWriteAppenderSubmitFail(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "diff.sql")

	etlFS, _, err := fileservice.GetForETL(ctx, nil, filePath)
	require.NoError(t, err)

	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	pool.Release()

	_, _, err = newSingleWriteAppender(ctx, pool, etlFS, "diff.sql", nil)
	require.Error(t, err)
}

func TestDataBranchOutputRemoveFileIgnoreError(t *testing.T) {
	ctx := context.Background()
	filePath := filepath.Join(t.TempDir(), "diff.sql")
	require.NoError(t, os.WriteFile(filePath, []byte("x"), 0o644))

	removeFileIgnoreError(ctx, "", filePath)

	_, err := os.Stat(filePath)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}
