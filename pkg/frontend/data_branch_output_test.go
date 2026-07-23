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
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pbtimestamp "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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

	require.Equal(t, "`db`.`t`", qualifiedTableName("db", "t"))
	require.Equal(t, "`d``b`.`t``1`", qualifiedTableName("d`b", "t`1"))
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

func TestDataBranchOutputFileNameAndHintQuotePathSeparators(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	ctrl := gomock.NewController(t)
	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableName().Return("base/name`quoted").AnyTimes()
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db/name`quoted",
		Name:   "base/name`quoted",
	}).AnyTimes()
	tarRel.EXPECT().GetTableName().Return(`child\name:quoted`).AnyTimes()

	outputDir := t.TempDir()
	stmt := &tree.DataBranchDiff{
		BaseTable: tree.TableName{
			AtTsExpr: &tree.AtTimeStamp{SnapshotName: "base/snapshot 1"},
		},
		TargetTable: tree.TableName{
			AtTsExpr: &tree.AtTimeStamp{SnapshotName: `target\snapshot%1`},
		},
		OutputOpt: &tree.DiffOutputOpt{DirPath: outputDir},
	}
	tblStuff := tableStuff{baseRel: baseRel, tarRel: tarRel}

	filePath, hint, _, release, cleanup, err := prepareFSForDiffAsFile(ctx, ses, stmt, tblStuff)
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NotNil(t, cleanup)
	t.Cleanup(release)
	t.Cleanup(cleanup)

	require.Equal(t, outputDir, filepath.Dir(filePath))
	require.Regexp(t, regexp.MustCompile(
		`^diff_child@5Cname@3Aquoted_target@5Csnapshot@251_base@2Fname@60quoted_base@2Fsnapshot@201_\d{8}_\d{6}\.sql$`,
	), filepath.Base(filePath))
	require.Equal(t,
		"DELETE FROM `db/name``quoted`.`base/name``quoted`, INSERT INTO `db/name``quoted`.`base/name``quoted`",
		hint,
	)
}

func TestDataBranchOutputFileNameSurvivesCSVFormattingAndLengthLimit(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("valid and invalid UTF-8", func(t *testing.T) {
		require.Equal(t, "a�b", encodeDiffFileNamePart("a�b"))
		require.Equal(t, "a@FFb", encodeDiffFileNamePart(string([]byte{'a', 0xff, 'b'})))
	})

	t.Run("CSV format string", func(t *testing.T) {
		for _, name := range []string{"Ād", "x%d", "x d", "x`d"} {
			t.Run(name, func(t *testing.T) {
				baseRel := mock_frontend.NewMockRelation(ctrl)
				tarRel := mock_frontend.NewMockRelation(ctrl)
				baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
				tarRel.EXPECT().GetTableName().Return(name).AnyTimes()

				fileName := makeFileName(nil, nil, tableStuff{baseRel: baseRel, tarRel: tarRel}) + ".csv"
				require.Equal(t, fileName, getExportFilePath(fileName, 0))
			})
		}
	})

	t.Run("long Unicode components", func(t *testing.T) {
		ctx := context.Background()
		ses := newValidateSession(t)
		longName := strings.Repeat("中", 100)

		baseRel := mock_frontend.NewMockRelation(ctrl)
		tarRel := mock_frontend.NewMockRelation(ctrl)
		baseRel.EXPECT().GetTableName().Return(longName).AnyTimes()
		baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
			DbName: "unicode_db",
			Name:   longName,
		}).AnyTimes()
		tarRel.EXPECT().GetTableName().Return(longName).AnyTimes()

		outputDir := t.TempDir()
		stmt := &tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{DirPath: outputDir}}
		filePath, _, _, release, cleanup, err := prepareFSForDiffAsFile(
			ctx, ses, stmt, tableStuff{baseRel: baseRel, tarRel: tarRel},
		)
		require.NoError(t, err)
		require.NotNil(t, release)
		require.NotNil(t, cleanup)
		t.Cleanup(release)
		t.Cleanup(cleanup)

		baseName := filepath.Base(filePath)
		require.LessOrEqual(t, len(baseName), maxDiffFileNameStemBytes+len(".sql"))
		require.True(t, utf8.ValidString(baseName))
		require.Regexp(t, regexp.MustCompile(`_[0-9a-f]{32}_\d{8}_\d{6}\.sql$`), baseName)
		require.Equal(t, outputDir, filepath.Dir(filePath))
	})
}

func TestDataBranchOutputTableSpec(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "base_db",
		Name:   "base_t",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
			{Name: "name", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: "__mo_diff_source", ColId: 3, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: "__mo_diff_flag", ColId: 4, Seqnum: 3, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
		},
	}).AnyTimes()
	tarRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "target_db",
		Name:   "target_t",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
			{Name: "name", ColId: 2, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: "__mo_diff_source", ColId: 3, Seqnum: 2, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
			{Name: "__mo_diff_flag", ColId: 4, Seqnum: 3, Typ: plan.Type{Id: int32(types.T_varchar), Width: 20}},
		},
	}).AnyTimes()

	tblStuff := tableStuff{baseRel: baseRel, tarRel: tarRel}
	tblStuff.def.colNames = []string{"id", "name", "__mo_diff_source", "__mo_diff_flag"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1, 2, 3}

	outName := tree.NewTableName(
		tree.Identifier("diff_out"),
		tree.ObjectNamePrefix{SchemaName: tree.Identifier("out_db"), ExplicitSchema: true},
		nil,
	)

	t.Run("all columns retain source names and avoid metadata collisions", func(t *testing.T) {
		output, err := newDiffOutputTable(ctx, ses, &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{As: *outName},
		}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, "out_db", output.databaseName)
		require.Equal(t, "diff_out", output.tableName)
		require.Equal(t, []string{
			"__mo_diff_source_1", "__mo_diff_flag_1",
			"id", "name", "__mo_diff_source", "__mo_diff_flag",
		}, output.columnNames)
		require.Equal(t, []int{0, 1, 2, 3}, output.projectedIdxes)

		sql, err := output.createSQL(ctx, tblStuff)
		require.NoError(t, err)
		require.Contains(t, sql, "create table `out_db`.`diff_out` (")
		require.Contains(t, sql, "`__mo_diff_source_1` varchar(255) default null")
		require.Contains(t, sql, "`__mo_diff_flag_1` varchar(16) default null")
		require.Contains(t, sql, "`id` BIGINT not null")
		require.NotContains(t, sql, " as select ")
	})

	t.Run("base snapshot supplies the materialized schema", func(t *testing.T) {
		snapshotTS := pbtimestamp.Timestamp{PhysicalTime: 42}
		snapshotTblStuff := tblStuff
		snapshotTblStuff.baseSnap = &plan.Snapshot{TS: &snapshotTS}

		output, err := newDiffOutputTable(ctx, ses, &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{As: *outName},
		}, snapshotTblStuff)
		require.NoError(t, err)
		sql, err := output.createSQL(ctx, snapshotTblStuff)
		require.NoError(t, err)
		require.Contains(t, sql, "`id` BIGINT not null")
		require.NotContains(t, sql, "{mo_ts=")
	})

	t.Run("projected columns retain request order", func(t *testing.T) {
		output, err := newDiffOutputTable(ctx, ses, &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{As: *outName},
			Columns: tree.IdentifierList{
				tree.Identifier("name"), tree.Identifier("id"), tree.Identifier("name"),
			},
		}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{1, 0}, output.projectedIdxes)
		require.Equal(t, []string{"__mo_diff_source", "__mo_diff_flag", "name", "id"}, output.columnNames)
	})

	t.Run("renamed target column retains target name and base type", func(t *testing.T) {
		renamedTargetDef := tblStuff.tarRel.GetTableDef(ctx)
		renamedTargetDef.Cols[1].Name = "display_name"
		renamedTblStuff := tblStuff
		renamedTblStuff.def.colNames = []string{"id", "display_name", "__mo_diff_source", "__mo_diff_flag"}

		output, err := newDiffOutputTable(ctx, ses, &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{As: *outName},
			Columns:   tree.IdentifierList{tree.Identifier("display_name")},
		}, renamedTblStuff)
		require.NoError(t, err)

		sql, err := output.createSQL(ctx, renamedTblStuff)
		require.NoError(t, err)
		require.Contains(t, sql, "`display_name` VARCHAR(20) default null")
		require.NotContains(t, sql, "`name` VARCHAR(20) default null")
	})
}

func TestDataBranchDiffCanExecuteInUncommittedTransaction(t *testing.T) {
	can, err := statementCanBeExecutedInUncommittedTransaction(
		context.Background(), newValidateSession(t), &tree.DataBranchDiff{},
	)
	require.NoError(t, err)
	require.True(t, can)
}

func TestMaterializeDiffOutputAsTable_InsertFailureDropsDestination(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "base_db",
		Name:   "base_t",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		},
	}).AnyTimes()
	tarRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "target_db",
		Name:   "target_t",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 1, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
		},
	}).AnyTimes()

	tblStuff := tableStuff{baseRel: baseRel, tarRel: tarRel}
	tblStuff.def.colNames = []string{"id"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType()}
	tblStuff.def.visibleIdxes = []int{0}
	tblStuff.def.pkColIdx = 0
	tblStuff.retPool = &retBatchList{}
	tblStuff.bufPool = &sync.Pool{New: func() any { return &bytes.Buffer{} }}

	bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.SetRowCount(1)
	defer tblStuff.retPool.freeAllRetBatches(mp)

	phase, err := newDataBranchOutputAsTablePhase(ctx, ses.proc)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, phase.close())
	}()
	require.NoError(t, phase.spool.append(batchWithKind{name: "branch", kind: diffUpdate, batch: bat}))
	phase.markProducerDone()

	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	var sqls []string
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
		sqls = append(sqls, sql)
		if strings.HasPrefix(sql, "insert into") {
			return moerr.NewInternalErrorNoCtx("injected insert failure")
		}
		return nil
	}).Times(3)
	bh.EXPECT().ClearExecResultSet().Times(2)

	dst := tree.NewTableName(
		tree.Identifier("diff_out"),
		tree.ObjectNamePrefix{SchemaName: tree.Identifier("out_db"), ExplicitSchema: true},
		nil,
	)
	err = phase.materialize(
		ctx,
		func() {},
		ses,
		bh,
		&tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{As: *dst}},
		tblStuff,
	)
	require.ErrorContains(t, err, "injected insert failure")
	require.Len(t, sqls, 3)
	require.True(t, strings.HasPrefix(sqls[0], "create table `out_db`.`diff_out`"))
	require.True(t, strings.HasPrefix(sqls[1], "insert into `out_db`.`diff_out`"))
	require.Equal(t, "drop table if exists `out_db`.`diff_out`", sqls[2])
}

func TestDataBranchOutputAsTablePhase_BlocksOutputSQLUntilProducerCompletes(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	phase, err := newDataBranchOutputAsTablePhase(ctx, ses.proc)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, phase.close())
	}()

	tblStuff := tableStuff{retPool: &retBatchList{}}
	tblStuff.def.colNames = []string{"id"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType()}
	tblStuff.def.pkColIdx = 0
	bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.SetRowCount(1)
	defer tblStuff.retPool.freeAllRetBatches(mp)

	retCh := make(chan batchWithKind)
	producerSent := make(chan struct{})
	allowProducerFinish := make(chan struct{})
	drained := make(chan error, 1)
	go func() {
		drained <- phase.drain(ctx, func() {}, tblStuff.retPool, retCh)
	}()
	go func() {
		retCh <- batchWithKind{name: "branch", kind: diffUpdate, batch: bat}
		close(producerSent)
		<-allowProducerFinish
		close(retCh)
	}()
	<-producerSent

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	dst := tree.NewTableName(
		tree.Identifier("diff_out"),
		tree.ObjectNamePrefix{SchemaName: tree.Identifier("out_db"), ExplicitSchema: true},
		nil,
	)
	err = phase.materialize(
		ctx,
		func() {},
		ses,
		bh,
		&tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{As: *dst}},
		tblStuff,
	)
	require.ErrorContains(t, err, "before diff production completed")

	close(allowProducerFinish)
	require.NoError(t, <-drained)
}

func TestDataBranchOutputSpool_RoundTrip(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	spool, err := newDataBranchOutputSpool(ctx, ses.proc)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spool.close())
	}()

	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{0, '\'', '\\', 0xff}, false, ses.proc.Mp()))
	bat.SetRowCount(1)
	defer bat.Clean(ses.proc.Mp())

	require.NoError(t, spool.append(batchWithKind{name: "branch_1", kind: diffUpdate, batch: bat}))
	require.NoError(t, spool.rewind())

	got, ok, err := spool.next()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "branch_1", got.name)
	require.Equal(t, diffUpdate, got.kind)
	require.Equal(t, 1, got.batch.RowCount())
	require.Equal(t, int64(42), vector.MustFixedColNoTypeCheck[int64](got.batch.Vecs[0])[0])
	require.Equal(t, []byte{0, '\'', '\\', 0xff}, got.batch.Vecs[1].GetBytesAt(0))

	_, ok, err = spool.next()
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDataBranchOutputSpoolRejectsOversizedMetadata(t *testing.T) {
	size := int32(dataBranchOutputSpoolMaxMetadataSize + 1)
	_, err := readDataBranchOutputSpoolString(bytes.NewReader(types.EncodeInt32(&size)))
	require.ErrorContains(t, err, "metadata is too large")
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

	t.Run("default output preserves decimal metadata", func(t *testing.T) {
		decimalTblStuff := tblStuff
		decimalTblStuff.def.colNames = []string{"id", "price"}
		decimalTblStuff.def.colTypes = []types.Type{
			types.T_int64.ToType(),
			types.New(types.T_decimal64, 10, 2),
		}
		decimalTblStuff.def.visibleIdxes = []int{0, 1}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   nil,
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, decimalTblStuff))

		mrs := ses.GetMysqlResultSet()
		col, err := mrs.GetColumn(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, "price", col.Name())
		require.Equal(t, defines.MYSQL_TYPE_DECIMAL, col.ColumnType())
		require.Equal(t, uint32(12), col.Length())
		mysqlCol, ok := col.(*MysqlColumn)
		require.True(t, ok)
		require.Equal(t, uint8(2), mysqlCol.Decimal())
	})

	t.Run("default output preserves year metadata", func(t *testing.T) {
		yearTblStuff := tblStuff
		yearTblStuff.def.colNames = []string{"id", "y"}
		yearTblStuff.def.colTypes = []types.Type{
			types.T_int64.ToType(),
			types.T_year.ToType(),
		}
		yearTblStuff.def.visibleIdxes = []int{0, 1}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   nil,
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, yearTblStuff))

		mrs := ses.GetMysqlResultSet()
		col, err := mrs.GetColumn(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, "y", col.Name())
		require.Equal(t, defines.MYSQL_TYPE_YEAR, col.ColumnType())
		require.Equal(t, uint32(types.MaxVarcharLen), col.Length())
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

	t.Run("output as table acknowledges its materialized result", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt: &tree.DiffOutputOpt{As: *tree.NewTableName(
				tree.Identifier("diff_out"), tree.ObjectNamePrefix{}, nil,
			)},
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))
		require.Equal(t, uint64(1), ses.GetMysqlResultSet().GetColumnCount())
		require.Equal(t, "TABLE CREATED", ses.GetMysqlResultSet().Columns[0].Name())
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

	t.Run("columns projection", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   nil,
			Columns:     tree.IdentifierList{tree.Identifier("name")},
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))

		mrs := ses.GetMysqlResultSet()
		// 2 meta columns (diff header + flag) + 1 projected column
		require.Equal(t, uint64(3), mrs.GetColumnCount())
		col2, err := mrs.GetColumn(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, "name", col2.Name())
	})

	t.Run("columns projection with limit", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		limit := int64(5)
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   &tree.DiffOutputOpt{Limit: &limit},
			Columns:     tree.IdentifierList{tree.Identifier("id")},
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))

		mrs := ses.GetMysqlResultSet()
		require.Equal(t, uint64(3), mrs.GetColumnCount())
		col2, err := mrs.GetColumn(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, "id", col2.Name())
	})

	t.Run("columns projection unknown column", func(t *testing.T) {
		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   nil,
			Columns:     tree.IdentifierList{tree.Identifier("nonexistent")},
		}
		err := buildOutputSchema(ctx, ses, stmt, tblStuff)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "nonexistent")
		require.Contains(t, err.Error(), "t2")
	})
}

func TestDataBranchOutputResolveProjectedIdxes(t *testing.T) {
	tblStuff := tableStuff{}
	tblStuff.def.colNames = []string{"id", "name", "age"}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}

	t.Run("nil columns returns nil", func(t *testing.T) {
		got, err := resolveProjectedIdxes(nil, tblStuff)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("single column", func(t *testing.T) {
		got, err := resolveProjectedIdxes(tree.IdentifierList{tree.Identifier("name")}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{1}, got)
	})

	t.Run("multiple columns preserve order", func(t *testing.T) {
		got, err := resolveProjectedIdxes(tree.IdentifierList{
			tree.Identifier("age"), tree.Identifier("id"),
		}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{2, 0}, got)
	})

	t.Run("duplicate columns deduplicated", func(t *testing.T) {
		got, err := resolveProjectedIdxes(tree.IdentifierList{
			tree.Identifier("id"), tree.Identifier("id"),
		}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{0}, got)
	})

	t.Run("case insensitive", func(t *testing.T) {
		got, err := resolveProjectedIdxes(tree.IdentifierList{tree.Identifier("NAME")}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{1}, got)
	})

	t.Run("unknown column returns error", func(t *testing.T) {
		_, err := resolveProjectedIdxes(tree.IdentifierList{tree.Identifier("xxx")}, tblStuff)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "xxx")
	})
}

func TestDataBranchOutputValidateProjectedColumns(t *testing.T) {
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
	tblStuff.def.visibleIdxes = []int{0, 1}

	limit := int64(5)

	t.Run("nil columns", func(t *testing.T) {
		stmt := &tree.DataBranchDiff{}
		require.NoError(t, validateProjectedColumns(stmt, tblStuff))
	})

	t.Run("count validates columns but remains supported", func(t *testing.T) {
		stmt := &tree.DataBranchDiff{
			Columns:   tree.IdentifierList{tree.Identifier("name")},
			OutputOpt: &tree.DiffOutputOpt{Count: true},
		}
		require.NoError(t, validateProjectedColumns(stmt, tblStuff))
	})

	t.Run("summary validates columns but remains supported", func(t *testing.T) {
		stmt := &tree.DataBranchDiff{
			Columns:   tree.IdentifierList{tree.Identifier("id")},
			OutputOpt: &tree.DiffOutputOpt{Summary: true},
		}
		require.NoError(t, validateProjectedColumns(stmt, tblStuff))
	})

	t.Run("row output validates columns", func(t *testing.T) {
		stmt := &tree.DataBranchDiff{
			Columns:   tree.IdentifierList{tree.Identifier("id")},
			OutputOpt: &tree.DiffOutputOpt{Limit: &limit},
		}
		require.NoError(t, validateProjectedColumns(stmt, tblStuff))
	})

	t.Run("unknown column returns invalid input", func(t *testing.T) {
		stmt := &tree.DataBranchDiff{
			Columns:   tree.IdentifierList{tree.Identifier("missing")},
			OutputOpt: &tree.DiffOutputOpt{Count: true},
		}
		err := validateProjectedColumns(stmt, tblStuff)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "missing")
		require.Contains(t, err.Error(), "t2")
	})

	t.Run("output file is rejected", func(t *testing.T) {
		stmt := &tree.DataBranchDiff{
			Columns:   tree.IdentifierList{tree.Identifier("name")},
			OutputOpt: &tree.DiffOutputOpt{DirPath: "/tmp"},
		}
		err := validateProjectedColumns(stmt, tblStuff)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
		require.Contains(t, err.Error(), "OUTPUT FILE")
	})
}

func TestDataBranchOutputShouldDiffAsCSV(t *testing.T) {
	t.Run("reject malformed result without panic", func(t *testing.T) {
		ok, err := shouldDiffAsCSV(executor.Result{})
		require.Error(t, err)
		require.False(t, ok)
	})

	t.Run("reject non-zero base row count", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed[uint64](bat.Vecs[0], 3, false, mp))
		bat.SetRowCount(1)

		ok, err := shouldDiffAsCSV(executor.Result{
			Batches: []*batch.Batch{bat},
			Mp:      mp,
		})
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("allow zero base row count", func(t *testing.T) {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)

		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
		require.NoError(t, vector.AppendFixed[uint64](bat.Vecs[0], 0, false, mp))
		bat.SetRowCount(1)

		ok, err := shouldDiffAsCSV(executor.Result{
			Batches: []*batch.Batch{bat},
			Mp:      mp,
		})
		require.NoError(t, err)
		require.True(t, ok)
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

	emptyKeyBuf := &bytes.Buffer{}
	require.Error(t, writeDeleteRowValuesWithColIdxes(nil, tblStuff, row, nil, emptyKeyBuf, false))
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
	require.Equal(t, "delete from `db1`.`t1` where `id` = 9 and `name` is null limit 1;\n", buf.String())
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

func TestDataBranchOutputInitAndDropApplyTablesWithWriteFile(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "base_t",
		deleteTable:      "__mo_diff_del_x",
		insertTable:      "__mo_diff_ins_x",
		deleteKeyNames:   []string{"id"},
		deleteStageNames: []string{"branch_apply_key_0"},
		visibleNames:     []string{"id", "name"},
	}

	var out bytes.Buffer
	writeFile := func(b []byte) error {
		_, err := out.Write(b)
		return err
	}

	require.NoError(t, initApplyTables(context.Background(), nil, nil, batchInfo, writeFile))
	require.NoError(t, dropApplyTables(context.Background(), nil, nil, batchInfo, writeFile))

	got := out.String()
	require.Contains(t, got, "drop table if exists `db1`.`__mo_diff_del_x`;\n")
	require.Contains(t, got, "drop table if exists `db1`.`__mo_diff_ins_x`;\n")
	require.Contains(t, got, "create table `db1`.`__mo_diff_del_x` as select `id` as `branch_apply_key_0` from `db1`.`base_t` where 1=0;\n")
	require.Contains(t, got, "create table `db1`.`__mo_diff_ins_x` as select `id`,`name` from `db1`.`base_t` where 1=0;\n")
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
	tblStuff.def.visibleIdxes = []int{0, 1}
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0, 1}

	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "t1",
		deleteTable:      "__mo_diff_del_x",
		insertTable:      "__mo_diff_ins_x",
		deleteKeyNames:   []string{"id", "name"},
		deleteStageNames: []string{"branch_apply_key_0", "branch_apply_key_1"},
		visibleNames:     []string{"id", "name"},
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
		batchInfo,
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
	require.Contains(t, got, "insert into `db1`.`__mo_diff_del_x` values (1,'a');\n")
	require.Contains(t, got, "delete from `db1`.`t1` where (`id`,`name`) in (select `branch_apply_key_0`,`branch_apply_key_1` from `db1`.`__mo_diff_del_x`);\n")
	require.Contains(t, got, "insert into `db1`.`t1` (`id`,`name`) values (2,'b');\n")
}

func TestDataBranchOutputTryFlushDeletesOrInserts(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:         "db1",
		baseTable:      "t1",
		deleteTable:    "__mo_diff_del_x",
		insertTable:    "__mo_diff_ins_x",
		deleteKeyNames: []string{"id"},
		visibleNames:   []string{"id", "name"},
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
			batchInfo,
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
		require.Contains(t, out.String(), "delete from `db1`.`t1` where `id` in (select `id` from `db1`.`__mo_diff_del_x`);\n")
		require.Contains(t, out.String(), "insert into `db1`.`t1` (`id`,`name`) select `id`,`name` from `db1`.`__mo_diff_ins_x`;\n")
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
			batchInfo,
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
			batchInfo,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			writeFile,
		)
		require.NoError(t, err)

		got := out.String()
		deletePos := strings.Index(got, "insert into `db1`.`__mo_diff_del_x`")
		insertPos := strings.Index(got, "insert into `db1`.`__mo_diff_ins_x`")
		require.NotEqual(t, -1, deletePos)
		require.NotEqual(t, -1, insertPos)
		require.Less(t, deletePos, insertPos)
	})
}

func TestDataBranchOutputBuildDataBranchApplyLayout(t *testing.T) {
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

	deleteByFullRow, deleteKeyColIdxes, info := buildDataBranchApplyLayout(
		context.Background(), &Session{}, tblStuff, dataBranchApplyModeOnlineMerge,
	)
	require.False(t, deleteByFullRow)
	require.Equal(t, []int{0, 2}, deleteKeyColIdxes)
	require.NotNil(t, info)
	require.Equal(t, "db1", info.dbName)
	require.Equal(t, "t1", info.baseTable)
	require.Equal(t, []string{"id", "age"}, info.deleteKeyNames)
	require.Equal(t, []string{"branch_apply_key_0", "branch_apply_key_1"}, info.deleteStageNames)
	require.Equal(t, []string{"id", "name", "age"}, info.visibleNames)
	require.False(t, info.disableInsertStage)
	require.True(t, strings.HasPrefix(info.deleteTable, "__mo_diff_del_"))
	require.True(t, strings.HasPrefix(info.insertTable, "__mo_diff_ins_"))

	fakeTblStuff := newFakePKBranchTableStuff(ctrl)
	deleteByFullRow, deleteKeyColIdxes, info = buildDataBranchApplyLayout(
		context.Background(), &Session{}, fakeTblStuff, dataBranchApplyModeOnlineMerge,
	)
	require.False(t, deleteByFullRow)
	require.Equal(t, []int{2}, deleteKeyColIdxes)
	require.NotNil(t, info)
	require.Equal(t, []string{"__mo_fake_pk_col"}, info.deleteKeyNames)
	require.Equal(t, []string{"branch_apply_key_0"}, info.deleteStageNames)
	require.Equal(t, []string{"id", "name"}, info.visibleNames)
	require.True(t, info.disableInsertStage)

	deleteByFullRow, deleteKeyColIdxes, info = buildDataBranchApplyLayout(
		context.Background(), &Session{}, fakeTblStuff, dataBranchApplyModeOnlinePKOnly,
	)
	require.False(t, deleteByFullRow)
	require.Equal(t, []int{0, 1}, deleteKeyColIdxes)
	require.NotNil(t, info)
	require.False(t, info.disableInsertStage)

	deleteByFullRow, deleteKeyColIdxes, info = buildDataBranchApplyLayout(
		context.Background(), &Session{}, fakeTblStuff, dataBranchApplyModePortableSQL,
	)
	require.True(t, deleteByFullRow)
	require.Nil(t, deleteKeyColIdxes)
	require.Nil(t, info)
}

func TestDataBranchOutputAppenderAppendRowAndFlushAll(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:         "db1",
		baseTable:      "t1",
		deleteTable:    "__mo_diff_del_x",
		insertTable:    "__mo_diff_ins_x",
		deleteKeyNames: []string{"id"},
		visibleNames:   []string{"id", "name"},
	}

	t.Run("append delete in full-row mode", func(t *testing.T) {
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}

		appender := sqlValuesAppender{
			ctx:             context.Background(),
			deleteByFullRow: true,
			batchInfo:       batchInfo,
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
			batchInfo:       batchInfo,
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
			batchInfo:       batchInfo,
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
		require.Contains(t, out.String(), "insert into `db1`.`__mo_diff_del_x` values (1);")
		require.Contains(t, out.String(), "insert into `db1`.`__mo_diff_ins_x` values (1,'a');")
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

func TestDataBranchOutputAppendBatchRowsAsSQLValues(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)

	t.Run("appends insert and delete rows", func(t *testing.T) {
		tmpValsBuffer := &bytes.Buffer{}
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}
		appender := sqlValuesAppender{
			ctx:               context.Background(),
			tblStuff:          tblStuff,
			deleteKeyColIdxes: []int{0},
			deleteCnt:         &deleteCnt,
			deleteBuf:         deleteBuf,
			insertCnt:         &insertCnt,
			insertBuf:         insertBuf,
		}

		insertBat := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{
			{int64(1), "alpha"},
			{int64(2), "beta"},
		})
		defer insertBat.Clean(ses.proc.Mp())

		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(),
			ses,
			tblStuff,
			batchWithKind{kind: diffInsert, batch: insertBat},
			tmpValsBuffer,
			appender,
		))
		require.Equal(t, 2, insertCnt)
		require.Equal(t, "(1,'alpha'),(2,'beta')", insertBuf.String())

		deleteBat := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{
			{int64(3), "gamma"},
			{int64(4), "delta"},
		})
		defer deleteBat.Clean(ses.proc.Mp())

		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(),
			ses,
			tblStuff,
			batchWithKind{kind: diffDelete, batch: deleteBat},
			tmpValsBuffer,
			appender,
		))
		require.Equal(t, 2, deleteCnt)
		require.Equal(t, "3,4", deleteBuf.String())
	})

	t.Run("returns shape mismatch error", func(t *testing.T) {
		tmpValsBuffer := &bytes.Buffer{}
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}
		appender := sqlValuesAppender{
			ctx:               context.Background(),
			tblStuff:          tblStuff,
			deleteKeyColIdxes: []int{0},
			deleteCnt:         &deleteCnt,
			deleteBuf:         deleteBuf,
			insertCnt:         &insertCnt,
			insertBuf:         insertBuf,
		}

		bat := batch.NewWithSize(2)
		bat.SetAttributes([]string{"id", "name"})
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		defer bat.Clean(ses.proc.Mp())

		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, ses.proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(2), false, ses.proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("only-one"), false, ses.proc.Mp()))
		bat.SetRowCount(2)

		err := appendBatchRowsAsSQLValues(
			context.Background(),
			ses,
			tblStuff,
			batchWithKind{kind: diffInsert, batch: bat},
			tmpValsBuffer,
			appender,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "batch shape mismatch")
	})
}

func TestDataBranchOutputNoPKDeleteModes(t *testing.T) {
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newFakePKBranchTableStuff(ctrl)

	t.Run("online merge deletes by fake pk", func(t *testing.T) {
		var out bytes.Buffer
		writeFile := func(b []byte) error {
			_, err := out.Write(b)
			return err
		}

		tmpValsBuffer := &bytes.Buffer{}
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}
		appender := newSQLValuesAppender(
			context.Background(),
			ses,
			nil,
			tblStuff,
			dataBranchApplyModeOnlineMerge,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			writeFile,
		)

		deleteBat := buildFakePKComparisonBatch(t, ses.proc.Mp(), [][]any{
			{int64(1), "alpha", uint64(101)},
			{int64(2), "beta", uint64(202)},
		})
		defer deleteBat.Clean(ses.proc.Mp())

		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(),
			ses,
			tblStuff,
			batchWithKind{kind: diffDelete, batch: deleteBat},
			tmpValsBuffer,
			appender,
		))
		require.Equal(t, 2, deleteCnt)
		require.Equal(t, "(101),(202)", deleteBuf.String())
		require.True(t, appender.batchInfo.disableInsertStage)
		require.NoError(t, appender.flushAll())

		got := out.String()
		require.Contains(t, got, "insert into `db1`.`__mo_diff_del_")
		require.Contains(t, got, "values (101),(202);")
		require.Equal(t, []string{"branch_apply_key_0"}, appender.batchInfo.deleteStageNames)
		require.Contains(t, got, "delete from `db1`.`base` where `__mo_fake_pk_col` in (select `branch_apply_key_0` from `db1`.`__mo_diff_del_")
		require.NotContains(t, got, "delete from `db1`.`base` where `id` =")
	})

	t.Run("portable sql deletes by full row", func(t *testing.T) {
		tmpValsBuffer := &bytes.Buffer{}
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}
		appender := newSQLValuesAppender(
			context.Background(),
			ses,
			nil,
			tblStuff,
			dataBranchApplyModePortableSQL,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			nil,
		)

		deleteBat := buildFakePKComparisonBatch(t, ses.proc.Mp(), [][]any{
			{int64(1), "alpha", uint64(101)},
			{int64(2), "beta", uint64(202)},
		})
		defer deleteBat.Clean(ses.proc.Mp())

		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(),
			ses,
			tblStuff,
			batchWithKind{kind: diffDelete, batch: deleteBat},
			tmpValsBuffer,
			appender,
		))
		require.True(t, appender.deleteByFullRow)
		require.Nil(t, appender.batchInfo)
		require.Equal(t, 2, deleteCnt)
		require.Contains(t, deleteBuf.String(), "delete from `db1`.`base` where `id` = 1 and `name` = 'alpha' limit 1;\n")
		require.Contains(t, deleteBuf.String(), "delete from `db1`.`base` where `id` = 2 and `name` = 'beta' limit 1;\n")
		require.NotContains(t, deleteBuf.String(), "__mo_fake_pk_col")
	})

	t.Run("online merge inserts keep direct insert path", func(t *testing.T) {
		var out bytes.Buffer
		writeFile := func(b []byte) error {
			_, err := out.Write(b)
			return err
		}

		tmpValsBuffer := &bytes.Buffer{}
		deleteCnt := 0
		insertCnt := 0
		deleteBuf := &bytes.Buffer{}
		insertBuf := &bytes.Buffer{}
		appender := newSQLValuesAppender(
			context.Background(),
			ses,
			nil,
			tblStuff,
			dataBranchApplyModeOnlineMerge,
			&deleteCnt,
			deleteBuf,
			&insertCnt,
			insertBuf,
			writeFile,
		)

		insertBat := buildFakePKInsertBatchWithoutFakePKValue(t, ses.proc.Mp(), [][]any{
			{int64(3), "gamma"},
		})
		defer insertBat.Clean(ses.proc.Mp())

		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(),
			ses,
			tblStuff,
			batchWithKind{kind: diffInsert, batch: insertBat},
			tmpValsBuffer,
			appender,
		))
		require.True(t, appender.batchInfo.disableInsertStage)
		require.NoError(t, appender.flushAll())
		require.Contains(t, out.String(), "insert into `db1`.`base` (`id`,`name`) values (3,'gamma');")
		require.NotContains(t, out.String(), "__mo_diff_ins_")
	})
}

func newFakePKBranchTableStuff(ctrl *gomock.Controller) tableStuff {
	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseDef := &plan.TableDef{
		DbName: "db1",
		Name:   "base",
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{"__mo_fake_pk_col"},
			PkeyColName: "__mo_fake_pk_col",
		},
	}

	baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(baseDef).AnyTimes()

	var tblStuff tableStuff
	tblStuff.baseRel = baseRel
	tblStuff.def.colNames = []string{"id", "name", "__mo_fake_pk_col"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_uint64.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1}
	tblStuff.def.pkColIdx = 2
	tblStuff.def.pkColIdxes = []int{0, 1}
	tblStuff.def.pkKind = fakeKind
	return tblStuff
}

func buildFakePKComparisonBatch(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"id", "name", "__mo_fake_pk_col"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())

	for _, row := range rows {
		require.Len(t, row, 3)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row[0].(int64), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(row[1].(string)), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], row[2].(uint64), false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}

func buildFakePKInsertBatchWithoutFakePKValue(t *testing.T, mp *mpool.MPool, rows [][]any) *batch.Batch {
	t.Helper()

	bat := batch.NewWithSize(3)
	bat.SetAttributes([]string{"id", "name", "__mo_fake_pk_col"})
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_uint64.ToType())

	for _, row := range rows {
		require.Len(t, row, 2)
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], row[0].(int64), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(row[1].(string)), false, mp))
	}
	bat.SetRowCount(len(rows))
	return bat
}
