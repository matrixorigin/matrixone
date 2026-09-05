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
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
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

	got, err := makeFileName(nil, nil, tblStuff)
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`^diff_t2_t1_\d{8}_\d{6}_[0-9a-f-]{36}$`), got)

	got, err = makeFileName(
		&tree.AtTimeStamp{SnapshotName: "sp1"},
		&tree.AtTimeStamp{SnapshotName: "sp2"},
		tblStuff,
	)
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`^diff_t2_sp2_t1_sp1_\d{8}_\d{6}_[0-9a-f-]{36}$`), got)
}

func TestDataBranchOutputMakeFileNameUUIDError(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableName().Return("base")
	tarRel.EXPECT().GetTableName().Return("target")

	_, err := makeFileNameWithUUID(nil, nil, tableStuff{baseRel: baseRel, tarRel: tarRel}, func() (uuid.UUID, error) {
		return uuid.Nil, errors.New("entropy unavailable")
	})
	require.ErrorContains(t, err, "generate data branch output file name: entropy unavailable")
}

func TestDataBranchOutputMakeFileNameConcurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	baseRel := mock_frontend.NewMockRelation(ctrl)
	tarRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableName().Return("base").AnyTimes()
	tarRel.EXPECT().GetTableName().Return("target").AnyTimes()
	tblStuff := tableStuff{baseRel: baseRel, tarRel: tarRel}

	const concurrency = 128
	type result struct {
		name string
		err  error
	}
	results := make(chan result, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			name, err := makeFileName(nil, nil, tblStuff)
			results <- result{name: name, err: err}
		}()
	}
	wg.Wait()
	close(results)

	unique := make(map[string]struct{}, concurrency)
	for result := range results {
		require.NoError(t, result.err)
		unique[result.name] = struct{}{}
	}
	require.Len(t, unique, concurrency)
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
	t.Cleanup(func() {
		require.NoError(t, release())
		cleanup()
	})

	require.Equal(t, outputDir, filepath.Dir(filePath))
	require.Regexp(t, regexp.MustCompile(
		`^diff_child@5Cname@3Aquoted_target@5Csnapshot@251_base@2Fname@60quoted_base@2Fsnapshot@201_\d{8}_\d{6}_[0-9a-f-]{36}\.sql$`,
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

				fileName, err := makeFileName(nil, nil, tableStuff{baseRel: baseRel, tarRel: tarRel})
				require.NoError(t, err)
				fileName += ".csv"
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
		t.Cleanup(func() {
			require.NoError(t, release())
			cleanup()
		})

		baseName := filepath.Base(filePath)
		require.LessOrEqual(t, len(baseName), maxDiffFileNameStemBytes+len(".sql"))
		require.True(t, utf8.ValidString(baseName))
		require.Regexp(t, regexp.MustCompile(`_[0-9a-f]{32}_\d{8}_\d{6}_[0-9a-f-]{36}\.sql$`), baseName)
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
	tblStuff.def.pkKind = normalKind
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0}

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

	t.Run("projected primary key precedes requested columns", func(t *testing.T) {
		output, err := newDiffOutputTable(ctx, ses, &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{As: *outName},
			Columns: tree.IdentifierList{
				tree.Identifier("name"), tree.Identifier("id"), tree.Identifier("name"),
			},
		}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1}, output.projectedIdxes)
		require.Equal(t, []string{"__mo_diff_source", "__mo_diff_flag", "id", "name"}, output.columnNames)
	})

	t.Run("target-only column uses target type and remains nullable", func(t *testing.T) {
		targetDef := tarRel.GetTableDef(ctx)
		targetDef.Cols = append(targetDef.Cols, &plan.ColDef{
			Name: "extra", ColId: 5, Seqnum: 4,
			Typ: plan.Type{Id: int32(types.T_int32), NotNullable: true},
		})
		defer func() { targetDef.Cols = targetDef.Cols[:len(targetDef.Cols)-1] }()

		targetOnlyTblStuff := tblStuff
		targetOnlyTblStuff.def.colNames = append(
			append([]string(nil), tblStuff.def.colNames...), "extra",
		)
		targetOnlyTblStuff.def.colTypes = append(
			append([]types.Type(nil), tblStuff.def.colTypes...), types.T_int32.ToType(),
		)
		targetOnlyTblStuff.def.visibleIdxes = append(
			append([]int(nil), tblStuff.def.visibleIdxes...), 4,
		)
		targetOnlyTblStuff.def.tarOnlyIdxes = []int{4}

		output, err := newDiffOutputTable(ctx, ses, &tree.DataBranchDiff{
			OutputOpt: &tree.DiffOutputOpt{As: *outName},
		}, targetOnlyTblStuff)
		require.NoError(t, err)
		sql, err := output.createSQL(ctx, targetOnlyTblStuff)
		require.NoError(t, err)
		require.Contains(t, sql, "`extra` INT default null")
		require.NotContains(t, sql, "`extra` INT not null")
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
	bat.Vecs[1] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{0, '\'', '\\', 0xff}, false, ses.proc.Mp()))
	bat.Vecs[1].SetIsBinaryString(true)
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
	require.True(t, got.batch.Vecs[1].GetBinaryStringMetadataAt(0))

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
	tblStuff.def.pkKind = normalKind
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0}

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
		require.Equal(t, uint32(4), col.Length())
	})

	t.Run("default output preserves binary charset metadata", func(t *testing.T) {
		binaryTblStuff := tblStuff
		binaryTblStuff.def.colNames = []string{"b", "bn", "vb", "s"}
		binaryTblStuff.def.colTypes = []types.Type{
			types.New(types.T_bit, 10, 0),
			types.New(types.T_binary, 8, 0),
			types.New(types.T_varbinary, 32, 0),
			types.New(types.T_varchar, 32, 0),
		}
		binaryTblStuff.def.visibleIdxes = []int{0, 1, 2, 3}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		stmt := &tree.DataBranchDiff{
			TargetTable: *target,
			BaseTable:   *base,
			OutputOpt:   nil,
		}
		require.NoError(t, buildOutputSchema(ctx, ses, stmt, binaryTblStuff))

		mrs := ses.GetMysqlResultSet()
		for idx, expectedType := range []defines.MysqlType{
			defines.MYSQL_TYPE_BIT,
			defines.MYSQL_TYPE_STRING,
			defines.MYSQL_TYPE_VAR_STRING,
			defines.MYSQL_TYPE_VAR_STRING,
		} {
			col, err := mrs.GetColumn(ctx, uint64(idx+2))
			require.NoError(t, err)
			require.Equal(t, expectedType, col.ColumnType())
			expectedCharset := uint16(charsetBinary)
			if idx == 3 {
				expectedCharset = uint16(Utf8mb4CollationID)
			}
			mysqlCol := col.(*MysqlColumn)
			require.Equal(t, expectedCharset, mysqlCol.Charset())
			if idx == 1 || idx == 2 {
				require.NotZero(t, mysqlCol.Flag()&uint16(defines.BINARY_FLAG))
			} else {
				require.Zero(t, mysqlCol.Flag()&uint16(defines.BINARY_FLAG))
			}
		}
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
		// 2 meta columns (diff header + flag) + the PK and requested column
		require.Equal(t, uint64(4), mrs.GetColumnCount())
		col2, err := mrs.GetColumn(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, "id", col2.Name())
		col3, err := mrs.GetColumn(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, "name", col3.Name())
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

func TestDataBranchOutputLimitBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		limit     int64
		wantRows  uint64
		wantStops int
	}{
		{name: "zero", limit: 0, wantRows: 0, wantStops: 1},
		{name: "one", limit: 1, wantRows: 1, wantStops: 0},
		{name: "exact row count", limit: 2, wantRows: 2, wantStops: 0},
		{name: "above row count", limit: 3, wantRows: 2, wantStops: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ses := newValidateSession(t)
			ses.SetMysqlResultSet(&MysqlResultSet{})

			ctrl := gomock.NewController(t)
			tblStuff := newTestBranchTableStuff(ctrl)
			bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
			t.Cleanup(func() {
				tblStuff.retPool.freeAllRetBatches(ses.proc.Mp())
			})
			for i, name := range []string{"one", "two"} {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i+1), false, ses.proc.Mp()))
				require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(name), false, ses.proc.Mp()))
				require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("hidden"), false, ses.proc.Mp()))
			}
			bat.SetRowCount(2)

			retCh := make(chan batchWithKind, 1)
			retCh <- batchWithKind{name: "child", kind: diffUpdate, batch: bat}
			close(retCh)

			stopCalls := 0
			stmt := &tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{Limit: &tt.limit}}
			require.NoError(t, satisfyDiffOutputOpt(
				ctx,
				cancel,
				func() { stopCalls++ },
				ses,
				nil,
				stmt,
				branchMetaInfo{},
				tblStuff,
				retCh,
			))
			require.Equal(t, tt.wantRows, ses.GetMysqlResultSet().GetRowCount())
			require.Equal(t, tt.wantStops, stopCalls)
		})
	}
}

func TestDataBranchOutputLimitUsesFinalPKOrder(t *testing.T) {
	tests := []struct {
		name    string
		limit   int64
		ids     []int64
		wantIDs []int64
	}{
		{name: "one row", limit: 1, ids: []int64{100, 1}, wantIDs: []int64{1}},
		{name: "multiple rows", limit: 2, ids: []int64{100, 1, 50}, wantIDs: []int64{1, 50}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			ses := newValidateSession(t)
			ses.SetMysqlResultSet(&MysqlResultSet{})

			ctrl := gomock.NewController(t)
			tblStuff := newTestBranchTableStuff(ctrl)
			t.Cleanup(func() {
				tblStuff.retPool.freeAllRetBatches(ses.proc.Mp())
			})

			retCh := make(chan batchWithKind, len(tt.ids))
			for _, id := range tt.ids {
				bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], id, false, ses.proc.Mp()))
				require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte{byte(id)}, false, ses.proc.Mp()))
				require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("hidden"), false, ses.proc.Mp()))
				bat.SetRowCount(1)
				retCh <- batchWithKind{name: "side", kind: diffInsert, batch: bat}
			}
			close(retCh)

			stopCalls := 0
			stmt := &tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{Limit: &tt.limit}}
			require.NoError(t, satisfyDiffOutputOpt(
				ctx,
				cancel,
				func() { stopCalls++ },
				ses,
				nil,
				stmt,
				branchMetaInfo{},
				tblStuff,
				retCh,
			))

			require.Equal(t, uint64(len(tt.wantIDs)), ses.GetMysqlResultSet().GetRowCount())
			for i, wantID := range tt.wantIDs {
				row, err := ses.GetMysqlResultSet().GetRow(ctx, uint64(i))
				require.NoError(t, err)
				require.Equal(t, wantID, row[2])
				require.Equal(t, []byte{byte(wantID)}, row[3])
			}
			require.Zero(t, stopCalls, "positive limits must consume every producer row before selecting the sorted prefix")
		})
	}
}

func TestDataBranchOutputColumnsIncludeCompositePrimaryKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ses := newValidateSession(t)
	ses.SetMysqlResultSet(&MysqlResultSet{})

	ctrl := gomock.NewController(t)
	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"org_id", "event_id", "val", "note"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1, 2, 3}
	tblStuff.def.pkKind = compositeKind
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0, 1}
	t.Cleanup(func() {
		tblStuff.retPool.freeAllRetBatches(ses.proc.Mp())
	})

	stmt := &tree.DataBranchDiff{
		Columns: tree.IdentifierList{tree.Identifier("val")},
	}
	require.NoError(t, buildOutputSchema(ctx, ses, stmt, tblStuff))
	require.Equal(t, uint64(5), ses.GetMysqlResultSet().GetColumnCount())
	for idx, name := range []string{"diff target against base", "flag", "org_id", "event_id", "val"} {
		col, err := ses.GetMysqlResultSet().GetColumn(ctx, uint64(idx))
		require.NoError(t, err)
		require.Equal(t, name, col.Name())
	}

	bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(1), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(19), false, ses.proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[3], []byte("upd"), false, ses.proc.Mp()))
	bat.SetRowCount(1)

	retCh := make(chan batchWithKind, 1)
	retCh <- batchWithKind{name: "target", kind: diffUpdate, batch: bat}
	close(retCh)
	require.NoError(t, satisfyDiffOutputOpt(
		ctx,
		cancel,
		func() {},
		ses,
		nil,
		stmt,
		branchMetaInfo{},
		tblStuff,
		retCh,
	))

	row, err := ses.GetMysqlResultSet().GetRow(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, []any{"target", diffUpdate, int64(1), int64(1), int64(19)}, row)
}

func BenchmarkDataBranchOutputLimitWideRows(b *testing.B) {
	const (
		rowCount    = 64
		payloadSize = 64 << 10
	)

	ses := newValidateSession(b)
	ctrl := gomock.NewController(b)
	tblStuff := newTestBranchTableStuff(ctrl)
	b.Cleanup(func() {
		tblStuff.retPool.freeAllRetBatches(ses.proc.Mp())
	})

	payload := bytes.Repeat([]byte{'x'}, payloadSize)
	limit := int64(1)
	stmt := &tree.DataBranchDiff{OutputOpt: &tree.DiffOutputOpt{Limit: &limit}}
	b.SetBytes(rowCount * payloadSize)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		b.StopTimer()
		ses.SetMysqlResultSet(&MysqlResultSet{})
		bat := tblStuff.retPool.acquireRetBatch(tblStuff, false)
		// The first row is the final LIMIT 1 winner. The remaining ascending
		// keys isolate the cost of rejecting wide payloads.
		for id := range rowCount {
			if err := vector.AppendFixed(bat.Vecs[0], int64(id), false, ses.proc.Mp()); err != nil {
				b.Fatal(err)
			}
			if err := vector.AppendBytes(bat.Vecs[1], payload, false, ses.proc.Mp()); err != nil {
				b.Fatal(err)
			}
			if err := vector.AppendBytes(bat.Vecs[2], []byte("hidden"), false, ses.proc.Mp()); err != nil {
				b.Fatal(err)
			}
		}
		bat.SetRowCount(rowCount)

		retCh := make(chan batchWithKind, 1)
		retCh <- batchWithKind{name: "child", kind: diffInsert, batch: bat}
		close(retCh)
		ctx, cancel := context.WithCancel(context.Background())
		b.StartTimer()

		err := satisfyDiffOutputOpt(
			ctx,
			cancel,
			func() {},
			ses,
			nil,
			stmt,
			branchMetaInfo{},
			tblStuff,
			retCh,
		)

		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if got := ses.GetMysqlResultSet().GetRowCount(); got != 1 {
			b.Fatalf("expected one retained row, got %d", got)
		}
	}
}

func TestDataBranchOutputResolveProjectedIdxes(t *testing.T) {
	tblStuff := tableStuff{}
	tblStuff.def.colNames = []string{"id", "name", "age"}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.pkKind = normalKind
	tblStuff.def.pkColIdxes = []int{0}

	t.Run("nil columns returns nil", func(t *testing.T) {
		got, err := resolveProjectedIdxes(nil, tblStuff)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("single column", func(t *testing.T) {
		got, err := resolveProjectedIdxes(tree.IdentifierList{tree.Identifier("name")}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1}, got)
	})

	t.Run("multiple columns preserve order", func(t *testing.T) {
		got, err := resolveProjectedIdxes(tree.IdentifierList{
			tree.Identifier("age"), tree.Identifier("id"),
		}, tblStuff)
		require.NoError(t, err)
		require.Equal(t, []int{0, 2}, got)
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
		require.Equal(t, []int{0, 1}, got)
	})

	t.Run("unknown column returns error", func(t *testing.T) {
		_, err := resolveProjectedIdxes(tree.IdentifierList{tree.Identifier("xxx")}, tblStuff)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Contains(t, err.Error(), "xxx")
	})

	t.Run("composite primary key columns precede requested columns", func(t *testing.T) {
		compositeTblStuff := tableStuff{}
		compositeTblStuff.def.colNames = []string{"region", "dept", "emp_id", "salary"}
		compositeTblStuff.def.visibleIdxes = []int{0, 1, 2, 3}
		compositeTblStuff.def.pkKind = compositeKind
		compositeTblStuff.def.pkColIdxes = []int{0, 1, 2}

		got, err := resolveProjectedIdxes(
			tree.IdentifierList{tree.Identifier("salary")}, compositeTblStuff,
		)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2, 3}, got)

		got, err = resolveProjectedIdxes(
			tree.IdentifierList{tree.Identifier("salary"), tree.Identifier("dept")}, compositeTblStuff,
		)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 2, 3}, got)
	})

	t.Run("composite primary key follows definition order", func(t *testing.T) {
		orderedTblStuff := tableStuff{}
		orderedTblStuff.def.colNames = []string{"value", "event_id", "org_id"}
		orderedTblStuff.def.visibleIdxes = []int{0, 1, 2}
		orderedTblStuff.def.pkKind = compositeKind
		orderedTblStuff.def.pkColIdxes = []int{2, 1}

		got, err := resolveProjectedIdxes(
			tree.IdentifierList{tree.Identifier("value")}, orderedTblStuff,
		)
		require.NoError(t, err)
		require.Equal(t, []int{2, 1, 0}, got)
	})

	t.Run("fake primary key is not added to projection", func(t *testing.T) {
		fakeTblStuff := tableStuff{}
		fakeTblStuff.def.colNames = []string{"a", "b", "c"}
		fakeTblStuff.def.visibleIdxes = []int{0, 1, 2}
		fakeTblStuff.def.pkKind = fakeKind
		fakeTblStuff.def.pkColIdxes = []int{0, 1, 2}

		got, err := resolveProjectedIdxes(
			tree.IdentifierList{tree.Identifier("c")}, fakeTblStuff,
		)
		require.NoError(t, err)
		require.Equal(t, []int{2}, got)
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
		defer bat.Clean(mp)
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
		defer bat.Clean(mp)
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

func TestSubmitCSVBatchForConversionReturnsCopyErrorBeforeSubmit(t *testing.T) {
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})

	dstMP, err := mpool.NewMPool("data-branch-csv-copy-failure", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		mpool.DeleteMPool(dstMP)
	})

	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], make([]byte, 2<<20), false, srcMP))
	bat.SetRowCount(1)
	t.Cleanup(func() {
		bat.Clean(srcMP)
	})

	ses := &Session{proc: testutil.NewProcessWithMPool(t, "", dstMP)}
	ep := &ExportConfig{}
	var workerWg sync.WaitGroup

	_, wantErr := bat.Dup(dstMP)
	require.Error(t, wantErr)
	require.Zero(t, dstMP.CurrNB())

	err = submitCSVBatchForConversion(
		context.Background(), ses, tableStuff{}, bat, ep, &workerWg,
	)
	require.EqualError(t, err, wantErr.Error())
	require.Zero(t, ep.Index.Load())
	require.Zero(t, dstMP.CurrNB())
	workerWg.Wait()
}

func TestSubmitCSVBatchForConversionCleansCopyOnSubmitError(t *testing.T) {
	srcMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(srcMP)
	})
	dstMP := mpool.MustNewZeroNoFixed()
	t.Cleanup(func() {
		mpool.DeleteMPool(dstMP)
	})

	bat := batch.NewOffHeapWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("value"), false, srcMP))
	bat.SetRowCount(1)
	t.Cleanup(func() {
		bat.Clean(srcMP)
	})

	worker, err := ants.NewPool(1)
	require.NoError(t, err)
	worker.Release()

	ses := &Session{proc: testutil.NewProcessWithMPool(t, "", dstMP)}
	ep := &ExportConfig{}
	var workerWg sync.WaitGroup

	err = submitCSVBatchForConversion(
		context.Background(), ses, tableStuff{worker: worker}, bat, ep, &workerWg,
	)
	require.Error(t, err)
	require.Zero(t, ep.Index.Load())
	require.Zero(t, dstMP.CurrNB())
	workerWg.Wait()
}

func TestFlushRemainingCSVBatchBytesSkipsCanceledIndexGap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ep := &ExportConfig{}
	ep.Index.Store(1)
	ep.ByteChan = make(chan *BatchByte)
	close(ep.ByteChan)

	done := make(chan error, 1)
	go func() {
		done <- flushRemainingCSVBatchBytes(ctx, ep)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("canceled CSV writer waited for a batch index that cannot be produced")
	}
}

func TestWaitForCSVConversionWorkersReturnsLateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var workerWg sync.WaitGroup
	workerWg.Add(1)

	done := make(chan error, 1)
	go func() {
		done <- waitForCSVConversionWorkers(ctx, &workerWg)
	}()

	cancel()
	workerWg.Done()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("waiting for CSV conversion workers did not terminate")
	}
}

func TestJoinCSVInputErrorDoesNotDuplicateObservedCancellation(t *testing.T) {
	require.EqualError(t, joinCSVInputError(context.Canceled, context.Canceled), context.Canceled.Error())
	require.ErrorIs(t, joinCSVInputError(nil, context.Canceled), context.Canceled)
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
	require.NoError(t, writeInsertRowValues(nil, tblStuff, row, insertBuf, tblStuff.def.visibleIdxes))
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
	tblStuff.def.colNames = []string{"id", "name_new"}
	tblStuff.def.baseColNames = []string{"id", "name"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.visibleIdxes = []int{0, 1}

	row := []any{int64(9), nil}
	buf := &bytes.Buffer{}
	require.NoError(t, writeDeleteRowSQLFull(context.Background(), nil, tblStuff, row, buf))
	require.Equal(t, "delete from `db1`.`t1` where `id` = 9 and `name` is null limit 1;\n", buf.String())

	tblStuff.def.colNames = []string{"f32", "f64", "nullable", "pos_inf", "neg_inf"}
	tblStuff.def.baseColNames = []string{"f32", "f64", "nullable", "pos_inf", "neg_inf"}
	tblStuff.def.colTypes = []types.Type{
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_float64.ToType(),
		types.T_float32.ToType(),
		types.T_float64.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1, 2, 3, 4}

	row = []any{
		math.Float32frombits(0x7fc00001),
		math.Float64frombits(0x7ff8000000000001),
		nil,
		float32(math.Inf(1)),
		math.Inf(-1),
	}
	buf.Reset()
	require.NoError(t, writeDeleteRowSQLFull(context.Background(), nil, tblStuff, row, buf))
	require.Equal(t,
		"delete from `db1`.`t1` where serial(`f32`) = serial(bit_cast(unhex('0100c07f') as float)) and "+
			"serial(`f64`) = serial(bit_cast(unhex('010000000000f87f') as double)) and `nullable` is null and "+
			"serial(`pos_inf`) = serial(cast('+Inf' as float)) and "+
			"serial(`neg_inf`) = serial(cast('-Inf' as double)) limit 1;\n",
		buf.String(),
	)
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

func TestDataBranchOutputDirectUpdateSQL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "t1",
	}).AnyTimes()

	tblStuff := tableStuff{baseRel: baseRel}
	tblStuff.def.baseColNames = []string{"f32", "f64", "tag", "note"}
	tblStuff.def.colTypes = []types.Type{
		types.T_float32.ToType(),
		types.T_float64.ToType(),
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
	}
	tblStuff.def.pkColIdxes = []int{0, 1, 2}
	tblStuff.def.writableIdxes = []int{0, 1, 2, 3}

	row := []any{
		math.Float32frombits(0x7fc00001),
		math.Float64frombits(0x8000000000000000),
		int64(7),
		"updated",
	}
	var buf bytes.Buffer
	statements, err := dataBranchDirectUpdateSQL(
		context.Background(), nil, tblStuff, row, &buf, true,
	)
	require.NoError(t, err)
	require.Equal(t,
		[]string{
			"insert into `db1`.`t1` (`f32`,`f64`,`tag`,`note`) values (" +
				"bit_cast(unhex('0100c07f') as float)," +
				"bit_cast(unhex('0000000000000080') as double),7,'updated')",
		},
		statements,
	)

	statements, err = dataBranchDirectUpdateSQL(
		context.Background(), nil, tblStuff, row, &buf, false,
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"update `db1`.`t1` set `note` = 'updated' where " +
			"serial(`f32`) = serial(bit_cast(unhex('0100c07f') as float)) and " +
			"serial(`f64`) = serial(bit_cast(unhex('0000000000000080') as double)) and " +
			"`tag` = 7 limit 1",
	}, statements)

	tblStuff.def.baseColNames = []string{"id", "note"}
	tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
	tblStuff.def.pkColIdxes = []int{0}
	tblStuff.def.writableIdxes = []int{0, 1}
	statements, err = dataBranchDirectUpdateSQL(
		context.Background(), nil, tblStuff, []any{int64(2), "updated"}, &buf, false,
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"update `db1`.`t1` set `note` = 'updated' where `id` = 2 limit 1",
	}, statements)
}

func TestDataBranchOutputInitAndDropApplyTablesWithWriteFile(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "base_t",
		deleteTable:      "__mo_diff_del_x",
		insertTable:      "__mo_diff_ins_x",
		updateTable:      "__mo_diff_upd_x",
		deleteKeyNames:   []string{"id"},
		deleteStageNames: []string{"branch_apply_key_0"},
		deleteKeyTypes:   []types.Type{types.T_int64.ToType()},
		writableNames:    []string{"id", "name"},
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
	require.Contains(t, got, "drop table if exists `db1`.`__mo_diff_upd_x`;\n")
	require.Contains(t, got, "create table `db1`.`__mo_diff_del_x` as select `id` as `branch_apply_key_0` from `db1`.`base_t` where 1=0;\n")
	require.Contains(t, got, "create table `db1`.`__mo_diff_ins_x` as select `id`,`name` from `db1`.`base_t` where 1=0;\n")
	require.Contains(t, got, "create table `db1`.`__mo_diff_upd_x` as select `id`,`name`,`id` as `branch_apply_key_0` from `db1`.`base_t` where 1=0;\n")
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
	tblStuff.def.colNames = []string{"id", "name_new"}
	tblStuff.def.baseColNames = []string{"id", "name"}
	tblStuff.def.visibleIdxes = []int{0, 1}
	tblStuff.def.writableIdxes = []int{0, 1}
	tblStuff.def.pkColIdx = 0
	tblStuff.def.pkColIdxes = []int{0, 1}

	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "t1",
		deleteTable:      "__mo_diff_del_x",
		insertTable:      "__mo_diff_ins_x",
		updateTable:      "__mo_diff_upd_x",
		deleteKeyNames:   []string{"id", "name"},
		deleteStageNames: []string{"branch_apply_key_0", "branch_apply_key_1"},
		deleteKeyTypes:   []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()},
		writableNames:    []string{"id", "name"},
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

	out.Reset()
	floatBatchInfo := *batchInfo
	floatBatchInfo.disableInsertStage = true
	floatBatchInfo.insertRowsIndividually = true
	deleteCnt, insertCnt := 0, 0
	deleteBuf, insertBuf := &bytes.Buffer{}, &bytes.Buffer{}
	appender := sqlValuesAppender{
		ctx: context.Background(), tblStuff: tblStuff, batchInfo: &floatBatchInfo,
		deleteCnt: &deleteCnt, deleteBuf: deleteBuf, insertCnt: &insertCnt,
		insertBuf: insertBuf, writeFile: writeFile,
	}
	require.NoError(t, appender.appendRow(diffInsert, []byte("(1,'first')")))
	require.NoError(t, appender.appendRow(diffInsert, []byte("(2,'second')")))
	require.NoError(t, appender.flushAll())
	require.Equal(t, 2, strings.Count(out.String(), "insert into `db1`.`t1` (`id`,`name`) values "))
	require.NotContains(t, out.String(), "(1,'first'),(2,'second')")
	require.NotContains(t, out.String(), "__mo_diff_ins_x")
}

func TestDataBranchOutputFlushSqlValuesUsesExactFloatKeyMatch(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "t1",
		deleteTable:      "__mo_diff_del_x",
		deleteKeyNames:   []string{"float_key", "double_key", "int_key"},
		deleteStageNames: []string{"branch_apply_key_0", "branch_apply_key_1", "branch_apply_key_2"},
		deleteKeyTypes: []types.Type{
			types.T_float32.ToType(),
			types.T_float64.ToType(),
			types.T_int64.ToType(),
		},
	}

	var out bytes.Buffer
	require.NoError(t, flushSqlValues(
		context.Background(), nil, nil, tableStuff{}, bytes.NewBufferString("(1,2,3)"),
		true, false, batchInfo, func(b []byte) error {
			_, err := out.Write(b)
			return err
		},
	))

	got := out.String()
	require.Contains(t, got, "insert into `db1`.`__mo_diff_del_x` values (1,2,3);\n")
	require.Contains(t, got,
		"delete branch_apply_base from `db1`.`t1` as branch_apply_base join `db1`.`__mo_diff_del_x` as branch_apply_stage on "+
			"serial(branch_apply_base.`float_key`) = serial(branch_apply_stage.`branch_apply_key_0`) AND "+
			"serial(branch_apply_base.`double_key`) = serial(branch_apply_stage.`branch_apply_key_1`) AND "+
			"branch_apply_base.`int_key` = branch_apply_stage.`branch_apply_key_2`;\n")
	require.Contains(t, got, "delete from `db1`.`__mo_diff_del_x`;\n")
}

func TestDataBranchOutputFlushStagedUpdateValues(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:            "db1",
		baseTable:         "t1",
		updateTable:       "__mo_diff_upd_x",
		deleteKeyNames:    []string{"org_id", "event_id"},
		deleteStageNames:  []string{"branch_apply_key_0", "branch_apply_key_1"},
		deleteKeyTypes:    []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		writableNames:     []string{"org_id", "event_id", "qty", "note"},
		stagedUpdateNames: []string{"qty", "note"},
	}

	var out bytes.Buffer
	require.NoError(t, flushStagedUpdateValues(
		context.Background(), nil, nil, []byte("(1,2,30,'changed',1,2),(2,1,40,null,2,1)"), batchInfo,
		func(b []byte) error {
			_, err := out.Write(b)
			return err
		},
	))

	got := out.String()
	require.Contains(t, got, "insert into `db1`.`__mo_diff_upd_x` values (1,2,30,'changed',1,2),(2,1,40,null,2,1);\n")
	require.Contains(t, got,
		"update `db1`.`t1` as branch_apply_base join `db1`.`__mo_diff_upd_x` as branch_apply_stage on "+
			"branch_apply_base.`org_id` = branch_apply_stage.`branch_apply_key_0` AND "+
			"branch_apply_base.`event_id` = branch_apply_stage.`branch_apply_key_1` set "+
			"branch_apply_base.`qty` = branch_apply_stage.`qty`,branch_apply_base.`note` = branch_apply_stage.`note`;\n")
	require.Contains(t, got, "delete from `db1`.`__mo_diff_upd_x`;\n")
}

func TestDataBranchOutputStagedUpdateGeneratedPrimaryKey(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:            "db1",
		baseTable:         "t1",
		updateTable:       "__mo_diff_upd_x",
		deleteKeyNames:    []string{"generated_key"},
		deleteStageNames:  []string{"branch_apply_key_0"},
		deleteKeyTypes:    []types.Type{types.T_int64.ToType()},
		writableNames:     []string{"input", "payload"},
		stagedUpdateNames: []string{"input", "payload"},
	}

	var out bytes.Buffer
	require.NoError(t, flushStagedUpdateValues(
		context.Background(), nil, nil, []byte("(1,'changed',2)"), batchInfo,
		func(b []byte) error {
			_, err := out.Write(b)
			return err
		},
	))

	require.Equal(t,
		"insert into `db1`.`__mo_diff_upd_x` values (1,'changed',2);\n"+
			"update `db1`.`t1` as branch_apply_base join `db1`.`__mo_diff_upd_x` as branch_apply_stage on branch_apply_base.`generated_key` = branch_apply_stage.`branch_apply_key_0` set branch_apply_base.`input` = branch_apply_stage.`input`,branch_apply_base.`payload` = branch_apply_stage.`payload`;\n"+
			"delete from `db1`.`__mo_diff_upd_x`;\n",
		out.String(),
	)
}

func TestDataBranchOutputFlushStagedSpecialUpdateValues(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:             "db1",
		baseTable:          "t1",
		updateTable:        "__mo_diff_upd_x",
		deleteKeyNames:     []string{"id"},
		deleteStageNames:   []string{"branch_apply_key_0"},
		deleteKeyTypes:     []types.Type{types.T_int64.ToType()},
		writableNames:      []string{"id", "payload", "status"},
		stagedUpdateNames:  []string{"payload"},
		specialUpdateNames: []string{"status"},
	}

	var out bytes.Buffer
	require.NoError(t, flushStagedUpdateValues(
		context.Background(), nil, nil, []byte("(1,'one','ready',1),(2,'two','new',2)"), batchInfo,
		func(b []byte) error {
			_, err := out.Write(b)
			return err
		},
	))

	got := out.String()
	require.Contains(t, got,
		"update `db1`.`t1` as branch_apply_base join `db1`.`__mo_diff_upd_x` as branch_apply_stage on "+
			"branch_apply_base.`id` = branch_apply_stage.`branch_apply_key_0` set branch_apply_base.`payload` = branch_apply_stage.`payload`;\n")
	require.Contains(t, got,
		"insert into `db1`.`t1` (`id`,`payload`,`status`) select `id`,`payload`,`status` from `db1`.`__mo_diff_upd_x` on duplicate key update `status` = values(`status`);\n")
	require.Equal(t, 1, strings.Count(got, "on duplicate key update `status` = values(`status`)"))
}

func TestDataBranchOutputStagedDeleteRejectsIncompleteKeyLayout(t *testing.T) {
	_, err := (&applyBatchInfo{
		deleteKeyNames:   []string{"id"},
		deleteStageNames: []string{"branch_apply_key_0"},
	}).stagedDeleteSQL("`db1`.`t1`", "`db1`.`delete_stage`")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid Data Branch staged delete key layout")
}

func TestDataBranchOutputTryFlushDeletesOrInserts(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "t1",
		deleteTable:      "__mo_diff_del_x",
		insertTable:      "__mo_diff_ins_x",
		updateTable:      "__mo_diff_upd_x",
		deleteKeyNames:   []string{"id"},
		deleteStageNames: []string{"branch_apply_key_0"},
		deleteKeyTypes:   []types.Type{types.T_int64.ToType()},
		writableNames:    []string{"id", "name"},
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
		require.Contains(t, out.String(), "delete from `db1`.`t1` where `id` in (select `branch_apply_key_0` from `db1`.`__mo_diff_del_x`);\n")
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
	tblStuff.def.baseColNames = []string{"id", "name", "age"}
	tblStuff.def.colTypes = []types.Type{
		types.T_float32.ToType(),
		types.T_varchar.ToType(),
		types.T_float64.ToType(),
	}
	tblStuff.def.pkColIdxes = []int{0, 2}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.writableIdxes = []int{0, 1, 2}
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
	require.Equal(t, []types.Type{types.T_float32.ToType(), types.T_float64.ToType()}, info.deleteKeyTypes)
	require.Equal(t, []string{"branch_apply_key_0", "branch_apply_key_1"}, info.deleteStageNames)
	require.Equal(t, []string{"id", "name", "age"}, info.writableNames)
	require.True(t, info.disableInsertStage)
	require.True(t, info.insertRowsIndividually)
	require.True(t, strings.HasPrefix(info.deleteTable, "__mo_diff_del_"))
	require.True(t, strings.HasPrefix(info.insertTable, "__mo_diff_ins_"))
	require.True(t, strings.HasPrefix(info.updateTable, "__mo_diff_upd_"))
	require.Equal(t, []int{0, 1, 2, 0, 2}, info.updateValueIdxes)

	fakeTblStuff := newFakePKBranchTableStuff(ctrl)
	deleteByFullRow, deleteKeyColIdxes, info = buildDataBranchApplyLayout(
		context.Background(), &Session{}, fakeTblStuff, dataBranchApplyModeOnlineMerge,
	)
	require.False(t, deleteByFullRow)
	require.Equal(t, []int{2}, deleteKeyColIdxes)
	require.NotNil(t, info)
	require.Equal(t, []string{"__mo_fake_pk_col"}, info.deleteKeyNames)
	require.Equal(t, []string{"branch_apply_key_0"}, info.deleteStageNames)
	require.Equal(t, []string{"id", "name"}, info.writableNames)
	require.True(t, info.disableInsertStage)
	require.False(t, info.insertRowsIndividually)

	deleteByFullRow, deleteKeyColIdxes, info = buildDataBranchApplyLayout(
		context.Background(), &Session{}, fakeTblStuff, dataBranchApplyModeOnlinePKOnly,
	)
	require.False(t, deleteByFullRow)
	require.Equal(t, []int{0, 1}, deleteKeyColIdxes)
	require.NotNil(t, info)
	require.False(t, info.disableInsertStage)
	require.False(t, info.insertRowsIndividually)

	deleteByFullRow, deleteKeyColIdxes, info = buildDataBranchApplyLayout(
		context.Background(), &Session{}, fakeTblStuff, dataBranchApplyModePortableSQL,
	)
	require.True(t, deleteByFullRow)
	require.Nil(t, deleteKeyColIdxes)
	require.Nil(t, info)
}

func TestDataBranchApplyLayoutUsesDestinationColumnNames(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	baseRel := mock_frontend.NewMockRelation(ctrl)
	baseRel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{
		DbName: "db1",
		Name:   "base",
	}).AnyTimes()
	baseRel.EXPECT().GetTableName().Return("base").AnyTimes()

	for _, tc := range []struct {
		name            string
		sourceName      string
		destinationName string
	}{
		{name: "source renamed", sourceName: "payload_new", destinationName: "payload"},
		{name: "destination renamed", sourceName: "payload", destinationName: "payload_new"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tblStuff := tableStuff{baseRel: baseRel}
			tblStuff.def.colNames = []string{"id", tc.sourceName}
			tblStuff.def.baseColNames = []string{"id", tc.destinationName}
			tblStuff.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()}
			tblStuff.def.pkColIdxes = []int{0}
			tblStuff.def.visibleIdxes = []int{0, 1}
			tblStuff.def.writableIdxes = []int{0, 1}
			tblStuff.def.pkKind = normalKind

			_, _, info := buildDataBranchApplyLayout(
				ctx, &Session{}, tblStuff, dataBranchApplyModeOnlineMerge,
			)
			require.NotNil(t, info)
			require.Equal(t, []string{"id"}, info.deleteKeyNames)
			require.Equal(t, []string{"id", tc.destinationName}, info.writableNames)
		})
	}
}

func TestDataBranchOutputAppenderAppendRowAndFlushAll(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:           "db1",
		baseTable:        "t1",
		deleteTable:      "__mo_diff_del_x",
		insertTable:      "__mo_diff_ins_x",
		updateTable:      "__mo_diff_upd_x",
		deleteKeyNames:   []string{"id"},
		deleteStageNames: []string{"branch_apply_key_0"},
		deleteKeyTypes:   []types.Type{types.T_int64.ToType()},
		writableNames:    []string{"id", "name"},
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

func TestDataBranchOutputBatchesMixedSchemaSpecialUpdates(t *testing.T) {
	batchInfo := &applyBatchInfo{
		dbName:             "db1",
		baseTable:          "base",
		updateTable:        "__mo_diff_upd_x",
		deleteKeyNames:     []string{"id"},
		deleteStageNames:   []string{"branch_apply_key_0"},
		deleteKeyTypes:     []types.Type{types.T_int64.ToType()},
		writableNames:      []string{"id", "payload", "status"},
		stagedUpdateNames:  []string{"payload"},
		specialUpdateNames: []string{"status"},
	}
	deleteCnt, insertCnt := 0, 0
	deleteBuf, insertBuf := &bytes.Buffer{}, &bytes.Buffer{}
	var out bytes.Buffer
	appender := sqlValuesAppender{
		ctx:         context.Background(),
		batchInfo:   batchInfo,
		deleteCnt:   &deleteCnt,
		deleteBuf:   deleteBuf,
		insertCnt:   &insertCnt,
		insertBuf:   insertBuf,
		updateState: &dataBranchUpdateBuffer{},
		writeFile: func(b []byte) error {
			_, err := out.Write(b)
			return err
		},
	}

	require.True(t, dataBranchStagesUpdate(appender, true, false, false))
	for i := 0; i < maxSqlBatchCnt+1; i++ {
		require.NoError(t, appender.appendRow(diffUpdate, []byte(fmt.Sprintf("(%d,'payload-%d','ready',%d)", i, i, i))))
	}
	require.NoError(t, appender.flushAll())

	got := out.String()
	// The count is bounded by stage batches, not the number of updated rows.
	require.Equal(t, 2, strings.Count(got, "insert into `db1`.`__mo_diff_upd_x` values"))
	require.Equal(t, 2, strings.Count(got, "on duplicate key update `status` = values(`status`)"))
	require.Less(t, strings.Count(got, "on duplicate key update `status` = values(`status`)"), maxSqlBatchCnt+1)
}

func TestDataBranchOutputNewSingleWriteAppenderNilWorker(t *testing.T) {
	_, _, err := newSingleWriteAppender(context.Background(), nil, nil, "unused")
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

	writeFile, release, err := newSingleWriteAppender(ctx, pool, etlFS, targetPath)
	require.NoError(t, err)

	require.NoError(t, writeFile([]byte("BEGIN;\n")))
	require.NoError(t, writeFile([]byte("COMMIT;\n")))
	require.NoError(t, release())

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

type lateFailingWriteFS struct {
	fileservice.FileService
	deleteCalls int
}

func (fs *lateFailingWriteFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	for _, entry := range vector.Entries {
		if entry.ReaderForWrite != nil {
			if _, err := io.Copy(io.Discard, entry.ReaderForWrite); err != nil {
				return err
			}
		}
	}
	return moerr.NewInternalErrorNoCtx("mock late write failure")
}

func (fs *lateFailingWriteFS) Delete(ctx context.Context, filePaths ...string) error {
	fs.deleteCalls++
	return fs.FileService.Delete(ctx, filePaths...)
}

type closeFailingMutator struct {
	closeCalls int
}

func (m *closeFailingMutator) Mutate(context.Context, ...fileservice.IOEntry) error {
	return nil
}

func (m *closeFailingMutator) Append(context.Context, ...fileservice.IOEntry) error {
	return nil
}

func (m *closeFailingMutator) Close() error {
	m.closeCalls++
	return moerr.NewInternalErrorNoCtx("mock mutator close failure")
}

type closeFailingMutableFS struct {
	fileservice.MutableFileService
	mutator fileservice.Mutator
}

func (fs *closeFailingMutableFS) NewMutator(
	context.Context,
	string,
) (fileservice.Mutator, error) {
	return fs.mutator, nil
}

func installDataBranchTestFileService(
	t *testing.T,
	ses *Session,
	fileService fileservice.FileService,
) {
	t.Helper()
	pu := getPu(ses.GetService())
	originalFS := pu.FileService
	pu.FileService = fileService
	t.Cleanup(func() {
		pu.FileService = originalFS
	})
}

func TestDataBranchOutputNewSingleWriteAppenderReturnsLateWriteFailure(t *testing.T) {
	ctx := context.Background()
	etlFS, _, err := fileservice.GetForETL(ctx, nil, filepath.Join(t.TempDir(), "diff.sql"))
	require.NoError(t, err)

	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()

	writeFile, release, err := newSingleWriteAppender(
		ctx,
		pool,
		&lateFailingWriteFS{FileService: etlFS},
		"diff.sql",
	)
	require.NoError(t, err)
	require.NoError(t, writeFile([]byte("BEGIN;\n")))
	require.NoError(t, writeFile([]byte("COMMIT;\n")))
	require.EqualError(t, release(), "internal error: mock late write failure")
}

func TestDataBranchOutputReturnsLateFileWriteFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ses := newValidateSession(t)
	ses.SetMysqlResultSet(&MysqlResultSet{})

	localFS, err := fileservice.NewLocalETLFS(defines.SharedFileServiceName, t.TempDir())
	require.NoError(t, err)
	targetFS := &lateFailingWriteFS{FileService: localFS}
	installDataBranchTestFileService(t, ses, targetFS)

	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()

	ctrl := gomock.NewController(t)
	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.pkKind = fakeKind
	tblStuff.worker = pool

	retCh := make(chan batchWithKind)
	close(retCh)
	stmt := &tree.DataBranchDiff{
		OutputOpt: &tree.DiffOutputOpt{DirPath: defines.SharedFileServiceName + ":/diff"},
	}

	err = satisfyDiffOutputOpt(
		ctx,
		cancel,
		func() {},
		ses,
		nil,
		stmt,
		branchMetaInfo{},
		tblStuff,
		retCh,
	)
	require.EqualError(t, err, "internal error: mock late write failure")
	require.Zero(t, ses.GetMysqlResultSet().GetRowCount())
	require.Equal(t, 2, targetFS.deleteCalls, "prepare and failed-output cleanup must both delete")
}

func TestDataBranchOutputMutableAppenderReturnsCloseFailure(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)

	localFS, err := fileservice.NewLocalETLFS(defines.SharedFileServiceName, t.TempDir())
	require.NoError(t, err)
	mutator := &closeFailingMutator{}
	targetFS := &closeFailingMutableFS{
		MutableFileService: localFS,
		mutator:            mutator,
	}
	installDataBranchTestFileService(t, ses, targetFS)

	ctrl := gomock.NewController(t)
	stmt := &tree.DataBranchDiff{
		OutputOpt: &tree.DiffOutputOpt{DirPath: defines.SharedFileServiceName + ":/diff"},
	}
	_, _, writeFile, release, cleanup, err := prepareFSForDiffAsFile(
		ctx,
		ses,
		stmt,
		newTestBranchTableStuff(ctrl),
	)
	require.NoError(t, err)
	require.NoError(t, writeFile([]byte("BEGIN;\nCOMMIT;\n")))
	require.EqualError(t, release(), "internal error: mock mutator close failure")
	require.Equal(t, 1, mutator.closeCalls)
	cleanup()
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

	writeFile, release, err := newSingleWriteAppender(
		ctx,
		pool,
		&failingWriteFS{FileService: etlFS},
		"diff.sql",
	)
	require.NoError(t, err)

	_ = writeFile([]byte("SOME SQL;\n"))
	require.EqualError(t, release(), "internal error: mock write failure")
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

	_, _, err = newSingleWriteAppender(ctx, pool, etlFS, "diff.sql")
	require.Error(t, err)
}

func TestNewApplyBatchInfoUsesCommonVisibleColumnsForEvolvedSchema(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"a", "__mo_cbkey_001a", "c", "b"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 2, 3}
	tblStuff.def.writableIdxes = []int{0, 2, 3}
	tblStuff.def.commonIdxes = []int{0, 1, 3}
	tblStuff.def.commonVisibleIdxes = []int{0, 3}
	tblStuff.def.commonWritableIdxes = []int{0, 3}
	tblStuff.def.tarOnlyIdxes = []int{2}
	tblStuff.def.baseColNames = []string{"a", "", "", "b"}

	info := newApplyBatchInfo(ctx, ses, tblStuff, []int{0}, false)
	require.NotNil(t, info)
	require.Equal(t, []string{"a"}, info.deleteKeyNames)
	require.Equal(t, []string{"a", "b"}, info.writableNames)
}

func TestNewApplyBatchInfoExcludesGeneratedColumns(t *testing.T) {
	ctx := context.Background()
	ses := newValidateSession(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tblStuff := newTestBranchTableStuff(ctrl)
	tblStuff.def.colNames = []string{"id", "value", "generated_value"}
	tblStuff.def.baseColNames = []string{"id", "value", "generated_value"}
	tblStuff.def.visibleIdxes = []int{0, 1, 2}
	tblStuff.def.writableIdxes = []int{0, 1}
	tblStuff.def.commonVisibleIdxes = []int{0, 1, 2}
	tblStuff.def.commonWritableIdxes = []int{0, 1}

	projected, err := resolveProjectedIdxes(
		tree.IdentifierList{tree.Identifier("generated_value")}, tblStuff,
	)
	require.NoError(t, err)
	require.Equal(t, []int{0, 2}, projected)

	info := newApplyBatchInfo(ctx, ses, tblStuff, []int{0}, false)
	require.NotNil(t, info)
	require.Equal(t, []string{"id", "value"}, info.writableNames)
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

	t.Run("stages ordinary primary-key updates", func(t *testing.T) {
		directUpdate, err := dataBranchDirectUpdateBatch(
			tblStuff, batchWithKind{kind: diffInsert, fromUpdate: true}, &applyBatchInfo{},
		)
		require.NoError(t, err)
		require.True(t, directUpdate)

		fakePKTable := tblStuff
		fakePKTable.def.pkKind = fakeKind
		directUpdate, err = dataBranchDirectUpdateBatch(
			fakePKTable, batchWithKind{kind: diffInsert, fromUpdate: true}, &applyBatchInfo{},
		)
		require.NoError(t, err)
		require.False(t, directUpdate)

		directUpdateTable := tblStuff
		directUpdateTable.def.baseColNames = []string{"id", "name", "hidden"}
		batchInfo := &applyBatchInfo{
			dbName:            "db1",
			baseTable:         "base",
			updateTable:       "__mo_diff_upd_x",
			deleteKeyNames:    []string{"id"},
			deleteStageNames:  []string{"branch_apply_key_0"},
			deleteKeyTypes:    []types.Type{types.T_int64.ToType()},
			writableNames:     []string{"id", "name"},
			stagedUpdateNames: []string{"name"},
			updateValueIdxes:  []int{0, 1, 0},
		}
		deleteCnt, insertCnt := 0, 0
		deleteBuf, insertBuf := &bytes.Buffer{}, &bytes.Buffer{}
		var out bytes.Buffer
		appender := sqlValuesAppender{
			ctx:         context.Background(),
			tblStuff:    directUpdateTable,
			batchInfo:   batchInfo,
			deleteCnt:   &deleteCnt,
			deleteBuf:   deleteBuf,
			insertCnt:   &insertCnt,
			insertBuf:   insertBuf,
			updateState: &dataBranchUpdateBuffer{},
			writeFile: func(b []byte) error {
				_, err := out.Write(b)
				return err
			},
		}

		deleteBat := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{{int64(2), "before"}})
		defer deleteBat.Clean(ses.proc.Mp())
		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(), ses, directUpdateTable,
			batchWithKind{kind: diffDelete, fromUpdate: true, batch: deleteBat},
			&bytes.Buffer{}, appender,
		))
		require.Empty(t, out.String())

		insertBat := buildVisibleComparisonBatch(t, ses.proc.Mp(), [][]any{{int64(2), "after"}})
		defer insertBat.Clean(ses.proc.Mp())
		require.NoError(t, appendBatchRowsAsSQLValues(
			context.Background(), ses, directUpdateTable,
			batchWithKind{kind: diffInsert, fromUpdate: true, batch: insertBat},
			&bytes.Buffer{}, appender,
		))
		require.Empty(t, out.String())
		require.NoError(t, appender.flushAll())
		require.Equal(t,
			"insert into `db1`.`__mo_diff_upd_x` values (2,'after',2);\n"+
				"update `db1`.`base` as branch_apply_base join `db1`.`__mo_diff_upd_x` as branch_apply_stage on branch_apply_base.`id` = branch_apply_stage.`branch_apply_key_0` set branch_apply_base.`name` = branch_apply_stage.`name`;\n"+
				"delete from `db1`.`__mo_diff_upd_x`;\n",
			out.String(),
		)
	})

	t.Run("partitions planner-incompatible value types from staged assignments", func(t *testing.T) {
		newSpecialTable := func(typ types.Type, enumValues string) (tableStuff, *plan.TableDef) {
			var tbl tableStuff
			tbl.def.colTypes = []types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), typ}
			tbl.def.writableIdxes = []int{0, 1, 2}
			tbl.def.pkColIdxes = []int{0}
			tbl.def.baseColNames = []string{"id", "payload", "value"}
			return tbl, &plan.TableDef{Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar)}},
				{Name: "value", Typ: plan.Type{Id: int32(typ.Oid), Enumvalues: enumValues}},
			}}
		}

		setTable, setDef := newSpecialTable(types.T_uint64.ToType(), "a,b,c")
		staged, special := dataBranchStagedUpdateColumnNames(setTable, setDef, setTable.def.writableIdxes)
		require.Equal(t, []string{"payload"}, staged)
		require.Equal(t, []string{"value"}, special)

		geometryTable, geometryDef := newSpecialTable(types.T_geometry32.ToType(), "")
		staged, special = dataBranchStagedUpdateColumnNames(geometryTable, geometryDef, geometryTable.def.writableIdxes)
		require.Equal(t, []string{"payload"}, staged)
		require.Equal(t, []string{"value"}, special)

		ordinaryTable, ordinaryDef := newSpecialTable(types.T_varchar.ToType(), "")
		staged, special = dataBranchStagedUpdateColumnNames(ordinaryTable, ordinaryDef, ordinaryTable.def.writableIdxes)
		require.Equal(t, []string{"payload", "value"}, staged)
		require.Empty(t, special)

		indexedEnumTable, indexedEnumDef := newSpecialTable(types.T_uint64.ToType(), "a,b,c")
		indexedEnumDef.Indexes = []*plan.IndexDef{{
			IndexName: "uk_value",
			Parts:     []string{"value"},
			Unique:    true,
		}}
		indexedEnumTable.def.indexedSpecialUpdateIdxes = dataBranchIndexedSpecialUpdateColIdxes(
			indexedEnumTable, indexedEnumDef, indexedEnumTable.def.writableIdxes,
		)
		require.Equal(t, []int{2}, indexedEnumTable.def.indexedSpecialUpdateIdxes)
		staged, special = dataBranchStagedUpdateColumnNames(indexedEnumTable, indexedEnumDef, indexedEnumTable.def.writableIdxes)
		require.Equal(t, []string{"payload"}, staged)
		require.Empty(t, special)
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
	// Exercise the direct no-PK SQL paths with a source-side rename. The row
	// layout keeps the source name while destination SQL must use baseColNames.
	tblStuff.def.colNames[1] = "name_new"

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
	tblStuff.def.baseColNames = []string{"id", "name", "__mo_fake_pk_col"}
	tblStuff.def.colTypes = []types.Type{
		types.T_int64.ToType(),
		types.T_varchar.ToType(),
		types.T_uint64.ToType(),
	}
	tblStuff.def.visibleIdxes = []int{0, 1}
	tblStuff.def.writableIdxes = []int{0, 1}
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
