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
	"container/heap"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/panjf2000/ants/v2"
)

func makeFileName(
	baseAtTsExpr *tree.AtTimeStamp,
	tarAtTsExpr *tree.AtTimeStamp,
	tblStuff tableStuff,
) (string, error) {
	return makeFileNameWithUUID(baseAtTsExpr, tarAtTsExpr, tblStuff, uuid.NewRandom)
}

func makeFileNameWithUUID(
	baseAtTsExpr *tree.AtTimeStamp,
	tarAtTsExpr *tree.AtTimeStamp,
	tblStuff tableStuff,
	newUUID func() (uuid.UUID, error),
) (string, error) {
	var (
		srcName  = encodeDiffFileNamePart(tblStuff.tarRel.GetTableName())
		baseName = encodeDiffFileNamePart(tblStuff.baseRel.GetTableName())
	)

	if baseAtTsExpr != nil {
		baseName = fmt.Sprintf("%s_%s", baseName, encodeDiffFileNamePart(baseAtTsExpr.SnapshotName))
	}

	if tarAtTsExpr != nil {
		srcName = fmt.Sprintf("%s_%s", srcName, encodeDiffFileNamePart(tarAtTsExpr.SnapshotName))
	}

	namePrefix := fmt.Sprintf("diff_%s_%s", srcName, baseName)
	id, err := newUUID()
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("generate data branch output file name: %v", err)
	}
	uniqueSuffix := fmt.Sprintf("_%s_%s", time.Now().UTC().Format("20060102_150405"), id)
	if len(namePrefix)+len(uniqueSuffix) <= maxDiffFileNameStemBytes {
		return namePrefix + uniqueSuffix, nil
	}

	digest := sha256.Sum256([]byte(namePrefix))
	digestSuffix := fmt.Sprintf("_%x%s", digest[:16], uniqueSuffix)
	return truncateDiffFileNamePrefix(namePrefix, maxDiffFileNameStemBytes-len(digestSuffix)) + digestSuffix, nil
}

const maxDiffFileNameStemBytes = 240

func encodeDiffFileNamePart(name string) string {
	const hex = "0123456789ABCDEF"

	var encoded strings.Builder
	encoded.Grow(len(name))
	for i := 0; i < len(name); {
		c := name[i]
		if c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' {
			encoded.WriteByte(c)
			i++
			continue
		}
		if c >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(name[i:])
			if !(r == utf8.RuneError && size == 1) && unicode.IsPrint(r) {
				encoded.WriteString(name[i : i+size])
				i += size
				continue
			}
		}
		encoded.WriteByte('@')
		encoded.WriteByte(hex[c>>4])
		encoded.WriteByte(hex[c&0x0f])
		i++
	}
	return encoded.String()
}

func truncateDiffFileNamePrefix(name string, maxBytes int) string {
	if len(name) <= maxBytes {
		return name
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(name[:end]) {
		end--
	}
	if escapeStart := strings.LastIndexByte(name[:end], '@'); escapeStart >= 0 && end-escapeStart < 3 {
		end = escapeStart
	}
	return name[:end]
}

type applyBatchInfo struct {
	dbName                 string
	baseTable              string
	deleteTable            string
	insertTable            string
	updateTable            string
	deleteKeyNames         []string
	deleteStageNames       []string
	deleteKeyTypes         []types.Type
	writableNames          []string
	stagedUpdateNames      []string
	specialUpdateNames     []string
	updateValueIdxes       []int
	disableInsertStage     bool
	insertRowsIndividually bool
}

func newSQLValuesAppender(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	mode dataBranchApplyMode,
	deleteCnt *int,
	deleteBuf *bytes.Buffer,
	insertCnt *int,
	insertBuf *bytes.Buffer,
	writeFile func([]byte) error,
) sqlValuesAppender {
	deleteByFullRow, deleteKeyColIdxes, batchInfo := buildDataBranchApplyLayout(ctx, ses, tblStuff, mode)
	return sqlValuesAppender{
		ctx:               ctx,
		ses:               ses,
		bh:                bh,
		tblStuff:          tblStuff,
		deleteByFullRow:   deleteByFullRow,
		deleteKeyColIdxes: deleteKeyColIdxes,
		batchInfo:         batchInfo,
		deleteCnt:         deleteCnt,
		deleteBuf:         deleteBuf,
		insertCnt:         insertCnt,
		insertBuf:         insertBuf,
		updateState:       &dataBranchUpdateBuffer{},
		writeFile:         writeFile,
	}
}

func (sva sqlValuesAppender) extraColIdxesForRow(kind string) []int {
	if kind != diffDelete || sva.deleteByFullRow {
		return nil
	}
	return sva.deleteKeyColIdxes
}

func buildDataBranchApplyLayout(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	mode dataBranchApplyMode,
) (deleteByFullRow bool, deleteKeyColIdxes []int, batchInfo *applyBatchInfo) {
	if mode == dataBranchApplyModePortableSQL && tblStuff.def.pkKind == fakeKind {
		return true, nil, nil
	}

	deleteKeyColIdxes = dataBranchDeleteKeyColIdxes(tblStuff, mode)
	disableInsertStage := tblStuff.def.pkKind == fakeKind && mode == dataBranchApplyModeOnlineMerge
	return false, deleteKeyColIdxes, newApplyBatchInfo(ctx, ses, tblStuff, deleteKeyColIdxes, disableInsertStage)
}

func dataBranchDeleteKeyColIdxes(tblStuff tableStuff, mode dataBranchApplyMode) []int {
	if tblStuff.def.pkKind == fakeKind && mode == dataBranchApplyModeOnlineMerge {
		return []int{tblStuff.def.pkColIdx}
	}
	return append([]int(nil), tblStuff.def.pkColIdxes...)
}

func newApplyBatchInfo(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	deleteKeyColIdxes []int,
	disableInsertStage bool,
) *applyBatchInfo {
	if len(deleteKeyColIdxes) == 0 {
		return nil
	}

	deleteKeyNames := make([]string, len(deleteKeyColIdxes))
	deleteStageNames := make([]string, len(deleteKeyColIdxes))
	deleteKeyTypes := make([]types.Type, len(deleteKeyColIdxes))
	insertRowsIndividually := false
	for i, idx := range deleteKeyColIdxes {
		deleteKeyNames[i] = tblStuff.def.baseColNames[idx]
		deleteStageNames[i] = fmt.Sprintf("branch_apply_key_%d", i)
		deleteKeyTypes[i] = tblStuff.def.colTypes[idx]
		insertRowsIndividually = insertRowsIndividually || isDataBranchFloatType(deleteKeyTypes[i])
	}
	// MatrixOne accepts bit-distinct FLOAT/DOUBLE primary keys when each row is
	// inserted independently. Multi-row INSERT and INSERT ... SELECT currently
	// compare those keys with scalar float semantics, which collapses NaN
	// payloads and signed zero. Keep the generated apply path aligned with the
	// bit-preserving primary-key identity used by storage.
	disableInsertStage = disableInsertStage || insertRowsIndividually

	writableIdxes := tblStuff.def.writableIdxes
	if len(tblStuff.def.tarOnlyIdxes) > 0 {
		writableIdxes = tblStuff.def.commonWritableIdxes
	}
	writableNames := make([]string, len(writableIdxes))
	for i, idx := range writableIdxes {
		writableNames[i] = tblStuff.def.baseColNames[idx]
	}
	var baseDef *plan2.TableDef
	if tblStuff.baseRel != nil {
		baseDef = tblStuff.baseRel.GetTableDef(ctx)
	}
	stagedUpdateNames, specialUpdateNames := dataBranchStagedUpdateColumnNames(tblStuff, baseDef, writableIdxes)
	updateValueIdxes := make([]int, 0, len(writableIdxes)+len(deleteKeyColIdxes))
	updateValueIdxes = append(updateValueIdxes, writableIdxes...)
	updateValueIdxes = append(updateValueIdxes, deleteKeyColIdxes...)

	seq := atomic.AddUint64(&diffTempTableSeq, 1)
	sessionTag := strings.ReplaceAll(ses.GetUUIDString(), "-", "")
	return &applyBatchInfo{
		dbName:                 tblStuff.baseRel.GetTableDef(ctx).DbName,
		baseTable:              tblStuff.baseRel.GetTableName(),
		deleteTable:            fmt.Sprintf(dataBranchApplyTablePrefix+"del_%s_%d", sessionTag, seq),
		insertTable:            fmt.Sprintf(dataBranchApplyTablePrefix+"ins_%s_%d", sessionTag, seq),
		updateTable:            fmt.Sprintf(dataBranchApplyTablePrefix+"upd_%s_%d", sessionTag, seq),
		deleteKeyNames:         deleteKeyNames,
		deleteStageNames:       deleteStageNames,
		deleteKeyTypes:         deleteKeyTypes,
		writableNames:          writableNames,
		stagedUpdateNames:      stagedUpdateNames,
		specialUpdateNames:     specialUpdateNames,
		updateValueIdxes:       updateValueIdxes,
		disableInsertStage:     disableInsertStage,
		insertRowsIndividually: insertRowsIndividually,
	}
}

func (batchInfo *applyBatchInfo) validateDeleteKeyLayout() error {
	if batchInfo == nil || len(batchInfo.deleteKeyNames) == 0 ||
		len(batchInfo.deleteStageNames) != len(batchInfo.deleteKeyNames) ||
		len(batchInfo.deleteKeyTypes) != len(batchInfo.deleteKeyNames) {
		return moerr.NewInternalErrorNoCtx("invalid Data Branch staged delete key layout")
	}
	return nil
}

func (batchInfo *applyBatchInfo) deleteNeedsExactFloatKeyMatch() bool {
	for _, typ := range batchInfo.deleteKeyTypes {
		if isDataBranchFloatType(typ) {
			return true
		}
	}
	return false
}

func (batchInfo *applyBatchInfo) stagedDeleteSQL(baseTable, deleteTable string) (string, error) {
	if err := batchInfo.validateDeleteKeyLayout(); err != nil {
		return "", err
	}
	deleteStageNames := batchInfo.deleteStageNames
	if !batchInfo.deleteNeedsExactFloatKeyMatch() {
		pkExpr := quoteIdentifierForSQL(batchInfo.deleteKeyNames[0])
		if len(batchInfo.deleteKeyNames) > 1 {
			pkExpr = fmt.Sprintf("(%s)", joinQuotedColumnNames(batchInfo.deleteKeyNames))
		}
		return fmt.Sprintf(
			"delete from %s where %s in (select %s from %s)",
			baseTable, pkExpr, joinQuotedColumnNames(deleteStageNames), deleteTable,
		), nil
	}

	const baseAlias = "branch_apply_base"
	const stageAlias = "branch_apply_stage"
	predicates := make([]string, len(batchInfo.deleteKeyNames))
	for i := range batchInfo.deleteKeyNames {
		left := fmt.Sprintf("%s.%s", baseAlias, quoteIdentifierForSQL(batchInfo.deleteKeyNames[i]))
		right := fmt.Sprintf("%s.%s", stageAlias, quoteIdentifierForSQL(deleteStageNames[i]))
		predicates[i] = dataBranchSQLKeyEqual(left, right, batchInfo.deleteKeyTypes[i])
	}
	return fmt.Sprintf(
		"delete %s from %s as %s join %s as %s on %s",
		baseAlias, baseTable, baseAlias, deleteTable, stageAlias, strings.Join(predicates, " AND "),
	), nil
}

func (batchInfo *applyBatchInfo) stagedUpdateSQL(baseTable, updateTable string) (string, error) {
	if err := batchInfo.validateDeleteKeyLayout(); err != nil {
		return "", err
	}
	if batchInfo.disableInsertStage {
		return "", moerr.NewInternalErrorNoCtx("Data Branch update staging is disabled")
	}

	const baseAlias = "branch_apply_base"
	const stageAlias = "branch_apply_stage"

	assignments := make([]string, 0, len(batchInfo.stagedUpdateNames))
	for _, name := range batchInfo.stagedUpdateNames {
		quotedName := quoteIdentifierForSQL(name)
		assignments = append(assignments, fmt.Sprintf("%s.%s = %s.%s", baseAlias, quotedName, stageAlias, quotedName))
	}
	if len(assignments) == 0 {
		return "", nil
	}

	predicates := make([]string, len(batchInfo.deleteKeyNames))
	for i := range batchInfo.deleteKeyNames {
		left := fmt.Sprintf("%s.%s", baseAlias, quoteIdentifierForSQL(batchInfo.deleteKeyNames[i]))
		right := fmt.Sprintf("%s.%s", stageAlias, quoteIdentifierForSQL(batchInfo.deleteStageNames[i]))
		predicates[i] = dataBranchSQLKeyEqual(left, right, batchInfo.deleteKeyTypes[i])
	}
	return fmt.Sprintf(
		"update %s as %s join %s as %s on %s set %s",
		baseTable, baseAlias, updateTable, stageAlias, strings.Join(predicates, " AND "), strings.Join(assignments, ","),
	), nil
}

// stagedSpecialUpsertSQL applies assignments that MatrixOne cannot execute in
// a multi-table UPDATE. The stage retains the full writable source row; an
// INSERT ... SELECT with ON DUPLICATE KEY UPDATE preserves the existing key
// while applying only those assignments.
func (batchInfo *applyBatchInfo) stagedSpecialUpsertSQL(baseTable, updateTable string) []string {
	if len(batchInfo.specialUpdateNames) == 0 {
		return nil
	}

	columns := joinQuotedColumnNames(batchInfo.writableNames)
	assignments := make([]string, len(batchInfo.specialUpdateNames))
	for i, name := range batchInfo.specialUpdateNames {
		quotedName := quoteIdentifierForSQL(name)
		assignments[i] = fmt.Sprintf("%s = values(%s)", quotedName, quotedName)
	}
	return []string{fmt.Sprintf(
		"insert into %s (%s) select %s from %s on duplicate key update %s",
		baseTable, columns, columns, updateTable, strings.Join(assignments, ","),
	)}
}

func mergeDiffs(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchMerge,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		insertCnt int
		deleteCnt int

		insertIntoVals = acquireBuffer(tblStuff.bufPool)
		deleteFromVals = acquireBuffer(tblStuff.bufPool)
		firstErr       error
		tmpValsBuffer  = acquireBuffer(tblStuff.bufPool)
	)

	defer func() {
		releaseBuffer(tblStuff.bufPool, insertIntoVals)
		releaseBuffer(tblStuff.bufPool, deleteFromVals)
		releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
	}()

	defer func() {
		cancel()
	}()

	appender := newSQLValuesAppender(
		ctx, ses, bh, tblStuff, dataBranchApplyModeOnlineMerge,
		&deleteCnt, deleteFromVals, &insertCnt, insertIntoVals, nil,
	)
	if err = initApplyTables(ctx, ses, bh, appender.batchInfo, appender.writeFile); err != nil {
		return err
	}
	defer func() {
		if err2 := dropApplyTables(ctx, ses, bh, appender.batchInfo, appender.writeFile); err2 != nil && err == nil {
			err = err2
		}
	}()

	// conflict option should be pushed down to the hash phase,
	// so the batch we received is conflict-free.
	for wrapped := range retCh {

		if firstErr != nil || ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		if err = appendBatchRowsAsSQLValues(
			ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
		); err != nil {
			firstErr = err
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
	}

	if err = appender.flushAll(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return firstErr
	}
	return
}

// resolveProjectedIdxes resolves the user-specified COLUMNS list to the
// effective visible output columns. Explicit primary-key columns are emitted
// first, in primary-key order, followed by the requested columns in request
// order. A fake primary key represents all visible columns and is not added to
// a projection. Returns nil if no COLUMNS clause was specified.
func resolveProjectedIdxes(columns tree.IdentifierList, tblStuff tableStuff) ([]int, error) {
	if columns == nil {
		return nil, nil
	}

	nameToIdx := make(map[string]int, len(tblStuff.def.visibleIdxes))
	for _, idx := range tblStuff.def.visibleIdxes {
		nameToIdx[strings.ToLower(tblStuff.def.colNames[idx])] = idx
	}

	projected := make([]int, 0, len(columns)+len(tblStuff.def.pkColIdxes))
	seen := make(map[int]bool, len(columns)+len(tblStuff.def.pkColIdxes))
	if tblStuff.def.pkKind != fakeKind {
		for _, idx := range tblStuff.def.pkColIdxes {
			if seen[idx] {
				continue
			}
			seen[idx] = true
			projected = append(projected, idx)
		}
	}

	for _, col := range columns {
		idx, ok := nameToIdx[strings.ToLower(string(col))]
		if !ok {
			if tblStuff.tarRel != nil && tblStuff.tarRel.GetTableName() != "" {
				return nil, moerr.NewInvalidInputNoCtxf(
					"column %q not found in table %q",
					string(col),
					tblStuff.tarRel.GetTableName(),
				)
			}
			return nil, moerr.NewInvalidInputNoCtxf("column %q not found", string(col))
		}
		if seen[idx] {
			continue
		}
		seen[idx] = true
		projected = append(projected, idx)
	}

	return projected, nil
}

func validateProjectedColumns(stmt *tree.DataBranchDiff, tblStuff tableStuff) error {
	if stmt == nil || stmt.Columns == nil {
		return nil
	}

	if _, err := resolveProjectedIdxes(stmt.Columns, tblStuff); err != nil {
		return err
	}

	if stmt.OutputOpt != nil && len(stmt.OutputOpt.DirPath) != 0 {
		return moerr.NewNotSupportedNoCtx(
			"DATA BRANCH DIFF COLUMNS is not supported with OUTPUT FILE",
		)
	}

	return nil
}

const (
	diffOutputSourceColumn   = "__mo_diff_source"
	diffOutputFlagColumn     = "__mo_diff_flag"
	diffOutputCleanupTimeout = 30 * time.Second
)

// diffOutputTable describes the ordinary table materialized by
// DATA BRANCH DIFF ... OUTPUT AS. It deliberately contains no branch metadata:
// the table is a durable snapshot of a diff, not a new branch node.
type diffOutputTable struct {
	databaseName   string
	tableName      string
	columnNames    []string
	projectedIdxes []int
}

func newDiffOutputTable(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (*diffOutputTable, error) {
	if stmt == nil || stmt.OutputOpt == nil || stmt.OutputOpt.As.ObjectName == "" {
		return nil, moerr.NewInternalErrorNoCtx("DATA BRANCH DIFF OUTPUT AS destination is empty")
	}

	databaseName, tableName, err := branchTableName(ctx, ses, stmt.OutputOpt.As)
	if err != nil {
		return nil, err
	}

	projectedIdxes, err := resolveProjectedIdxes(stmt.Columns, tblStuff)
	if err != nil {
		return nil, err
	}
	if projectedIdxes == nil {
		projectedIdxes = append([]int(nil), tblStuff.def.visibleIdxes...)
	}

	columnNames := make([]string, 0, len(projectedIdxes)+2)
	usedNames := make(map[string]struct{}, len(projectedIdxes)+2)
	for _, idx := range projectedIdxes {
		name := tblStuff.def.colNames[idx]
		usedNames[strings.ToLower(name)] = struct{}{}
	}
	columnNames = append(columnNames,
		nextAvailableDiffOutputColumnName(diffOutputSourceColumn, usedNames),
		nextAvailableDiffOutputColumnName(diffOutputFlagColumn, usedNames),
	)
	for _, idx := range projectedIdxes {
		columnNames = append(columnNames, tblStuff.def.colNames[idx])
	}

	return &diffOutputTable{
		databaseName:   databaseName,
		tableName:      tableName,
		columnNames:    columnNames,
		projectedIdxes: projectedIdxes,
	}, nil
}

func nextAvailableDiffOutputColumnName(base string, used map[string]struct{}) string {
	for suffix := 0; ; suffix++ {
		name := base
		if suffix > 0 {
			name = fmt.Sprintf("%s_%d", base, suffix)
		}
		lowerName := strings.ToLower(name)
		if _, exists := used[lowerName]; exists {
			continue
		}
		used[lowerName] = struct{}{}
		return name
	}
}

func (output *diffOutputTable) qualifiedName() string {
	return qualifiedTableName(output.databaseName, output.tableName)
}

func (output *diffOutputTable) createSQL(ctx context.Context, tblStuff tableStuff) (string, error) {
	baseTableDef := tblStuff.baseRel.GetTableDef(ctx)
	if baseTableDef == nil {
		return "", moerr.NewInternalErrorNoCtx("DATA BRANCH DIFF OUTPUT AS base table definition is unavailable")
	}
	targetTableDef := tblStuff.tarRel.GetTableDef(ctx)
	if targetTableDef == nil {
		return "", moerr.NewInternalErrorNoCtx("DATA BRANCH DIFF OUTPUT AS target table definition is unavailable")
	}

	projectedNames := make([]string, len(output.projectedIdxes))
	projectedTargetOnly := make([]bool, len(output.projectedIdxes))
	targetOnlyIdxes := make(map[int]struct{}, len(tblStuff.def.tarOnlyIdxes))
	for _, idx := range tblStuff.def.tarOnlyIdxes {
		targetOnlyIdxes[idx] = struct{}{}
	}
	for i, idx := range output.projectedIdxes {
		projectedNames[i] = tblStuff.def.colNames[idx]
		_, projectedTargetOnly[i] = targetOnlyIdxes[idx]
	}
	outputColDefs, targetOnly, err := dataBranchOutputColumnDefs(
		targetTableDef, baseTableDef, projectedNames, projectedTargetOnly,
	)
	if err != nil {
		return "", err
	}

	columnDefs := []string{
		fmt.Sprintf("%s varchar(255) default null", quoteIdentifierForSQL(output.columnNames[0])),
		fmt.Sprintf("%s varchar(16) default null", quoteIdentifierForSQL(output.columnNames[1])),
	}
	for i, name := range projectedNames {
		colDef := outputColDefs[i]

		columnDef := fmt.Sprintf("%s %s", quoteIdentifierForSQL(name), plan2.FormatColType(colDef.Typ))
		if colDef.Typ.NotNullable && !targetOnly[i] {
			columnDef += " not null"
		} else {
			columnDef += " default null"
		}
		columnDefs = append(columnDefs, columnDef)
	}

	// Use the relation definition already resolved for the DIFF source. In
	// particular, a named snapshot can refer to a table version whose name has
	// since been dropped or recreated, so name-based time-travel SQL is not a
	// reliable way to derive this schema.
	return fmt.Sprintf("create table %s (%s)", output.qualifiedName(), strings.Join(columnDefs, ",")), nil
}

func dataBranchColumnDefByName(tableDef *plan2.TableDef, name string) *plan2.ColDef {
	for _, colDef := range tableDef.Cols {
		if colDef != nil && strings.EqualFold(colDef.Name, name) {
			return colDef
		}
	}
	return nil
}

func dataBranchColumnDefByIdentity(tableDef *plan2.TableDef, sourceColDef *plan2.ColDef) *plan2.ColDef {
	if tableDef == nil || sourceColDef == nil || sourceColDef.ColId == 0 {
		return nil
	}
	for _, colDef := range tableDef.Cols {
		if colDef != nil &&
			colDef.ColId == sourceColDef.ColId &&
			colDef.Seqnum == sourceColDef.Seqnum {
			return colDef
		}
	}
	return nil
}

func dataBranchColumnDefByLogicalName(tableDef *plan2.TableDef, sourceColDef *plan2.ColDef) *plan2.ColDef {
	if tableDef == nil || sourceColDef == nil {
		return nil
	}
	if colDef := dataBranchColumnDefByName(tableDef, sourceColDef.Name); colDef != nil {
		return colDef
	}
	if originName := sourceColDef.GetOriginCaseName(); !strings.EqualFold(originName, sourceColDef.Name) {
		if colDef := dataBranchColumnDefByName(tableDef, originName); colDef != nil {
			return colDef
		}
	}
	for _, colDef := range tableDef.Cols {
		if colDef != nil && !strings.EqualFold(colDef.GetOriginCaseName(), colDef.Name) &&
			strings.EqualFold(colDef.GetOriginCaseName(), sourceColDef.Name) {
			return colDef
		}
	}
	return nil
}

func dataBranchEndpointColumnDef(tableDef *plan2.TableDef, sourceColDef *plan2.ColDef) *plan2.ColDef {
	if colDef := dataBranchColumnDefByLogicalName(tableDef, sourceColDef); colDef != nil {
		return colDef
	}
	// A rename on an ordinary clone keeps the physical identity even when the
	// endpoint definition no longer carries OriginName. Later path validation
	// still rejects DROP/ADD discontinuities before DIFF or MERGE reads data.
	colDef := dataBranchColumnDefByIdentity(tableDef, sourceColDef)
	if colDef == nil ||
		isDataBranchUserVisibleColumn(colDef) != isDataBranchUserVisibleColumn(sourceColDef) ||
		!isDataBranchLogicalTypeEquivalent(colDef.Typ, sourceColDef.Typ) ||
		colDef.NotNull != sourceColDef.NotNull {
		return nil
	}
	return colDef
}

// dataBranchColumnsByIdentity maps each source column name to the column with
// the same stable identity in the destination definition. Column names are
// intentionally not part of schema equivalence: a branch may rename a column
// without changing its identity, so physical reads from another branch or an
// ancestor must resolve that branch's local name through ColId and Seqnum.
func dataBranchOutputColumnDefs(
	sourceTableDef *plan2.TableDef,
	destinationTableDef *plan2.TableDef,
	sourceColumnNames []string,
	sourceTargetOnly []bool,
) ([]*plan2.ColDef, []bool, error) {
	if sourceTableDef == nil || destinationTableDef == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("DATA BRANCH table definition is unavailable")
	}
	if len(sourceColumnNames) != len(sourceTargetOnly) {
		return nil, nil, moerr.NewInternalErrorNoCtx("DATA BRANCH output column classification is unavailable")
	}

	outputColDefs := make([]*plan2.ColDef, len(sourceColumnNames))
	targetOnly := make([]bool, len(sourceColumnNames))
	for i, sourceColumnName := range sourceColumnNames {
		sourceColDef := dataBranchColumnDefByName(sourceTableDef, sourceColumnName)
		if sourceColDef == nil {
			return nil, nil, moerr.NewInternalErrorNoCtxf(
				"DATA BRANCH source column %q is unavailable", sourceColumnName,
			)
		}
		outputColDefs[i] = dataBranchColumnDefByIdentity(destinationTableDef, sourceColDef)
		if outputColDefs[i] == nil {
			if !sourceTargetOnly[i] {
				return nil, nil, moerr.NewInternalErrorNoCtxf(
					"DATA BRANCH destination column for source column %q is unavailable", sourceColumnName,
				)
			}
			// A target-only column has no base identity. Keep its target type in
			// the materialized schema, but make the output column nullable because
			// base-side DIFF rows project target-only values as NULL.
			outputColDefs[i] = sourceColDef
			targetOnly[i] = true
		}
	}
	return outputColDefs, targetOnly, nil
}

func (output *diffOutputTable) insertSQL(values *bytes.Buffer) string {
	return fmt.Sprintf(
		"insert into %s (%s) values %s",
		output.qualifiedName(), joinQuotedColumnNames(output.columnNames), values.String(),
	)
}

func materializeDiffOutputAsTable(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
	spool *dataBranchOutputSpool,
) (err error) {
	output, err := newDiffOutputTable(ctx, ses, stmt, tblStuff)
	if err != nil {
		cancel()
		return err
	}

	created := false
	defer func() {
		if err == nil || !created {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.WithoutCancel(ctx), diffOutputCleanupTimeout,
		)
		defer cleanupCancel()
		if cleanupErr := execDataBranchOutputSQL(
			cleanupCtx, bh, fmt.Sprintf("drop table if exists %s", output.qualifiedName()),
		); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	var createSQL string
	if createSQL, err = output.createSQL(ctx, tblStuff); err != nil {
		cancel()
		return err
	}
	if err = execDataBranchOutputSQL(ctx, bh, createSQL); err != nil {
		cancel()
		return err
	}
	created = true

	values := acquireBuffer(tblStuff.bufPool)
	defer releaseBuffer(tblStuff.bufPool, values)

	flush := func() error {
		if values.Len() == 0 {
			return nil
		}
		if flushErr := execDataBranchOutputSQL(ctx, bh, output.insertSQL(values)); flushErr != nil {
			return flushErr
		}
		values.Reset()
		return nil
	}

	var (
		firstErr error
		rowCount int
		row      = make([]any, len(tblStuff.def.colNames))
	)
	for {
		wrapped, ok, nextErr := spool.next()
		if nextErr != nil {
			firstErr = nextErr
			break
		}
		if !ok {
			break
		}
		if ctx.Err() != nil {
			firstErr = ctx.Err()
		}

		for rowIdx := range wrapped.batch.RowCount() {
			if firstErr != nil {
				break
			}
			rowValues := acquireBuffer(tblStuff.bufPool)
			rowValues.Reset()
			rowValues.WriteByte('(')
			writeEscapedSQLString(rowValues, []byte(wrapped.name))
			rowValues.WriteByte(',')
			writeEscapedSQLString(rowValues, []byte(wrapped.kind))

			for _, colIdx := range output.projectedIdxes {
				vec := wrapped.batch.Vecs[colIdx]
				if rowIdx >= vec.Length() {
					firstErr = moerr.NewInternalErrorNoCtxf(
						"data branch OUTPUT AS batch shape mismatch: row=%d batchRows=%d col=%d vecLen=%d type=%s",
						rowIdx, wrapped.batch.RowCount(), colIdx, vec.Length(), vec.GetType().String(),
					)
					break
				}
				if extractErr := extractDataBranchSQLRowValue(ctx, ses, vec, colIdx, row, rowIdx); extractErr != nil {
					firstErr = extractErr
					break
				}
				rowValues.WriteByte(',')
				if formatErr := formatValIntoString(ses, row[colIdx], tblStuff.def.colTypes[colIdx], rowValues); formatErr != nil {
					firstErr = formatErr
					break
				}
			}
			if firstErr == nil {
				rowValues.WriteByte(')')
				additionalBytes := rowValues.Len()
				if values.Len() > 0 {
					additionalBytes++
				}
				if values.Len()+additionalBytes >= maxSqlBatchSize || rowCount+1 >= maxSqlBatchCnt {
					if flushErr := flush(); flushErr != nil {
						firstErr = flushErr
					}
					rowCount = 0
				}
				if firstErr == nil {
					if values.Len() > 0 {
						values.WriteByte(',')
					}
					values.Write(rowValues.Bytes())
					rowCount++
				}
			}
			releaseBuffer(tblStuff.bufPool, rowValues)
			if firstErr != nil {
				cancel()
				break
			}
		}
		if firstErr != nil {
			break
		}
	}

	if firstErr != nil {
		return firstErr
	}
	if err = flush(); err != nil {
		return err
	}
	// The destination is complete. Subsequent response/query-result persistence
	// failures must not remove a successfully materialized user table.
	created = false

	// DataBranchDiff is a result-row statement at the protocol layer. Returning
	// a zero-column result set is not a valid MySQL response, so acknowledge the
	// materialized table explicitly.
	mrs := ses.GetMysqlResultSet()
	mrs.AddRow([]any{output.qualifiedName()})
	return trySaveQueryResult(ctx, ses, mrs)
}

func execDataBranchOutputSQL(ctx context.Context, bh BackgroundExec, sql string) error {
	if err := bh.Exec(ctx, sql); err != nil {
		return err
	}
	bh.ClearExecResultSet()
	return nil
}

type diffOutputPKValues struct {
	values     []any
	pkColIdxes []int
}

func (values diffOutputPKValues) len() int {
	if values.pkColIdxes == nil {
		return len(values.values)
	}
	return len(values.pkColIdxes)
}

func (values diffOutputPKValues) at(idx int) any {
	if values.pkColIdxes == nil {
		return values.values[idx]
	}
	return values.values[values.pkColIdxes[idx]+2]
}

func compareDiffOutputPKValues(a, b diffOutputPKValues) int {
	for idx := range a.len() {
		if cmp := types.CompareValue(a.at(idx), b.at(idx)); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func compareDiffOutputRows(a, b []any, pkColIdxes []int) int {
	return compareDiffOutputPKValues(
		diffOutputPKValues{values: a, pkColIdxes: pkColIdxes},
		diffOutputPKValues{values: b, pkColIdxes: pkColIdxes},
	)
}

// diffOutputRowHeap is a max-heap by primary key. Keeping the largest retained
// row at the root lets OUTPUT LIMIT select the globally smallest rows while
// holding at most LIMIT rows in memory.
type diffOutputRowHeap struct {
	rows       [][]any
	pkColIdxes []int
}

func (h diffOutputRowHeap) Len() int { return len(h.rows) }

func (h diffOutputRowHeap) Less(i, j int) bool {
	return compareDiffOutputRows(h.rows[i], h.rows[j], h.pkColIdxes) > 0
}

func (h diffOutputRowHeap) Swap(i, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

func (h *diffOutputRowHeap) Push(value any) {
	h.rows = append(h.rows, value.([]any))
}

func (h *diffOutputRowHeap) Pop() any {
	last := len(h.rows) - 1
	value := h.rows[last]
	h.rows[last] = nil
	h.rows = h.rows[:last]
	return value
}

func satisfyDiffOutputOpt(
	ctx context.Context,
	cancel context.CancelFunc,
	stop func(),
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		mrs      = ses.GetMysqlResultSet()
		first    error
		hitLimit bool
	)

	defer func() {
		cancel()
	}()

	if stmt.OutputOpt == nil || stmt.OutputOpt.Limit != nil {
		rows := make([][]any, 0, 100)

		// Resolve column projection (nil means show all visible columns).
		displayIdxes, resolveErr := resolveProjectedIdxes(stmt.Columns, tblStuff)
		if resolveErr != nil {
			return resolveErr
		}

		// Determine which columns to extract from batch vectors.
		// When projecting, only extract display columns + PK columns (for sorting).
		extractIdxes := tblStuff.def.visibleIdxes
		rowSize := len(tblStuff.def.visibleIdxes) + 2
		if displayIdxes != nil {
			seen := make(map[int]bool, len(displayIdxes)+len(tblStuff.def.pkColIdxes))
			extractIdxes = make([]int, 0, len(displayIdxes)+len(tblStuff.def.pkColIdxes))
			for _, idx := range displayIdxes {
				if !seen[idx] {
					seen[idx] = true
					extractIdxes = append(extractIdxes, idx)
				}
			}
			for _, idx := range tblStuff.def.pkColIdxes {
				if !seen[idx] {
					seen[idx] = true
					extractIdxes = append(extractIdxes, idx)
				}
			}
			// Sparse row: indexed by colIdx+2, so size must accommodate the largest index.
			rowSize = slices.Max(extractIdxes) + 3
		}

		var limit *int64
		if stmt.OutputOpt != nil {
			limit = stmt.OutputOpt.Limit
		}
		if limit != nil && *limit == 0 {
			// The limit is already satisfied before consuming any rows (LIMIT 0).
			hitLimit = true
			stop()
		}
		rowHeap := diffOutputRowHeap{
			rows:       rows,
			pkColIdxes: tblStuff.def.pkColIdxes,
		}
		materializeIdxes := extractIdxes
		var candidatePK []any
		if limit != nil && *limit > 0 {
			candidatePK = make([]any, len(tblStuff.def.pkColIdxes))
			pkIdxes := make(map[int]struct{}, len(tblStuff.def.pkColIdxes))
			for _, idx := range tblStuff.def.pkColIdxes {
				pkIdxes[idx] = struct{}{}
			}
			materializeIdxes = make([]int, 0, len(extractIdxes))
			for _, idx := range extractIdxes {
				if _, isPK := pkIdxes[idx]; !isPK {
					materializeIdxes = append(materializeIdxes, idx)
				}
			}
		}

		materializeRow := func(
			wrapped batchWithKind,
			rowIdx int,
			idxes []int,
			pkValues []any,
		) ([]any, error) {
			row := make([]any, rowSize)
			row[0] = wrapped.name
			row[1] = wrapped.kind
			for idx, colIdx := range tblStuff.def.pkColIdxes {
				if pkValues != nil {
					row[colIdx+2] = pkValues[idx]
				}
			}
			for _, colIdx := range idxes {
				if err := extractRowFromVector(
					ctx, ses, wrapped.batch.Vecs[colIdx], colIdx+2, row, rowIdx, false,
				); err != nil {
					return nil, err
				}
			}
			return row, nil
		}

		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if hitLimit {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			for rowIdx := range wrapped.batch.RowCount() {
				if limit == nil {
					row, materializeErr := materializeRow(wrapped, rowIdx, extractIdxes, nil)
					if materializeErr != nil {
						return materializeErr
					}
					rowHeap.rows = append(rowHeap.rows, row)
					continue
				}

				for idx, colIdx := range tblStuff.def.pkColIdxes {
					if err = extractRowFromVector(
						ctx, ses, wrapped.batch.Vecs[colIdx], idx, candidatePK, rowIdx, false,
					); err != nil {
						return
					}
				}

				fillsHeap := int64(len(rowHeap.rows)) < *limit
				replacesRoot := !fillsHeap && compareDiffOutputPKValues(
					diffOutputPKValues{values: candidatePK},
					diffOutputPKValues{values: rowHeap.rows[0], pkColIdxes: rowHeap.pkColIdxes},
				) < 0
				if !fillsHeap && !replacesRoot {
					continue
				}

				row, materializeErr := materializeRow(wrapped, rowIdx, materializeIdxes, candidatePK)
				if materializeErr != nil {
					return materializeErr
				}
				if fillsHeap {
					rowHeap.rows = append(rowHeap.rows, row)
					if int64(len(rowHeap.rows)) == *limit {
						heap.Init(&rowHeap)
					}
				} else {
					rowHeap.rows[0] = row
					heap.Fix(&rowHeap, 0)
				}
			}
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}

		rows = rowHeap.rows
		slices.SortFunc(rows, func(a, b []any) int {
			return compareDiffOutputRows(a, b, tblStuff.def.pkColIdxes)
		})

		if displayIdxes != nil {
			// Column projection: build compact rows with only projected columns.
			for _, row := range rows {
				projRow := make([]any, len(displayIdxes)+2)
				projRow[0] = row[0]
				projRow[1] = row[1]
				for j, colIdx := range displayIdxes {
					projRow[j+2] = row[colIdx+2]
				}
				mrs.AddRow(projRow)
			}
		} else {
			for _, row := range rows {
				mrs.AddRow(row)
			}
		}

	} else if stmt.OutputOpt.Count {
		cnt := int64(0)
		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			cnt += int64(wrapped.batch.RowCount())
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}
		mrs.AddRow([]any{cnt})

	} else if stmt.OutputOpt.Summary {
		var (
			targetInsertCnt int64
			targetDeleteCnt int64
			targetUpdateCnt int64
			baseInsertCnt   int64
			baseDeleteCnt   int64
			baseUpdateCnt   int64
		)

		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			var (
				targetMetric *int64
				baseMetric   *int64
			)

			switch wrapped.kind {
			case diffInsert:
				targetMetric = &targetInsertCnt
				baseMetric = &baseInsertCnt
			case diffDelete:
				targetMetric = &targetDeleteCnt
				baseMetric = &baseDeleteCnt
			case diffUpdate:
				targetMetric = &targetUpdateCnt
				baseMetric = &baseUpdateCnt
			default:
				first = moerr.NewInternalErrorNoCtxf("unknown diff kind %q", wrapped.kind)
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			cnt := int64(wrapped.batch.RowCount())
			switch wrapped.side {
			case diffSideTarget:
				*targetMetric += cnt
			case diffSideBase:
				*baseMetric += cnt
			default:
				first = moerr.NewInternalErrorNoCtxf("unknown diff side for summary, kind=%q, table=%q", wrapped.kind, wrapped.name)
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}

		if first != nil {
			return first
		}

		mrs.AddRow([]any{"INSERTED", targetInsertCnt, baseInsertCnt})
		mrs.AddRow([]any{"DELETED", targetDeleteCnt, baseDeleteCnt})
		mrs.AddRow([]any{"UPDATED", targetUpdateCnt, baseUpdateCnt})

	} else if stmt.OutputOpt.As.ObjectName != "" {
		return moerr.NewInternalErrorNoCtx("DATA BRANCH OUTPUT AS requires the materialization phase")

	} else if len(stmt.OutputOpt.DirPath) != 0 {
		var (
			insertCnt int
			deleteCnt int

			deleteFromValsBuffer = acquireBuffer(tblStuff.bufPool)
			insertIntoValsBuffer = acquireBuffer(tblStuff.bufPool)
			tmpValsBuffer        = acquireBuffer(tblStuff.bufPool)

			fileHint     string
			fullFilePath string
			writeFile    func([]byte) error
			release      func() error
			cleanup      func()
			succeeded    bool
		)

		defer func() {
			if release != nil {
				err = errors.Join(err, release())
			}
			if !succeeded && cleanup != nil {
				cleanup()
			}
			releaseBuffer(tblStuff.bufPool, deleteFromValsBuffer)
			releaseBuffer(tblStuff.bufPool, insertIntoValsBuffer)
			releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
		}()

		if fullFilePath, fileHint, writeFile, release, cleanup, err = prepareFSForDiffAsFile(
			ctx, ses, stmt, tblStuff,
		); err != nil {
			return
		}

		appender := newSQLValuesAppender(
			ctx, ses, bh, tblStuff, dataBranchApplyModePortableSQL,
			&deleteCnt, deleteFromValsBuffer, &insertCnt, insertIntoValsBuffer, writeFile,
		)
		if writeFile != nil {
			// Make generated SQL runnable in one transaction.
			if err = writeFile([]byte("BEGIN;\n")); err != nil {
				return
			}
		}
		if err = initApplyTables(ctx, ses, bh, appender.batchInfo, appender.writeFile); err != nil {
			return
		}

		for wrapped := range retCh {
			if first != nil {
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}
			if ctx.Err() != nil {
				first = ctx.Err()
				cancel()
				tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
				continue
			}

			if wrapped.name == tblStuff.tarRel.GetTableName() {
				if err = appendBatchRowsAsSQLValues(
					ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
				); err != nil {
					first = err
					cancel()
					tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
					continue
				}
			} else if wrapped.name == tblStuff.baseRel.GetTableName() {
				if wrapped.kind == diffInsert {
					wrapped.kind = diffDelete
				}
				if wrapped.kind == diffDelete {
					if err = appendBatchRowsAsSQLValues(
						ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
					); err != nil {
						first = err
						cancel()
						tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
						continue
					}
				}
			}

			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
		}

		if first != nil {
			return first
		}

		if err = appender.flushAll(); err != nil {
			first = err
			cancel()
		}
		if first == nil {
			if err = dropApplyTables(ctx, ses, bh, appender.batchInfo, appender.writeFile); err != nil {
				return err
			}
		}
		if first == nil && writeFile != nil {
			if err = writeFile([]byte("COMMIT;\n")); err != nil {
				return err
			}
		}
		if err = release(); err != nil {
			release = nil
			return err
		}
		release = nil

		succeeded = true
		mrs.AddRow([]any{fullFilePath, fileHint})
	}

	if first != nil {
		return first
	}
	if hitLimit {
		return trySaveQueryResult(context.Background(), ses, mrs)
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func buildOutputSchema(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (err error) {

	var (
		mrs      = ses.GetMysqlResultSet()
		showCols []*MysqlColumn
	)
	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	if stmt.OutputOpt == nil || stmt.OutputOpt.Limit != nil {
		// output all rows OR
		// output limited rows (is this can be pushed down to hash-phase?)

		displayIdxes := tblStuff.def.visibleIdxes
		if stmt.Columns != nil {
			if displayIdxes, err = resolveProjectedIdxes(stmt.Columns, tblStuff); err != nil {
				return
			}
		}

		showCols = make([]*MysqlColumn, 0, 2)
		showCols = append(showCols, new(MysqlColumn), new(MysqlColumn))
		showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[0].SetName(fmt.Sprintf(
			"diff %s against %s", tblStuff.tarRel.GetTableName(), tblStuff.baseRel.GetTableName()),
		)
		showCols[1].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[1].SetName("flag")

		for _, idx := range displayIdxes {
			nCol := new(MysqlColumn)
			if err = setMysqlColumnTypeInfo(ctx, tblStuff.def.colTypes[idx], nCol); err != nil {
				return
			}

			nCol.SetName(tblStuff.def.colNames[idx])
			showCols = append(showCols, nCol)
		}

	} else if stmt.OutputOpt.Summary {
		targetName := tree.StringWithOpts(&stmt.TargetTable, dialect.MYSQL, tree.WithSingleQuoteString())
		baseName := tree.StringWithOpts(&stmt.BaseTable, dialect.MYSQL, tree.WithSingleQuoteString())

		showCols = append(showCols, &MysqlColumn{}, &MysqlColumn{}, &MysqlColumn{})
		showCols[0].SetName("metric")
		showCols[0].SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols[1].SetName(targetName)
		showCols[1].SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		showCols[2].SetName(baseName)
		showCols[2].SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	} else if stmt.OutputOpt.Count {
		// count(*) of diff rows
		showCols = append(showCols, &MysqlColumn{})
		showCols[0].SetName("COUNT(*)")
		showCols[0].SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	} else if stmt.OutputOpt.As.ObjectName != "" {
		col := new(MysqlColumn)
		col.SetName("TABLE CREATED")
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		showCols = append(showCols, col)
	} else if len(stmt.OutputOpt.DirPath) != 0 {
		// output as file
		col1 := new(MysqlColumn)
		col1.SetName("FILE SAVED TO")
		col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		col2 := new(MysqlColumn)
		col2.SetName("HINT")
		col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		showCols = append(showCols, col1, col2)
	} else {
		return moerr.NewNotSupportedNoCtx(fmt.Sprintf("%v", stmt.OutputOpt))
	}

	for _, col := range showCols {
		mrs.AddColumn(col)
	}

	return nil
}

func tryDiffAsCSV(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (bool, error) {

	if stmt.OutputOpt == nil {
		return false, nil
	}

	if len(stmt.OutputOpt.DirPath) == 0 {
		return false, nil
	}

	sql := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s",
		qualifiedTableName(
			tblStuff.baseRel.GetTableDef(ctx).DbName,
			tblStuff.baseRel.GetTableDef(ctx).Name,
		),
	)

	if tblStuff.baseSnap != nil && tblStuff.baseSnap.TS != nil {
		sql += fmt.Sprintf("{mo_ts=%d}", tblStuff.baseSnap.TS.PhysicalTime)
	}

	var (
		err    error
		sqlRet executor.Result
	)

	if sqlRet, err = runSql(ctx, ses, bh, sql, nil, nil); err != nil {
		return false, err
	}
	defer sqlRet.Close()

	ok, err := shouldDiffAsCSV(sqlRet)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	return true, writeCSV(ctx, ses, tblStuff, bh, stmt)
}

func shouldDiffAsCSV(sqlRet executor.Result) (bool, error) {
	if len(sqlRet.Batches) != 1 ||
		sqlRet.Batches[0] == nil ||
		sqlRet.Batches[0].RowCount() != 1 ||
		sqlRet.Batches[0].VectorCount() != 1 ||
		len(sqlRet.Batches[0].Vecs) != 1 ||
		sqlRet.Batches[0].Vecs[0] == nil {
		return false, moerr.NewInternalErrorNoCtxf("cannot get count(*) of base table")
	}

	return vector.GetFixedAtWithTypeCheck[uint64](sqlRet.Batches[0].Vecs[0], 0) == 0, nil
}

func submitCSVBatchForConversion(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	ep *ExportConfig,
	workerWg *sync.WaitGroup,
) error {
	copied, err := bat.Dup(ses.proc.Mp())
	if err != nil {
		return err
	}

	idx := ep.Index.Add(1)
	workerWg.Add(1)
	if err = tblStuff.worker.Submit(func() {
		defer workerWg.Done()
		constructByte(ctx, ses, copied, idx, ep.ByteChan, ep)
	}); err != nil {
		workerWg.Done()
		copied.Clean(ses.proc.Mp())
		ep.Index.Add(-1)
		return err
	}
	return nil
}

func flushRemainingCSVBatchBytes(ctx context.Context, ep *ExportConfig) error {
	if ctx.Err() != nil || ep.WriteIndex.Load() == ep.Index.Load() {
		return nil
	}
	return exportAllDataFromBatches(ep)
}

func waitForCSVConversionWorkers(inputCtx context.Context, workerWg *sync.WaitGroup) error {
	workerWg.Wait()
	return inputCtx.Err()
}

func joinCSVInputError(err, inputErr error) error {
	if inputErr == nil || errors.Is(err, inputErr) {
		return err
	}
	return errors.Join(err, inputErr)
}

func writeCSV(
	inputCtx context.Context,
	ses *Session,
	tblStuff tableStuff,
	bh BackgroundExec,
	stmt *tree.DataBranchDiff,
) (err error) {

	ctx, cancelCtx := context.WithCancel(inputCtx)
	defer cancelCtx()

	// SQLExecutor do not support snapshot read, we must use the MO_TS
	snap := ""
	if tblStuff.tarSnap != nil {
		snap = fmt.Sprintf("{MO_TS=%d}", tblStuff.tarSnap.TS.PhysicalTime)
	}

	// output as csv
	sql := fmt.Sprintf("SELECT * FROM %s%s;",
		qualifiedTableName(
			tblStuff.tarRel.GetTableDef(ctx).DbName,
			tblStuff.tarRel.GetTableDef(ctx).Name,
		),
		snap,
	)

	var (
		stop       bool
		wg         sync.WaitGroup
		errChan    = make(chan error, 1)
		streamChan = make(chan executor.Result, runtime.NumCPU())

		writerWg  sync.WaitGroup
		workerWg  sync.WaitGroup
		writerErr = make(chan error, 1)
		closeByte sync.Once

		mrs        = &MysqlResultSet{}
		sqlRetHint string
		sqlRetPath string
		fileName   string
		cleanup    func()
	)
	if fileName, err = makeFileName(stmt.BaseTable.AtTsExpr, stmt.TargetTable.AtTsExpr, tblStuff); err != nil {
		return
	}

	ep := &ExportConfig{
		userConfig: newDiffCSVUserConfig(),
		service:    ses.service,
	}

	for range tblStuff.def.visibleIdxes {
		mrs.AddColumn(&MysqlColumn{})
	}

	ep.init()
	ep.ctx = ctx
	ep.mrs = mrs
	ep.ByteChan = make(chan *BatchByte, runtime.NumCPU())

	ep.DefaultBufSize = getPu(ses.GetService()).SV.ExportDataDefaultFlushSize
	initExportFileParam(ep, mrs)

	sqlRetHint = ep.userConfig.String()
	fileName += ".csv"

	var (
		ok        bool
		stagePath string
	)

	sqlRetPath = path.Join(stmt.OutputOpt.DirPath, fileName)

	if stagePath, ok, err = tryDecodeStagePath(ses, stmt.OutputOpt.DirPath); err != nil {
		return
	} else if ok {
		sqlRetPath = strings.Replace(sqlRetPath, "stage:/", "stage://", 1)
		ep.userConfig.StageFilePath = path.Join(stagePath, fileName)
		if err = openNewFile(ctx, ep, mrs); err != nil {
			return
		}
		cleanup = func() {
			removeFileIgnoreError(context.Background(), ep.service, ep.userConfig.StageFilePath)
		}
	} else {
		ep.userConfig.FilePath = path.Join(stmt.OutputOpt.DirPath, fileName)
		if err = openNewFile(ctx, ep, mrs); err != nil {
			return
		}
		cleanup = func() {
			removeFileIgnoreError(context.Background(), ep.service, ep.userConfig.FilePath)
		}
	}

	writerWg.Add(1)
	if err = tblStuff.worker.Submit(func() {
		defer writerWg.Done()
		for bb := range ep.ByteChan {
			if bb.err != nil {
				select {
				case writerErr <- bb.err:
				default:
				}
				return
			}
			ep.BatchMap[bb.index] = bb.writeByte
			for {
				value, ok := ep.BatchMap[ep.WriteIndex.Load()+1]
				if !ok {
					break
				}
				if err2 := writeToCSVFile(ep, value); err2 != nil {
					select {
					case writerErr <- err2:
					default:
					}
					return
				}
				ep.WriteIndex.Add(1)
				ep.BatchMap[ep.WriteIndex.Load()] = nil
			}
		}
		if err2 := flushRemainingCSVBatchBytes(ctx, ep); err2 != nil {
			select {
			case writerErr <- err2:
			default:
			}
		}
	}); err != nil {
		writerWg.Done()
		return err
	}

	defer func() {
		if err != nil && cleanup != nil {
			cleanup()
		}
	}()

	defer func() {
		closeByte.Do(func() {
			close(ep.ByteChan)
		})
		writerWg.Wait()
		closeErr := Close(ep)
		ep.Close()
		if err == nil {
			err = closeErr
		} else {
			err = errors.Join(err, closeErr)
		}
	}()

	wg.Add(1)
	if err = tblStuff.worker.Submit(func() {
		defer func() {
			wg.Done()
			close(streamChan)
			close(errChan)
		}()
		if _, err2 := runSql(ctx, ses, bh, sql, streamChan, errChan); err2 != nil {
			select {
			case errChan <- err2:
			default:
			}
		}
	}); err != nil {
		wg.Done()
		return err
	}

	streamOpen, errOpen := true, true
	for streamOpen || errOpen {
		select {
		case <-inputCtx.Done():
			err = errors.Join(err, inputCtx.Err())
			stop = true
		case e := <-writerErr:
			if e != nil {
				err = errors.Join(err, e)
			}
			stop = true
			cancelCtx()
		case e, ok := <-errChan:
			if !ok {
				errOpen = false
				continue
			}
			err = errors.Join(err, e)
			cancelCtx()
			stop = true
		case sqlRet, ok := <-streamChan:
			if !ok {
				streamOpen = false
				continue
			}
			if stop {
				sqlRet.Close()
				continue
			}
			for _, bat := range sqlRet.Batches {
				if submitErr := submitCSVBatchForConversion(
					ctx, ses, tblStuff, bat, ep, &workerWg,
				); submitErr != nil {
					err = errors.Join(err, submitErr)
					stop = true
					cancelCtx()
					break
				}
			}
			sqlRet.Close()
		}
	}

	wg.Wait()
	inputErr := waitForCSVConversionWorkers(inputCtx, &workerWg)
	closeByte.Do(func() {
		close(ep.ByteChan)
	})
	writerWg.Wait()
	if inputErr == nil {
		inputErr = inputCtx.Err()
	}
	err = joinCSVInputError(err, inputErr)

	select {
	case e := <-writerErr:
		err = errors.Join(err, e)
	default:
	}

	if err != nil {
		return err
	}

	mrs = ses.GetMysqlResultSet()
	mrs.AddRow([]any{sqlRetPath, sqlRetHint})

	return trySaveQueryResult(ctx, ses, mrs)
}

func newDiffCSVUserConfig() *tree.ExportParam {
	fields := tree.NewFields(
		tree.DefaultFieldsTerminated,
		false,
		tree.DefaultFieldsEnclosedBy[0],
		tree.DefaultFieldsEscapedBy[0],
	)
	return &tree.ExportParam{
		Outfile: true,
		Fields:  fields,
		Lines:   tree.NewLines("", "\n"),
		Header:  false,
	}
}

func writeDeleteRowSQLFull(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	// Use NULL-aware equality and LIMIT 1 to preserve duplicate-row semantics.
	buf.WriteString(fmt.Sprintf(
		"delete from %s where ",
		qualifiedTableName(
			tblStuff.baseRel.GetTableDef(ctx).DbName,
			tblStuff.baseRel.GetTableDef(ctx).Name,
		),
	))
	var literal bytes.Buffer
	for i, idx := range tblStuff.def.visibleIdxes {
		if i > 0 {
			buf.WriteString(" and ")
		}
		colName := quoteIdentifierForSQL(tblStuff.def.baseColNames[idx])
		if row[idx] == nil {
			buf.WriteString(colName)
			buf.WriteString(" is null")
		} else {
			literal.Reset()
			if err := formatValIntoString(ses, row[idx], tblStuff.def.colTypes[idx], &literal); err != nil {
				return err
			}
			buf.WriteString(dataBranchSQLKeyEqual(
				colName, literal.String(), tblStuff.def.colTypes[idx],
			))
		}
	}
	buf.WriteString(" limit 1;\n")
	return nil
}

func extractDataBranchApplyRow(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	rowIdx int,
	extraColIdxes []int,
	row []any,
	errorPrefix string,
) (err error) {
	extractCol := func(colIdx int) error {
		vec := bat.Vecs[colIdx]
		if rowIdx >= vec.Length() {
			return moerr.NewInternalErrorNoCtxf(
				"%s: row=%d batchRows=%d col=%d vecLen=%d type=%s",
				errorPrefix, rowIdx, bat.RowCount(), colIdx, vec.Length(), vec.GetType().String(),
			)
		}
		if err = extractDataBranchSQLRowValue(ctx, ses, vec, colIdx, row, rowIdx); err != nil {
			return err
		}
		return nil
	}

	for _, colIdx := range tblStuff.def.visibleIdxes {
		if err = extractCol(colIdx); err != nil {
			return err
		}
	}
	for i, colIdx := range extraColIdxes {
		alreadyExtracted := false
		for _, visibleIdx := range tblStuff.def.visibleIdxes {
			if visibleIdx == colIdx {
				alreadyExtracted = true
				break
			}
		}
		if alreadyExtracted {
			continue
		}
		for j := 0; j < i; j++ {
			if extraColIdxes[j] == colIdx {
				alreadyExtracted = true
				break
			}
		}
		if alreadyExtracted {
			continue
		}
		if err = extractCol(colIdx); err != nil {
			return err
		}
	}
	return nil
}

func appendDataBranchApplyRowAsSQLValues(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	kind string,
	row []any,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
) (err error) {
	tmpValsBuffer.Reset()
	if kind == diffDelete {
		if appender.deleteByFullRow {
			if err = writeDeleteRowSQLFull(ctx, ses, tblStuff, row, tmpValsBuffer); err != nil {
				return err
			}
		} else if appender.batchInfo != nil {
			if err = writeDeleteRowValuesWithColIdxes(
				ses, tblStuff, row, appender.deleteKeyColIdxes, tmpValsBuffer, true,
			); err != nil {
				return err
			}
		} else {
			if err = writeDeleteRowValuesWithColIdxes(
				ses, tblStuff, row, appender.deleteKeyColIdxes, tmpValsBuffer, false,
			); err != nil {
				return err
			}
		}
	} else if kind == diffUpdate {
		if appender.batchInfo == nil {
			return moerr.NewInternalErrorNoCtx("Data Branch update staging is unavailable")
		}
		if len(appender.batchInfo.updateValueIdxes) == 0 {
			return moerr.NewInternalErrorNoCtx("Data Branch update staging has no values")
		}
		if err = writeInsertRowValues(
			ses, tblStuff, row, tmpValsBuffer, appender.batchInfo.updateValueIdxes,
		); err != nil {
			return err
		}
	} else {
		insertIdxes := tblStuff.def.writableIdxes
		if len(tblStuff.def.tarOnlyIdxes) > 0 {
			insertIdxes = tblStuff.def.commonWritableIdxes
		}
		if err = writeInsertRowValues(ses, tblStuff, row, tmpValsBuffer, insertIdxes); err != nil {
			return err
		}
	}

	if tmpValsBuffer.Len() == 0 {
		return nil
	}

	return appender.appendRow(kind, tmpValsBuffer.Bytes())
}

func appendBatchRowsAsSQLValues(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
) (err error) {
	directUpdate, err := dataBranchDirectUpdateBatch(tblStuff, wrapped, appender.batchInfo)
	if err != nil {
		return err
	}
	if directUpdate {
		if wrapped.kind == diffDelete {
			return nil
		}
	}
	//seenCols := make(map[int]struct{}, len(tblStuff.def.visibleIdxes))
	row := make([]any, len(tblStuff.def.colNames))
	extraColIdxes := appender.extraColIdxesForRow(wrapped.kind)
	stageUpdate := dataBranchStagesUpdate(
		appender, directUpdate, wrapped.requiresNativeUpdate, wrapped.restoreMissing,
	)
	if stageUpdate {
		extraColIdxes = append(extraColIdxes, appender.deleteKeyColIdxes...)
	}

	for rowIdx := range wrapped.batch.RowCount() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err = extractDataBranchApplyRow(
			ctx, ses, tblStuff, wrapped.batch, rowIdx, extraColIdxes, row,
			"data branch output batch shape mismatch",
		); err != nil {
			return
		}
		err = appendOrStageDataBranchApplyRow(
			ctx, ses, tblStuff, wrapped.kind, row, tmpValsBuffer, appender,
			directUpdate, stageUpdate, wrapped.restoreMissing,
		)
		if err != nil {
			return
		}
	}

	return nil
}

func dataBranchDirectUpdateBatch(
	tblStuff tableStuff,
	wrapped batchWithKind,
	batchInfo *applyBatchInfo,
) (bool, error) {
	if tblStuff.def.pkKind == fakeKind || !wrapped.fromUpdate || batchInfo == nil {
		return false, nil
	}
	if wrapped.kind != diffDelete && wrapped.kind != diffInsert {
		return false, moerr.NewInternalErrorNoCtxf("unexpected Data Branch update batch kind %q", wrapped.kind)
	}
	return true, nil
}

func dataBranchStagesUpdate(
	appender sqlValuesAppender,
	directUpdate, requiresNativeUpdate, restoreMissing bool,
) bool {
	return directUpdate && !requiresNativeUpdate && !restoreMissing && appender.batchInfo != nil &&
		!appender.batchInfo.disableInsertStage && !appender.batchInfo.deleteNeedsExactFloatKeyMatch()
}

// dataBranchStagedUpdateColumnNames partitions writable non-key columns by
// the apply form they support. SET/ENUM and spatial assignments use a staged
// ON DUPLICATE KEY UPDATE except when they are part of a unique secondary
// index: MatrixOne rejects those ODKU assignments, so changed rows take the
// native UPDATE path. Every other assignment remains in the staged UPDATE
// JOIN path, so mixed schemas stay batched for ordinary rows.
func dataBranchStagedUpdateColumnNames(
	tblStuff tableStuff,
	baseDef *plan2.TableDef,
	writableIdxes []int,
) (stagedNames, specialNames []string) {
	indexedSpecial := make(map[int]struct{}, len(tblStuff.def.indexedSpecialUpdateIdxes))
	for _, idx := range tblStuff.def.indexedSpecialUpdateIdxes {
		indexedSpecial[idx] = struct{}{}
	}
	for _, idx := range writableIdxes {
		if slices.Contains(tblStuff.def.pkColIdxes, idx) {
			continue
		}
		if idx < 0 || idx >= len(tblStuff.def.colTypes) {
			continue
		}
		if dataBranchSpecialUpdateColumn(tblStuff, baseDef, idx) {
			if _, indexed := indexedSpecial[idx]; indexed {
				continue
			}
			specialNames = append(specialNames, tblStuff.def.baseColNames[idx])
		} else {
			stagedNames = append(stagedNames, tblStuff.def.baseColNames[idx])
		}
	}
	return stagedNames, specialNames
}

func dataBranchIndexedSpecialUpdateColIdxes(
	tblStuff tableStuff,
	baseDef *plan2.TableDef,
	writableIdxes []int,
) []int {
	if baseDef == nil {
		return nil
	}
	uniqueColumns := make(map[string]struct{})
	for _, index := range baseDef.Indexes {
		if index == nil || !index.Unique {
			continue
		}
		for _, part := range index.Parts {
			uniqueColumns[strings.ToLower(catalog.ResolveAlias(part))] = struct{}{}
		}
	}
	if len(uniqueColumns) == 0 {
		return nil
	}

	idxes := make([]int, 0)
	for _, idx := range writableIdxes {
		if slices.Contains(tblStuff.def.pkColIdxes, idx) ||
			!dataBranchSpecialUpdateColumn(tblStuff, baseDef, idx) {
			continue
		}
		if _, unique := uniqueColumns[strings.ToLower(tblStuff.def.baseColNames[idx])]; unique {
			idxes = append(idxes, idx)
		}
	}
	return idxes
}

func dataBranchSpecialUpdateColumn(
	tblStuff tableStuff,
	baseDef *plan2.TableDef,
	idx int,
) bool {
	if idx < 0 || idx >= len(tblStuff.def.colTypes) {
		return false
	}
	switch tblStuff.def.colTypes[idx].Oid {
	case types.T_geometry, types.T_geometry32:
		return true
	}
	if idx >= len(tblStuff.def.baseColNames) {
		return false
	}
	baseCol := dataBranchColumnDefByName(baseDef, tblStuff.def.baseColNames[idx])
	return baseCol != nil && baseCol.Typ.Enumvalues != ""
}

func appendOrStageDataBranchApplyRow(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	kind string,
	row []any,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
	directUpdate, stageUpdate bool,
	restoreMissing bool,
) error {
	if stageUpdate {
		return appendDataBranchApplyRowAsSQLValues(
			ctx, ses, tblStuff, diffUpdate, row, tmpValsBuffer, appender,
		)
	}
	if directUpdate && restoreMissing {
		return appendDataBranchApplyRowAsSQLValues(
			ctx, ses, tblStuff, diffInsert, row, tmpValsBuffer, appender,
		)
	}
	return appendOrExecuteDataBranchApplyRow(
		ctx, ses, tblStuff, kind, row, tmpValsBuffer, appender, directUpdate, restoreMissing,
	)
}

func appendOrExecuteDataBranchApplyRow(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	kind string,
	row []any,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
	directUpdate bool,
	restoreMissing bool,
) error {
	if !directUpdate {
		return appendDataBranchApplyRowAsSQLValues(
			ctx, ses, tblStuff, kind, row, tmpValsBuffer, appender,
		)
	}

	statements, err := dataBranchDirectUpdateSQL(
		ctx, ses, tblStuff, row, tmpValsBuffer, restoreMissing,
	)
	if err != nil {
		return err
	}
	return execSQLStatements(ctx, ses, appender.bh, appender.writeFile, statements)
}

// dataBranchDirectUpdateSQL applies a source-side non-key update without
// deleting its destination row first. The exact key predicate preserves
// FLOAT/DOUBLE identity as well as ordinary primary-key equality. A row marked
// restoreMissing is known by the diff to have been independently deleted from
// the destination and is restored with one direct INSERT ... VALUES.
func dataBranchDirectUpdateSQL(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
	restoreMissing bool,
) ([]string, error) {
	writableIdxes := tblStuff.def.writableIdxes
	if len(tblStuff.def.tarOnlyIdxes) > 0 {
		writableIdxes = tblStuff.def.commonWritableIdxes
	}
	qualifiedName := qualifiedTableName(
		tblStuff.baseRel.GetTableDef(ctx).DbName,
		tblStuff.baseRel.GetTableDef(ctx).Name,
	)

	buf.Reset()
	buf.WriteString("update ")
	buf.WriteString(qualifiedName)
	buf.WriteString(" set ")
	written := 0
	for _, idx := range writableIdxes {
		if slices.Contains(tblStuff.def.pkColIdxes, idx) {
			continue
		}
		if written > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(quoteIdentifierForSQL(tblStuff.def.baseColNames[idx]))
		buf.WriteString(" = ")
		if err := formatValIntoString(ses, row[idx], tblStuff.def.colTypes[idx], buf); err != nil {
			return nil, err
		}
		written++
	}
	if written == 0 {
		return nil, moerr.NewInternalErrorNoCtx("Data Branch update has no writable non-key columns")
	}
	buf.WriteString(" where ")
	if err := writeExactDataBranchKeyPredicate(ses, tblStuff, row, buf); err != nil {
		return nil, err
	}
	buf.WriteString(" limit 1")
	updateSQL := buf.String()
	if !restoreMissing {
		return []string{updateSQL}, nil
	}

	buf.Reset()
	buf.WriteString("insert into ")
	buf.WriteString(qualifiedName)
	buf.WriteString(" (")
	buf.WriteString(strings.Join(quotedBaseColumnNamesByIdxes(tblStuff, writableIdxes), ","))
	buf.WriteString(") values ")
	if err := writeInsertRowValues(ses, tblStuff, row, buf, writableIdxes); err != nil {
		return nil, err
	}
	return []string{buf.String()}, nil
}

func writeExactDataBranchKeyPredicate(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	var literal bytes.Buffer
	for i, idx := range tblStuff.def.pkColIdxes {
		if i > 0 {
			buf.WriteString(" and ")
		}
		literal.Reset()
		if err := formatValIntoString(ses, row[idx], tblStuff.def.colTypes[idx], &literal); err != nil {
			return err
		}
		buf.WriteString(dataBranchSQLKeyEqual(
			quoteIdentifierForSQL(tblStuff.def.baseColNames[idx]),
			literal.String(),
			tblStuff.def.colTypes[idx],
		))
	}
	return nil
}

func prepareFSForDiffAsFile(
	ctx context.Context,
	ses *Session,
	stmt *tree.DataBranchDiff,
	tblStuff tableStuff,
) (
	sqlRetPath, sqlRetHint string,
	writeFile func([]byte) error,
	release func() error,
	cleanup func(),
	err error,
) {
	var (
		ok           bool
		stagePath    string
		fileName     string
		fullFilePath string
	)

	if fileName, err = makeFileName(stmt.BaseTable.AtTsExpr, stmt.TargetTable.AtTsExpr, tblStuff); err != nil {
		return
	}
	fileName += ".sql"

	sqlRetPath = path.Join(stmt.OutputOpt.DirPath, fileName)

	if stagePath, ok, err = tryDecodeStagePath(ses, stmt.OutputOpt.DirPath); err != nil {
		return
	} else if ok {
		sqlRetPath = strings.Replace(sqlRetPath, "stage:/", "stage://", 1)
		fullFilePath = path.Join(stagePath, fileName)
	} else {
		fullFilePath = path.Join(stmt.OutputOpt.DirPath, fileName)
	}

	baseTableName := qualifiedTableName(
		tblStuff.baseRel.GetTableDef(ctx).DbName,
		tblStuff.baseRel.GetTableName(),
	)
	sqlRetHint = fmt.Sprintf("DELETE FROM %s, INSERT INTO %s", baseTableName, baseTableName)

	var (
		targetFS   fileservice.FileService
		targetPath string
		fsPath     fileservice.Path
	)

	if fsPath, err = fileservice.ParsePath(fullFilePath); err != nil {
		return
	}

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(ses.GetService()).FileService
		targetPath = fullFilePath
	} else {
		// ensure local dir exists when using implicit local ETL path
		if fsPath.Service == "" {
			if mkErr := os.MkdirAll(filepath.Dir(fullFilePath), 0o755); mkErr != nil {
				err = mkErr
				return
			}
		}

		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, fullFilePath); err != nil {
			return
		}
		targetFS = etlFS
	}

	cleanup = func() {
		_ = targetFS.Delete(context.Background(), targetPath)
	}

	defer func() {
		if err != nil {
			if cleanup != nil {
				cleanup()
			}
			targetFS.Close(ctx)
		}
	}()

	if err = targetFS.Delete(ctx, targetPath); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return
	}

	if mfs, ok := targetFS.(fileservice.MutableFileService); ok {
		// SQL diff needs append, so pre-create the file and return an appender.
		if err = targetFS.Write(ctx, fileservice.IOVector{
			FilePath: targetPath,
			Entries: []fileservice.IOEntry{
				{Size: 0, Data: []byte{}},
			},
		}); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
			return
		}

		var mut fileservice.Mutator
		if mut, err = mfs.NewMutator(ctx, targetPath); err != nil {
			return
		}

		writeFile = func(fileContent []byte) error {
			return mut.Append(ctx, fileservice.IOEntry{
				Size: int64(len(fileContent)),
				Data: fileContent,
			})
		}

		release = func() error {
			closeErr := mut.Close()
			targetFS.Close(ctx)
			return closeErr
		}
	} else {
		if writeFile, release, err = newSingleWriteAppender(
			ctx, tblStuff.worker, targetFS, targetPath,
		); err != nil {
			return
		}
	}

	return
}

func newSingleWriteAppender(
	ctx context.Context,
	worker *ants.Pool,
	targetFS fileservice.FileService,
	targetPath string,
) (writeFile func([]byte) error, release func() error, err error) {
	pr, pw := io.Pipe()
	done := make(chan error, 1)

	if worker == nil {
		err = moerr.NewInternalErrorNoCtx("worker pool is nil")
		return
	}

	if err = worker.Submit(func() {
		defer close(done)
		vec := fileservice.IOVector{
			FilePath: targetPath,
			Entries: []fileservice.IOEntry{
				{
					ReaderForWrite: pr,
					Size:           -1,
				},
			},
		}
		if wErr := targetFS.Write(ctx, vec); wErr != nil {
			_ = pr.CloseWithError(wErr)
			done <- wErr
			return
		}
		done <- pr.Close()
	}); err != nil {
		_ = pr.Close()
		_ = pw.Close()
		return
	}

	writeFile = func(fileContent []byte) error {
		if len(fileContent) == 0 {
			return nil
		}
		_, err := pw.Write(fileContent)
		return err
	}

	release = func() error {
		closeErr := pw.Close()
		writeErr := <-done
		targetFS.Close(ctx)
		return errors.Join(closeErr, writeErr)
	}

	return
}

func removeFileIgnoreError(ctx context.Context, service, filePath string) {
	if len(filePath) == 0 {
		return
	}

	fsPath, err := fileservice.ParsePath(filePath)
	if err != nil {
		return
	}

	var (
		targetFS   fileservice.FileService
		targetPath string
	)

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(service).FileService
		targetPath = filePath
	} else {
		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, filePath); err != nil {
			return
		}
		targetFS = etlFS
	}

	_ = targetFS.Delete(ctx, targetPath)
	targetFS.Close(ctx)
}

func tryFlushDeletesOrInserts(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	newKind string,
	newValsLen int,
	newRowCnt int,
	deleteByFullRow bool,
	batchInfo *applyBatchInfo,
	deleteCnt *int,
	deletesBuf *bytes.Buffer,
	insertCnt *int,
	insertBuf *bytes.Buffer,
	writeFile func([]byte) error,
) (err error) {

	flushDeletes := func() error {
		if err = flushSqlValues(
			ctx, ses, bh, tblStuff, deletesBuf, true, deleteByFullRow, batchInfo, writeFile,
		); err != nil {
			return err
		}

		*deleteCnt = 0
		deletesBuf.Reset()
		return nil
	}

	flushInserts := func() error {
		if err = flushSqlValues(
			ctx, ses, bh, tblStuff, insertBuf, false, false, batchInfo, writeFile,
		); err != nil {
			return err
		}

		*insertCnt = 0
		insertBuf.Reset()
		return nil
	}

	// if wrapped is nil, means force flush
	if newKind != "" {
		if newKind == diffDelete {
			if deletesBuf.Len()+newValsLen >= maxSqlBatchSize ||
				*deleteCnt+newRowCnt >= maxSqlBatchCnt {
				return flushDeletes()
			}
		} else {
			insertRowsIndividually := batchInfo != nil &&
				batchInfo.insertRowsIndividually && *insertCnt > 0
			if insertBuf.Len()+newValsLen >= maxSqlBatchSize ||
				*insertCnt+newRowCnt >= maxSqlBatchCnt || insertRowsIndividually {
				if *deleteCnt > 0 {
					if err = flushDeletes(); err != nil {
						return err
					}
				}
				return flushInserts()
			}
		}
		return nil
	}

	if *deleteCnt > 0 {
		if err = flushDeletes(); err != nil {
			return err
		}
	}

	if *insertCnt > 0 {
		if err = flushInserts(); err != nil {
			return err
		}
	}

	return nil
}

type sqlValuesAppender struct {
	ctx               context.Context
	ses               *Session
	bh                BackgroundExec
	tblStuff          tableStuff
	deleteByFullRow   bool
	deleteKeyColIdxes []int
	batchInfo         *applyBatchInfo
	deleteCnt         *int
	deleteBuf         *bytes.Buffer
	insertCnt         *int
	insertBuf         *bytes.Buffer
	updateState       *dataBranchUpdateBuffer
	writeFile         func([]byte) error
}

type dataBranchUpdateBuffer struct {
	cnt int
	buf bytes.Buffer
}

func (sva sqlValuesAppender) flushAll() error {
	if err := sva.flushDeletesOrInserts(); err != nil {
		return err
	}
	return sva.flushUpdates()
}

func (sva sqlValuesAppender) flushDeletesOrInserts() error {
	return tryFlushDeletesOrInserts(
		sva.ctx, sva.ses, sva.bh, sva.tblStuff, "",
		0, 0, sva.deleteByFullRow, sva.batchInfo, sva.deleteCnt, sva.deleteBuf, sva.insertCnt, sva.insertBuf, sva.writeFile,
	)
}

func (sva sqlValuesAppender) flushUpdates() error {
	if sva.updateState == nil || sva.updateState.cnt == 0 {
		return nil
	}
	if err := sva.flushDeletesOrInserts(); err != nil {
		return err
	}
	if err := flushStagedUpdateValues(
		sva.ctx, sva.ses, sva.bh, sva.updateState.buf.Bytes(), sva.batchInfo, sva.writeFile,
	); err != nil {
		return err
	}
	sva.updateState.cnt = 0
	sva.updateState.buf.Reset()
	return nil
}

func writeInsertRowValues(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
	idxes []int,
) error {
	buf.WriteString("(")
	if err := writeRowValueList(ses, tblStuff, row, buf, idxes); err != nil {
		return err
	}
	buf.WriteString(")")
	return nil
}

func writeRowValueList(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
	idxes []int,
) error {
	for i, idx := range idxes {
		if err := formatValIntoString(ses, row[idx], tblStuff.def.colTypes[idx], buf); err != nil {
			return err
		}
		if i != len(idxes)-1 {
			buf.WriteString(",")
		}
	}
	return nil
}

func quotedBaseColumnNamesByIdxes(tblStuff tableStuff, idxes []int) []string {
	names := make([]string, len(idxes))
	for i, idx := range idxes {
		names[i] = quoteIdentifierForSQL(tblStuff.def.baseColNames[idx])
	}
	return names
}

func quotedColumnNames(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteIdentifierForSQL(name)
	}
	return quoted
}

func joinQuotedColumnNames(names []string) string {
	return strings.Join(quotedColumnNames(names), ",")
}

func writeDeleteRowValues(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	return writeDeleteRowValuesWithColIdxes(ses, tblStuff, row, tblStuff.def.pkColIdxes, buf, false)
}

func writeDeleteRowValuesAsTuple(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	buf *bytes.Buffer,
) error {
	return writeDeleteRowValuesWithColIdxes(ses, tblStuff, row, tblStuff.def.pkColIdxes, buf, true)
}

func writeDeleteRowValuesWithColIdxes(
	ses *Session,
	tblStuff tableStuff,
	row []any,
	colIdxes []int,
	buf *bytes.Buffer,
	alwaysTuple bool,
) error {
	if len(colIdxes) == 0 {
		return moerr.NewInternalErrorNoCtx("data branch delete key columns are empty")
	}
	if alwaysTuple || len(colIdxes) > 1 {
		buf.WriteString("(")
	}
	for i, colIdx := range colIdxes {
		if err := formatValIntoString(ses, row[colIdx], tblStuff.def.colTypes[colIdx], buf); err != nil {
			return err
		}
		if i != len(colIdxes)-1 {
			buf.WriteString(",")
		}
	}
	if alwaysTuple || len(colIdxes) > 1 {
		buf.WriteString(")")
	}
	return nil
}

func (sva sqlValuesAppender) appendRow(kind string, rowValues []byte) error {
	if kind == diffUpdate {
		return sva.appendUpdateRow(rowValues)
	}
	if sva.updateState != nil && sva.updateState.cnt > 0 {
		if err := sva.flushUpdates(); err != nil {
			return err
		}
	}

	var (
		targetBuf *bytes.Buffer
		rowCnt    *int
	)

	if kind == diffDelete {
		targetBuf = sva.deleteBuf
		rowCnt = sva.deleteCnt
		if sva.deleteByFullRow {
			newValsLen := len(rowValues)
			if err := tryFlushDeletesOrInserts(
				sva.ctx, sva.ses, sva.bh, sva.tblStuff, kind, newValsLen, 1, sva.deleteByFullRow,
				sva.batchInfo, sva.deleteCnt, sva.deleteBuf, sva.insertCnt, sva.insertBuf, sva.writeFile,
			); err != nil {
				return err
			}

			targetBuf.Write(rowValues)
			*rowCnt++
			return nil
		}
	} else {
		targetBuf = sva.insertBuf
		rowCnt = sva.insertCnt
	}

	newValsLen := len(rowValues)
	if targetBuf.Len() > 0 {
		newValsLen++
	}

	if err := tryFlushDeletesOrInserts(
		sva.ctx, sva.ses, sva.bh, sva.tblStuff, kind, newValsLen, 1, sva.deleteByFullRow,
		sva.batchInfo, sva.deleteCnt, sva.deleteBuf, sva.insertCnt, sva.insertBuf, sva.writeFile,
	); err != nil {
		return err
	}

	if targetBuf.Len() > 0 {
		targetBuf.WriteString(",")
	}
	targetBuf.Write(rowValues)
	*rowCnt++
	return nil
}

func (sva sqlValuesAppender) appendUpdateRow(rowValues []byte) error {
	if sva.updateState == nil {
		return moerr.NewInternalErrorNoCtx("Data Branch update buffer is not initialized")
	}
	if sva.batchInfo == nil || sva.batchInfo.disableInsertStage {
		return moerr.NewInternalErrorNoCtx("Data Branch update staging is unavailable")
	}
	if sva.updateState.cnt == 0 {
		if err := sva.flushDeletesOrInserts(); err != nil {
			return err
		}
	}

	newValsLen := len(rowValues)
	if sva.updateState.buf.Len() > 0 {
		newValsLen++
	}
	if sva.updateState.buf.Len()+newValsLen >= maxSqlBatchSize ||
		sva.updateState.cnt+1 >= maxSqlBatchCnt {
		if err := sva.flushUpdates(); err != nil {
			return err
		}
	}
	if sva.updateState.buf.Len() > 0 {
		sva.updateState.buf.WriteString(",")
	}
	sva.updateState.buf.Write(rowValues)
	sva.updateState.cnt++
	return nil
}

func qualifiedTableName(dbName, tableName string) string {
	return fmt.Sprintf("%s.%s", quoteIdentifierForSQL(dbName), quoteIdentifierForSQL(tableName))
}

func execSQLStatements(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	writeFile func([]byte) error,
	stmts []string,
) error {
	return execSQLStatementsWithMode(ctx, ses, bh, writeFile, stmts, false)
}

func execRetryableSQLStatements(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	writeFile func([]byte) error,
	stmts []string,
) error {
	return execSQLStatementsWithMode(ctx, ses, bh, writeFile, stmts, true)
}

func execSQLStatementsWithMode(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	writeFile func([]byte) error,
	stmts []string,
	retryable bool,
) error {
	for _, stmt := range stmts {
		if stmt == "" {
			continue
		}
		if writeFile != nil {
			if err := writeFile([]byte(stmt + ";\n")); err != nil {
				return err
			}
			continue
		}
		var (
			ret executor.Result
			err error
		)
		if retryable {
			ret, err = runSqlWithBackExec(ctx, ses, bh, stmt)
		} else {
			ret, err = runSql(ctx, ses, bh, stmt, nil, nil)
		}
		if len(ret.Batches) > 0 && ret.Mp != nil {
			ret.Close()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func initApplyTables(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	batchInfo *applyBatchInfo,
	writeFile func([]byte) error,
) error {
	if batchInfo == nil {
		return nil
	}
	if err := batchInfo.validateDeleteKeyLayout(); err != nil {
		return err
	}

	baseTable := qualifiedTableName(batchInfo.dbName, batchInfo.baseTable)
	deleteTable := qualifiedTableName(batchInfo.dbName, batchInfo.deleteTable)
	insertTable := qualifiedTableName(batchInfo.dbName, batchInfo.insertTable)
	updateTable := qualifiedTableName(batchInfo.dbName, batchInfo.updateTable)

	deleteStageNames := batchInfo.deleteStageNames
	deleteSelectExprs := make([]string, len(batchInfo.deleteKeyNames))
	for i := range batchInfo.deleteKeyNames {
		deleteSelectExprs[i] = fmt.Sprintf(
			"%s as %s",
			quoteIdentifierForSQL(batchInfo.deleteKeyNames[i]),
			quoteIdentifierForSQL(deleteStageNames[i]),
		)
	}
	deleteCols := strings.Join(deleteSelectExprs, ",")
	insertCols := joinQuotedColumnNames(batchInfo.writableNames)
	updateSelectExprs := make([]string, 0, len(batchInfo.writableNames)+len(batchInfo.deleteKeyNames))
	for _, name := range batchInfo.writableNames {
		updateSelectExprs = append(updateSelectExprs, quoteIdentifierForSQL(name))
	}
	for i := range batchInfo.deleteKeyNames {
		updateSelectExprs = append(updateSelectExprs, fmt.Sprintf(
			"%s as %s",
			quoteIdentifierForSQL(batchInfo.deleteKeyNames[i]),
			quoteIdentifierForSQL(batchInfo.deleteStageNames[i]),
		))
	}
	updateCols := strings.Join(updateSelectExprs, ",")

	stmts := []string{
		fmt.Sprintf("drop table if exists %s", deleteTable),
		fmt.Sprintf("create table %s as select %s from %s where 1=0", deleteTable, deleteCols, baseTable),
	}
	if !batchInfo.disableInsertStage {
		stmts = append(stmts,
			fmt.Sprintf("drop table if exists %s", insertTable),
			fmt.Sprintf("create table %s as select %s from %s where 1=0", insertTable, insertCols, baseTable),
			fmt.Sprintf("drop table if exists %s", updateTable),
			fmt.Sprintf("create table %s as select %s from %s where 1=0", updateTable, updateCols, baseTable),
		)
	}

	return execRetryableSQLStatements(ctx, ses, bh, writeFile, stmts)
}

func dropApplyTables(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	batchInfo *applyBatchInfo,
	writeFile func([]byte) error,
) error {
	if batchInfo == nil {
		return nil
	}

	deleteTable := qualifiedTableName(batchInfo.dbName, batchInfo.deleteTable)
	insertTable := qualifiedTableName(batchInfo.dbName, batchInfo.insertTable)
	updateTable := qualifiedTableName(batchInfo.dbName, batchInfo.updateTable)

	stmts := []string{
		fmt.Sprintf("drop table if exists %s", deleteTable),
	}
	if !batchInfo.disableInsertStage {
		stmts = append(stmts,
			fmt.Sprintf("drop table if exists %s", insertTable),
			fmt.Sprintf("drop table if exists %s", updateTable),
		)
	}
	return execRetryableSQLStatements(ctx, ses, bh, writeFile, stmts)
}

func flushStagedUpdateValues(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	values []byte,
	batchInfo *applyBatchInfo,
	writeFile func([]byte) error,
) error {
	if len(values) == 0 {
		return nil
	}
	if batchInfo == nil || batchInfo.disableInsertStage {
		return moerr.NewInternalErrorNoCtx("Data Branch update staging is unavailable")
	}
	if err := batchInfo.validateDeleteKeyLayout(); err != nil {
		return err
	}

	baseTable := qualifiedTableName(batchInfo.dbName, batchInfo.baseTable)
	updateTable := qualifiedTableName(batchInfo.dbName, batchInfo.updateTable)
	insertStmt := fmt.Sprintf("insert into %s values %s", updateTable, values)
	updateStmt, err := batchInfo.stagedUpdateSQL(baseTable, updateTable)
	if err != nil {
		return err
	}
	specialUpdateStmts := batchInfo.stagedSpecialUpsertSQL(baseTable, updateTable)
	clearStmt := fmt.Sprintf("delete from %s", updateTable)
	stmts := make([]string, 0, 2+len(specialUpdateStmts))
	stmts = append(stmts, insertStmt)
	if updateStmt != "" {
		stmts = append(stmts, updateStmt)
	}
	stmts = append(stmts, specialUpdateStmts...)
	stmts = append(stmts, clearStmt)
	return execRetryableSQLStatements(ctx, ses, bh, writeFile, stmts)
}

// if `writeFile` is not nil, the sql will be flushed down into this file, or
// the sql will be executed by bh.
func flushSqlValues(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	tblStuff tableStuff,
	buf *bytes.Buffer,
	isDeleteFrom bool,
	deleteByFullRow bool,
	batchInfo *applyBatchInfo,
	writeFile func([]byte) error,
) (err error) {

	if buf.Len() == 0 {
		return nil
	}

	if isDeleteFrom && deleteByFullRow {
		if writeFile != nil {
			return writeFile(buf.Bytes())
		}

		statements := bytes.Split(buf.Bytes(), []byte(";\n"))
		for _, stmt := range statements {
			stmt = bytes.TrimSpace(stmt)
			if len(stmt) == 0 {
				continue
			}
			var ret executor.Result
			ret, err = runSql(ctx, ses, bh, string(stmt)+";", nil, nil)
			ret.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}

	if batchInfo != nil {
		baseTable := qualifiedTableName(batchInfo.dbName, batchInfo.baseTable)
		deleteTable := qualifiedTableName(batchInfo.dbName, batchInfo.deleteTable)
		insertTable := qualifiedTableName(batchInfo.dbName, batchInfo.insertTable)

		if isDeleteFrom {
			insertStmt := fmt.Sprintf("insert into %s values %s", deleteTable, buf.String())
			deleteStmt, err := batchInfo.stagedDeleteSQL(baseTable, deleteTable)
			if err != nil {
				return err
			}
			clearStmt := fmt.Sprintf("delete from %s", deleteTable)
			return execRetryableSQLStatements(ctx, ses, bh, writeFile, []string{insertStmt, deleteStmt, clearStmt})
		}

		if !batchInfo.disableInsertStage {
			insertStmt := fmt.Sprintf("insert into %s values %s", insertTable, buf.String())
			cols := joinQuotedColumnNames(batchInfo.writableNames)
			applyStmt := fmt.Sprintf(
				"insert into %s (%s) select %s from %s",
				baseTable, cols, cols, insertTable,
			)
			clearStmt := fmt.Sprintf("delete from %s", insertTable)
			return execRetryableSQLStatements(ctx, ses, bh, writeFile, []string{insertStmt, applyStmt, clearStmt})
		}
	}

	sqlBuffer := acquireBuffer(tblStuff.bufPool)
	defer releaseBuffer(tblStuff.bufPool, sqlBuffer)

	initInsertIntoBuf := func() {
		insertIdxes := tblStuff.def.writableIdxes
		if len(tblStuff.def.tarOnlyIdxes) > 0 {
			insertIdxes = tblStuff.def.commonWritableIdxes
		}
		sqlBuffer.WriteString(fmt.Sprintf(
			"insert into %s (%s) values ",
			qualifiedTableName(
				tblStuff.baseRel.GetTableDef(ctx).DbName,
				tblStuff.baseRel.GetTableDef(ctx).Name,
			),
			strings.Join(quotedBaseColumnNamesByIdxes(tblStuff, insertIdxes), ","),
		))
	}

	initDeleteFromBuf := func() {
		if len(tblStuff.def.pkColIdxes) == 1 {
			sqlBuffer.WriteString(fmt.Sprintf(
				"delete from %s where %s in (",
				qualifiedTableName(
					tblStuff.baseRel.GetTableDef(ctx).DbName,
					tblStuff.baseRel.GetTableDef(ctx).Name,
				),
				quoteIdentifierForSQL(tblStuff.def.baseColNames[tblStuff.def.pkColIdx]),
			))
		} else {
			pkNames := quotedBaseColumnNamesByIdxes(tblStuff, tblStuff.def.pkColIdxes)
			sqlBuffer.WriteString(fmt.Sprintf(
				"delete from %s where (%s) in (",
				qualifiedTableName(
					tblStuff.baseRel.GetTableDef(ctx).DbName,
					tblStuff.baseRel.GetTableDef(ctx).Name,
				),
				strings.Join(pkNames, ","),
			))
		}
	}

	if isDeleteFrom {
		initDeleteFromBuf()
		sqlBuffer.Write(buf.Bytes())
		sqlBuffer.WriteString(")")
	} else {
		initInsertIntoBuf()
		sqlBuffer.Write(buf.Bytes())
	}

	sqlBuffer.WriteString(";\n")

	if writeFile != nil {
		err = writeFile(sqlBuffer.Bytes())
	} else {
		var (
			ret executor.Result
		)

		defer func() {
			ret.Close()
		}()
		ret, err = runSql(ctx, ses, bh, sqlBuffer.String(), nil, nil)
	}

	return err
}
func validateOutputDirPath(ctx context.Context, ses *Session, dirPath string) (err error) {
	if len(dirPath) == 0 {
		return nil
	}

	var (
		stagePath    string
		ok           bool
		inputDirPath = dirPath
	)

	if stagePath, ok, err = tryDecodeStagePath(ses, dirPath); err != nil {
		return
	} else if ok {
		dirPath = stagePath
	}

	var fsPath fileservice.Path
	if fsPath, err = fileservice.ParsePath(dirPath); err != nil {
		return
	}

	if fsPath.Service == "" {
		var info os.FileInfo
		if info, err = os.Stat(dirPath); err != nil {
			if os.IsNotExist(err) {
				return moerr.NewInvalidInputNoCtxf("output directory %s does not exist", inputDirPath)
			}
			return
		}
		if !info.IsDir() {
			return moerr.NewInvalidInputNoCtxf("output directory %s is not a directory", inputDirPath)
		}
		return nil
	}

	var (
		targetFS   fileservice.FileService
		targetPath string
		entry      *fileservice.DirEntry
	)

	if fsPath.Service == defines.SharedFileServiceName {
		targetFS = getPu(ses.GetService()).FileService
		targetPath = dirPath
	} else {
		var etlFS fileservice.ETLFileService
		if etlFS, targetPath, err = fileservice.GetForETL(ctx, nil, dirPath); err != nil {
			return
		}
		targetFS = etlFS
		defer targetFS.Close(ctx)
	}

	if len(strings.Trim(targetPath, "/")) == 0 {
		// service root: try listing to ensure the bucket/root is reachable
		for _, err = range targetFS.List(ctx, targetPath) {
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					return moerr.NewInvalidInputNoCtxf("output directory %s does not exist", inputDirPath)
				}
				return err
			}
			// any entry means list works
			break
		}
		return nil
	}

	if entry, err = targetFS.StatFile(ctx, targetPath); err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			// fallthrough to List-based directory detection
		} else {
			return
		}
	}

	// StatFile succeeded: reject if it's a file, accept if the FS can stat directories.
	if entry != nil {
		if !entry.IsDir {
			return moerr.NewInvalidInputNoCtxf("output directory %s is not a directory", inputDirPath)
		}
		return nil
	}

	// StatFile can't prove directory existence (common for object storage). Use parent List to
	// detect the child entry and its type.
	trimmedPath := strings.TrimRight(targetPath, "/")
	if len(trimmedPath) == 0 {
		// root of the service, treat as directory
		return nil
	}
	parent, base := path.Split(trimmedPath)
	parent = strings.TrimRight(parent, "/")
	if len(base) == 0 {
		// target is root
		return nil
	}

	for entry, err = range targetFS.List(ctx, parent) {
		if err != nil {
			return err
		}
		if entry.Name != base {
			continue
		}
		if !entry.IsDir {
			return moerr.NewInvalidInputNoCtxf("output directory %s is not a directory", inputDirPath)
		}
		return nil
	}

	return moerr.NewInvalidInputNoCtxf("output directory %s does not exist", inputDirPath)
}
