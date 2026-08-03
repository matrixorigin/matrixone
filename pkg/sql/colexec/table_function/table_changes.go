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

package table_function

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type changeWatermarkState struct {
	simpleOneBatchState
}

func changeWatermarkPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &changeWatermarkState{}, nil
}

func (s *changeWatermarkState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	_ process.Analyzer,
) error {
	s.startPreamble(tf, proc, nthRow)
	watermark := types.TimestampToTS(proc.GetTxnOperator().SnapshotTS()).ToString()
	if err := vector.AppendBytes(s.batch.Vecs[0], []byte(watermark), false, proc.Mp()); err != nil {
		return err
	}
	s.batch.SetRowCount(1)
	return nil
}

type tableChangesState struct {
	batch             *batch.Batch
	called            bool
	from              types.TS
	to                types.TS
	accountID         uint32
	tableDef          *plan.TableDef
	relation          engine.Relation
	handle            engine.ChangesHandle
	isAccountFiltered bool
	accountColumnIdx  int
}

func tableChangesPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	var err error
	tf.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	if err != nil {
		return nil, err
	}
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	return &tableChangesState{accountColumnIdx: -1}, nil
}

func (s *tableChangesState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	_ process.Analyzer,
) error {
	if err := s.closeHandle(); err != nil {
		return err
	}
	s.called = false
	s.relation = nil
	s.tableDef = nil
	s.isAccountFiltered = false
	s.accountColumnIdx = -1
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	} else {
		s.batch.CleanOnlyData()
	}

	databaseName, err := requiredTableChangesString(proc, tf.ctr.argVecs[0], nthRow, "database name")
	if err != nil {
		return err
	}
	tableName, err := requiredTableChangesString(proc, tf.ctr.argVecs[1], nthRow, "table name")
	if err != nil {
		return err
	}
	after, err := requiredTableChangesString(proc, tf.ctr.argVecs[2], nthRow, "after")
	if err != nil {
		return err
	}
	until, err := requiredTableChangesString(proc, tf.ctr.argVecs[3], nthRow, "until")
	if err != nil {
		return err
	}
	s.from, err = parseTableChangesTS(after, true)
	if err != nil {
		return moerr.NewInvalidInputf(proc.Ctx, "invalid table_changes after: %s", err)
	}
	s.to, err = parseTableChangesTS(until, false)
	if err != nil {
		return moerr.NewInvalidInputf(proc.Ctx, "invalid table_changes until: %s", err)
	}
	if !s.from.IsEmpty() {
		if !s.from.LT(&s.to) {
			return moerr.NewInvalidInput(proc.Ctx, "table_changes until must be greater than after")
		}
		s.from = s.from.Next()
	}

	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok {
		return moerr.NewInternalError(proc.Ctx, "engine is missing from table_changes context")
	}
	s.accountID, err = defines.GetAccountId(proc.Ctx)
	if err != nil {
		return err
	}
	relationCtx := proc.Ctx
	if len(tf.Params) > 0 && tf.Params[0] == 1 {
		s.isAccountFiltered = true
		relationCtx = defines.AttachAccountId(relationCtx, catalog.System_Account)
	}
	db, err := e.Database(relationCtx, databaseName, proc.GetTxnOperator())
	if err != nil {
		return err
	}
	s.relation, err = db.Relation(relationCtx, tableName, nil)
	if err != nil {
		return err
	}
	s.tableDef = s.relation.CopyTableDef(relationCtx)
	if err := validateRuntimeTableChangesSource(s.tableDef); err != nil {
		return err
	}
	if s.isAccountFiltered {
		s.isAccountFiltered = true
		idx, ok := s.tableDef.Name2ColIndex["account_id"]
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "account-filtered table has no account_id column")
		}
		s.accountColumnIdx = int(idx)
	}
	return nil
}

func (s *tableChangesState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if s.called {
		return vm.CancelResult, nil
	}
	if s.handle == nil {
		ctx := engine.WithSnapshotReadPolicy(proc.Ctx, engine.SnapshotReadPolicyVisibleState)
		if s.isAccountFiltered {
			// Cluster and shared catalog tables are physically owned by the
			// system account. Read that physical change stream, then apply the
			// caller's account_id predicate while materializing rows.
			ctx = defines.AttachAccountId(ctx, catalog.System_Account)
		}
		handle, err := s.relation.CollectChanges(ctx, s.from, s.to, false, proc.Mp())
		if err != nil {
			return vm.CancelResult, err
		}
		s.handle = handle
	}
	for {
		data, tombstone, _, err := s.handle.Next(proc.Ctx, proc.Mp())
		if err != nil {
			return vm.CancelResult, err
		}
		if data == nil && tombstone == nil {
			s.called = true
			if err := s.closeHandle(); err != nil {
				return vm.CancelResult, err
			}
			return vm.CancelResult, nil
		}

		s.batch.CleanOnlyData()
		if err := s.appendInsertRows(tf.Attrs, data, proc); err != nil {
			cleanTableChangesSource(data, tombstone, proc)
			return vm.CancelResult, err
		}
		if err := s.appendDeleteRows(tf.Attrs, tombstone, proc); err != nil {
			cleanTableChangesSource(data, tombstone, proc)
			return vm.CancelResult, err
		}
		cleanTableChangesSource(data, tombstone, proc)
		if s.batch.RowCount() > 0 {
			return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
		}
	}
}

func (s *tableChangesState) end(_ *TableFunction, _ *process.Process) error {
	return s.closeHandle()
}

func (s *tableChangesState) reset(_ *TableFunction, _ *process.Process) {
	_ = s.closeHandle()
	s.called = false
	s.relation = nil
	s.tableDef = nil
	if s.batch != nil {
		s.batch.CleanOnlyData()
	}
}

func (s *tableChangesState) free(_ *TableFunction, proc *process.Process, _ bool, _ error) {
	_ = s.closeHandle()
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
		s.batch = nil
	}
}

func (s *tableChangesState) closeHandle() error {
	if s.handle == nil {
		return nil
	}
	err := s.handle.Close()
	s.handle = nil
	return err
}

func validateRuntimeTableChangesSource(tableDef *plan.TableDef) error {
	if tableDef == nil {
		return moerr.NewInvalidInputNoCtx("table_changes source table does not exist")
	}
	if tableDef.IsTemporary {
		return moerr.NewNotSupportedNoCtx("table_changes does not support temporary tables")
	}
	switch tableDef.TableType {
	case catalog.SystemOrdinaryRel, catalog.SystemClusterRel:
	default:
		return moerr.NewNotSupportedNoCtxf(
			"table_changes does not support table type %q",
			tableDef.TableType,
		)
	}
	if tableDef.Partition != nil {
		return moerr.NewNotSupportedNoCtx("table_changes does not support partitioned tables")
	}
	if tableDef.Pkey == nil ||
		len(tableDef.Pkey.Names) == 0 ||
		tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return moerr.NewNotSupportedNoCtx("table_changes requires an explicit primary key")
	}
	if tableDef.TableType == catalog.SystemClusterRel &&
		!containsTableChangesKey(tableDef.Pkey.Names, "account_id") {
		return moerr.NewNotSupportedNoCtx(
			"table_changes requires cluster table primary keys to include account_id",
		)
	}
	return nil
}

func containsTableChangesKey(names []string, target string) bool {
	for _, name := range names {
		if strings.EqualFold(name, target) {
			return true
		}
	}
	return false
}

func requiredTableChangesString(
	proc *process.Process,
	vec *vector.Vector,
	row int,
	name string,
) (string, error) {
	if vec == nil || vec.GetNulls().Contains(uint64(row)) {
		return "", moerr.NewInvalidInputf(proc.Ctx, "table_changes %s cannot be NULL", name)
	}
	switch vec.GetType().Oid {
	case types.T_varchar, types.T_char, types.T_text:
		return vec.GetStringAt(row), nil
	default:
		return "", moerr.NewInvalidInputf(proc.Ctx, "table_changes %s must be a string", name)
	}
}

func parseTableChangesTS(value string, allowEmpty bool) (types.TS, error) {
	if value == "" && allowEmpty {
		return types.TS{}, nil
	}
	physicalText, logicalText, ok := strings.Cut(value, "-")
	if !ok || physicalText == "" || logicalText == "" || strings.Contains(logicalText, "-") {
		return types.TS{}, moerr.NewInvalidInputNoCtx("timestamp must use physical-logical format")
	}
	physical, err := strconv.ParseInt(physicalText, 10, 64)
	if err != nil || physical < 0 {
		return types.TS{}, moerr.NewInvalidInputNoCtx("timestamp physical part must be a non-negative int64")
	}
	logical, err := strconv.ParseUint(logicalText, 10, 32)
	if err != nil {
		return types.TS{}, moerr.NewInvalidInputNoCtx("timestamp logical part must be a uint32")
	}
	return types.BuildTS(physical, uint32(logical)), nil
}

func (s *tableChangesState) appendInsertRows(
	attrs []string,
	src *batch.Batch,
	proc *process.Process,
) error {
	if src == nil {
		return nil
	}
	for row := 0; row < src.RowCount(); row++ {
		if s.isAccountFiltered {
			accountID := vector.GetFixedAtNoTypeCheck[uint32](src.Vecs[s.accountColumnIdx], row)
			if accountID != s.accountID {
				continue
			}
		}
		commitTS := vector.GetFixedAtNoTypeCheck[types.TS](src.Vecs[len(src.Vecs)-1], row)
		if err := s.appendChangeRow(attrs, "insert", commitTS, src, row, nil, proc); err != nil {
			return err
		}
	}
	return nil
}

func (s *tableChangesState) appendDeleteRows(
	attrs []string,
	src *batch.Batch,
	proc *process.Process,
) error {
	if src == nil {
		return nil
	}
	for row := 0; row < src.RowCount(); row++ {
		keyValues, err := s.deleteKeyValues(src.Vecs[0], row)
		if err != nil {
			return err
		}
		if s.isAccountFiltered {
			accountValue, ok := keyValues["account_id"]
			if !ok {
				return moerr.NewInternalError(proc.Ctx, "account-filtered delete has no account_id key")
			}
			accountID, ok := accountValue.(uint32)
			if !ok || accountID != s.accountID {
				continue
			}
		}
		commitTS := vector.GetFixedAtNoTypeCheck[types.TS](src.Vecs[1], row)
		if err := s.appendChangeRow(attrs, "delete", commitTS, nil, 0, keyValues, proc); err != nil {
			return err
		}
	}
	return nil
}

func (s *tableChangesState) deleteKeyValues(keyVec *vector.Vector, row int) (map[string]any, error) {
	values := make(map[string]any, len(s.tableDef.Pkey.Names))
	if len(s.tableDef.Pkey.Names) == 1 {
		values[s.tableDef.Pkey.Names[0]] = vector.GetAny(keyVec, row, false)
		return values, nil
	}
	tuple, err := types.Unpack(keyVec.GetBytesAt(row))
	if err != nil {
		return nil, err
	}
	if len(tuple) != len(s.tableDef.Pkey.Names) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"unexpected composite primary key part count %d, expected %d",
			len(tuple),
			len(s.tableDef.Pkey.Names),
		)
	}
	for i, name := range s.tableDef.Pkey.Names {
		values[name] = tuple[i]
	}
	return values, nil
}

func (s *tableChangesState) appendChangeRow(
	attrs []string,
	changeType string,
	commitTS types.TS,
	src *batch.Batch,
	srcRow int,
	deleteKeys map[string]any,
	proc *process.Process,
) error {
	for outputCol, attr := range attrs {
		var err error
		switch attr {
		case "change_type":
			err = vector.AppendBytes(s.batch.Vecs[outputCol], []byte(changeType), false, proc.Mp())
		case "commit_ts":
			err = vector.AppendBytes(s.batch.Vecs[outputCol], []byte(commitTS.ToString()), false, proc.Mp())
		case "table_id":
			err = vector.AppendFixed(s.batch.Vecs[outputCol], s.tableDef.TblId, false, proc.Mp())
		case "schema_version":
			err = vector.AppendFixed(s.batch.Vecs[outputCol], s.tableDef.Version, false, proc.Mp())
		default:
			sourceIdx, ok := s.tableDef.Name2ColIndex[strings.ToLower(attr)]
			if !ok {
				return moerr.NewInternalErrorNoCtxf("table_changes source column %q not found", attr)
			}
			if src != nil {
				isNull := src.Vecs[sourceIdx].IsNull(uint64(srcRow))
				value := vector.GetAny(src.Vecs[sourceIdx], srcRow, false)
				err = vector.AppendAny(s.batch.Vecs[outputCol], value, isNull, proc.Mp())
			} else if value, ok := deleteKeys[strings.ToLower(attr)]; ok {
				err = vector.AppendAny(s.batch.Vecs[outputCol], value, false, proc.Mp())
			} else {
				err = vector.AppendAny(s.batch.Vecs[outputCol], nil, true, proc.Mp())
			}
		}
		if err != nil {
			return err
		}
	}
	s.batch.AddRowCount(1)
	return nil
}

func cleanTableChangesSource(data, tombstone *batch.Batch, proc *process.Process) {
	if data != nil {
		data.Clean(proc.Mp())
	}
	if tombstone != nil {
		tombstone.Clean(proc.Mp())
	}
}
