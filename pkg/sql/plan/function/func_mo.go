// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statsinfopb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	AllColumns = "*"
)

// XXX Porting mo functions to function2.
// Mo function unit tests are not ported, because it is too heavy and does not test enough cases.
// Mo functions are better tested with bvt.

const (
	MoTableRowsSizeForceUpdateVarName    = "mo_table_stats.force_update"
	MoTableRowSizeUseOldImplVarName      = "mo_table_stats.use_old_impl"
	MoTableRowSizeResetUpdateTimeVarName = "mo_table_stats.reset_update_time"
)

func GetUseOldImplVariable(proc *process.Process) bool {
	var (
		err        error
		useOldImpl interface{}
	)

	if proc == nil || proc.Base == nil || proc.GetResolveVariableFunc() == nil {
		return false
	}

	if useOldImpl, err = proc.GetResolveVariableFunc()(
		MoTableRowSizeUseOldImplVarName, true, false); err != nil {
		logutil.Info("get sys variable failed",
			zap.String("variable", MoTableRowSizeUseOldImplVarName),
			zap.Error(err))

		return false
	}

	if useOldImpl == nil {
		return false
	}

	return strings.ToLower(useOldImpl.(string)) == "yes"
}

func GetForceUpdateVariable(proc *process.Process) bool {
	var (
		err         error
		forceUpdate interface{}
	)

	if proc == nil || proc.Base == nil || proc.GetResolveVariableFunc() == nil {
		return false
	}

	if forceUpdate, err = proc.GetResolveVariableFunc()(
		MoTableRowsSizeForceUpdateVarName, true, false); err != nil {
		logutil.Info("get sys variable failed",
			zap.String("variable", MoTableRowsSizeForceUpdateVarName),
			zap.Error(err))

		return false
	}

	return strings.ToLower(forceUpdate.(string)) == "yes"
}

func GetResetUpdateTimeVariable(proc *process.Process) bool {
	var (
		err             error
		resetUpdateTime interface{}
	)

	if proc == nil || proc.Base == nil || proc.GetResolveVariableFunc() == nil {
		return false
	}

	if resetUpdateTime, err = proc.GetResolveVariableFunc()(
		MoTableRowSizeResetUpdateTimeVarName, true, false); err != nil {
		logutil.Info("get sys variable failed",
			zap.String("variable", MoTableRowSizeResetUpdateTimeVarName),
			zap.Error(err))

		return false
	}

	return strings.ToLower(resetUpdateTime.(string)) == "yes"
}

func MoTableSize(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

	useOldStr := GetUseOldImplVariable(proc)
	if useOldStr {
		// the old implement
		return MoTableSizeOld(iVecs, result, proc, length, selectList)
	}

	return MoTableSizeNew(iVecs, result, proc, length, selectList)
}

func MoTableRows(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

	useOldStr := GetUseOldImplVariable(proc)
	if useOldStr {
		// the old implement
		return MoTableRowsOld(iVecs, result, proc, length, selectList)
	}

	return MoTableRowsNew(iVecs, result, proc, length, selectList)
}

//#region MoTableSizeRows New Implements

// MoTableSize/Rows get the estimated table size/rows in bytes
// an account can only query these tables that belongs to it.
// some special cases:
// 1. cluster table

// MoTableStatsResolver is synchronous and must not be retained by the executor.
type MoTableStatsResolver = func() (accs, dbs, tbls []uint64, err error)

// GetMoTableSizeRowsFuncType dispatches to the caller engine. A nil error
// with handled=false requests the existing old SQL implementation.
type GetMoTableSizeRowsFuncType = func(
	context.Context, engine.Engine, MoTableStatsResolver, bool, bool) (values []uint64, err error, handled bool)

var GetMoTableSizeFunc atomic.Pointer[GetMoTableSizeRowsFuncType]
var GetMoTableRowsFunc atomic.Pointer[GetMoTableSizeRowsFuncType]

type subscription struct {
	valid bool

	oriAccId      uint64
	oriDatabaseId uint64
	oriTableId    uint64

	oriTableName    string
	oriDatabaseName string
}

func (s subscription) String() string {
	return fmt.Sprintf("valid: %v, oriAcc(%d), oriDatabase(%d-%s), oriTable(%d-%s)",
		s.valid,
		s.oriAccId,
		s.oriDatabaseId,
		s.oriDatabaseName,
		s.oriTableId,
		s.oriTableName)
}

func isSubscribedTable(
	proc *process.Process,
	reqAcc uint32,
	db engine.Database,
	dbName, tblName string,
) (sub subscription, err error) {

	var (
		sql  string
		ret  [][]interface{}
		meta *plan.SubscriptionMeta
	)

	if db.IsSubscription(proc.Ctx) {
		defer func() {
			if err != nil {
				sub.valid = false

				metaInfo := ""
				if meta != nil {
					metaInfo = fmt.Sprintf("ACC(%s,%d)-DB(%s)-TBLS(%s)",
						meta.AccountName, meta.AccountId, meta.DbName, meta.Tables)
				}

				logutil.Error("MO_TABLE_SIZE/ROWS",
					zap.String("source", "isSubscribedTable"),
					zap.Error(err),
					zap.String("sub meta", metaInfo),
					zap.Uint32("request acc", reqAcc),
					zap.String("db name", dbName),
					zap.String("tbl name", tblName),
					zap.String("subscription", sub.String()),
					zap.String("sql", commonutil.Abbreviate(sql, 500)),
				)
			}
		}()

		meta, err = proc.GetSessionInfo().SqlHelper.GetSubscriptionMeta(dbName)
		if err != nil {
			return sub,
				moerr.NewInternalErrorNoCtx(fmt.Sprintf("get subscription meta failed, err: %v", err))
		}

		if meta.Tables != pubsub.TableAll && !strings.Contains(meta.Tables, tblName) {
			return sub, moerr.NewInternalErrorNoCtx("no such subscribed table")
		}

		// check passed, get acc, db, tbl info
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		ctx = defines.AttachAccountId(ctx, uint32(sysAccountID))
		defer cancel()

		// The publisher's database and table names flow into a mo_catalog
		// query as string literals and may legally contain quotes (for
		// example a subscribed table named odd'name). Quote them through the
		// shared escape helper so one quoted identifier cannot break out of
		// the literal and fail every information_schema.TABLES read of the
		// subscription.
		sql = fmt.Sprintf(`
					select 
    					reldatabase_id, rel_id 
					from 
					    mo_catalog.mo_tables 
					where 
					    account_id = %d and reldatabase = %s and relname = %s;`,
			meta.AccountId, sqlquote.String(meta.DbName), sqlquote.String(tblName))

		ctx = process.ContextWithWarningSink(ctx, proc.WarningSink)
		ret, err = proc.GetSessionInfo().SqlHelper.ExecSqlWithCtx(ctx, sql)
		if err != nil {
			return sub,
				moerr.NewInternalErrorNoCtx(fmt.Sprintf("exec get subscribed tbl info sql failed, err: %v", err))
		}

		if len(ret) != 1 {
			return sub,
				moerr.NewInternalErrorNoCtx(fmt.Sprintf("get the subscribed tbl info empty: %s", tblName))
		}

		sub.valid = true
		sub.oriAccId = uint64(meta.AccountId)
		sub.oriDatabaseId = ret[0][0].(uint64)
		sub.oriTableId = ret[0][1].(uint64)
		sub.oriTableName = tblName
		sub.oriDatabaseName = meta.DbName

		return sub, nil
	}

	sub.valid = false
	return sub, nil
}

// checkSubscribedIndexRelation resolves the published relation in the
// publisher's account using the executing statement's transaction. The
// subscription metadata gives IDs but not the relation kind, and an older
// index relation may have been renamed outside the reserved namespace.
func checkSubscribedIndexRelation(proc *process.Process, eng engine.Engine, txn client.TxnOperator, sub subscription) error {
	ctx := defines.AttachAccountId(proc.Ctx, uint32(sub.oriAccId))
	db, err := eng.Database(ctx, sub.oriDatabaseName, txn)
	if err != nil {
		return err
	}
	rel, err := db.Relation(ctx, sub.oriTableName, nil)
	if err != nil {
		return err
	}
	if rel.GetDBID(ctx) != sub.oriDatabaseId || rel.GetTableID(ctx) != sub.oriTableId {
		return moerr.NewInternalError(proc.Ctx, "subscription relation changed during statistics lookup")
	}
	return process.CheckIndexRelationDefinitionAccess(proc, rel.GetTableName(), rel.GetTableDef(ctx))
}

func MoTableSizeRowsHelper(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	executor *atomic.Pointer[GetMoTableSizeRowsFuncType],
	fallback executeLogicOfOverload,
) (err error) {

	var (
		ok  bool
		db  engine.Database
		rel engine.Relation

		dbNull, tblNull   bool
		dbBytes, tblBytes []byte

		dbName, tblName string

		eng engine.Engine
		txn client.TxnOperator

		ret                   []uint64
		positions             []int
		accIds, dbIds, tblIds []uint64

		forceUpdate     = GetForceUpdateVariable(proc)
		resetUpdateTime = GetResetUpdateTimeVariable(proc)

		rs   *vector.FunctionResult[int64]
		dbs  vector.FunctionParameterWrapper[types.Varlena]
		tbls vector.FunctionParameterWrapper[types.Varlena]
	)

	rs = vector.MustFunctionResult[int64](result)
	dbs = vector.GenerateFunctionStrParameter(iVecs[0])
	tbls = vector.GenerateFunctionStrParameter(iVecs[1])

	eng = proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if eng == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSizeRows: mo table engine is nil")
	}

	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSizeRows: txn operator is nil")
	}

	accountId := proc.Ctx.Value(defines.TenantIDKey{}).(uint32)

	decodeNames := func(i uint64) (string, string, bool) {
		dbBytes, dbNull = dbs.GetStrValue(i)
		tblBytes, tblNull = tbls.GetStrValue(i)
		if dbNull || tblNull {
			return "", "", false
		}
		return string(dbBytes), string(tblBytes), true
	}

	defer func() {
		if err != nil {
			var names []string
			for i := uint64(0); i < uint64(length); i++ {
				d, t, _ := decodeNames(i)
				names = append(names, fmt.Sprintf("[%s-%s]; ", d, t))
			}

			logutil.Error("MoTableSizeRows",
				zap.Error(err),
				zap.String("db", dbName),
				zap.String("table", tblName),
				zap.Uint32("account id", accountId),
				zap.String("tbl list", strings.Join(names, ",")))
		}
	}()

	txn = proc.GetTxnOperator()

	resolve := func() ([]uint64, []uint64, []uint64, error) {
		positions = make([]int, length)

		for i := uint64(0); i < uint64(length); i++ {
			if dbName, tblName, ok = decodeNames(i); !ok {
				positions[i] = -1 // NULL
				continue
			}

			if err = process.CheckIndexRelationAccess(proc, tblName, "", 0); err != nil {
				return nil, nil, nil, err
			}
			if ok, err = specialTableFilterForNonSys(proc.Ctx, dbName, tblName); ok && err == nil {
				positions[i] = -2 // special table
				continue
			}

			if err != nil {
				return nil, nil, nil, err
			}

			if db, err = eng.Database(proc.Ctx, dbName, txn); err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					return nil, nil, nil, moerr.NewInternalErrorNoCtxf("db not exist: %s(%s)",
						dbName, "OkExpectedEOB")
				}
				return nil, nil, nil, err
			}

			positions[i] = len(tblIds)
			var sub subscription
			if sub, err = isSubscribedTable(
				proc, accountId, db, dbName, tblName); err != nil {
				return nil, nil, nil, err
			} else if sub.valid {
				if err = checkSubscribedIndexRelation(proc, eng, txn, sub); err != nil {
					return nil, nil, nil, err
				}
				accIds = append(accIds, sub.oriAccId)
				dbIds = append(dbIds, sub.oriDatabaseId)
				tblIds = append(tblIds, sub.oriTableId)
			} else {
				if rel, err = db.Relation(proc.Ctx, tblName, nil); err != nil {
					if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
						return nil, nil, nil, moerr.NewInternalErrorNoCtxf("tbl not exist: %s-%s(%s)",
							dbName, tblName, "OkExpectedEOB")
					}
					return nil, nil, nil, err
				}

				if err = process.CheckIndexRelationDefinitionAccess(proc, rel.GetTableName(), rel.GetTableDef(proc.Ctx)); err != nil {
					return nil, nil, nil, err
				}
				accIds = append(accIds, uint64(accountId))
				dbIds = append(dbIds, uint64(rel.GetDBID(proc.Ctx)))
				tblIds = append(tblIds, uint64(rel.GetTableID(proc.Ctx)))
			}
		}

		return accIds, dbIds, tblIds, nil
	}

	fn := executor.Load()
	if fn == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSizeRows: statistics executor is nil")
	}
	var handled bool
	ret, err, handled = (*fn)(
		proc.Ctx, eng, resolve,
		forceUpdate,
		resetUpdateTime)

	if err != nil {
		return err
	}

	if !handled {
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		return fallback(iVecs, result, proc, length, selectList)
	}
	if len(ret) != len(tblIds) {
		return moerr.NewInternalError(proc.Ctx, "MoTableSizeRows: invalid statistics cardinality")
	}
	for _, position := range positions {
		var value int64
		if position >= 0 {
			value = int64(ret[position])
		}
		if err = rs.Append(value, position == -1); err != nil {
			return err
		}
	}

	return nil
}

func MoTableSizeNew(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

	return MoTableSizeRowsHelper(iVecs, result, proc, length, selectList, &GetMoTableSizeFunc, MoTableSizeOld)
}

func MoTableRowsNew(
	iVecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {

	return MoTableSizeRowsHelper(iVecs, result, proc, length, selectList, &GetMoTableRowsFunc, MoTableRowsOld)
}

//#endregion MoTableSizeRows New Implements

//#region MoTableSizeRows Old Implements

func MoTableRowsOld(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	// XXX WTF
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableRows: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	var ok bool
	// XXX old code starts a new transaction.   why?
	for i := uint64(0); i < uint64(length); i++ {
		foolCtx := proc.Ctx

		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			var rel engine.Relation
			dbStr, tblStr := string(db), string(tbl)
			if err = process.CheckIndexRelationAccess(proc, tblStr, "", 0); err != nil {
				return err
			}

			if ok, err = specialTableFilterForNonSys(foolCtx, dbStr, tblStr); ok && err == nil {
				if err = rs.Append(int64(0), false); err != nil {
					return err
				}
				continue
			}

			if err != nil {
				return err
			}

			var dbo engine.Database
			dbo, err = e.Database(foolCtx, dbStr, txn)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					var buf bytes.Buffer
					for j := uint64(0); j < uint64(length); j++ {
						db2, _ := dbs.GetStrValue(j)
						tbl2, _ := tbls.GetStrValue(j)

						dbStr2 := functionUtil.QuickBytesToStr(db2)
						tblStr2 := functionUtil.QuickBytesToStr(tbl2)

						buf.WriteString(fmt.Sprintf("%s-%s; ", dbStr2, tblStr2))
					}

					logutil.Error(fmt.Sprintf("db not found when mo_table_size: %s-%s, extra: %s", dbStr, tblStr, buf.String()))
					return moerr.NewInvalidArgNoCtx("db not found when mo_table_size", fmt.Sprintf("%s-%s", dbStr, tblStr))
				}
				return err
			}

			var accId uint32
			accId, err = defines.GetAccountId(foolCtx)
			if err != nil {
				return err
			}

			var sub subscription
			if sub, err = isSubscribedTable(
				proc, accId, dbo, dbStr, tblStr); err != nil {
				logutil.Error("MoTableRowsOld",
					zap.String("source", "isSubscribeTable"),
					zap.Error(err))
				return err
			} else if sub.valid {
				// subscription
				foolCtx = defines.AttachAccountId(foolCtx, uint32(sub.oriAccId))
				dbo, err = e.Database(foolCtx, sub.oriDatabaseName, txn)
				if err != nil {
					return err
				}
			}

			rel, err = dbo.Relation(foolCtx, tblStr, nil)
			if err != nil {
				return err
			}
			if err = process.CheckIndexRelationDefinitionAccess(proc, rel.GetTableName(), rel.GetTableDef(foolCtx)); err != nil {
				return err
			}

			var rows uint64
			if rows, err = rel.Rows(foolCtx); err != nil {
				return err
			}

			if err = rs.Append(int64(rows), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func MoTableSizeOld(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableSize: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	var ok bool
	// XXX old code starts a new transaction.   why?
	for i := uint64(0); i < uint64(length); i++ {
		foolCtx := proc.Ctx

		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			var rel engine.Relation
			dbStr, tblStr := string(db), string(tbl)
			if err = process.CheckIndexRelationAccess(proc, tblStr, "", 0); err != nil {
				return err
			}

			if ok, err = specialTableFilterForNonSys(foolCtx, dbStr, tblStr); ok && err == nil {
				if err = rs.Append(int64(0), false); err != nil {
					return err
				}
				continue
			}

			if err != nil {
				return err
			}

			var dbo engine.Database
			dbo, err = e.Database(foolCtx, dbStr, txn)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					var buf bytes.Buffer
					for j := uint64(0); j < uint64(length); j++ {
						db2, _ := dbs.GetStrValue(j)
						tbl2, _ := tbls.GetStrValue(j)

						dbStr2 := functionUtil.QuickBytesToStr(db2)
						tblStr2 := functionUtil.QuickBytesToStr(tbl2)

						buf.WriteString(fmt.Sprintf("%s#%s; ", dbStr2, tblStr2))
					}

					originalAccId, _ := defines.GetAccountId(proc.Ctx)
					attachedAccId, _ := defines.GetAccountId(foolCtx)

					logutil.Error(
						fmt.Sprintf("db not found when mo_table_size: %s#%s, acc: %d-%d, extra: %s",
							dbStr, tblStr, attachedAccId, originalAccId, buf.String()))
					return moerr.NewInvalidArgNoCtx("db not found when mo_table_size", fmt.Sprintf("%s-%s", dbStr, tblStr))
				}
				return err
			}

			var accId uint32
			accId, err = defines.GetAccountId(foolCtx)
			if err != nil {
				return err
			}

			var sub subscription
			if sub, err = isSubscribedTable(
				proc, accId, dbo, dbStr, tblStr); err != nil {
				logutil.Error("MoTableSizeOld",
					zap.String("source", "isSubscribeTable"),
					zap.Error(err))
				return err
			} else if sub.valid {
				// subscription
				foolCtx = defines.AttachAccountId(foolCtx, uint32(sub.oriAccId))
				dbo, err = e.Database(foolCtx, sub.oriDatabaseName, txn)
				if err != nil {
					return err
				}
			}

			rel, err = dbo.Relation(foolCtx, tblStr, nil)
			if err != nil {
				return err
			}
			if err = process.CheckIndexRelationDefinitionAccess(proc, rel.GetTableName(), rel.GetTableDef(foolCtx)); err != nil {
				return err
			}

			var oSize, iSize uint64
			if oSize, err = originalTableSize(foolCtx, dbo, rel); err != nil {
				return err
			}
			if iSize, err = indexesTableSize(foolCtx, dbo, rel); err != nil {
				return err
			}

			if err = rs.Append(int64(oSize+iSize), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func originalTableSize(ctx context.Context, db engine.Database, rel engine.Relation) (size uint64, err error) {
	return getTableSize(ctx, db, rel)
}

func getTableSize(ctx context.Context, db engine.Database, rel engine.Relation) (size uint64, err error) {
	return rel.Size(ctx, AllColumns)
}

func indexesTableSize(ctx context.Context, db engine.Database, rel engine.Relation) (totalSize uint64, err error) {
	var irel engine.Relation
	var size uint64
	for _, idef := range rel.GetTableDef(ctx).Indexes {
		if idef == nil {
			continue
		}
		if irel, err = db.Relation(ctx, idef.IndexTableName, nil); err != nil {
			logutil.Info("indexesTableSize->Relation",
				zap.String("originTable", rel.GetTableName()),
				zap.String("indexTableName", idef.IndexTableName),
				zap.Error(err))
			continue
		}

		if size, err = getTableSize(ctx, db, irel); err != nil {
			logutil.Info("indexesTableSize->getTableSize",
				zap.String("originTable", rel.GetTableName()),
				zap.String("indexTableName", idef.IndexTableName),
				zap.Error(err))
			continue
		}

		totalSize += size
	}

	// this is a quick fix for the issue of the indexTableName is empty.
	// the empty indexTableName causes the `SQL parser err: table "" does not exist` err and
	// then the mo_table_size call will fail.
	// this fix does not fix the issue but only avoids the failure caused by it.
	err = nil
	return totalSize, err
}

//#endregion

//#region MoTableIndexSize

// indexSizeCandidateTableNames returns the hidden object names backing the
// secondary and unique indexes of tableDef. The prefix filter mirrors the
// SHOW TABLE STATUS Index_length scope, and the empty-name guard keeps the
// known legacy empty IndexTableName tolerant instead of failing whole calls.
func indexSizeCandidateTableNames(tableDef *plan.TableDef) []string {
	var names []string
	if tableDef == nil {
		return names
	}
	for _, idef := range tableDef.Indexes {
		if idef == nil {
			continue
		}
		name := idef.IndexTableName
		if name == "" {
			continue
		}
		if strings.HasPrefix(name, catalog.UniqueIndexTableNamePrefix) ||
			strings.HasPrefix(name, catalog.SecondaryIndexTableNamePrefix) {
			names = append(names, name)
		}
	}
	return names
}

// MoTableIndexSize returns the estimated bytes stored by a table's hidden
// secondary/unique index objects. It backs information_schema.TABLES
// INDEX_LENGTH the same way mo_table_size backs DATA_LENGTH, so both columns
// stay sourced from one statistics implementation.
func MoTableIndexSize(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {
	if GetUseOldImplVariable(proc) {
		return moTableIndexSizeOld(ivecs, result, proc, length)
	}
	return moTableIndexSizeNew(ivecs, result, proc, length)
}

// moTableIndexSizeOld resolves each hidden index object directly and sums the
// engine-reported sizes, mirroring the old mo_table_size implementation.
func moTableIndexSizeOld(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
) (err error) {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableIndexSize: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	for i := uint64(0); i < uint64(length); i++ {
		foolCtx := proc.Ctx

		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		dbStr, tblStr := string(db), string(tbl)

		if ok, ferr := specialTableFilterForNonSys(foolCtx, dbStr, tblStr); ok && ferr == nil {
			if err = rs.Append(int64(0), false); err != nil {
				return err
			}
			continue
		} else if ferr != nil {
			return ferr
		}

		dbo, derr := e.Database(foolCtx, dbStr, txn)
		if derr != nil {
			return derr
		}

		accId, aerr := defines.GetAccountId(foolCtx)
		if aerr != nil {
			return aerr
		}

		if sub, serr := isSubscribedTable(proc, accId, dbo, dbStr, tblStr); serr != nil {
			return serr
		} else if sub.valid {
			// hidden index objects live in the publishing account's database
			foolCtx = defines.AttachAccountId(foolCtx, uint32(sub.oriAccId))
			dbo, derr = e.Database(foolCtx, sub.oriDatabaseName, txn)
			if derr != nil {
				return derr
			}
		}

		rel, rerr := dbo.Relation(foolCtx, tblStr, nil)
		if rerr != nil {
			return rerr
		}

		var sum int64
		for _, hidden := range indexSizeCandidateTableNames(rel.GetTableDef(foolCtx)) {
			irel, ierr := dbo.Relation(foolCtx, hidden, nil)
			if ierr != nil {
				logutil.Info("moTableIndexSizeOld->Relation",
					zap.String("originTable", tblStr),
					zap.String("indexTableName", hidden),
					zap.Error(ierr))
				continue
			}
			size, serr := getTableSize(foolCtx, dbo, irel)
			if serr != nil {
				logutil.Info("moTableIndexSizeOld->getTableSize",
					zap.String("originTable", tblStr),
					zap.String("indexTableName", hidden),
					zap.Error(serr))
				continue
			}
			if size > uint64(math.MaxInt64-sum) {
				sum = math.MaxInt64
				break
			}
			sum += int64(size)
		}

		if err = rs.Append(sum, false); err != nil {
			return err
		}
	}
	return nil
}

// moTableIndexSizeNew flattens every (account, database, hidden index table)
// triplet across the batch and asks the shared mo_table_stats size executor
// for all of them at once, then sums each row's slice.
func moTableIndexSizeNew(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
) (err error) {
	var (
		ret                   []uint64
		positions             []int
		ends                  []int
		accIds, dbIds, tblIds []uint64

		dbBytes, tblBytes []byte
		dbNull, tblNull   bool
		dbName, tblName   string
	)

	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	eng, eok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !eok || eng == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableIndexSize: mo table engine is nil")
	}
	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableIndexSize: txn operator is nil")
	}
	txn := proc.GetTxnOperator()
	accountId := proc.Ctx.Value(defines.TenantIDKey{}).(uint32)

	forceUpdate := GetForceUpdateVariable(proc)
	resetUpdateTime := GetResetUpdateTimeVariable(proc)

	positions = make([]int, length)
	ends = make([]int, length)

	defer func() {
		if err != nil {
			logutil.Error("MoTableIndexSize",
				zap.Error(err),
				zap.String("db", dbName),
				zap.String("table", tblName),
				zap.Uint32("account id", accountId))
		}
	}()

	resolve := func() ([]uint64, []uint64, []uint64, error) {
		for i := uint64(0); i < uint64(length); i++ {
			positions[i] = len(tblIds)
			ends[i] = len(tblIds)

			dbBytes, dbNull = dbs.GetStrValue(i)
			tblBytes, tblNull = tbls.GetStrValue(i)
			if dbNull || tblNull {
				positions[i] = -1
				continue
			}
			dbName, tblName = string(dbBytes), string(tblBytes)

			if ok, ferr := specialTableFilterForNonSys(proc.Ctx, dbName, tblName); ok && ferr == nil {
				continue
			} else if ferr != nil {
				return nil, nil, nil, ferr
			}

			db, derr := eng.Database(proc.Ctx, dbName, txn)
			if derr != nil {
				if moerr.IsMoErrCode(derr, moerr.OkExpectedEOB) {
					return nil, nil, nil, moerr.NewInternalErrorNoCtxf("db not exist: %s(%s)",
						dbName, "OkExpectedEOB")
				}
				return nil, nil, nil, derr
			}

			ctx := proc.Ctx
			acc := uint64(accountId)
			if sub, serr := isSubscribedTable(proc, accountId, db, dbName, tblName); serr != nil {
				return nil, nil, nil, serr
			} else if sub.valid {
				// hidden index objects live in the publishing account's database
				acc = sub.oriAccId
				ctx = defines.AttachAccountId(ctx, uint32(sub.oriAccId))
				if db, derr = eng.Database(ctx, sub.oriDatabaseName, txn); derr != nil {
					return nil, nil, nil, derr
				}
			}

			rel, rerr := db.Relation(ctx, tblName, nil)
			if rerr != nil {
				if moerr.IsMoErrCode(rerr, moerr.OkExpectedEOB) {
					return nil, nil, nil, moerr.NewInternalErrorNoCtxf("tbl not exist: %s-%s(%s)",
						dbName, tblName, "OkExpectedEOB")
				}
				return nil, nil, nil, rerr
			}

			for _, hidden := range indexSizeCandidateTableNames(rel.GetTableDef(ctx)) {
				irel, ierr := db.Relation(ctx, hidden, nil)
				if ierr != nil {
					logutil.Info("moTableIndexSizeNew->Relation",
						zap.String("originTable", tblName),
						zap.String("indexTableName", hidden),
						zap.Error(ierr))
					continue
				}
				accIds = append(accIds, acc)
				dbIds = append(dbIds, uint64(irel.GetDBID(ctx)))
				tblIds = append(tblIds, uint64(irel.GetTableID(ctx)))
			}
			ends[i] = len(tblIds)
		}
		return accIds, dbIds, tblIds, nil
	}

	fn := GetMoTableSizeFunc.Load()
	if fn == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableIndexSize: statistics executor is nil")
	}
	var handled bool
	ret, err, handled = (*fn)(
		proc.Ctx, eng, resolve,
		forceUpdate,
		resetUpdateTime)
	if err != nil {
		return err
	}
	if !handled {
		if cerr := proc.Ctx.Err(); cerr != nil {
			return cerr
		}
		return moTableIndexSizeOld(ivecs, result, proc, length)
	}
	if len(ret) != len(tblIds) {
		return moerr.NewInternalError(proc.Ctx, "MoTableIndexSize: invalid statistics cardinality")
	}
	for i := 0; i < length; i++ {
		null := positions[i] < 0
		var sum int64
		if !null {
			for _, v := range ret[positions[i]:ends[i]] {
				if v > uint64(math.MaxInt64-sum) {
					sum = math.MaxInt64
					break
				}
				sum += int64(v)
			}
		}
		if err = rs.Append(sum, null); err != nil {
			return err
		}
	}
	return nil
}

//#endregion

//#region MoTableDataSize

// MoTableDataSize returns the estimated storage bytes of a table's base
// objects, excluding the hidden secondary/unique index objects published by
// mo_table_index_size. DATA_LENGTH (and the AVG_ROW_LENGTH ratio) source it
// because the legacy mo_table_size implementation reports data plus index
// bytes: routing DATA_LENGTH through it would double count the index bytes
// next to INDEX_LENGTH in legacy statistics mode and in engine fallback.
// mo_table_size keeps its published mode-dependent totals unchanged; this
// builtin is the stable base-byte source shared by both statistics modes.
func MoTableDataSize(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {
	if GetUseOldImplVariable(proc) {
		return moTableDataSizeOld(ivecs, result, proc, length, selectList)
	}
	return MoTableDataSizeNew(ivecs, result, proc, length, selectList)
}

// MoTableDataSizeNew follows the new statistics implementation: the shared
// resolver batches the (account, database, table) triplets and the registered
// size executor publishes the alpha-backed base-table bytes. Engine fallback
// keeps the data/index decomposition on the per-relation legacy path.
func MoTableDataSizeNew(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {
	return MoTableSizeRowsHelper(ivecs, result, proc, length, selectList, &GetMoTableSizeFunc, moTableDataSizeOld)
}

// moTableDataSizeOld mirrors the legacy mo_table_size resolution (special
// tables, subscription metadata, engine metadata resolution) but appends only
// the base-object size, leaving the hidden index objects to
// mo_table_index_size.
func moTableDataSizeOld(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {
	rs := vector.MustFunctionResult[int64](result)
	dbs := vector.GenerateFunctionStrParameter(ivecs[0])
	tbls := vector.GenerateFunctionStrParameter(ivecs[1])

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableDataSize: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	var ok bool
	for i := uint64(0); i < uint64(length); i++ {
		foolCtx := proc.Ctx

		db, dbnull := dbs.GetStrValue(i)
		tbl, tblnull := tbls.GetStrValue(i)
		if dbnull || tblnull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			var rel engine.Relation
			dbStr, tblStr := string(db), string(tbl)

			if ok, err = specialTableFilterForNonSys(foolCtx, dbStr, tblStr); ok && err == nil {
				if err = rs.Append(int64(0), false); err != nil {
					return err
				}
				continue
			}

			if err != nil {
				return err
			}

			var dbo engine.Database
			dbo, err = e.Database(foolCtx, dbStr, txn)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
					return moerr.NewInvalidArgNoCtx("db not found when mo_table_data_size",
						fmt.Sprintf("%s-%s", dbStr, tblStr))
				}
				return err
			}

			var accId uint32
			accId, err = defines.GetAccountId(foolCtx)
			if err != nil {
				return err
			}

			var sub subscription
			if sub, err = isSubscribedTable(
				proc, accId, dbo, dbStr, tblStr); err != nil {
				logutil.Error("moTableDataSizeOld",
					zap.String("source", "isSubscribeTable"),
					zap.Error(err))
				return err
			} else if sub.valid {
				// subscription
				foolCtx = defines.AttachAccountId(foolCtx, uint32(sub.oriAccId))
				dbo, err = e.Database(foolCtx, sub.oriDatabaseName, txn)
				if err != nil {
					return err
				}
			}

			rel, err = dbo.Relation(foolCtx, tblStr, nil)
			if err != nil {
				return err
			}

			var oSize uint64
			if oSize, err = originalTableSize(foolCtx, dbo, rel); err != nil {
				return err
			}

			if err = rs.Append(int64(oSize), false); err != nil {
				return err
			}
		}
	}
	return nil
}

//#endregion

var specialRegexp = regexp.MustCompile(fmt.Sprintf("%s|%s|%s",
	catalog.MO_TABLES, catalog.MO_DATABASE, catalog.MO_COLUMNS))

func specialTableFilterForNonSys(ctx context.Context, dbStr, tblStr string) (bool, error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return false, err
	}

	if accountId == sysAccountID || dbStr != catalog.MO_CATALOG {
		return false, nil
	}

	if specialRegexp.MatchString(tblStr) || isClusterTable(dbStr, tblStr) {
		return true, nil
	}

	return false, nil
}

// MoTableColMax return the max value of the column
func MoTableColMax(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moTableColMaxMinImpl("mo_table_col_max", ivecs, result, proc, length, selectList)
}

// MoTableColMax return the max value of the column
func MoTableColMin(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moTableColMaxMinImpl("mo_table_col_min", ivecs, result, proc, length, selectList)
}

func moTableColMaxMinImpl(fnName string, parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableColMaxMin: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	dbNames := vector.GenerateFunctionStrParameter(parameters[0])
	tableNames := vector.GenerateFunctionStrParameter(parameters[1])
	columnNames := vector.GenerateFunctionStrParameter(parameters[2])

	minMaxIdx := 0
	if fnName == "mo_table_col_max" {
		minMaxIdx = 1
	}

	rs := vector.MustFunctionResult[types.Varlena](result)

	sysAccountCtx := proc.Ctx
	if accountId, err := defines.GetAccountId(proc.Ctx); err != nil {
		return err
	} else if accountId != uint32(sysAccountID) {
		sysAccountCtx = defines.AttachAccountId(proc.Ctx, uint32(sysAccountID))
	}

	for i := uint64(0); i < uint64(length); i++ {
		getValueFailed := false
		db, null1 := dbNames.GetStrValue(i)
		table, null2 := tableNames.GetStrValue(i)
		column, null3 := columnNames.GetStrValue(i)
		if null1 || null2 || null3 {
			if appendErr := rs.AppendMustNullForBytesResult(); appendErr != nil {
				return appendErr
			}
		} else {
			dbStr, tableStr, columnStr := string(db), string(table), string(column)

			// Magic code. too confused.
			if tableStr == "mo_database" || tableStr == "mo_tables" || tableStr == "mo_columns" || tableStr == "sys_async_task" {
				return moerr.NewInvalidInputf(proc.Ctx, "%s has bad input table %s", fnName, tableStr)
			}
			if columnStr == "__mo_rowid" {
				return moerr.NewInvalidInputf(proc.Ctx, "%s has bad input column %s", fnName, columnStr)
			}

			ctx := proc.Ctx
			if isClusterTable(dbStr, tableStr) {
				//if it is the cluster table in the general account, switch into the sys account
				ctx = sysAccountCtx
			}

			db, err := e.Database(ctx, dbStr, txn)
			if err != nil {
				return err
			}

			if db.IsSubscription(ctx) {
				// get sub info
				var sub *plan.SubscriptionMeta
				if sub, err = proc.GetSessionInfo().SqlHelper.GetSubscriptionMeta(dbStr); err != nil {
					return err
				}
				if sub != nil && !pubsub.InSubMetaTables(sub, tableStr) {
					return moerr.NewInternalErrorf(ctx, "table %s not found in publication %s", tableStr, sub.Name)
				}

				// replace with pub account id
				ctx = defines.AttachAccountId(ctx, uint32(sub.AccountId))
				// replace with real dbname(sub.DbName)
				if db, err = e.Database(ctx, sub.DbName, txn); err != nil {
					return err
				}
			}

			rel, err := db.Relation(ctx, tableStr, nil)
			if err != nil {
				return err
			}
			if err = process.CheckIndexRelationDefinitionAccess(proc, rel.GetTableName(), rel.GetTableDef(ctx)); err != nil {
				return err
			}
			tableColumns, err := rel.TableColumns(ctx)
			if err != nil {
				return err
			}
			ranges, err := rel.Ranges(ctx, engine.DefaultRangesParam)
			if err != nil {
				return err
			}

			if ranges.DataCnt() == 0 {
				getValueFailed = true
			} else if ranges.DataCnt() == 1 {
				first := ranges.GetBlockInfo(0)
				if first.IsMemBlk() {
					getValueFailed = true
				}
			} else {
				getValueFailed = true
				// BUG： if user delete the max or min value within the same txn, the result will be wrong.
				tValues, _, er := rel.MaxAndMinValues(ctx)
				if er != nil {
					return er
				}

				// BUG: if user drop the col and add it back with the same name within the same txn, the result will be wrong.
				for j := range tableColumns {
					if strings.EqualFold(tableColumns[j].Name, columnStr) {
						strval := getValueInStr(tValues[j][minMaxIdx])
						if err = rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(strval)); err != nil {
							return err
						}
						getValueFailed = false
						break
					}
				}
			}
			if getValueFailed {
				if appendErr := rs.AppendMustNullForBytesResult(); appendErr != nil {
					return appendErr
				}
			}
		}
	}
	return nil
}

//#region MoTableColNdv

// ndvEstimateFromStatsInfo extracts the published distinct-value estimate of
// one column. A missing statistics object, an unpublished column or a
// non-finite estimate means "unknown" and renders NULL instead of a false
// known zero; a published estimate is rounded and stays bounded by the
// published table row count when that bound is finite and positive.
// The supplied column name is resolved to the statistics source's canonical
// name before the lookup (#30910): mo_indexes and mo_columns keep the
// declared spelling, while the statistics publish each column under the
// engine's canonical lowercased name, so an exact miss falls back to the
// canonical spelling.
func ndvEstimateFromStatsInfo(info *statsinfopb.StatsInfo, column string) *int64 {
	if info == nil {
		return nil
	}
	ndv, ok := info.NdvMap[column]
	if !ok {
		ndv, ok = info.NdvMap[strings.ToLower(column)]
	}
	if !ok || math.IsNaN(ndv) || math.IsInf(ndv, 0) || ndv < 0 {
		return nil
	}
	if info.TableCnt > 0 && !math.IsInf(info.TableCnt, 0) && !math.IsNaN(info.TableCnt) &&
		ndv > info.TableCnt {
		ndv = info.TableCnt
	}
	value := int64(math.Round(ndv))
	if value < 0 {
		value = 0
	}
	return &value
}

// indexPartPrefixTruncatesColumn reports whether the recorded index metadata
// configures a character-prefix length for the given index key part. A
// truncated key population describes LEFT(column, length), and the engine's
// statistics publish only the full-column distinct-value estimate, so the
// part's cardinality must render NULL instead of substituting the
// full-column estimate (#30910). Recorded metadata that cannot be decoded
// renders the estimate unavailable rather than guessing. Matching is
// case-insensitive because mo_indexes keeps the column's declared spelling
// while the params key uses the parser's compared spelling.
func indexPartPrefixTruncatesColumn(indexParams, column string) bool {
	if indexParams == "" {
		return false
	}
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(indexParams)
	if err != nil {
		return true
	}
	for part, length := range prefixLengths {
		if length > 0 && strings.EqualFold(part, column) {
			return true
		}
	}
	return false
}

// MoTableColNdv returns the engine-statistics distinct-value estimate of one
// column, published by ANALYZE TABLE and the engine's maintained statistics.
// Resolution follows the mo_table_col_max family, including subscription and
// cluster-table account switching; unavailable estimates render NULL.
// The optional fourth parameter is the index metadata's algo_params: when the
// index part is configured with a character prefix the estimate is unavailable
// and renders NULL, never the full-column NDV of the truncated column.
//#region PublicationStatisticsConfig

// statisticsPublicationConfigPrefix is the in-band config prefix a bound
// publisher branch appends to the statistics estimator calls of the persisted
// STATISTICS body. The persisted body keeps the plain direct shapes; only the
// publisher branch of the subscription UNION carries this argument, so no
// user-visible signature depends on the format.
const statisticsPublicationConfigPrefix = "pubacct="

// publicationAccountIDFromConfig parses the in-band publisher account marker.
// An absent prefix or an overflowing id means the call is not
// publication-bound and the caller keeps its direct resolution semantics.
// Zero is the sys account id: every publication created by the sys session
// (`from sys publication` subscriptions included) records
// mo_subs.pub_account_id = 0, so a baked "pubacct=0" is a valid binding.
func publicationAccountIDFromConfig(config string) (uint32, bool) {
	rest, ok := strings.CutPrefix(config, statisticsPublicationConfigPrefix)
	if !ok || rest == "" {
		return 0, false
	}
	id, err := strconv.ParseUint(rest, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}

// publicationAccountIDFromVecArg reads the in-band publisher account config of
// one input row. A NULL config input means the same as an absent one.
func publicationAccountIDFromVecArg(
	cfg vector.FunctionParameterWrapper[types.Varlena],
	i uint64,
) (uint32, bool) {
	if cfg == nil {
		return 0, false
	}
	rawCfg, cfgNull := cfg.GetStrValue(i)
	if cfgNull {
		return 0, false
	}
	return publicationAccountIDFromConfig(string(rawCfg))
}

// publishedColStats resolves one publication-bound estimate input to the
// relation statistics under the published account. A resolution failure
// renders an unavailable estimate (a nil info) instead of failing the whole
// metadata read: publisher branch rows are guarded by the branch's visibility
// predicates already, and a database or relation name that no longer resolves
// at the reading transaction (for example a historical subscription alias
// recorded by a snapshot read) publishes NULL per CARDINALITY's contract
// rather than aborting the query (#30910).
func publishedColStats(
	ctx context.Context,
	e engine.Engine,
	txn client.TxnOperator,
	pubAccountID uint32,
	dbName, tblName string,
) (*statsinfopb.StatsInfo, error) {
	pubCtx := defines.AttachAccountId(ctx, pubAccountID)
	dbo, err := e.Database(pubCtx, dbName, txn)
	if err != nil {
		return nil, nil
	}
	rel, err := dbo.Relation(pubCtx, tblName, nil)
	if err != nil {
		return nil, nil
	}
	// A non-blocking read answers from the published cache only; it never
	// waits to synthesize a new estimate for this projection.
	return rel.Stats(pubCtx, false)
}

//#endregion

func MoTableColNdv(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {
	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableColNdv: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	dbNames := vector.GenerateFunctionStrParameter(ivecs[0])
	tableNames := vector.GenerateFunctionStrParameter(ivecs[1])
	columnNames := vector.GenerateFunctionStrParameter(ivecs[2])
	var indexParams vector.FunctionParameterWrapper[types.Varlena]
	var publicationConfig vector.FunctionParameterWrapper[types.Varlena]
	if len(ivecs) > 3 {
		indexParams = vector.GenerateFunctionStrParameter(ivecs[3])
	}
	if len(ivecs) > 4 {
		publicationConfig = vector.GenerateFunctionStrParameter(ivecs[4])
	}

	rs := vector.MustFunctionResult[int64](result)

	sysAccountCtx := proc.Ctx
	if accountId, aerr := defines.GetAccountId(proc.Ctx); aerr != nil {
		return aerr
	} else if accountId != uint32(sysAccountID) {
		sysAccountCtx = defines.AttachAccountId(proc.Ctx, uint32(sysAccountID))
	}

	for i := uint64(0); i < uint64(length); i++ {
		db, dbNull := dbNames.GetStrValue(i)
		table, tableNull := tableNames.GetStrValue(i)
		column, columnNull := columnNames.GetStrValue(i)
		if dbNull || tableNull || columnNull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		dbStr, tableStr, columnStr := string(db), string(table), string(column)
		// absent metadata (the three-argument call) means no prefix is
		// configured; NULL metadata behaves like the empty string so a
		// legacy unparametrized index still publishes its full-column
		// estimate rather than a false NULL
		indexParamsStr := ""
		if len(ivecs) > 3 {
			if params, paramsNull := indexParams.GetStrValue(i); !paramsNull {
				indexParamsStr = string(params)
			}
		}

		ctx := proc.Ctx
		if isClusterTable(dbStr, tableStr) {
			// the physical object of a cluster table is owned by the sys account
			ctx = sysAccountCtx
		}

		var info *statsinfopb.StatsInfo
		if pubAccountID, pubBound := publicationAccountIDFromVecArg(publicationConfig, i); pubBound {
			// A publisher branch resolves through its baked publisher
			// account without any per-row subscription metadata lookup:
			// the branch arms of the subscription UNION run as parallel
			// scopes, and an internal metadata resolution on the shared
			// session executor from more than one arm interleaves internal
			// result sets and corrupts them. Unresolvable names publish
			// the unavailable estimate as NULL.
			if info, err = publishedColStats(ctx, e, txn, pubAccountID, dbStr, tableStr); err != nil {
				return err
			}
			if info == nil {
				if err = rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
		} else {
			dbo, derr := e.Database(ctx, dbStr, txn)
			if derr != nil {
				// keep the missing-database error in the same user-facing form as
				// the mo_table_rows/mo_table_size family instead of leaking the
				// internal OkExpectedEOB marker
				if moerr.IsMoErrCode(derr, moerr.OkExpectedEOB) {
					return moerr.NewInternalErrorNoCtxf("db not exist: %s(%s)",
						dbStr, "OkExpectedEOB")
				}
				return derr
			}

			if dbo.IsSubscription(ctx) {
				var sub *plan.SubscriptionMeta
				if sub, derr = proc.GetSessionInfo().SqlHelper.GetSubscriptionMeta(dbStr); derr != nil {
					return derr
				}
				if sub != nil && !pubsub.InSubMetaTables(sub, tableStr) {
					return moerr.NewInternalErrorf(ctx, "table %s not found in publication %s", tableStr, sub.Name)
				}

				// the publish-side account owns the statistics
				ctx = defines.AttachAccountId(ctx, uint32(sub.AccountId))
				if dbo, derr = e.Database(ctx, sub.DbName, txn); derr != nil {
					return derr
				}
			}

			rel, rerr := dbo.Relation(ctx, tableStr, nil)
			if rerr != nil {
				return rerr
			}

			// A non-blocking read answers from the published cache only; it never
			// waits to synthesize a new estimate for this projection.
			info, rerr = rel.Stats(ctx, false)
			if rerr != nil {
				return rerr
			}
		}

		// A character-prefix key part describes a truncated population and the
		// statistics publish no per-prefix estimate: the part's cardinality is
		// unavailable and must render NULL, never the full-column estimate
		// (#30910). The relation is resolved first so name and scope errors
		// keep their existing user-visible forms.
		if indexPartPrefixTruncatesColumn(indexParamsStr, columnStr) {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		if value := ndvEstimateFromStatsInfo(info, columnStr); value == nil {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(*value, false); err != nil {
				return err
			}
		}
	}
	return nil
}

//#endregion

//#region MoTableColNullCnt

// nullCntFromStatsInfo extracts the published count of NULL values of one
// column. A missing statistics object or an unpublished column means
// "unknown" and renders NULL instead of a false known zero. The column name
// resolves to the statistics source's canonical name before the lookup, the
// same fallback the distinct-value accessor uses (#30910).
func nullCntFromStatsInfo(info *statsinfopb.StatsInfo, column string) *int64 {
	if info == nil {
		return nil
	}
	cnt, ok := info.NullCntMap[column]
	if !ok {
		cnt, ok = info.NullCntMap[strings.ToLower(column)]
	}
	if !ok {
		return nil
	}
	if cnt > math.MaxInt64 {
		cnt = math.MaxInt64
	}
	value := int64(cnt)
	if value < 0 {
		return nil
	}
	return &value
}

// MoTableColNullCnt returns the engine-statistics NULL-value count of one
// column, published by ANALYZE TABLE alongside the distinct-value estimate.
// The index CARDINALITY publications (information_schema.STATISTICS and
// SHOW INDEX) consume it to count the NULL value group MySQL index statistics
// give an indexed column: equal NULL values form one key group, so a
// populated nullable index publishes the distinct estimate plus one when
// NULLs are present, instead of the raw NDV's zero for an all-NULL
// population (#30910). The plain distinct-value estimator keeps its own
// contract; this builtin is the NULL-population projection. Resolution
// follows MoTableColNdv, including subscription and cluster-table account
// switching and the publication-bound arm that resolves the publisher
// identity baked by the subscription branch without per-row internal
// lookups; unpublished columns and unresolvable publication-bound names
// render NULL.
func MoTableColNullCnt(
	ivecs []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) (err error) {
	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || proc.GetTxnOperator() == nil {
		return moerr.NewInternalError(proc.Ctx, "MoTableColNullCnt: txn operator is nil")
	}
	txn := proc.GetTxnOperator()

	dbNames := vector.GenerateFunctionStrParameter(ivecs[0])
	tableNames := vector.GenerateFunctionStrParameter(ivecs[1])
	columnNames := vector.GenerateFunctionStrParameter(ivecs[2])
	var publicationConfig vector.FunctionParameterWrapper[types.Varlena]
	if len(ivecs) > 3 {
		publicationConfig = vector.GenerateFunctionStrParameter(ivecs[3])
	}

	rs := vector.MustFunctionResult[int64](result)

	sysAccountCtx := proc.Ctx
	if accountId, aerr := defines.GetAccountId(proc.Ctx); aerr != nil {
		return aerr
	} else if accountId != uint32(sysAccountID) {
		sysAccountCtx = defines.AttachAccountId(proc.Ctx, uint32(sysAccountID))
	}

	for i := uint64(0); i < uint64(length); i++ {
		db, dbNull := dbNames.GetStrValue(i)
		table, tableNull := tableNames.GetStrValue(i)
		column, columnNull := columnNames.GetStrValue(i)
		if dbNull || tableNull || columnNull {
			if err = rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		dbStr, tableStr, columnStr := string(db), string(table), string(column)

		ctx := proc.Ctx
		if isClusterTable(dbStr, tableStr) {
			// the physical object of a cluster table is owned by the sys account
			ctx = sysAccountCtx
		}

		var info *statsinfopb.StatsInfo
		if pubAccountID, pubBound := publicationAccountIDFromVecArg(publicationConfig, i); pubBound {
			// The publisher branch resolves through its baked publisher
			// account with no per-row internal subscription lookup, and an
			// unresolvable historical name renders the unavailable NULL
			// population instead of failing the metadata read; see
			// MoTableColNdv's publication arm for the shared rationale.
			if info, err = publishedColStats(ctx, e, txn, pubAccountID, dbStr, tableStr); err != nil {
				return err
			}
			if info == nil {
				if err = rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
		} else {
			dbo, derr := e.Database(ctx, dbStr, txn)
			if derr != nil {
				// keep the missing-database error in the same user-facing form as
				// the mo_table_rows/mo_table_size family instead of leaking the
				// internal OkExpectedEOB marker
				if moerr.IsMoErrCode(derr, moerr.OkExpectedEOB) {
					return moerr.NewInternalErrorNoCtxf("db not exist: %s(%s)",
						dbStr, "OkExpectedEOB")
				}
				return derr
			}

			if dbo.IsSubscription(ctx) {
				var sub *plan.SubscriptionMeta
				if sub, derr = proc.GetSessionInfo().SqlHelper.GetSubscriptionMeta(dbStr); derr != nil {
					return derr
				}
				if sub != nil && !pubsub.InSubMetaTables(sub, tableStr) {
					return moerr.NewInternalErrorf(ctx, "table %s not found in publication %s", tableStr, sub.Name)
				}

				// the publish-side account owns the statistics
				ctx = defines.AttachAccountId(ctx, uint32(sub.AccountId))
				if dbo, derr = e.Database(ctx, sub.DbName, txn); derr != nil {
					return derr
				}
			}

			rel, rerr := dbo.Relation(ctx, tableStr, nil)
			if rerr != nil {
				return rerr
			}

			// A non-blocking read answers from the published cache only; it never
			// waits to synthesize a new estimate for this projection.
			info, rerr = rel.Stats(ctx, false)
			if rerr != nil {
				return rerr
			}
		}

		if value := nullCntFromStatsInfo(info, columnStr); value == nil {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err = rs.Append(*value, false); err != nil {
				return err
			}
		}
	}
	return nil
}

//#endregion

func getValueInStr(value any) string {
	switch v := value.(type) {
	case bool:
		if v {
			return "true"
		} else {
			return "false"
		}
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(uint64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(int64(v), 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 32)
	case string:
		return v
	case []byte:
		return string(v)
	case []float32:
		// Used by zonemap Min,Max
		// Used by MO_TABLE_COL_MAX
		return types.ArrayToString[float32](v)
	case []float64:
		return types.ArrayToString[float64](v)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case types.Date:
		return v.String()
	case types.Time:
		return v.String()
	case types.Datetime:
		return v.String()
	case types.Timestamp:
		return v.String()
	case bytejson.ByteJson:
		return v.String()
	case types.Uuid:
		return v.String()
	case types.Decimal64:
		return v.Format(0)
	case types.Decimal128:
		return v.Format(0)
	case types.Enum:
		return v.String()
	default:
		return ""
	}
}

func isClusterTable(dbName, name string) bool {
	if dbName == moCatalog {
		//if it is neither among the tables nor the index table,
		//it is the cluster table.
		if _, ok := predefinedTables[name]; !ok && !isIndexTable(name) {
			return true
		}
	}
	return false
}

func isIndexTable(name string) bool {
	return strings.HasPrefix(name, catalog.IndexTableNamePrefix)
}

const (
	moCatalog    = "mo_catalog"
	sysAccountID = 0
)

var (
	predefinedTables = map[string]int8{
		catalog.MODatabaseDefaults:    0,
		"mo_database":                 0,
		"mo_tables":                   0,
		"mo_columns":                  0,
		"mo_account":                  0,
		"mo_user":                     0,
		"mo_role":                     0,
		"mo_user_grant":               0,
		"mo_role_grant":               0,
		"mo_role_privs":               0,
		"mo_user_defined_function":    0,
		"mo_stored_procedure":         0,
		"mo_mysql_compatibility_mode": 0,
		catalog.MOAutoIncrTable:       0,
		"mo_indexes":                  0,
		"mo_pubs":                     0,
		"mo_stages":                   0,
		"mo_snapshots":                0,
		"mo_pitr":                     0,
		catalog.MO_TABLE_STATS:        0,
		catalog.MO_MERGE_SETTINGS:     0,
		catalog.MO_BRANCH_METADATA:    0,
		catalog.MO_CCPR_LOG:           0,
		catalog.MO_CCPR_TABLES:        0,
		catalog.MO_CCPR_DBS:           0,

		catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME: 0,
		catalog.MO_FEATURE_LIMIT:                      0,
		catalog.MO_FEATURE_REGISTRY:                   0,
	}
)

// enum("a","b","c") -> CastIndexToValue(1) -> "a"
// CastIndexToValue returns enum type index according to the value
func CastIndexToValue(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	indexs := vector.GenerateFunctionFixedTypeParameter[types.Enum](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		typeEnum, typeEnumNull := typeEnums.GetStrValue(i)
		indexVal, indexnull := indexs.GetValue(i)
		if typeEnumNull || indexnull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			// MySQL stores an invalid ENUM value as index 0. It is distinct from
			// NULL and displays as the empty string.
			if indexVal == 0 {
				if err := rs.AppendBytes([]byte{}, false); err != nil {
					return err
				}
				continue
			}
			typeEnumVal := functionUtil.QuickBytesToStr(typeEnum)
			var enumVlaue string

			enumVlaue, err := types.ParseEnumIndex(typeEnumVal, indexVal)
			if err != nil {
				return err
			}

			if err = rs.AppendBytes([]byte(enumVlaue), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// enum("a","b","c") -> CastValueToIndex("a") -> 1
// CastValueToIndex returns enum type index according to the value
func CastValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Enum](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	enumValues := vector.GenerateFunctionStrParameter(ivecs[1])
	var valueIndex *enumValueIndex
	indexAttempted := false
	allowAdjustment := allowEnumSetAdjustment(proc)
	var warnings process.WarningAccumulator
	defer warnings.Reset()

	for i := uint64(0); i < uint64(length); i++ {
		if enumSetConversionSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		typeEnum, typeEnumNull := typeEnums.GetStrValue(i)
		enumValue, enumValNull := enumValues.GetStrValue(i)
		if typeEnumNull || enumValNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			typeEnumVal := functionUtil.QuickBytesToStr(typeEnum)
			enumStr := functionUtil.QuickBytesToStr(enumValue)
			if !indexAttempted && ivecs[0].IsConst() {
				indexAttempted = true
				valueIndex = buildEnumValueIndexForBatch(typeEnumVal, length)
			}

			var (
				index     types.Enum
				truncated bool
				err       error
			)
			if valueIndex != nil {
				index, truncated, err = valueIndex.parse(enumStr)
			} else {
				index, truncated, err = types.ParseEnumAssignment(typeEnumVal, enumStr)
			}
			if err != nil {
				return err
			}
			if truncated {
				if err := recordEnumSetTruncation(proc, &warnings, allowAdjustment, "ENUM", enumStr); err != nil {
					return err
				}
			}

			if err := rs.Append(index, false); err != nil {
				return err
			}
		}
	}
	warnings.Flush(proc)
	return nil
}

const (
	enumValueIndexMinRows   = 64
	enumValueIndexMinLabels = 8
)

// Small batches and short definitions stay on ParseEnum's existing linear
// path: benchmarks show that hash/fold overhead does not amortize for fewer
// than eight labels, especially for numeric and case-insensitive inputs. The
// index is reserved for batches large enough to amortize both maps.
func buildEnumValueIndexForBatch(enumValues string, length int) *enumValueIndex {
	if length < enumValueIndexMinRows {
		return nil
	}
	labelCount := strings.Count(enumValues, ",") + 1
	if labelCount < enumValueIndexMinLabels || labelCount > types.MaxEnumLen {
		return nil
	}
	return buildEnumValueIndex(enumValues)
}

// enumValueIndex makes every label lookup O(1) after Unicode case folding. The
// exact map keeps the planner-generated display-to-index path allocation-free
// per row, while foldedIndexes preserves ParseEnum's case-insensitive behavior.
type enumValueIndex struct {
	values        []string
	exactIndexes  map[string]types.Enum
	foldedIndexes map[string]types.Enum
}

func buildEnumValueIndex(enumValues string) *enumValueIndex {
	if enumValues == "" {
		return nil
	}

	values := strings.Split(enumValues, ",")
	if len(values) > types.MaxEnumLen {
		return nil
	}
	index := &enumValueIndex{
		values:        values,
		exactIndexes:  make(map[string]types.Enum, len(values)),
		foldedIndexes: make(map[string]types.Enum, len(values)),
	}
	for i, value := range values {
		foldKey := commonutil.EqualFoldKey(value)
		firstIndex, ok := index.foldedIndexes[foldKey]
		if !ok {
			firstIndex = types.Enum(i + 1)
			index.foldedIndexes[foldKey] = firstIndex
		}
		index.exactIndexes[value] = firstIndex
	}
	return index
}

func (index *enumValueIndex) parse(value string) (types.Enum, bool, error) {
	for {
		if enumIndex := index.exactIndexes[value]; enumIndex != 0 {
			return enumIndex, false, nil
		}
		if enumIndex := index.foldedIndexes[commonutil.EqualFoldKey(value)]; enumIndex != 0 {
			return enumIndex, false, nil
		}
		trimmed := strings.TrimRight(value, " ")
		if trimmed == value {
			return types.ParseEnumNumericText(value, len(index.values))
		}
		value = trimmed
	}
}

// enum("a","b","c") -> CastIndexValueToIndex(1) -> 1
// CastIndexValueToIndex returns enum type index according to the index value
func CastIndexValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return castIntegerToEnum[uint16](ivecs, result, proc, length, selectList)
}

func castIntegerToEnum[T uint16 | uint64 | int64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Enum](result)
	typeEnums := vector.GenerateFunctionStrParameter(ivecs[0])
	enumIndexValues := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])
	allowAdjustment := allowEnumSetAdjustment(proc)
	var warnings process.WarningAccumulator
	defer warnings.Reset()

	for i := uint64(0); i < uint64(length); i++ {
		if enumSetConversionSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		typeEnum, typeEnumNull := typeEnums.GetStrValue(i)
		enumValueIndex, enumValNull := enumIndexValues.GetValue(i)
		if typeEnumNull || enumValNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			typeEnumVal := functionUtil.QuickBytesToStr(typeEnum)
			index, truncated, err := types.ParseEnumNumberAssignment(typeEnumVal, uint64(enumValueIndex))
			if err != nil {
				return err
			}
			if truncated {
				if err := recordEnumSetTruncation(proc, &warnings, allowAdjustment, "ENUM", enumValueIndex); err != nil {
					return err
				}
			}

			if err = rs.Append(index, false); err != nil {
				return err
			}
		}
	}
	warnings.Flush(proc)
	return nil
}

// set("a","b","c") -> CastSetIndexToValue(3) -> "a,b"
func CastSetIndexToValue(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	typeSets := vector.GenerateFunctionStrParameter(ivecs[0])
	indexes := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[1])

	for i := uint64(0); i < uint64(length); i++ {
		typeSet, typeSetNull := typeSets.GetStrValue(i)
		indexVal, indexNull := indexes.GetValue(i)
		if typeSetNull || indexNull {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		setValue, err := types.ParseSetIndex(functionUtil.QuickBytesToStr(typeSet), indexVal)
		if err != nil {
			return err
		}
		if err = rs.AppendBytes([]byte(setValue), false); err != nil {
			return err
		}
	}
	return nil
}

// set("a","b","c") -> CastSetValueToIndex("a,b") -> 3
func CastSetValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[uint64](result)
	typeSets := vector.GenerateFunctionStrParameter(ivecs[0])
	setValues := vector.GenerateFunctionStrParameter(ivecs[1])
	allowAdjustment := allowEnumSetAdjustment(proc)
	var warnings process.WarningAccumulator
	defer warnings.Reset()

	for i := uint64(0); i < uint64(length); i++ {
		if enumSetConversionSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		typeSet, typeSetNull := typeSets.GetStrValue(i)
		setValue, setValueNull := setValues.GetStrValue(i)
		if typeSetNull || setValueNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		input := functionUtil.QuickBytesToStr(setValue)
		index, truncated, err := types.ParseSetAssignment(functionUtil.QuickBytesToStr(typeSet), input)
		if err != nil {
			return err
		}
		if truncated {
			if err := recordEnumSetTruncation(proc, &warnings, allowAdjustment, "SET", input); err != nil {
				return err
			}
		}
		if err = rs.Append(index, false); err != nil {
			return err
		}
	}
	warnings.Flush(proc)
	return nil
}

func statementIgnore(proc *process.Process) bool {
	return proc != nil && proc.Base != nil && proc.GetStmtProfile().GetStatementIgnore()
}

func allowEnumSetAdjustment(proc *process.Process) bool {
	return proc != nil && proc.Base != nil && (statementIgnore(proc) || !isStrictSqlMode(proc))
}

func enumSetConversionSkipped(selection *FunctionSelectList, row uint64) bool {
	return selection != nil && (selection.IgnoreAllRow() || (!selection.ShouldEvalAllRow() && selection.Contains(row)))
}

func recordEnumSetTruncation(proc *process.Process, warnings *process.WarningAccumulator, allowAdjustment bool, typ string, value any) error {
	if allowAdjustment {
		if warnings.Total == 0 {
			warnings.SetWarningRetentionForProcess(proc)
		}
		if !warnings.NeedsDiagnostic() {
			warnings.AddCount()
			return nil
		}
	}
	text, ok := value.(string)
	if !ok {
		text = fmt.Sprint(value)
	}
	// BoundWarningMessage caps bytes, clips at UTF-8 boundaries and clones
	// borrowed input so a retained diagnostic cannot pin a large source vector.
	message := fmt.Sprintf("Data truncated for %s value: '%s'", typ, process.BoundWarningMessage(text, 128))
	if allowAdjustment {
		warnings.Add(moerr.WARN_DATA_TRUNCATED, message)
		return nil
	}
	ctx := moerr.Context()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	return moerr.NewEnumSetValueTruncated(ctx, message)
}

// set("a","b","c") -> CastSetIndexValueToIndex(3) -> 3
func CastSetIndexValueToIndex(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return castIntegerToSet[uint64](ivecs, result, proc, length, selectList)
}

func castIntegerToSet[T uint64 | int64](ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[uint64](result)
	typeSets := vector.GenerateFunctionStrParameter(ivecs[0])
	setIndexValues := vector.GenerateFunctionFixedTypeParameter[T](ivecs[1])
	allowAdjustment := allowEnumSetAdjustment(proc)
	var warnings process.WarningAccumulator
	defer warnings.Reset()

	for i := uint64(0); i < uint64(length); i++ {
		if enumSetConversionSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		typeSet, typeSetNull := typeSets.GetStrValue(i)
		setIndexValue, setIndexNull := setIndexValues.GetValue(i)
		if typeSetNull || setIndexNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}

		bitmap, err := types.SetValidBitmap(functionUtil.QuickBytesToStr(typeSet))
		if err != nil {
			return err
		}
		index := uint64(setIndexValue) & bitmap
		if index != uint64(setIndexValue) {
			if err := recordEnumSetTruncation(proc, &warnings, allowAdjustment, "SET", setIndexValue); err != nil {
				return err
			}
		}
		if err = rs.Append(index, false); err != nil {
			return err
		}
	}
	warnings.Flush(proc)
	return nil
}

// CastGeometryToSubtype validates a geometry value against a column's subtype
// constraint and normalizes the stored bytes to bare WKB. The first argument is
// the column subtype name (e.g. "POINT", or empty for a generic GEOMETRY
// column); SRID is enforced at bind time from the value/column types, not here,
// because the WKB payload does not carry an SRID.
func CastGeometryToSubtype(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(targetSubtype, payload []byte) ([]byte, error) {
		columnSubtype := strings.ToUpper(strings.TrimSpace(functionUtil.QuickBytesToStr(targetSubtype)))
		// A "32:" prefix marks a GEOMETRY32 (float32-coordinate) column.
		float32Column := false
		if strings.HasPrefix(columnSubtype, "32:") {
			float32Column = true
			columnSubtype = strings.TrimPrefix(columnSubtype, "32:")
		}
		// Tolerate any legacy "SUBTYPE;SRID=n" metadata: keep only the subtype.
		if idx := strings.IndexByte(columnSubtype, ';'); idx >= 0 {
			columnSubtype = strings.TrimSpace(columnSubtype[:idx])
		}
		if strings.HasPrefix(columnSubtype, "SRID=") {
			columnSubtype = ""
		}
		return NormalizeGeometryForStorage(proc, payload, columnSubtype, float32Column)
	}, selectList)
}

// NormalizeGeometryForStorage validates a geometry payload (WKB or legacy WKT)
// against a column's subtype constraint and returns the bare WKB to store:
// float32-coordinate WKB when float32Column is set, float64 otherwise. It is the
// shared core of the cast_geometry_to_subtype runtime cast and the external-table
// CSV loader, so both produce byte-identical, validated geometry. columnSubtype is
// the upper-cased subtype keyword (e.g. "POINT"); "" or "GEOMETRY" accepts any
// subtype. SRID is enforced at bind time from the value/column types, not here,
// because the WKB payload does not carry an SRID.
func NormalizeGeometryForStorage(proc *process.Process, payload []byte, columnSubtype string, float32Column bool) ([]byte, error) {
	wkt, valueSubtype, _, _, err := validateGeometryPayload(payload, maxPointsInGeometryLimit(proc))
	if err != nil {
		return nil, err
	}
	if columnSubtype != "" && columnSubtype != "GEOMETRY" && valueSubtype != "GEOMETRY" && valueSubtype != columnSubtype {
		return nil, moerr.NewInvalidInputNoCtxf("cannot store %s in %s column", valueSubtype, columnSubtype)
	}
	if float32Column {
		return encodeGeometryPayloadFloat32(wkt)
	}
	return encodeGeometryPayload(wkt, 0, false), nil
}

func CastJsonToArray(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryBytesBytesToBytesWithErrorCheck(ivecs, result, proc, length, func(targetArrayType, payload []byte) ([]byte, error) {
		declaredType := strings.TrimSpace(functionUtil.QuickBytesToStr(targetArrayType))
		elemSpec, err := parseTypedArrayElementSpec(declaredType)
		if err != nil {
			return nil, err
		}

		bj := types.DecodeJson(payload)
		if bj.Type != bytejson.TpCodeArray {
			return nil, moerr.NewInvalidInputNoCtxf("cannot store JSON %s in %s column", bj.TYPE(), declaredType)
		}
		if !typedArrayElementsCompatible(elemSpec, bj) {
			return nil, moerr.NewInvalidInputNoCtxf("cannot store JSON value with incompatible element type in %s column", declaredType)
		}
		return payload, nil
	}, selectList)
}

type typedArrayElementSpec struct {
	name     string
	width    int
	unsigned bool
	nested   *typedArrayElementSpec
}

func parseTypedArrayElementSpec(declaredType string) (typedArrayElementSpec, error) {
	arraySpec, err := parseTypedArrayTypeSpec(declaredType)
	if err != nil {
		return typedArrayElementSpec{}, err
	}
	if arraySpec.nested == nil {
		return typedArrayElementSpec{}, moerr.NewInvalidInputNoCtxf("invalid ARRAY type metadata %s", declaredType)
	}
	return *arraySpec.nested, nil
}

func parseTypedArrayTypeSpec(declaredType string) (typedArrayElementSpec, error) {
	raw := strings.TrimSpace(declaredType)
	lower := strings.ToLower(raw)
	if !strings.HasPrefix(lower, "array(") || !strings.HasSuffix(raw, ")") {
		return typedArrayElementSpec{}, moerr.NewInvalidInputNoCtxf("invalid ARRAY type metadata %s", declaredType)
	}
	elem, err := parseTypedArrayScalarSpec(strings.TrimSpace(raw[len("array(") : len(raw)-1]))
	if err != nil {
		return typedArrayElementSpec{}, err
	}
	return typedArrayElementSpec{name: "array", nested: &elem}, nil
}

func parseTypedArrayScalarSpec(rawType string) (typedArrayElementSpec, error) {
	raw := strings.TrimSpace(rawType)
	if raw == "" {
		return typedArrayElementSpec{}, moerr.NewInvalidInputNoCtx("invalid ARRAY element type metadata")
	}
	if strings.HasPrefix(strings.ToLower(raw), "array(") {
		return parseTypedArrayTypeSpec(raw)
	}

	lower := strings.ToLower(raw)
	unsigned := false
	if strings.HasSuffix(lower, " unsigned") {
		unsigned = true
		lower = strings.TrimSpace(strings.TrimSuffix(lower, " unsigned"))
	} else if strings.HasSuffix(lower, " signed") {
		lower = strings.TrimSpace(strings.TrimSuffix(lower, " signed"))
	}

	name := lower
	params := ""
	if idx := strings.IndexByte(lower, '('); idx >= 0 {
		if !strings.HasSuffix(lower, ")") {
			return typedArrayElementSpec{}, moerr.NewInvalidInputNoCtxf("invalid ARRAY element type metadata %s", rawType)
		}
		name = strings.TrimSpace(lower[:idx])
		params = strings.TrimSpace(lower[idx+1 : len(lower)-1])
	}

	spec := typedArrayElementSpec{name: normalizeTypedArrayElementName(name), width: -1, unsigned: unsigned}
	if params != "" {
		if width, ok := parseFirstArrayTypeParam(params); ok {
			spec.width = width
		}
	}
	if spec.name == "" {
		return typedArrayElementSpec{}, moerr.NewInvalidInputNoCtxf("unsupported ARRAY element type %s", rawType)
	}
	return spec, nil
}

func normalizeTypedArrayElementName(name string) string {
	switch strings.TrimSpace(name) {
	case "bool", "boolean":
		return "bool"
	case "int1":
		return "tinyint"
	case "int2":
		return "smallint"
	case "int3":
		return "mediumint"
	case "int4":
		return "int"
	case "int8":
		return "bigint"
	case "tinyint", "smallint", "int", "integer", "mediumint", "bigint":
		if name == "integer" {
			return "int"
		}
		return name
	case "float", "double", "real":
		return "float"
	case "decimal", "numeric", "dec":
		return "decimal"
	case "char", "varchar", "text":
		return name
	case "binary", "varbinary", "blob":
		return name
	case "json", "date", "time", "datetime", "timestamp", "year", "uuid":
		return name
	default:
		return ""
	}
}

func parseFirstArrayTypeParam(params string) (int, bool) {
	first, _, _ := strings.Cut(params, ",")
	first = strings.TrimSpace(first)
	if first == "" {
		return -1, false
	}
	width, err := strconv.Atoi(first)
	if err != nil {
		return -1, false
	}
	return width, true
}

func typedArrayElementsCompatible(spec typedArrayElementSpec, array bytejson.ByteJson) bool {
	for i := 0; i < array.GetElemCnt(); i++ {
		if !typedArrayElementCompatible(spec, array.GetArrayElem(i)) {
			return false
		}
	}
	return true
}

func typedArrayElementCompatible(spec typedArrayElementSpec, elem bytejson.ByteJson) bool {
	if elem.IsNull() {
		return true
	}

	switch spec.name {
	case "array":
		return elem.Type == bytejson.TpCodeArray && spec.nested != nil && typedArrayElementsCompatible(*spec.nested, elem)
	case "json":
		return true
	case "bool":
		return elem.Type == bytejson.TpCodeLiteral && elem.Data[0] != bytejson.LiteralNull
	case "tinyint":
		return jsonIntegerFits(elem, -128, 127, spec.unsigned, 255)
	case "smallint":
		return jsonIntegerFits(elem, -32768, 32767, spec.unsigned, 65535)
	case "mediumint":
		return jsonIntegerFits(elem, -1<<23, 1<<23-1, spec.unsigned, 1<<24-1)
	case "int":
		return jsonIntegerFits(elem, -2147483648, 2147483647, spec.unsigned, 4294967295)
	case "bigint":
		return jsonIntegerFits(elem, -1<<63, 1<<63-1, spec.unsigned, ^uint64(0))
	case "float", "decimal":
		return jsonNumberCompatible(elem)
	case "char", "varchar":
		return elem.Type == bytejson.TpCodeString && jsonStringFitsWidth(elem, spec.width)
	case "text":
		return elem.Type == bytejson.TpCodeString
	case "binary", "varbinary":
		return (elem.Type == bytejson.TpCodeString || elem.Type == bytejson.TpCodeBlob || elem.Type == bytejson.TpCodeOpaque || elem.Type == bytejson.TpCodeBit) && jsonBinaryStringFitsWidth(elem, spec.width)
	case "blob":
		return elem.Type == bytejson.TpCodeString || elem.Type == bytejson.TpCodeBlob || elem.Type == bytejson.TpCodeOpaque || elem.Type == bytejson.TpCodeBit
	case "date":
		return elem.Type == bytejson.TpCodeDate || jsonStringParses(elem, func(s string) bool {
			_, err := types.ParseDateCast(s)
			return err == nil
		})
	case "time":
		return elem.Type == bytejson.TpCodeTime || jsonStringParses(elem, func(s string) bool {
			_, err := types.ParseTime(s, 6)
			return err == nil
		})
	case "datetime":
		return elem.Type == bytejson.TpCodeDatetime || elem.Type == bytejson.TpCodeDate || jsonStringParses(elem, func(s string) bool {
			_, err := types.ParseDatetime(s, 6)
			return err == nil
		})
	case "timestamp":
		return elem.Type == bytejson.TpCodeDatetime || jsonStringParses(elem, func(s string) bool {
			_, err := types.ParseTimestamp(time.UTC, s, 6)
			return err == nil
		})
	case "year":
		return jsonYearCompatible(elem)
	case "uuid":
		return jsonStringParses(elem, func(s string) bool {
			_, err := types.ParseUuid(s)
			return err == nil
		})
	default:
		return false
	}
}

func jsonIntegerFits(elem bytejson.ByteJson, signedMin, signedMax int64, unsigned bool, unsignedMax uint64) bool {
	switch elem.Type {
	case bytejson.TpCodeInt64:
		v := elem.GetInt64()
		if unsigned {
			return v >= 0 && uint64(v) <= unsignedMax
		}
		return v >= signedMin && v <= signedMax
	case bytejson.TpCodeUint64:
		v := elem.GetUint64()
		if unsigned {
			return v <= unsignedMax
		}
		return v <= uint64(signedMax)
	default:
		return false
	}
}

func jsonNumberCompatible(elem bytejson.ByteJson) bool {
	switch elem.Type {
	case bytejson.TpCodeInt64, bytejson.TpCodeUint64, bytejson.TpCodeFloat64, bytejson.TpCodeDecimal:
		return true
	default:
		return false
	}
}

func jsonStringFitsWidth(elem bytejson.ByteJson, width int) bool {
	if width < 0 {
		return true
	}
	return utf8.RuneCount(elem.GetString()) <= width
}

func jsonBinaryStringFitsWidth(elem bytejson.ByteJson, width int) bool {
	if width < 0 {
		return true
	}
	if length, ok := bytejson.BinaryJSONPayloadLen(elem); ok {
		return length <= width
	}
	s, err := elem.Unquote()
	if err != nil {
		return false
	}
	return len(s) <= width
}

func jsonStringParses(elem bytejson.ByteJson, parse func(string) bool) bool {
	if elem.Type != bytejson.TpCodeString {
		return false
	}
	s, err := elem.Unquote()
	return err == nil && parse(s)
}

func jsonYearCompatible(elem bytejson.ByteJson) bool {
	switch elem.Type {
	case bytejson.TpCodeInt64:
		return elem.GetInt64() >= 0 && elem.GetInt64() <= 9999
	case bytejson.TpCodeUint64:
		return elem.GetUint64() <= 9999
	case bytejson.TpCodeString:
		s, err := elem.Unquote()
		if err != nil {
			return false
		}
		year, err := strconv.ParseUint(s, 10, 16)
		return err == nil && year <= 9999
	default:
		return false
	}
}

// CastNanoToTimestamp returns timestamp string according to the nano
func CastNanoToTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	nanos := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[0])

	layout := "2006-01-02 15:04:05.999999999"
	for i := uint64(0); i < uint64(length); i++ {
		nano, null := nanos.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			t := time.Unix(0, nano).UTC()
			if err := rs.AppendBytes([]byte(t.Format(layout)), false); err != nil {
				return err
			}
		}
	}
	return nil
}

// CastRangeValueUnit returns the value in hour unit according to the range value and unit
func CastRangeValueUnit(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryFixedStrToFixedWithErrorCheck[uint8, int64](ivecs, result, proc, length,
		castRangevalueUnitToHourUnit, selectList)
}

func castRangevalueUnitToHourUnit(value uint8, unit string) (int64, error) {
	switch unit {
	case "h":
		return int64(value), nil
	case "d":
		return int64(value) * 24, nil
	case "mo":
		return int64(value) * 24 * 30, nil
	case "y":
		return int64(value) * 24 * 365, nil
	default:
		return -1, moerr.NewInvalidArgNoCtx("invalid pitr time unit %s", unit)
	}
}
