// Copyright 2021 Matrix Origin
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
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"reflect"
	gotrace "runtime/trace"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const schedulingPreviewTimeout = 100 * time.Millisecond

const (
	preparedCursorDefaultMaxBytes  uint64 = 100 << 20
	preparedCursorHardMaxBytes     uint64 = 1 << 30
	preparedCursorMaxRows          uint64 = 1 << 20
	preparedCursorBytesPerMegabyte uint64 = 1 << 20
)

func createDropDatabaseErrorInfo() string {
	return "CREATE/DROP of database is not supported in transactions"
}

func onlyCreateStatementErrorInfo() string {
	return "Only CREATE of DDL is supported in transactions"
}

func administrativeCommandIsUnsupportedInTxnErrorInfo() string {
	return "administrative command is unsupported in transactions"
}

func unclassifiedStatementInUncommittedTxnErrorInfo() string {
	return "unclassified statement appears in uncommitted transaction"
}

func dataBranchMergePickTxnErrorInfo() string {
	return "DATA BRANCH MERGE/PICK is not supported in transactions"
}

func dataBranchMergeTxnNotAllowed(ses *Session) bool {
	return ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT) &&
		!ses.proc.GetTxnOperator().TxnOptions().ByBegin
}

func dataBranchPickTxnNotAllowed(ses *Session) bool {
	return ses.GetTxnHandler().InMultiStmtTransactionMode() ||
		ses.proc.GetTxnOperator().TxnOptions().ByBegin
}

func writeWriteConflictsErrorInfo() string {
	return "Write conflicts detected. Previous transaction need to be aborted."
}

const (
	prefixPrepareStmtName       = "__mo_stmt_id"
	prefixPrepareStmtSessionVar = "__mo_stmt_var"
)

func getPrepareStmtName(stmtID uint32) string {
	var buf [32]byte
	b := append(buf[:0], prefixPrepareStmtName...)
	b = append(b, '_')
	b = strconv.AppendUint(b, uint64(stmtID), 10)
	return string(b)
}

// GetPrepareStmtName returns the session name for a binary-protocol prepared
// statement. The proxy uses the same identity when carrying a forwarded
// COM_STMT_CLOSE through connection migration.
func GetPrepareStmtName(stmtID uint32) string {
	return getPrepareStmtName(stmtID)
}

func parsePrepareStmtID(s string) uint32 {
	if strings.HasPrefix(s, prefixPrepareStmtName) {
		ss := strings.Split(s, "_")
		v, err := strconv.ParseUint(ss[len(ss)-1], 10, 64)
		if err != nil {
			return 0
		}
		return uint32(v)
	}
	return 0
}

func GetPrepareStmtID(ctx context.Context, name string) (int, error) {
	idx := len(prefixPrepareStmtName) + 1
	if idx >= len(name) {
		return -1, moerr.NewInternalError(ctx, "can not get Prepare stmtID")
	}
	return strconv.Atoi(name[idx:])
}

func transferSessionConnType2StatisticConnType(c ConnType) statistic.ConnType {
	switch c {
	case ConnTypeUnset:
		return statistic.ConnTypeUnknown
	case ConnTypeInternal:
		return statistic.ConnTypeInternal
	case ConnTypeExternal:
		return statistic.ConnTypeExternal
	default:
		panic("unknown connection type")
	}
}

func transferSessionConnType2ResourceConnType(c ConnType) resource.ConnType {
	switch c {
	case ConnTypeInternal:
		return resource.ConnInternal
	case ConnTypeExternal:
		return resource.ConnExternal
	default:
		return resource.ConnUnknown
	}
}

var RecordStatement = func(ctx context.Context, ses *Session, proc *process.Process, cw ComputationWrapper, envBegin time.Time, envStmt, sqlType string, useEnv bool) (context.Context, error) {
	// set StatementID
	var stmID uuid.UUID
	var statement tree.Statement = nil
	var text string
	if cw != nil {
		copy(stmID[:], cw.GetUUID())
		statement = cw.GetAst()
	}
	envStmt = redactStatementTextForLogging(statement, envStmt)

	if cw != nil {
		ses.ast = statement
		binExec, prepareName := cw.BinaryExecute()
		execSql := makeExecuteSql(ctx, ses, statement, binExec, prepareName)
		if len(execSql) != 0 {
			bb := strings.Builder{}
			bb.WriteString(envStmt)
			bb.WriteString(" // ")
			bb.WriteString(execSql)
			text = commonutil.Abbreviate(bb.String(), int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
		} else {
			// ignore envStmt == ""
			// case: exec `set @t = 2;` will trigger an internal query with the same session.
			// If you need real sql, can try:
			//	+ fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
			//	+ cw.GetAst().Format(fmtCtx)
			//  + envStmt = fmtCtx.String()
			text = commonutil.Abbreviate(envStmt, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
		}
	} else {
		u, _ := util.FastUuid()
		stmID = uuid.UUID(u)
		text = commonutil.Abbreviate(envStmt, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
	}
	// A prepared execution adds its prepared SQL and parameter values after
	// envStmt has been redacted. Redact the completed diagnostic payload too:
	// this is the final boundary before either session state or statement
	// telemetry can retain it.
	text = redactStatementTextForLogging(nil, text)
	ses.SetStmtId(stmID)
	stmtTyp := getStatementType(statement).GetStatementType()
	queryTyp := getStatementType(statement).GetQueryType()
	ses.SetStmtType(stmtTyp)
	ses.SetQueryType(queryTyp)
	ses.SetSqlSourceType(sqlType)
	ses.SetSqlOfStmt(text)
	if proc != nil {
		// RecordStatement mutates the session profile in place; refresh the
		// process view so statement-dependent cached decisions are recomputed.
		proc.SetStmtProfile(&ses.stmtProfile)
	}
	ses.stmtProfile.SetStatementRuntimeProfile(stmtTyp, queryTyp, isIgnoreStatement(statement))

	//note: txn id here may be empty
	// add by #9907, set the result of last_query_id(), this will pass those isCmdFieldListSql() from client.
	// fixme: this op leads all internal/background executor got NULL result if call last_query_id().
	if sqlType != constant.InternalSql {
		ses.pushQueryId(types.Uuid(stmID).String())
	}

	// -------------------------------------
	// Gen StatementInfo
	// -------------------------------------

	if !motrace.GetTracerProvider().IsEnable() {
		return ctx, nil
	}
	if sqlType == constant.InternalSql && envStmt == "" {
		// case: exec `set @ t= 2;` will trigger an internal query with the same session, like: `select 2 from dual`
		// ignore internal EMPTY query.
		return ctx, nil
	}
	// A same-session derived statement is implementation work of the visible
	// client statement. Keep its independent StatsInfo, but share the existing
	// resource root and StatementInfo lifecycle instead of replacing either.
	if ses.IsDerivedStmt() && resource.RootFromContext(ctx) != nil {
		return ctx, nil
	}

	// Only a StatementInfo owns and closes the statement memory epoch. Create
	// the root and epoch after every path that deliberately skips recording.
	root := resource.NewRoot(transferSessionConnType2ResourceConnType(ses.connType))
	ctx = resource.ContextWithRoot(ctx, root)
	var resourceMPeakEpoch *mpool.ResourcePeakEpoch
	if proc != nil {
		pool := proc.Mp()
		if pool != nil {
			resourceMPeakEpoch = pool.StartResourcePeakEpoch()
		}
		root.SetMemoryPeakPreview(func() (uint64, bool) {
			if pool == nil || resourceMPeakEpoch == nil {
				return 0, false
			}
			return pool.ResourcePeakLiveBytes(resourceMPeakEpoch)
		})
	}

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal") // pls task care of mce.GetDoQueryFunc() call case.
	}
	stm := motrace.NewStatementInfo()
	// set TransactionID
	var txn TxnOperator
	var err error
	// fixme: use ses.GetTxnId to simple.
	if handler := ses.GetTxnHandler(); handler.InActiveTxn() {
		txn = handler.GetTxn()
		if err != nil {
			return nil, err
		}
		stm.SetTxnID(txn.Txn().ID)
	}
	// set SessionID
	copy(stm.SessionID[:], ses.GetUUID())
	copy(stm.StatementID[:], stmID[:])
	requestAt := envBegin
	if !useEnv {
		requestAt = time.Now()
	}

	stm.ConnectionId = ses.GetConnectionID()
	stm.Account = tenant.GetTenant()
	stm.AccountID = tenant.GetTenantID()
	stm.RoleId = tenant.GetDefaultRoleID()
	//stm.RoleId = proc.GetSessionInfo().RoleId
	stm.User = tenant.GetUser()
	stm.Host = ses.respr.GetStr(PEER)
	stm.Database = ses.respr.GetStr(DBNAME)
	stm.StatementFingerprint = "" // fixme= (Reserved)
	stm.StatementTag = ""         // fixme= (Reserved)
	stm.SqlSourceType = sqlType
	stm.RequestAt = requestAt
	stm.StatementType = getStatementType(statement).GetStatementType()
	stm.QueryType = getStatementType(statement).GetQueryType()
	stm.ConnType = transferSessionConnType2StatisticConnType(ses.connType)
	stm.SetResourceRoot(root)
	if proc != nil {
		stm.SetResourceMemoryPoolEpoch(proc.Mp(), resourceMPeakEpoch)
	}
	if sqlType == constant.InternalSql && isCmdFieldListSql(envStmt) {
		// fix original issue #8165
		stm.User = ""
	}
	if ses.disableAgg {
		stm.DisableAgg()
	}
	// RecordStatementSql need to be the last calling before Report
	stm.RecordStatementSql(text, envStmt)
	stm.Report(ctx) // pls keep it simple: Only call Report twice at most.
	ses.SetTStmt(stm)

	return ctx, nil
}

func redactStatementTextForLogging(statement tree.Statement, text string) string {
	// __mo_query is a user-supplied MongoDB filter or pipeline. It is valid in
	// ordinary SELECT statements, whose AST formatting deliberately preserves
	// string literals, so neither the default branch nor a re-rendered AST is a
	// safe diagnostic representation. This is the last common boundary before
	// session state and statement telemetry retain the SQL text. Redact the
	// whole statement rather than trying to recognize one SQL expression shape:
	// invalid, nested, or future selector forms must not become a logging leak.
	if diagnostic := sqlmongodb.RedactSQLForDiagnostics(text); diagnostic != text {
		return diagnostic
	}

	switch stmt := statement.(type) {
	case *tree.CreateIcebergCatalog, *tree.AlterIcebergCatalog,
		*tree.CreateMongoDBConnection, *tree.AlterMongoDBConnection:
		return tree.String(statement, dialect.MYSQL)
	case *tree.CreateTable:
		// A datastream external table's WITH options may carry an 'apikey'
		// secret, and an ESQL/SQL foreign table's inline 'config' carries
		// credentials; re-rendering the AST redacts them
		// (DataStreamOption.Format / ForeignTableOption.Format), so the raw
		// CREATE text never reaches statement logging.
		if stmt.DataStreamParam != nil || stmt.ForeignParam != nil || stmt.KafkaParam != nil {
			return tree.String(statement, dialect.MYSQL)
		}
		return text
	default:
		return text
	}
}

// redactStatementErrorForLogging replaces a parser echo of __mo_query before it
// reaches a client, statement telemetry, or the terminal statement logger.
func redactStatementErrorForLogging(err error, text string) error {
	if err == nil || sqlmongodb.RedactSQLForDiagnostics(text) == text {
		return err
	}
	return moerr.NewParseErrorNoCtx("parse error in <redacted MongoDB __mo_query statement>")
}

func isIgnoreStatement(statement tree.Statement) bool {
	switch stmt := statement.(type) {
	case *tree.Insert:
		return len(stmt.OnDuplicateUpdate) == 1 && stmt.OnDuplicateUpdate[0] == nil
	case *tree.Update:
		return stmt.Ignore
	case *tree.Load:
		return isLoadDataIgnore(stmt)
	default:
		return false
	}
}

func isLoadDataIgnore(stmt *tree.Load) bool {
	if stmt == nil {
		return false
	}
	_, ok := stmt.DuplicateHandling.(*tree.DuplicateKeyIgnore)
	return ok
}

func refreshProcessStmtProfileForPreparedStmt(proc *process.Process, statement tree.Statement) {
	if proc == nil || statement == nil {
		return
	}

	stmtProfile := proc.GetStmtProfile()
	stmtProfile.SetStatementRuntimeProfile(
		getStatementType(statement).GetStatementType(),
		getStatementType(statement).GetQueryType(),
		isIgnoreStatement(statement),
	)
}

var RecordParseErrorStatement = func(ctx context.Context, ses *Session, proc *process.Process, envBegin time.Time,
	envStmt []string, sqlTypes []string, err error) (context.Context, error) {
	retErr := moerr.NewParseError(ctx, err.Error())
	/*
		!!!NOTE: the sql may be empty string.
		So, the sqlTypes may be empty slice.
	*/
	sqlType := ""
	if len(sqlTypes) > 0 {
		sqlType = sqlTypes[0]
	} else {
		sqlType = constant.ExternSql
	}
	finishParseError := func(last bool) {
		if last && ses.deferStatementCompletion(retErr) {
			return
		}
		if ses.tStmt != nil {
			ses.tStmt.EndStatement(ctx, retErr, 0, 0, 0)
		}
		ses.SetTStmt(nil)
	}
	if len(envStmt) > 0 {
		for i, sql := range envStmt {
			if i < len(sqlTypes) {
				sqlType = sqlTypes[i]
			}
			ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, sql, sqlType, true)
			if err != nil {
				return nil, err
			}
			finishParseError(i == len(envStmt)-1)
		}
	} else {
		ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, "", sqlType, true)
		if err != nil {
			return nil, err
		}
		finishParseError(true)
	}

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	incStatementErrorsCounter(tenant.GetTenant(), tenant.GetTenantID(), nil)
	return ctx, nil
}

// RecordStatementTxnID record txnID after TxnBegin or Compile(autocommit=1)
var RecordStatementTxnID = func(ctx context.Context, fses FeSession) error {
	var ses *Session
	var ok bool
	if ses, ok = fses.(*Session); !ok {
		return nil
	}
	var txn TxnOperator
	var err error
	if ses == nil {
		return nil
	}

	if stm := ses.tStmt; stm != nil && stm.IsZeroTxnID() {
		if handler := ses.GetTxnHandler(); handler.InActiveTxn() {
			// simplify the logic of TxnOperator. refer to https://github.com/matrixorigin/matrixone/pull/13436#pullrequestreview-1779063200
			txn = handler.GetTxn()
			if err != nil {
				return err
			}
			stm.SetTxnID(txn.Txn().ID)
			ses.SetTxnId(txn.Txn().ID)
		}
		// simplify the logic of query's CollectionTxnOperator. refer to https://github.com/matrixorigin/matrixone/pull/13625
		// only call at the beginning / or the end of query's life-cycle.
		// stm.Report(ctx)
	}

	// set frontend statement's txn-id
	if upSes := ses.upstream; upSes != nil && upSes.tStmt != nil && upSes.tStmt.IsZeroTxnID() /* not record txn-id */ {
		// background session has valid txn
		if handler := ses.GetTxnHandler(); handler.InActiveTxn() {
			txn = handler.GetTxn()
			if err != nil {
				return err
			}
			// set upstream (the frontend session) statement's txn-id
			// PS: only skip ONE txn
			if stmt := upSes.tStmt; stmt.NeedSkipTxn() /* normally set by determineUserHasPrivilegeSet */ {
				// need to skip the whole txn, so it records the skipped txn-id
				stmt.SetSkipTxn(false)
				stmt.SetSkipTxnId(txn.Txn().ID)
			} else if txnId := txn.Txn().ID; !stmt.SkipTxnId(txnId) {
				upSes.tStmt.SetTxnID(txnId)
			}
		}
	}
	return nil
}

func handleShowTableStatus(ses *Session, execCtx *ExecCtx, stmt *tree.ShowTableStatus) error {
	var db engine.Database
	var err error

	txnOp := ses.GetTxnHandler().GetTxn()
	ctx := execCtx.reqCtx

	bh := ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	subMeta, err := getSubscriptionMeta(ctx, stmt.DbName, ses, txnOp, bh)
	if err != nil {
		return err
	}

	dbName := stmt.DbName
	if subMeta != nil {
		dbName = subMeta.DbName
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(subMeta.AccountId))
	}

	if db, err = ses.GetTxnHandler().GetStorage().Database(ctx, dbName, txnOp); err != nil {
		return err
	}

	getRoleName := func(roleId uint32) (roleName string, err error) {
		accountId, err := defines.GetAccountId(ctx)
		if err != nil {
			return
		}

		if accountId != sysAccountID && roleId == moAdminRoleID {
			roleName = accountAdminRoleName
			return
		}

		sql := getSqlForRoleNameOfRoleId(int64(roleId))

		var rets []ExecResult
		if rets, err = executeSQLInBackgroundSession(ctx, bh, sql); err != nil {
			return "", err
		}

		if !execResultArrayHasData(rets) {
			return "", moerr.NewInternalError(ctx, "get role name failed")
		}

		if roleName, err = rets[0].GetString(ctx, 0, 0); err != nil {
			return "", err
		}
		return roleName, nil
	}

	needRowsAndSizeTableTypes := []string{catalog.SystemOrdinaryRel, catalog.SystemMaterializedRel}

	getTableStats := func(tblNames []string) (rows, sizes map[string]int64, err error) {
		if len(tblNames) == 0 {
			return
		}

		// set session variable
		if err = ses.SetSessionSysVar(ctx, "mo_table_stats.force_update", "yes"); err != nil {
			return
		}
		ses.markMigrationSystemVarReplayable("mo_table_stats.force_update", true)
		defer func() {
			if restoreErr := ses.SetSessionSysVar(ctx, "mo_table_stats.force_update", "no"); restoreErr != nil {
				ses.markMigrationSystemVarReplayable("mo_table_stats.force_update", false)
				return
			}
			ses.markMigrationSystemVarReplayable("mo_table_stats.force_update", true)
		}()

		sqlBuilder := strings.Builder{}
		sqlBuilder.WriteString("select tbl, mo_table_rows(db, tbl), mo_table_size(db, tbl) from (values ")
		for i, tblName := range tblNames {
			if i != 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString(fmt.Sprintf("row('%s', '%s')", dbName, tblName))
		}
		sqlBuilder.WriteString(") as tmp(db, tbl)")

		// get table stats
		var rets []ExecResult
		if rets, err = ExeSqlInBgSes(ctx, bh, sqlBuilder.String()); err != nil {
			return
		}

		var tblName string
		rows = make(map[string]int64, len(tblNames))
		sizes = make(map[string]int64, len(tblNames))
		for _, result := range rets {
			for i := uint64(0); i < result.GetRowCount(); i++ {
				if tblName, err = result.GetString(ctx, i, 0); err != nil {
					return
				}
				if rows[tblName], err = result.GetInt64(ctx, i, 1); err != nil {
					return
				}
				if sizes[tblName], err = result.GetInt64(ctx, i, 2); err != nil {
					return
				}
			}
		}
		return
	}

	getIndexSizes := func(indexTableNames []string) (map[string]int64, error) {
		if len(indexTableNames) == 0 {
			return map[string]int64{}, nil
		}
		sqlBuilder := strings.Builder{}
		sqlBuilder.WriteString("select tbl, mo_table_size(db, tbl) from (values ")
		for i, tblName := range indexTableNames {
			if i != 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString(fmt.Sprintf("row('%s', '%s')", dbName, tblName))
		}
		sqlBuilder.WriteString(") as tmp(db, tbl)")
		rets, err := ExeSqlInBgSes(ctx, bh, sqlBuilder.String())
		if err != nil {
			return nil, err
		}
		sizes := make(map[string]int64, len(indexTableNames))
		var tblName string
		for _, result := range rets {
			for r := uint64(0); r < result.GetRowCount(); r++ {
				if tblName, err = result.GetString(ctx, r, 0); err != nil {
					return nil, err
				}
				if sizes[tblName], err = result.GetInt64(ctx, r, 1); err != nil {
					return nil, err
				}
			}
		}
		return sizes, nil
	}

	// For some system tables (for example `system.statement_info`), tenant queries
	// are rewritten to sys account with account-level filtering. mo_table_rows/size
	// does not reflect that rewritten visibility in all cases, so fallback to
	// count(*) for Rows when mo_table_rows is empty/zero to keep SHOW TABLE STATUS
	// consistent with SELECT semantics without paying the extra cost when stats are
	// already populated.
	getSpecialTableRows := func(tblNames []string, tableRows map[string]int64) (map[string]int64, error) {
		rows := make(map[string]int64)
		if len(tblNames) == 0 {
			return rows, nil
		}
		accountId, err := defines.GetAccountId(ctx)
		if err != nil {
			return nil, err
		}
		if accountId == sysAccountID {
			return rows, nil
		}

		escapeIdent := func(name string) string {
			return strings.ReplaceAll(name, "`", "``")
		}

		for _, tblName := range tblNames {
			if !ShouldSwitchToSysAccount(dbName, tblName) {
				continue
			}
			if currentRows, ok := tableRows[tblName]; ok && currentRows > 0 {
				continue
			}
			sql := fmt.Sprintf("select count(*) from `%s`.`%s`", escapeIdent(dbName), escapeIdent(tblName))
			rets, err := ExeSqlInBgSes(ctx, bh, sql)
			if err != nil {
				return nil, err
			}
			if !execResultArrayHasData(rets) {
				continue
			}
			cnt, err := rets[0].GetInt64(ctx, 0, 0)
			if err != nil {
				return nil, err
			}
			rows[tblName] = cnt
		}

		return rows, nil
	}

	var tblNames []string
	var tblIdxes []int
	// baseTable -> index table names (secondary/unique only), for Index_length
	type baseIndexPair struct{ base, index string }
	var baseIndexPairs []baseIndexPair
	indexTableSet := make(map[string]struct{})

	mrs := ses.GetMysqlResultSet()
	for i, row := range ses.data {
		tableName := string(row[0].([]byte))
		// check if the table is in the subscription meta
		if subMeta != nil && !pubsub.InSubMetaTables(subMeta, tableName) {
			continue
		}

		r, err := db.Relation(ctx, tableName, nil)
		if err != nil {
			return err
		}

		if slices.Contains(needRowsAndSizeTableTypes, r.GetTableDef(ctx).TableType) {
			tblNames = append(tblNames, tableName)
			tblIdxes = append(tblIdxes, i)
			tableDef := r.GetTableDef(ctx)
			for _, idx := range tableDef.GetIndexes() {
				itn := idx.GetIndexTableName()
				if strings.HasPrefix(itn, catalog.UniqueIndexTableNamePrefix) || strings.HasPrefix(itn, catalog.SecondaryIndexTableNamePrefix) {
					baseIndexPairs = append(baseIndexPairs, baseIndexPair{tableName, itn})
					indexTableSet[itn] = struct{}{}
				}
			}
		} else if r.GetTableDef(ctx).TableType == catalog.SystemViewRel {
			for i := 0; i < 16; i++ {
				// only remain name and created_time
				if i == 0 || i == 10 {
					continue
				}
				row[i] = nil
			}
			// comment
			row[16] = "VIEW"
		}
		roleId := row[17].(uint32)
		// role name
		if tableName == catalog.MO_DATABASE || tableName == catalog.MO_TABLES || tableName == catalog.MO_COLUMNS {
			row[18] = moAdminRoleName
		} else {
			if row[18], err = getRoleName(roleId); err != nil {
				return err
			}
		}
		mrs.AddRow(row)
	}

	// calculate table row and size
	rows, sizes, err := getTableStats(tblNames)
	if err != nil {
		return err
	}
	specialRows, err := getSpecialTableRows(tblNames, rows)
	if err != nil {
		return err
	}

	indexLengths := make(map[string]int64, len(tblNames))
	if len(indexTableSet) > 0 {
		indexTableNames := make([]string, 0, len(indexTableSet))
		for k := range indexTableSet {
			indexTableNames = append(indexTableNames, k)
		}
		indexSizes, err := getIndexSizes(indexTableNames)
		if err != nil {
			return err
		}
		for _, p := range baseIndexPairs {
			indexLengths[p.base] += indexSizes[p.index]
		}
	}

	for i, tblName := range tblNames {
		idx := tblIdxes[i]
		if cnt, ok := specialRows[tblName]; ok {
			ses.data[idx][3] = cnt
		} else {
			ses.data[idx][3] = rows[tblName]
		}
		if rows[tblName] > 0 {
			ses.data[idx][4] = sizes[tblName] / rows[tblName]
		} else {
			ses.data[idx][4] = int64(0)
		}
		ses.data[idx][5] = sizes[tblName]
		ses.data[idx][7] = indexLengths[tblName]
	}
	return nil
}

// getDataFromPipeline: extract the data from the pipeline.
// obj: session
func getDataFromPipeline(obj FeSession, execCtx *ExecCtx, bat *batch.Batch, crs *perfcounter.CounterSet) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.WriteDataToClient")
	defer task.End()
	ses := obj.(*Session)

	begin := time.Now()
	err := ses.GetResponser().RespResult(execCtx, crs, bat)
	if err != nil {
		return err
	}
	tTime := time.Since(begin)
	n := 0
	if !isPerformStatement(execCtx.stmt) && bat != nil {
		n = bat.RowCount()
		ses.sentRows.Add(int64(n))
	}

	ses.Debugf(execCtx.reqCtx, "rowCount %v \n"+
		"time of getDataFromPipeline : %s \n",
		n,
		tTime)

	stats := statistic.StatsInfoFromContext(execCtx.reqCtx)
	stats.AddOutputTimeConsumption(tTime)
	return nil
}

// newPreparedStmtCursor creates the bounded result retained for COM_STMT_FETCH.
// query_result_maxsize is already the account-level result budget used by the
// frontend. Reusing it keeps cursor retention configurable without introducing
// a second session variable; the hard cap prevents an accidentally huge value
// from turning the prepared-statement quota into an unbounded heap multiplier.
func newPreparedStmtCursor(ses *Session) *preparedStmtCursor {
	limit := currentPreparedCursorLimit(ses, preparedCursorDefaultMaxBytes)
	return &preparedStmtCursor{
		result:      &MysqlResultSet{},
		maxBytes:    limit,
		maxBytesSet: ses != nil && ses.sesSysVars != nil,
		maxRows:     preparedCursorMaxRows,
		owner:       ses,
	}
}

func currentPreparedCursorLimit(ses *Session, fallback uint64) uint64 {
	limit := fallback
	if limit == 0 {
		limit = preparedCursorDefaultMaxBytes
	}
	if ses == nil {
		return limit
	}

	// A live session owns the dynamic query_result_maxsize value. Do not cache
	// it in preparedCursorLimit: clients can lower or raise the variable after
	// closing a cursor, and the next cursor must observe the new budget.
	if ses.sesSysVars != nil {
		if value, err := ses.GetSessionSysVar(QueryResultMaxsize); err == nil {
			var megabytes uint64
			valid := false
			switch v := value.(type) {
			case uint64:
				megabytes = v
				valid = true
			case int64:
				if v >= 0 {
					megabytes = uint64(v)
					valid = true
				}
			case uint32:
				megabytes = uint64(v)
				valid = true
			case int32:
				if v >= 0 {
					megabytes = uint64(v)
					valid = true
				}
			}
			if valid {
				maxMegabytes := preparedCursorHardMaxBytes / preparedCursorBytesPerMegabyte
				if megabytes > maxMegabytes {
					megabytes = maxMegabytes
				}
				return megabytes * preparedCursorBytesPerMegabyte
			}
		}
		return limit
	}

	// Partially initialized sessions in unit tests do not have a sysvar map.
	// Keep the atomic field as a compatibility fallback for those callers; it
	// is never populated by a live session anymore.
	if existing := ses.preparedCursorLimit.Load(); existing != 0 {
		return existing
	}
	return limit
}

func (ses *Session) tryReservePreparedCursorBytes(bytes, fallbackLimit uint64) bool {
	if ses == nil || bytes == 0 {
		return true
	}
	limit := currentPreparedCursorLimit(ses, fallbackLimit)
	for {
		current := ses.preparedCursorBytes.Load()
		if current > limit || bytes > limit-current {
			return false
		}
		if ses.preparedCursorBytes.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

func (ses *Session) releasePreparedCursorBytes(bytes uint64) {
	if ses == nil || bytes == 0 {
		return
	}
	for {
		current := ses.preparedCursorBytes.Load()
		if current == 0 {
			return
		}
		next := uint64(0)
		if bytes < current {
			next = current - bytes
		}
		if ses.preparedCursorBytes.CompareAndSwap(current, next) {
			return
		}
	}
}

func estimatePreparedCursorBatchBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}
	dataBytes := bat.Size()
	if allocated := bat.Allocated(); allocated > dataBytes {
		dataBytes = allocated
	}
	if dataBytes < 0 {
		return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
	}
	// Each retained row owns a []any backing array and may also own converted
	// strings/arrays (for example DECIMAL and temporal values). Charge a
	// conservative per-column allowance in addition to the source vector bytes.
	rowBytes := uint64(len(bat.Vecs))*32 + 64
	rows := uint64(bat.RowCount())
	if rows > math.MaxUint64/rowBytes {
		return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
	}
	overhead := rows * rowBytes
	if uint64(dataBytes) > math.MaxUint64-overhead {
		return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
	}
	total := uint64(dataBytes) + overhead
	materialized, err := estimatePreparedCursorMaterializedBytes(bat)
	if err != nil {
		return 0, err
	}
	if materialized > math.MaxUint64-total {
		return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
	}
	return total + materialized, nil
}

// estimatePreparedCursorMaterializedBytes charges allocations that are created
// while fillResultSet converts vectors into retained []any rows. In
// particular, vecuint8 values are rendered with ArrayToString and decimal
// values with Format, so their retained representations can be several times
// larger than the raw vector.
// Other array families are copied into a new typed slice and are charged for
// that second backing store as well.
func estimatePreparedCursorMaterializedBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}
	var total uint64
	add := func(value uint64) error {
		if value > math.MaxUint64-total {
			return moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
		}
		total += value
		return nil
	}
	for _, vec := range bat.Vecs {
		if vec == nil || vec.IsConstNull() {
			continue
		}
		rows := bat.RowCount()
		switch vec.GetType().Oid {
		case types.T_array_uint8:
			for row := 0; row < rows; row++ {
				if vec.GetNulls().Contains(uint64(row)) {
					continue
				}
				arr := vector.GetArrayAt[uint8](vec, row)
				// ArrayToString uses at most three decimal digits per
				// uint8 plus ", " separators and two brackets.
				displayBytes := uint64(2)
				if len(arr) > 0 {
					if uint64(len(arr)) > math.MaxUint64/5 {
						return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
					}
					displayBytes = uint64(len(arr)) * 5
				}
				// Include the string header/allocation slack in addition to
				// the character bytes. The row/column overhead above covers
				// the []any slot itself.
				if displayBytes > math.MaxUint64-16 {
					return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
				}
				if err := add(displayBytes + 16); err != nil {
					return 0, err
				}
			}
		case types.T_array_float32:
			if err := estimatePreparedCursorArrayCopyBytes(vec, rows, 4, add); err != nil {
				return 0, err
			}
		case types.T_array_float64:
			if err := estimatePreparedCursorArrayCopyBytes(vec, rows, 8, add); err != nil {
				return 0, err
			}
		case types.T_array_bf16, types.T_array_float16:
			if err := estimatePreparedCursorArrayCopyBytes(vec, rows, 2, add); err != nil {
				return 0, err
			}
		case types.T_array_int8:
			if err := estimatePreparedCursorArrayCopyBytes(vec, rows, 1, add); err != nil {
				return 0, err
			}
		case types.T_decimal64, types.T_decimal128, types.T_decimal256:
			for row := 0; row < rows; row++ {
				if vec.GetNulls().Contains(uint64(row)) {
					continue
				}
				var display string
				switch vec.GetType().Oid {
				case types.T_decimal64:
					display = vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, row).Format(vec.GetType().Scale)
				case types.T_decimal128:
					display = vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, row).Format(vec.GetType().Scale)
				case types.T_decimal256:
					display = vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, row).Format(vec.GetType().Scale)
				}
				if err := addFormattedPreparedCursorValueBytes(uint64(len(display)), add); err != nil {
					return 0, err
				}
			}
		case types.T_geometry, types.T_geometry32:
			// fillResultSet exposes geometry values as WKT bytes rather than
			// retaining the compact WKB payload.  A WKB point uses 16 bytes
			// per coordinate pair (8 for GEOMETRY32), while the rendered WKT
			// can be substantially larger for large LINESTRING/POLYGON values.
			// Decode the same payload before reserving the cursor budget so the
			// retained representation cannot exceed the reservation.
			for row := 0; row < rows; row++ {
				if vec.GetNulls().Contains(uint64(row)) {
					continue
				}
				text, err := planfunction.GeometryPayloadToText(vec.GetBytesAt(row))
				if err != nil {
					return 0, err
				}
				// Include the []byte backing allocation and allocator/header
				// slack in addition to the WKT characters.
				textBytes := uint64(len(text))
				if textBytes > math.MaxUint64-32 {
					return 0, moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
				}
				if err := add(textBytes + 32); err != nil {
					return 0, err
				}
			}
		}
	}
	return total, nil
}

func addFormattedPreparedCursorValueBytes(displayBytes uint64, add func(uint64) error) error {
	// fillResultSet retains a separately allocated string for formatted values.
	// Include the string backing allocation and allocator/header slack in
	// addition to the displayed bytes; the []any slot is charged by the row
	// overhead in estimatePreparedCursorBatchBytes.
	if displayBytes > math.MaxUint64-32 {
		return moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
	}
	return add(displayBytes + 32)
}

func estimatePreparedCursorArrayCopyBytes(
	vec *vector.Vector,
	rows int,
	elementBytes uint64,
	add func(uint64) error,
) error {
	for row := 0; row < rows; row++ {
		if vec.GetNulls().Contains(uint64(row)) {
			continue
		}
		var length int
		switch vec.GetType().Oid {
		case types.T_array_float32:
			length = len(vector.GetArrayAt[float32](vec, row))
		case types.T_array_float64:
			length = len(vector.GetArrayAt[float64](vec, row))
		case types.T_array_bf16:
			length = len(vector.GetArrayAt[types.BF16](vec, row))
		case types.T_array_float16:
			length = len(vector.GetArrayAt[types.Float16](vec, row))
		case types.T_array_int8:
			length = len(vector.GetArrayAt[int8](vec, row))
		}
		bytes := uint64(length)
		if bytes > math.MaxUint64/elementBytes {
			return moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
		}
		bytes *= elementBytes
		// Account for the slice header and allocator slack in the copied
		// value. The raw element bytes are the dominant component.
		if bytes > math.MaxUint64-24 {
			return moerr.NewInternalErrorNoCtx("prepared cursor result size overflow")
		}
		if err := add(bytes + 24); err != nil {
			return err
		}
	}
	return nil
}

// capturePreparedCursorBatch materializes a binary prepared-statement result
// for COM_STMT_FETCH. The ordinary output callback sends each batch directly
// to the client; a read-only server cursor must retain those rows until the
// client asks for them. Retention is bounded and accounted across all active
// cursors in the session. The result metadata is installed before the pipeline
// starts; wire column definitions and the cursor terminator are sent only
// after successful materialization.
func capturePreparedCursorBatch(ses *Session, execCtx *ExecCtx, bat *batch.Batch) error {
	if bat == nil {
		return nil
	}
	if execCtx == nil {
		return moerr.NewInternalErrorNoCtx("prepared cursor execution context is missing")
	}
	if execCtx.prepareStmt == nil || execCtx.prepareStmt.cursor == nil {
		return moerr.NewInternalError(execCtx.reqCtx, "prepared cursor state is missing")
	}
	cursor := execCtx.prepareStmt.cursor
	if cursor.result == nil {
		cursor.result = &MysqlResultSet{}
	}
	if cursor.owner == nil {
		cursor.owner = ses
	}
	if !cursor.maxBytesSet {
		if cursor.maxBytes == 0 {
			cursor.maxBytes = preparedCursorDefaultMaxBytes
		}
		cursor.maxBytesSet = true
	}
	if cursor.maxRows == 0 {
		cursor.maxRows = preparedCursorMaxRows
	}
	rowCount := cursor.result.GetRowCount()
	if rowCount > cursor.maxRows || uint64(bat.RowCount()) > cursor.maxRows-rowCount {
		return moerr.NewInvalidInputf(execCtx.reqCtx,
			"prepared cursor result exceeds the %d-row limit", cursor.maxRows)
	}
	estimated, err := estimatePreparedCursorBatchBytes(bat)
	if err != nil {
		return err
	}
	// query_result_maxsize is dynamic. Apply its current value to an active
	// cursor as well as to newly created cursors: a decrease takes effect
	// before more rows are retained, and an increase is not stranded behind a
	// stale per-cursor snapshot.
	effectiveLimit := currentPreparedCursorLimit(ses, cursor.maxBytes)
	if cursor.bytes > effectiveLimit || estimated > effectiveLimit-cursor.bytes {
		return moerr.NewInvalidInputf(execCtx.reqCtx,
			"prepared cursor result exceeds the %d MB memory limit", effectiveLimit/preparedCursorBytesPerMegabyte)
	}
	if !ses.tryReservePreparedCursorBytes(estimated, effectiveLimit) {
		return moerr.NewInvalidInputf(execCtx.reqCtx,
			"prepared cursor session result exceeds the %d MB memory limit", effectiveLimit/preparedCursorBytesPerMegabyte)
	}
	startRows := len(cursor.result.Data)
	committed := false
	defer func() {
		if !committed {
			// Keep the result set unchanged if extraction returns an error or
			// panics after appending a partial row prefix.
			cursor.result.Data = cursor.result.Data[:startRows]
			ses.releasePreparedCursorBytes(estimated)
		}
	}()
	if err = fillResultSet(execCtx.reqCtx, bat, ses, cursor.result); err != nil {
		return err
	}
	cursor.bytes += estimated
	committed = true
	return nil
}

func doUse(ctx context.Context, ses FeSession, db string) (err error) {
	defer RecordStatementTxnID(ctx, ses)

	// In order to be compatible with various GUI clients and BI tools, lower case db name if it's a mysql system db
	if slices.Contains(mysql.CaseInsensitiveDbs, strings.ToLower(db)) {
		db = strings.ToLower(db)
	}

	var dbMeta engine.Database
	txnHandler := ses.GetTxnHandler()
	txn := txnHandler.GetTxn()
	//TODO: check meta data
	if dbMeta, err = getPu(ses.GetService()).StorageEngine.Database(ctx, db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, db)
	}

	if dbMeta.IsSubscription(ctx) {
		bh := ses.GetShareTxnBackgroundExec(ctx, false)
		defer bh.Close()
		if _, err = checkSubscriptionValid(ctx, ses, db, bh); err != nil {
			return
		}
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	ses.Debugf(ctx, "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())
	return
}

func handleChangeDB(ses FeSession, execCtx *ExecCtx, db string) error {
	return doUse(execCtx.reqCtx, ses, db)
}

func handleDump(ses FeSession, execCtx *ExecCtx, dump *tree.MoDump) error {
	return doDumpQueryResult(execCtx.reqCtx, ses.(*Session), dump.ExportParams)
}

func doCmdFieldList(reqCtx context.Context, ses *Session, _ *InternalCmdFieldList) error {
	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return moerr.NewNoDB(reqCtx)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	//NOTE: it costs too much time.
	//It just reduces the information in the auto-completion (auto-rehash) of the mysql client.
	//var attrs []ColumnInfo
	//
	//if tableInfos == nil || db != dbName {
	//	txnHandler := ses.GetTxnHandler()
	//	eng := ses.GetStorage()
	//	db, err := eng.Database(reqCtx, dbName, txnHandler.GetTxn())
	//	if err != nil {
	//		return err
	//	}
	//
	//	names, err := db.Relations(reqCtx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, name := range names {
	//		table, err := db.Relation(reqCtx, name)
	//		if err != nil {
	//			return err
	//		}
	//
	//		defs, err := table.TableDefs(reqCtx)
	//		if err != nil {
	//			return err
	//		}
	//		for _, def := range defs {
	//			if attr, ok := def.(*engine.AttributeDef); ok {
	//				attrs = append(attrs, &engineColumnInfo{
	//					name: attr.Attr.Name,
	//					typ:  attr.Attr.Type,
	//				})
	//			}
	//		}
	//	}
	//
	//	if tableInfos == nil {
	//		tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := tableInfos[tableName]
	//if !ok {
	//	//just give the empty info when there is no such table.
	//	attrs = make([]ColumnInfo, 0)
	//} else {
	//	attrs = cols
	//}
	//
	//for _, c := range attrs {
	//	col := new(MysqlColumn)
	//	col.SetName(c.GetName())
	//	err = convertEngineTypeToMysqlType(c.GetType(), col)
	//	if err != nil {
	//		return err
	//	}
	//
	//	/*
	//		mysql CMD_FIELD_LIST response: send the column definition per column
	//	*/
	//	err = proto.SendColumnDefinitionPacket(col, int(COM_FIELD_LIST))
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

/*
handle cmd CMD_FIELD_LIST
*/
func handleCmdFieldList(ses FeSession, execCtx *ExecCtx, icfl *InternalCmdFieldList) error {
	var err error

	ses.SetMysqlResultSet(nil)
	err = doCmdFieldList(execCtx.reqCtx, ses.(*Session), icfl)
	if err != nil {
		return err
	}

	return err
}

func doSetVar(
	ses *Session,
	execCtx *ExecCtx,
	sv *tree.SetVar,
	sql string,
	preparedExpression bool,
) error {
	if preparedExpression && len(sv.Assignments) > 1 {
		for _, assign := range sv.Assignments {
			if assign.System || assign.SetNames {
				return moerr.NewNotSupported(execCtx.reqCtx,
					"prepared multi-assignment SET supports user variables only")
			}
		}
	}

	var err error = nil
	var ok bool
	var userVarIsBin bool
	var userVarType plan.Type
	var userVarPrepareParamKind vector.PrepareParamKind
	type evaluatedAssignment struct {
		assign                  *tree.VarAssignmentExpr
		value                   interface{}
		userVarIsBin            bool
		valueType               plan.Type
		userVarPrepareParamKind vector.PrepareParamKind
	}
	type systemVarReplayabilitySnapshot struct {
		replayable bool
		tracked    bool
	}
	previousSystemReplayability := make(map[string]systemVarReplayabilitySnapshot)
	captureSystemReplayability := func(name string) {
		name = canonicalSystemVariableName(name)
		if _, captured := previousSystemReplayability[name]; captured {
			return
		}
		replayable, tracked := ses.getMigrationSystemVarReplayability(name)
		previousSystemReplayability[name] = systemVarReplayabilitySnapshot{
			replayable: replayable,
			tracked:    tracked,
		}
	}
	for _, assign := range sv.Assignments {
		if assign.SetNames {
			for _, name := range []string{
				"character_set_client", "character_set_connection", "character_set_results",
			} {
				captureSystemReplayability(name)
			}
		} else if assign.System {
			captureSystemReplayability(assign.Name)
		}
	}
	var preparedItems []*plan.SetVariablesItem
	if preparedExpression {
		if cw, ok := execCtx.cw.(*TxnComputationWrapper); ok && cw.plan != nil {
			if setVariables := cw.plan.GetDcl().GetSetVariables(); setVariables != nil {
				preparedItems = setVariables.Items
			}
		}
	}
	evaluateAssignment := func(index int, assign *tree.VarAssignmentExpr) (evaluatedAssignment, error) {
		isBin := false
		prepareParamKind := vector.PrepareParamNone
		var value interface{}
		var valueType plan.Type
		var evalErr error
		if index < len(preparedItems) && preparedItems[index].Value != nil {
			if preparedPlanExprContainsSubquery(preparedItems[index].Value) {
				value, valueType, evalErr = getPreparedPlanExprValueWithSubqueries(
					assign.Value, preparedItems[index].Value, ses, execCtx, &prepareParamKind, &isBin)
			} else {
				value, valueType, evalErr = getPreparedPlanExprValueWithMeta(
					preparedItems[index].Value, ses, execCtx, &prepareParamKind, &isBin)
			}
		} else {
			value, valueType, evalErr = getExprValueWithPrepareMeta(
				assign.Value, ses, execCtx, preparedExpression, nil, &prepareParamKind, &isBin)
		}
		if evalErr != nil {
			return evaluatedAssignment{}, evalErr
		}

		if systemVar, exists := gSysVarsDefs[assign.Name]; exists {
			if isDefault, isBool := value.(bool); isBool && isDefault {
				if scope, isTxnIsolation := transactionIsolationAssignmentScope(assign); isTxnIsolation {
					value, evalErr = transactionIsolationDefaultValue(
						execCtx.reqCtx, ses, scope)
					if evalErr != nil {
						return evaluatedAssignment{}, evalErr
					}
				} else {
					value = systemVar.Default
				}
			}
		}
		return evaluatedAssignment{
			assign:                  assign,
			value:                   value,
			userVarIsBin:            isBin,
			valueType:               valueType,
			userVarPrepareParamKind: prepareParamKind,
		}, nil
	}
	setVarFunc := func(system, global bool, name string, value interface{}, sql string) error {
		var oldValueRaw interface{}
		if system {
			if global {
				if err = doCheckRole(execCtx.reqCtx, ses); err != nil {
					return err
				}
				if err = ses.SetGlobalSysVar(execCtx.reqCtx, name, value); err != nil {
					return err
				}
			} else {
				if strings.ToLower(name) == "autocommit" {
					if oldValueRaw, err = ses.GetSessionSysVar("autocommit"); err != nil {
						return err
					}
				}
				if err = ses.SetSessionSysVar(execCtx.reqCtx, name, value); err != nil {
					return err
				}
				if strings.ToLower(name) == "autocommit" {
					var oldValue, newValue bool

					if oldValue, err = valueIsBoolTrue(oldValueRaw); err != nil {
						return err
					}

					if newValue, err = valueIsBoolTrue(value); err != nil {
						return err
					}

					if err = ses.GetTxnHandler().SetAutocommit(execCtx, oldValue, newValue); err != nil {
						return err
					}
				}
			}
		} else {
			err = ses.setUserDefinedVarWithTypeAndKindAndReplayability(
				name, value, sql, userVarIsBin, userVarType, userVarPrepareParamKind,
				!preparedExpression && sql != "" && execCtx.singleStatementQuery)
			if err != nil {
				return err
			}
		}
		return nil
	}
	markSystemReplayability := func(assign *tree.VarAssignmentExpr) {
		mark := func(name string, replayable bool) {
			name = canonicalSystemVariableName(name)
			if replayable {
				if previous, captured := previousSystemReplayability[name]; captured && previous.tracked && !previous.replayable {
					// A later captured assignment cannot prove that an earlier
					// prepared assignment was replayable.
					replayable = false
				}
			}
			ses.markMigrationSystemVarReplayable(name, replayable)
		}
		if !assign.System && !assign.SetNames {
			return
		}
		replayable := !preparedExpression && sql != "" && execCtx.singleStatementQuery
		if assign.SetNames {
			for _, name := range []string{
				"character_set_client", "character_set_connection", "character_set_results",
			} {
				mark(name, replayable)
			}
			return
		}
		if assign.Global {
			if def, ok := gSysVarsDefs[canonicalSystemVariableName(assign.Name)]; ok && def.Scope == ScopeBoth {
				// SET GLOBAL changes the value inherited by a future session but
				// leaves this session's value unchanged. Replaying it on a legacy
				// target after the handshake would therefore lose the source value.
				ses.markMigrationSystemVarReplayable(assign.Name, false)
				return
			}
			// Only global variables with a session-migration runtime side
			// effect need replayability tracking. A prepared or multi-statement
			// SET GLOBAL for these variables is not present in the proxy raw
			// stream, so legacy targets must fail closed instead of silently
			// losing the source runtime value.
			if hasMigrationRuntimeSideEffect(assign.Name) {
				mark(assign.Name, replayable)
			}
			return
		}
		mark(assign.Name, replayable)
	}

	applyAssignment := func(item evaluatedAssignment) error {
		assign := item.assign
		name := assign.Name
		value := item.value
		userVarIsBin = item.userVarIsBin
		userVarType = item.valueType
		userVarPrepareParamKind = item.userVarPrepareParamKind

		//TODO : fix SET NAMES after parser is ready
		if assign.SetNames {
			//replaced into three system variable:
			//character_set_client, character_set_connection, and character_set_results
			replacedBy := []string{
				"character_set_client", "character_set_connection", "character_set_results",
			}
			for _, rb := range replacedBy {
				err = setVarFunc(assign.System, assign.Global, rb, value, sql)
				if err != nil {
					return err
				}
			}
		} else if scope, isTxnIsolation := transactionIsolationAssignmentScope(assign); isTxnIsolation {
			switch scope {
			case tree.TransactionScopeNext:
				def := gSysVarsDefs[canonicalSystemVariableName(name)]
				converted, convertErr := def.GetType().Convert(value)
				if convertErr != nil {
					return convertErr
				}
				isolation, isolationErr := txnIsolationFromSystemValue(execCtx.reqCtx, converted)
				if isolationErr != nil {
					return isolationErr
				}
				txnHandler := ses.GetTxnHandler()
				if txnHandler == nil {
					return moerr.NewInternalError(execCtx.reqCtx, "transaction handler is not initialized")
				}
				allowCurrentStatementTxn := execCtx.txnOpt.activeTxnAtStartKnown &&
					!execCtx.txnOpt.activeTxnAtStart
				if err := txnHandler.setNextTxnIsolation(
					execCtx.reqCtx, isolation, allowCurrentStatementTxn); err != nil {
					return err
				}
				ses.markMigrationSystemVarReplayable(
					migrationNextTxnIsolationKey, !preparedExpression && sql != "" && execCtx.singleStatementQuery)
				return nil
			case tree.TransactionScopeSession:
				return setVarFunc(true, false, name, value, sql)
			case tree.TransactionScopeGlobal:
				return setVarFunc(true, true, name, value, sql)
			default:
				return moerr.NewInvalidInputf(execCtx.reqCtx,
					"unsupported transaction scope %d", scope)
			}
		} else if assign.System && name == "clear_privilege_cache" {
			//if it is global variable, it does nothing.
			if !assign.Global {
				//if the value is 'on or off', just invalidate the privilege cache
				ok, err = valueIsBoolTrue(value)
				if err != nil {
					return err
				}

				if ok {
					cache := ses.GetPrivilegeCache()
					if cache != nil {
						cache.invalidate()
					}
					// Clearing the cache is also the explicit synchronization point
					// for externally changed role membership. Refresh it now, outside
					// the caller's transaction snapshot, instead of allowing the next
					// authorization check to repopulate the cache from stale state.
					_, _, err = validateActiveRoleGrantForAuthorization(execCtx.reqCtx, ses)
					if err != nil {
						return err
					}
				}
				err = setVarFunc(assign.System, assign.Global, name, value, sql)
				if err != nil {
					return err
				}
			}
		} else if assign.System && name == "enable_privilege_cache" {
			_, err = valueIsBoolTrue(value)
			if err != nil {
				return err
			}

			// Every session cache-mode assignment is a synchronization boundary.
			// In particular, enabling must discard decisions that may have been
			// produced while caching was disabled before a concurrent REVOKE.
			// SET GLOBAL does not change this session's cache mode.
			if !assign.Global {
				cache := ses.GetPrivilegeCache()
				if cache != nil {
					cache.invalidate()
				}
			}
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		} else if assign.System && name == "optimizer_hints" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			ses.applySessionSysVarSideEffects(name, value)
		} else if assign.System && name == "runtime_filter_limit_in" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			ses.applySessionSysVarSideEffects(name, value)
		} else if assign.System && name == "runtime_filter_limit_bloom_filter" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			ses.applySessionSysVarSideEffects(name, value)
		} else if assign.System && name == "disable_agg_statement" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			ses.applySessionSysVarSideEffects(name, value)
		} else {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		}
		return err
	}

	if preparedExpression && len(sv.Assignments) > 1 {
		type userDefinedVarSnapshot struct {
			value  *UserDefinedVar
			exists bool
		}
		original := make(map[string]userDefinedVarSnapshot, len(sv.Assignments))
		ses.mu.Lock()
		for _, assign := range sv.Assignments {
			name := strings.ToLower(assign.Name)
			if _, captured := original[name]; captured {
				continue
			}
			value, exists := ses.userDefinedVars[name]
			if value != nil {
				copied := *value
				value = &copied
			}
			original[name] = userDefinedVarSnapshot{value: value, exists: exists}
		}
		ses.mu.Unlock()

		completed := false
		defer func() {
			if completed {
				return
			}
			ses.mu.Lock()
			defer ses.mu.Unlock()
			for name, snapshot := range original {
				if snapshot.exists {
					ses.userDefinedVars[name] = snapshot.value
				} else {
					delete(ses.userDefinedVars, name)
				}
			}
		}()

		for index, assign := range sv.Assignments {
			item, evalErr := evaluateAssignment(index, assign)
			if evalErr != nil {
				return evalErr
			}
			if err = applyAssignment(item); err != nil {
				return err
			}
			markSystemReplayability(assign)
		}
		completed = true
		return nil
	}

	for index, assign := range sv.Assignments {
		item, evalErr := evaluateAssignment(index, assign)
		if evalErr != nil {
			return evalErr
		}
		if err = applyAssignment(item); err != nil {
			return err
		}
		markSystemReplayability(assign)
	}
	return nil
}

/*
handle setvar
*/
func handleSetVar(ses FeSession, execCtx *ExecCtx, sv *tree.SetVar, sql string) error {
	err := doSetVar(
		ses.(*Session), execCtx, sv, sql, preparedSetExpression(execCtx))
	if err != nil {
		return err
	}

	return nil
}

func handleSetTransaction(ses *Session, execCtx *ExecCtx, stmt *tree.SetTransaction) error {
	var isolationCharacteristic *tree.TransactionCharacteristic
	var accessCharacteristic *tree.TransactionCharacteristic
	for i, characteristic := range stmt.CharacterList {
		if characteristic == nil {
			return moerr.NewInvalidInputf(execCtx.reqCtx,
				"transaction characteristic %d is empty", i+1)
		}
		if characteristic.IsLevel {
			if isolationCharacteristic != nil {
				return moerr.NewInvalidInput(execCtx.reqCtx,
					"transaction isolation level specified more than once")
			}
			isolationCharacteristic = characteristic
		} else {
			if accessCharacteristic != nil {
				return moerr.NewInvalidInput(execCtx.reqCtx,
					"transaction access mode specified more than once")
			}
			accessCharacteristic = characteristic
		}
	}

	if accessCharacteristic != nil {
		var accessMode string
		switch accessCharacteristic.Access {
		case tree.ACCESS_MODE_READ_ONLY:
			accessMode = "READ ONLY"
		case tree.ACCESS_MODE_READ_WRITE:
			accessMode = "READ WRITE"
		default:
			return moerr.NewInvalidInputf(execCtx.reqCtx,
				"unsupported transaction access mode %d", accessCharacteristic.Access)
		}
		return moerr.NewNotSupported(execCtx.reqCtx,
			"transaction access mode "+accessMode+" is not supported")
	}
	if isolationCharacteristic == nil {
		return moerr.NewInvalidInput(execCtx.reqCtx,
			"transaction characteristic list must not be empty")
	}

	var value string
	var isolation pbtxn.TxnIsolation
	switch isolationCharacteristic.Isolation {
	case tree.ISOLATION_LEVEL_REPEATABLE_READ:
		value = "REPEATABLE-READ"
		isolation = pbtxn.TxnIsolation_SI
	case tree.ISOLATION_LEVEL_READ_COMMITTED:
		value = "READ-COMMITTED"
		isolation = pbtxn.TxnIsolation_RC
	case tree.ISOLATION_LEVEL_READ_UNCOMMITTED:
		return moerr.NewNotSupported(execCtx.reqCtx,
			"transaction isolation level READ-UNCOMMITTED is not supported")
	case tree.ISOLATION_LEVEL_SERIALIZABLE:
		return moerr.NewNotSupported(execCtx.reqCtx,
			"transaction isolation level SERIALIZABLE is not supported")
	default:
		return moerr.NewInvalidInputf(execCtx.reqCtx, "unsupported transaction isolation level %d", isolationCharacteristic.Isolation)
	}

	switch stmt.Scope {
	case tree.TransactionScopeNext:
		txnHandler := ses.GetTxnHandler()
		if txnHandler == nil {
			return moerr.NewInternalError(execCtx.reqCtx, "transaction handler is not initialized")
		}
		allowCurrentStatementTxn := execCtx.txnOpt.activeTxnAtStartKnown &&
			!execCtx.txnOpt.activeTxnAtStart
		if err := txnHandler.setNextTxnIsolation(
			execCtx.reqCtx,
			isolation,
			allowCurrentStatementTxn,
		); err != nil {
			return err
		}
		ses.markMigrationSystemVarReplayable(migrationNextTxnIsolationKey, false)
	case tree.TransactionScopeSession:
		if err := ses.SetSessionSysVar(execCtx.reqCtx, "transaction_isolation", value); err != nil {
			return err
		}
	case tree.TransactionScopeGlobal:
		if err := doCheckRole(execCtx.reqCtx, ses); err != nil {
			return err
		}
		if err := ses.SetGlobalSysVar(execCtx.reqCtx, "transaction_isolation", value); err != nil {
			return err
		}
	default:
		return moerr.NewInvalidInputf(execCtx.reqCtx, "unsupported transaction scope %d", stmt.Scope)
	}
	return nil
}

func preparedSetExpression(execCtx *ExecCtx) bool {
	if execCtx == nil {
		return false
	}
	cw, ok := execCtx.cw.(*TxnComputationWrapper)
	return ok && cw.ifIsExeccute
}

func doShowErrors(ses *Session, execCtx *ExecCtx) error {

	levelCol := new(MysqlColumn)
	levelCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	levelCol.SetName("Level")

	CodeCol := new(MysqlColumn)
	CodeCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	CodeCol.SetName("Code")

	MsgCol := new(MysqlColumn)
	MsgCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	MsgCol.SetName("Message")

	mrs := ses.GetMysqlResultSet()

	mrs.AddColumn(levelCol)
	mrs.AddColumn(CodeCol)
	mrs.AddColumn(MsgCol)

	info := ses.diagnosticsSnapshot()
	showErrorsOnly := false
	if execCtx != nil {
		_, showErrorsOnly = execCtx.stmt.(*tree.ShowErrors)
	}

	for i := info.length() - 1; i >= 0; i-- {
		row := make([]interface{}, 3)
		row[0] = "Error"
		if i < len(info.levels) && info.levels[i] != "" {
			row[0] = info.levels[i]
		}
		if showErrorsOnly && !strings.EqualFold(row[0].(string), "Error") {
			continue
		}
		row[1] = int16(info.codes[i])
		row[2] = info.msgs[i]
		mrs.AddRow(row)
	}
	return trySaveQueryResult(execCtx.reqCtx, ses, mrs)
}

func handleShowErrors(ses FeSession, execCtx *ExecCtx) error {
	err := doShowErrors(ses.(*Session), execCtx)
	if err != nil {
		return err
	}
	return err
}

func isDiagnosticsStatement(stmt tree.Statement) bool {
	switch stmt.(type) {
	case *tree.ShowErrors, *tree.ShowWarnings:
		return true
	default:
		return false
	}
}

func isTopLevelClientStatement(ses *Session, execCtx *ExecCtx, input *UserInput) bool {
	return ses != nil && execCtx != nil && input != nil &&
		!input.isInternal() && !execCtx.inMigration && !ses.IsDerivedStmt()
}

func resetDiagnosticsForStatement(ses *Session, execCtx *ExecCtx, input *UserInput, stmt tree.Statement) {
	if isTopLevelClientStatement(ses, execCtx, input) && !isDiagnosticsStatement(stmt) {
		ses.resetDiagnostics()
	}
}

func doShowVariables(ses *Session, execCtx *ExecCtx, sv *tree.ShowVariables) error {
	if sv.Like != nil && sv.Where != nil {
		return moerr.NewSyntaxError(execCtx.reqCtx, "like clause and where clause cannot exist at the same time")
	}

	var err error
	useGlobal := sv.Global
	if useGlobal {
		bh := ses.GetBackgroundExec(execCtx.reqCtx)
		defer bh.Close()
		if err = ses.refreshGlobalSysVars(execCtx.reqCtx, bh); err != nil {
			return err
		}
	}

	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("Variable_name")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Value")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sv.Like != nil {
		hasLike = true
		if sv.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sv.Like.Right.String())
	}

	rows := make([][]interface{}, 0, len(gSysVarsDefs))
	for name, def := range gSysVarsDefs {
		if hasLike {
			s := name
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}

		var value interface{}
		if useGlobal {
			if value, err = ses.GetGlobalSysVar(name); err != nil {
				continue
			}
		} else {
			if value, err = ses.GetSessionSysVar(name); err != nil {
				continue
			}
		}

		if boolType, ok := def.GetType().(SystemVariableBoolType); ok {
			if boolType.IsTrue(value) {
				value = "on"
			} else {
				value = "off"
			}
		}
		rows = append(rows, []interface{}{name, value})
	}

	if sv.Where != nil {
		bat, _, err := convertRowsIntoBatch(execCtx.proc.Mp(), mrs.Columns, rows)
		defer cleanBatch(execCtx.proc.Mp(), bat)
		if err != nil {
			return err
		}
		binder := plan2.NewDefaultBinder(execCtx.reqCtx, nil, nil, plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"variable_name", "value"})
		planExpr, err := binder.BindExpr(sv.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(execCtx.proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(execCtx.proc, []*batch.Batch{bat}, nil)
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedColWithTypeCheck[bool](vec)
		sels := vector.GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels, false)
		vector.PutSels(sels)

		v0 := vector.GenerateFunctionStrParameter(bat.Vecs[0])
		v1 := vector.GenerateFunctionStrParameter(bat.Vecs[1])
		rows = rows[:bat.Vecs[0].Length()]
		for i := range rows {
			s0, isNull := v0.GetStrValue(uint64(i))
			if isNull {
				rows[i][0] = ""
			} else {
				rows[i][0] = string(s0)
			}
			s1, isNull := v1.GetStrValue(uint64(i))
			if isNull {
				rows[i][1] = ""
			} else {
				rows[i][1] = string(s1)
			}
		}
	}

	//sort by name
	slices.SortFunc(rows, func(a, b []interface{}) int {
		return cmp.Compare(a[0].(string), b[0].(string))
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return trySaveQueryResult(execCtx.reqCtx, ses, mrs)
}

/*
handle show variables
*/
func handleShowVariables(ses FeSession, execCtx *ExecCtx, sv *tree.ShowVariables) error {
	return doShowVariables(ses.(*Session), execCtx, sv)
}

func handleAnalyzeStmt(ses *Session, execCtx *ExecCtx, stmt *tree.AnalyzeStmt) error {
	ses.EnterFPrint(FPHandleAnalyzeStmt)
	defer ses.ExitFPrint(FPHandleAnalyzeStmt)

	// Authorization probes execute as derived SELECT statements.
	prevInsideStmt := ses.ReplaceDerivedStmt(true)
	defer func() {
		ses.ReplaceDerivedStmt(prevInsideStmt)
	}()
	if tcc := ses.GetTxnCompileCtx(); tcc != nil {
		defer tcc.SetExecCtx(execCtx)
		tcc.SetExecCtx(execCtx)
	}

	if len(stmt.Entries) == 0 {
		return moerr.NewInternalError(execCtx.reqCtx, "ANALYZE TABLE requires at least one table")
	}
	return handleAnalyzeStatsStmt(ses, execCtx, stmt)
}

func analyzeStatsPublicationAllowed(execCtx *ExecCtx) bool {
	return execCtx != nil &&
		execCtx.txnOpt.activeTxnAtStartKnown &&
		!execCtx.txnOpt.activeTxnAtStart
}

func analyzeTableOwnsPersistentStats(tableDef *plan.TableDef) bool {
	if tableDef == nil || tableDef.IsTemporary || tableDef.ViewSql != nil {
		return false
	}
	switch tableDef.TableType {
	case "",
		catalog.SystemOrdinaryRel,
		catalog.SystemIndexRel,
		catalog.SystemMaterializedRel,
		catalog.SystemClusterRel,
		catalog.SystemPartitionRel:
		return true
	default:
		return false
	}
}

func executeAnalyzeDerivedQuery(ses *Session, outerExecCtx *ExecCtx, sql string) (*MysqlResultSet, error) {
	liveResponder := ses.GetResponser()
	proto := &analyzeDerivedProtocol{
		internalProtocol: &internalProtocol{
			result:      &internalExecResult{},
			stashResult: true,
		},
		live: liveResponder,
	}
	ses.ReplaceResponser(&analyzeDerivedResponder{
		MysqlResp: NewMysqlResp(proto),
		live:      liveResponder,
	})
	defer ses.ReplaceResponser(liveResponder)

	tempExecCtx := ExecCtx{ses: ses, reqCtx: outerExecCtx.reqCtx}
	defer func() {
		tempExecCtx.Close()
		if tcc := ses.GetTxnCompileCtx(); tcc != nil {
			tcc.SetExecCtx(outerExecCtx)
		}
	}()
	policy := &rewritePolicySnapshot{enabled: outerExecCtx.rewriteEnabled}
	if outerExecCtx.input != nil && outerExecCtx.input.rewritePolicy != nil {
		policy = outerExecCtx.input.rewritePolicy
	}
	derivedInput := &UserInput{
		sql:                       sql,
		rewritePolicy:             policy,
		rewritePolicyMaterialized: true,
	}
	if err := doComQuery(ses, &tempExecCtx, derivedInput); err != nil {
		return nil, err
	}
	return proto.swapOutResult().resultSet, nil
}

// analyzeDerivedProtocol keeps the internal protocol's output sink while exposing
// the live connection properties to code executing the derived statement.
type analyzeDerivedProtocol struct {
	*internalProtocol
	live Responser
}

var _ MysqlRrWr = (*analyzeDerivedProtocol)(nil)

func (p *analyzeDerivedProtocol) GetStr(id PropertyID) string { return p.live.GetStr(id) }
func (p *analyzeDerivedProtocol) GetU32(id PropertyID) uint32 { return p.live.GetU32(id) }
func (p *analyzeDerivedProtocol) GetU8(id PropertyID) uint8   { return p.live.GetU8(id) }
func (p *analyzeDerivedProtocol) GetBool(id PropertyID) bool  { return p.live.GetBool(id) }
func (p *analyzeDerivedProtocol) ConnectionID() uint32 {
	if live, ok := p.live.MysqlRrWr().(interface{ ConnectionID() uint32 }); ok {
		return live.ConnectionID()
	}
	return p.live.GetU32(CONNID)
}
func (p *analyzeDerivedProtocol) Peer() string {
	if live, ok := p.live.MysqlRrWr().(interface{ Peer() string }); ok {
		return live.Peer()
	}
	return p.live.GetStr(PEER)
}
func (p *analyzeDerivedProtocol) GetCapability() uint32 {
	if live, ok := p.live.MysqlRrWr().(interface{ GetCapability() uint32 }); ok {
		return live.GetCapability()
	}
	return p.live.GetU32(CAPABILITY)
}
func (p *analyzeDerivedProtocol) GetSequenceId() uint8 {
	if live, ok := p.live.MysqlRrWr().(interface{ GetSequenceId() uint8 }); ok {
		return live.GetSequenceId()
	}
	return p.live.GetU8(SEQUENCEID)
}
func (p *analyzeDerivedProtocol) IsEstablished() bool {
	if live, ok := p.live.MysqlRrWr().(interface{ IsEstablished() bool }); ok {
		return live.IsEstablished()
	}
	return p.live.GetBool(ESTABLISHED)
}
func (p *analyzeDerivedProtocol) IsTlsEstablished() bool {
	if live, ok := p.live.MysqlRrWr().(interface{ IsTlsEstablished() bool }); ok {
		return live.IsTlsEstablished()
	}
	return p.live.GetBool(TLS_ESTABLISHED)
}

type analyzeDerivedResponder struct {
	*MysqlResp
	live Property
}

var _ Responser = (*analyzeDerivedResponder)(nil)
var _ queryResultFinalizer = (*analyzeDerivedResponder)(nil)

func (r *analyzeDerivedResponder) GetStr(id PropertyID) string { return r.live.GetStr(id) }
func (r *analyzeDerivedResponder) GetU32(id PropertyID) uint32 { return r.live.GetU32(id) }
func (r *analyzeDerivedResponder) GetU8(id PropertyID) uint8   { return r.live.GetU8(id) }
func (r *analyzeDerivedResponder) GetBool(id PropertyID) bool  { return r.live.GetBool(id) }

func resolveAnalyzeDatabase(tcc *TxnCompilerContext, tbl *tree.TableName) string {
	if dbName := string(tbl.Schema()); dbName != "" {
		return dbName
	}
	return tcc.DefaultDatabase()
}

// resolveTableVisibleColumns returns the names of all visible (non-hidden) columns
// of the given table. Used by ANALYZE TABLE without an explicit column list.
func resolveTableVisibleColumns(ses *Session, ctx context.Context, tbl *tree.TableName) (tree.IdentifierList, error) {
	tcc := ses.GetTxnCompileCtx()
	dbName := resolveAnalyzeDatabase(tcc, tbl)
	if dbName == "" {
		return nil, moerr.NewNoDB(ctx)
	}
	tblName := string(tbl.Name())

	snapshot, err := resolveSnapshot(ses, tbl.AtTsExpr)
	if err != nil {
		return nil, err
	}

	_, tableDef, err := tcc.Resolve(dbName, tblName, snapshot)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx, dbName, tblName)
	}

	var cols tree.IdentifierList
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		cols = append(cols, tree.Identifier(col.GetOriginCaseName()))
	}
	if len(cols) == 0 {
		return nil, moerr.NewInternalErrorf(ctx, "ANALYZE TABLE: no visible columns found for table %s", tblName)
	}
	return cols, nil
}

func handleCheckTableStmt(ses FeSession, execCtx *ExecCtx, stmt *tree.CheckTableStmt) error {
	msg := "CHECK TABLE is not supported in MatrixOne"
	switch stmt.Option {
	case tree.CheckTableOptionExtended:
		msg = "CHECK TABLE ... EXTENDED is not supported in MatrixOne"
	case tree.CheckTableOptionForUpgrade:
		msg = "CHECK TABLE ... FOR UPGRADE is not supported in MatrixOne"
	}
	return moerr.NewNotSupported(execCtx.reqCtx, msg)
}

func handleShowProfileStmt(ses FeSession, execCtx *ExecCtx, stmt *tree.ShowProfileStmt) error {
	msg := "SHOW PROFILE is not supported in MatrixOne"
	if stmt.ForQuery > 0 {
		msg = fmt.Sprintf("SHOW PROFILE FOR QUERY %d is not supported in MatrixOne", stmt.ForQuery)
	}
	return moerr.NewNotSupported(execCtx.reqCtx, msg)
}

func doExplainStmt(reqCtx context.Context, ses *Session, stmt *tree.ExplainStmt, statementSQL ...string) error {

	//1. generate the plan
	es, err := getExplainOption(reqCtx, stmt.Options)
	if err != nil {
		return err
	}
	//get query optimizer and execute Optimize
	exPlan, err := buildPlanWithAuthorization(reqCtx, ses, ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if exPlan.GetDcl() != nil && exPlan.GetDcl().GetExecute() != nil {
		//replace the plan of the EXECUTE by the plan generated by the PREPARE
		execPlan := exPlan.GetDcl().GetExecute()
		replaced, _, err := ses.GetTxnCompileCtx().InitExecuteStmtParam(execPlan)
		if err != nil {
			return err
		}

		exPlan = replaced
		paramVals := ses.GetTxnCompileCtx().tcw.ParamVals()
		if len(paramVals) > 0 {
			//replace the param var in the plan by the param value
			exPlan, err = plan2.FillValuesOfParamsInPlan(reqCtx, exPlan, paramVals)
			if err != nil {
				return err
			}
			if exPlan == nil {
				return moerr.NewInternalError(reqCtx, "failed to copy exPlan")
			}
		}
	}
	rawSQL := ""
	if len(statementSQL) > 0 {
		rawSQL = statementSQL[0]
	}
	return writeExplainResult(reqCtx, ses, stmt, exPlan, es, rawSQL, nil)
}

func writeExplainResult(
	reqCtx context.Context,
	ses *Session,
	stmt *tree.ExplainStmt,
	exPlan *plan.Plan,
	es *explain.ExplainOptions,
	rawSQL string,
	sqlMode *string,
) error {
	if exPlan.GetQuery() == nil {
		return moerr.NewNotSupported(reqCtx, "the sql query plan does not support explain.")
	}
	txnHaveDDL := sessionTxnHaveDDL(ses)
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(exPlan.GetQuery())

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	err := explainQuery.ExplainPlan(reqCtx, buffer, es)
	if err != nil {
		return err
	}
	if explainSchedulingEnabled(ses) {
		if rawSQL == "" {
			rawSQL = ses.GetSql()
		}
		// EXPLAIN EXECUTE replaces the outer EXECUTE plan with the prepared
		// query above. Its scheduling intent belongs to that same inner SQL,
		// not to the outer EXPLAIN fragment.
		if execute, ok := stmt.Statement.(*tree.Execute); ok {
			if prepared, getErr := ses.GetPrepareStmt(reqCtx, string(execute.Name)); getErr == nil {
				rawSQL = prepared.Sql
				sqlMode = &prepared.schedulingSQLMode
			}
		}
		schedulingPreview := previewQueryScheduling(
			reqCtx, ses, exPlan.GetQuery(), txnHaveDDL, rawSQL, sqlMode)
		appendSchedulingExplain(buffer, schedulingPreview)
	}
	if err = reqCtx.Err(); err != nil {
		return err
	}

	//2. fill the result set
	//column
	explainColName := plan2.GetPlanTitle(explainQuery.QueryPlan, txnHaveDDL)
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col1.SetName(explainColName)
	setMysqlColumnTypeMetadata(col1, types.New(types.T_varchar, 0, 0))
	setCharacter(col1)

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)

	for _, line := range buffer.Lines {
		mrs.AddRow([]any{line})
	}

	return trySaveQueryResult(reqCtx, ses, mrs)
}

// previewQueryScheduling owns the production best-effort latency policy: the
// preview runs under its own schedulingPreviewTimeout so a slow or blocked
// engine cannot delay the EXPLAIN response. A nil sqlMode means "use the
// session's current mode". Callers that need to observe the scheduling
// decision itself use previewQuerySchedulingInContext below.
func previewQueryScheduling(
	ctx context.Context,
	ses *Session,
	query *plan.Query,
	txnHaveDDL bool,
	rawSQL string,
	sqlMode *string,
) schedule.Trace {
	if ctx == nil {
		ctx = context.Background()
	}
	previewCtx, cancel := context.WithTimeout(ctx, schedulingPreviewTimeout)
	defer cancel()
	return previewQuerySchedulingInContext(previewCtx, ses, query, txnHaveDDL, rawSQL, sqlMode)
}

// previewQuerySchedulingInContext computes a preview under the caller-owned
// context. The frontend wrapper above owns the best-effort latency policy;
// callers that need to observe the scheduling decision itself can provide a
// lifecycle context without racing that decision against an unrelated clock.
func previewQuerySchedulingInContext(
	ctx context.Context,
	ses *Session,
	query *plan.Query,
	txnHaveDDL bool,
	rawSQL string,
	sqlMode *string,
) schedule.Trace {
	if ses == nil {
		return compile.PreviewQueryScheduling(compile.SchedulingPreviewRequest{
			Context: ctx,
			Query:   query,
		})
	}
	tenant := ""
	if info := ses.GetTenantInfo(); info != nil {
		tenant = info.GetTenant()
	}
	intent := querySchedulingIntentForStatement(ses, rawSQL)
	if sqlMode != nil {
		intent = querySchedulingIntentForStatementWithSQLMode(ses, rawSQL, *sqlMode)
	}
	return compile.PreviewQueryScheduling(compile.SchedulingPreviewRequest{
		Context:    ctx,
		Query:      query,
		Engine:     ses.GetTxnHandler().GetStorage(),
		Process:    ses.GetProc(),
		Address:    currentCNPipelineAddress(ses),
		IsInternal: ses.GetIsInternal(),
		Tenant:     tenant,
		Username:   ses.GetUserName(),
		CNLabel:    ses.getCNLabels(),
		Intent:     intent,
		TxnHasDDL:  txnHaveDDL,
	})
}

func appendSchedulingExplain(buffer *explain.ExplainDataBuffer, trace schedule.Trace) {
	if buffer == nil {
		return
	}
	for _, line := range schedule.ExplainLines(trace) {
		buffer.PushNewLine(line, false, 0)
	}
}

func explainSchedulingEnabled(ses *Session) bool {
	if ses == nil {
		return false
	}
	value, err := ses.GetSessionSysVar(enableExplainScheduling)
	if err != nil {
		return false
	}
	boolType, ok := gSysVarsDefs[enableExplainScheduling].Type.(SystemVariableBoolType)
	return ok && boolType.IsTrue(value)
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func handleExplainStmt(ses FeSession, execCtx *ExecCtx, stmt *tree.ExplainStmt) error {
	rawSQL := execCtx.sqlOfStmt
	if carrier, ok := execCtx.cw.(interface{ SchedulingSQL() string }); ok && carrier.SchedulingSQL() != "" {
		rawSQL = carrier.SchedulingSQL()
	}
	if txnCW, ok := execCtx.cw.(*TxnComputationWrapper); ok && txnCW.ifIsExeccute {
		es, err := getExplainOption(execCtx.reqCtx, stmt.Options)
		if err != nil {
			return err
		}
		exPlan, err := preparedExplainPlan(execCtx.reqCtx, txnCW)
		if err != nil {
			return err
		}
		if preparedSQL := txnCW.preparedSchedulingSQL; preparedSQL != "" {
			rawSQL = preparedSQL
		}
		var sqlMode *string
		if txnCW.hasPreparedSchedulingSQLMode {
			sqlMode = &txnCW.preparedSchedulingSQLMode
		}
		return writeExplainResult(execCtx.reqCtx, ses.(*Session), stmt, exPlan, es, rawSQL, sqlMode)
	}
	return doExplainStmt(execCtx.reqCtx, ses.(*Session), stmt, rawSQL)
}

func preparedExplainPlan(ctx context.Context, txnCW *TxnComputationWrapper) (*plan.Plan, error) {
	exPlan := txnCW.Plan()
	if exPlan == nil {
		return nil, moerr.NewInternalError(ctx, "prepared EXPLAIN has no plan")
	}
	if paramVals := txnCW.ParamVals(); len(paramVals) > 0 {
		return plan2.FillValuesOfParamsInPlan(ctx, exPlan, paramVals)
	}
	return exPlan, nil
}

func extractPrepareStmtSQL(ctx context.Context, sql, sqlMode string) (string, error) {
	scanner := mysql.NewScannerWithSQLMode(dialect.MYSQL, sql, mysql.ParseSQLModeFlags(sqlMode))
	defer mysql.PutScanner(scanner)

	if token, _ := scanner.Scan(); token != mysql.PREPARE {
		return "", moerr.NewInvalidInput(ctx, "invalid PREPARE statement")
	}
	if token, _ := scanner.Scan(); token == mysql.EofChar() || token == mysql.LEX_ERROR {
		return "", moerr.NewInvalidInput(ctx, "invalid PREPARE statement name")
	}
	if token, _ := scanner.Scan(); token != mysql.FROM {
		return "", moerr.NewInvalidInput(ctx, "invalid PREPARE statement delimiter")
	}

	preparedStart := scanner.Pos
	preparedSQL := sql[preparedStart:]
	if scanner.CommentFlag {
		scanner.TakeExecutableCommentEnd()
		var commentEnd int
		for commentEnd == 0 {
			previousPos := scanner.Pos
			token, _ := scanner.Scan()
			commentEnd = scanner.TakeExecutableCommentEnd()
			if commentEnd == 0 &&
				(token == mysql.EofChar() || token == mysql.LEX_ERROR || scanner.Pos == previousPos) {
				return "", moerr.NewInvalidInput(ctx, "invalid PREPARE executable comment")
			}
		}
		insideComment := strings.TrimSpace(sql[preparedStart : commentEnd-2])
		afterComment := strings.TrimLeftFunc(sql[commentEnd:], unicode.IsSpace)
		switch {
		case insideComment == "":
			preparedSQL = afterComment
		case afterComment == "":
			preparedSQL = insideComment
		default:
			preparedSQL = insideComment + " " + afterComment
		}
	}

	return strings.TrimLeftFunc(preparedSQL, unicode.IsSpace), nil
}

func doPrepareStmt(execCtx *ExecCtx, ses *Session, st *tree.PrepareStmt, sql string, paramTypes []byte) (*PrepareStmt, error) {
	return doPrepareStmtInSession(execCtx, ses, ses, st, sql, paramTypes)
}

func doPrepareStmtInSession(
	execCtx *ExecCtx,
	owner *Session,
	executionSes FeSession,
	st *tree.PrepareStmt,
	sql string,
	paramTypes []byte,
) (*PrepareStmt, error) {
	originSql, err := extractPrepareStmtSQL(execCtx.reqCtx, sql, sessionSQLModeForParser(owner))
	if err != nil {
		return nil, err
	}
	prepareStmt, err := createPrepareStmtInSession(execCtx, owner, executionSes, originSql, st, st.Stmt)
	if err != nil {
		return nil, err
	}
	if len(paramTypes) > 0 {
		prepareStmt.ParamTypes = paramTypes
	}

	if err = owner.SetPrepareStmt(execCtx.reqCtx, prepareStmt.Name, prepareStmt); err != nil {
		prepareStmt.Close()
		return nil, err
	}
	return prepareStmt, nil
}

// handlePrepareStmt
func handlePrepareStmt(ses FeSession, execCtx *ExecCtx, st *tree.PrepareStmt, sql string) (*PrepareStmt, error) {
	return doPrepareStmt(execCtx, ses.(*Session), st, sql, execCtx.executeParamTypes)
}

func handlePrepareVar(ses *Session, execCtx *ExecCtx, st *tree.PrepareVar) (*PrepareStmt, error) {
	return doPrepareVarInSession(ses, ses, execCtx, st)
}

func prepareSQLFromUserVar(ses FeSession, name string) (string, error) {
	p, err := ses.GetUserDefinedVar(name)
	if err != nil {
		return "", err
	}
	// MySQL converts numeric and NULL user variables to statement text so that
	// the SQL parser reports the invalid statement consistently.
	if p.Value == nil {
		return "NULL", nil
	}
	return fmt.Sprint(p.Value), nil
}

func doPrepareVarInSession(owner *Session, executionSes FeSession, execCtx *ExecCtx, st *tree.PrepareVar) (*PrepareStmt, error) {
	wrapper := &tree.PrepareString{
		Name: st.Name,
		Sql:  st.Var,
	}
	prepareSQL, err := prepareSQLFromUserVar(owner, st.Var)
	if err != nil {
		return nil, err
	}
	wrapper.Sql = prepareSQL

	return doPrepareStringInSession(owner, executionSes, execCtx, wrapper)
}

func doPrepareString(ses *Session, execCtx *ExecCtx, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareStringInSession(ses, ses, execCtx, st)
}

func doPrepareStringInSession(owner *Session, executionSes FeSession, execCtx *ExecCtx, st *tree.PrepareString) (*PrepareStmt, error) {
	rewritten, innerStmt, remapDb, err := prepareStringStatement(execCtx, owner, st.Sql)
	if err != nil {
		return nil, err
	}
	// buildPrepare only understands PrepareStmt and otherwise reparses the
	// original PrepareString text. Pass the already rewritten/remapped AST so
	// authorization and planning see the same policy snapshot that is saved for
	// later EXECUTE and schema-change rebuilds.
	prepareNode := tree.NewPrepareStmt(st.Name, innerStmt)
	defer prepareNode.Free()

	previousRemapDb := execCtx.remapDb
	execCtx.remapDb = remapDb
	prepareStmt, err := createPrepareStmtInSession(execCtx, owner, executionSes, rewritten, prepareNode, innerStmt)
	execCtx.remapDb = previousRemapDb
	if err != nil {
		innerStmt.Free()
		return nil, err
	}

	if err = owner.SetPrepareStmt(execCtx.reqCtx, prepareStmt.Name, prepareStmt); err != nil {
		prepareStmt.Close()
		return nil, err
	}
	return prepareStmt, nil
}

func prepareStringStatement(execCtx *ExecCtx, ses *Session, sql string) (string, tree.Statement, map[string]string, error) {
	rewritten := sql
	var err error
	if execCtx.rewriteEnabled {
		rewritten, err = rewriteSQLFromMaterializedPolicyWithSQLMode(
			execCtx.reqCtx, execCtx.sqlOfStmt, sql, sessionSQLModeForParser(ses), parserLowerCaseTableNames(ses))
		if err != nil {
			return sql, nil, nil, err
		}
	}

	v, err := ses.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		return rewritten, nil, nil, err
	}

	stmts, err := mysql.ParseWithSQLMode(
		execCtx.reqCtx,
		rewritten,
		v.(int64),
		sessionSQLModeForParser(ses),
	)
	if err != nil {
		return rewritten, nil, nil, err
	}
	if len(stmts) != 1 {
		for _, stmt := range stmts {
			stmt.Free()
		}
		return rewritten, nil, nil, moerr.NewInvalidInput(execCtx.reqCtx,
			"prepared statement must contain exactly one statement")
	}

	var remapDb map[string]string
	if execCtx.rewriteEnabled {
		parserSQLMode := sessionSQLModeForParser(ses)
		if err = parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
			execCtx.reqCtx, stmts, rewritten, parserSQLMode, v.(int64)); err != nil {
			stmts[0].Free()
			return rewritten, nil, nil, err
		}
		remaps, err := extractRemapDbByStatementWithSQLMode(execCtx.reqCtx, rewritten, parserSQLMode)
		if err != nil {
			stmts[0].Free()
			return rewritten, nil, nil, err
		}
		if err = applyRemapDbByStatement(execCtx.reqCtx, stmts, remaps, v.(int64)); err != nil {
			stmts[0].Free()
			return rewritten, nil, nil, err
		}
		remapDb = remaps[0]
	}
	return rewritten, stmts[0], remapDb, nil
}

// handlePrepareString
func handlePrepareString(ses FeSession, execCtx *ExecCtx, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ses.(*Session), execCtx, st)
}

func createPrepareStmt(
	execCtx *ExecCtx,
	ses *Session,
	originSQL string,
	stmt tree.Statement,
	saveStmt tree.Statement) (*PrepareStmt, error) {
	return createPrepareStmtInSession(execCtx, ses, ses, originSQL, stmt, saveStmt)
}

func createPrepareStmtInSession(
	execCtx *ExecCtx,
	owner *Session,
	executionSes FeSession,
	originSQL string,
	stmt tree.Statement,
	saveStmt tree.Statement) (*PrepareStmt, error) {
	// A preceding statement may have run nested/background SQL and left the
	// compiler context pointing at a temporary ExecCtx that has already been
	// closed. PREPARE plans synchronously against the current request context.
	if execCtx.proc != nil {
		executionSes.GetTxnCompileCtx().SetExecCtx(execCtx)
	}

	cloneSQL := preparedCloneSQL(saveStmt, executionSes.GetTxnCompileCtx().DefaultDatabase())
	executionProc := owner.proc
	if executionSes.IsBackgroundSession() {
		executionProc = execCtx.proc
	}
	var preparePlan *plan.Plan
	protocolVersion := currentProtocolVersion(executionProc)
	err := execCtx.withRootSQL(originSQL, func() (err error) {
		preparePlan, err = buildPlanWithAuthorization(execCtx.reqCtx, executionSes, executionSes.GetTxnCompileCtx(), stmt)
		return err
	})
	if err != nil {
		return nil, err
	}
	prepareTs := currentTxnSnapshotTSForProcess(executionProc)

	schedulingSQLMode := sessionSQLModeForParser(owner)
	prepareSchedulingIntent := querySchedulingIntentForStatementWithSQLMode(
		owner, originSQL, schedulingSQLMode)
	var comp *compile.Compile
	prepareControl := preparePlan.GetDcl().GetPrepare()
	_, isQueryPlan := prepareControl.Plan.Plan.(*plan.Plan_Query)
	if !executionSes.IsBackgroundSession() &&
		isQueryPlan &&
		shouldCachePrepareCompile(prepareControl.Plan) &&
		(!prepareSchedulingIntent.Explicit ||
			schedule.ValidateSchedulingIntent(prepareSchedulingIntent) != "") {
		//only DQL & DML will pre compile
		comp, err = createCompile(execCtx, executionSes, executionProc, originSQL, originSQL, &schedulingSQLMode, saveStmt, prepareControl.Plan, &prepareTs, false, owner.GetOutputCallback(execCtx), true, nil, nil)
		if err != nil {
			if !moerr.IsMoErrCode(err, moerr.ErrCantCompileForPrepare) {
				return nil, err
			}
		}
		// do not save ap query now()
		if comp != nil && !comp.IsTpQuery() {
			comp.SetIsPrepare(false)
			comp.Release()
			comp = nil
		}
	}

	fixedIntegerParamPositions, hasPaginationParams, hasLagLeadParams :=
		preparedFixedIntegerParamPositions(prepareControl.Plan)
	prepareStmt := &PrepareStmt{
		Name:             preparePlan.GetDcl().GetPrepare().GetName(),
		Sql:              originSQL,
		compile:          comp,
		PreparePlan:      preparePlan,
		PrepareStmt:      saveStmt,
		NativeMode:       owner.sqlModeHasMatrixOneNative(),
		OnlyFullGroupBy:  owner.sqlModeHasOnlyFullGroupBy(),
		BoolSumAvg:       owner.sqlModeHasEnableBoolSumAvg(),
		sqlModeFlagsSet:  true,
		remapDb:          maps.Clone(execCtx.remapDb),
		defaultDatabase:  executionSes.GetTxnCompileCtx().GetDatabase(),
		tempTableVersion: owner.GetTempTableVersion(),
		ddlVersion:       owner.getDDLVersion(),
		cloneSQL:         cloneSQL,
		protocolVersion:  protocolVersion,
		numericOverloadParamPositions: plan2.PreparedPlanNumericFallbackParamPositions(
			prepareControl.Plan),
		directResultParamPositions: plan2.PreparedPlanDirectResultParamPositions(
			prepareControl.Plan),
		directResultParamPositionsSet: true,
		jsonComparisonParamPositions: plan2.PreparedJSONComparisonParamPositions(
			prepareControl.Plan),
		fixedIntegerParamPositions: fixedIntegerParamPositions,
		hasPaginationParams:        hasPaginationParams,
		hasLagLeadParams:           hasLagLeadParams,
		getFromSendLongData:        make(map[int]struct{}),
		schedulingSQLMode:          schedulingSQLMode,
	}
	prepareStmt.refreshNumericPrefixConsumer(
		prepareControl.Plan, len(prepareControl.ParamTypes))
	prepareStmt.directResultParamPositions = plan2.PreparedPlanDirectResultParamPositions(prepareControl.Plan)
	prepareStmt.directResultParamPositionsSet = true

	_, ok := preparePlan.GetDcl().Control.(*plan.DataControl_Prepare)
	if ok {
		columns := getPreparedResultColumns(prepareStmt, sessionTxnHaveDDL(executionSes))
		resper := execCtx.resper
		if executionSes.IsBackgroundSession() {
			resper = owner.GetResponser()
		}
		if prepareStmt.ColDefData, err = resper.MysqlRrWr().MakeColumnDefData(execCtx.reqCtx, columns); err != nil {
			logutil.Errorf("Error make column def data for prepare statement: %v", err)
		}
	}
	if execCtx.input != nil {
		sqlSourceTypes := execCtx.input.getSqlSourceTypes()
		prepareStmt.IsCloudNonuser = slices.Contains(sqlSourceTypes, constant.CloudNoUserSql)
	}
	prepareStmt.Ts = prepareTs
	return prepareStmt, nil
}

func preparedCloneSQL(stmt tree.Statement, defaultDatabase string) string {
	clone, ok := stmt.(*tree.CloneTable)
	if !ok {
		return ""
	}
	executionClone := *clone
	executionClone.SrcTable = clone.SrcTable
	executionClone.CreateTable = clone.CreateTable
	if executionClone.SrcTable.SchemaName == "" {
		executionClone.SrcTable.SchemaName = tree.Identifier(defaultDatabase)
	}
	executionClone.SrcTable.ExplicitSchema = true
	if executionClone.CreateTable.Table.SchemaName == "" && executionClone.ToAccountOpt == nil {
		executionClone.CreateTable.Table.SchemaName = tree.Identifier(defaultDatabase)
	}
	executionClone.CreateTable.Table.ExplicitSchema =
		executionClone.CreateTable.Table.SchemaName != ""
	return tree.StringWithOpts(
		&executionClone,
		dialect.MYSQL,
		tree.WithQuoteIdentifier(),
		tree.WithSingleQuoteString(),
	)
}

func freshPreparedCloneStatement(
	ctx context.Context,
	prepareStmt *PrepareStmt,
) (tree.Statement, bool, error) {
	if prepareStmt == nil {
		return nil, false, moerr.NewInternalError(ctx, "prepared statement is nil")
	}
	if prepareStmt.cloneSQL == "" {
		return prepareStmt.PrepareStmt, false, nil
	}
	stmts, err := mysql.ParseWithSQLMode(ctx, prepareStmt.cloneSQL, 0, prepareStmt.schedulingSQLMode)
	if err != nil {
		return nil, false, err
	}
	if len(stmts) != 1 {
		for _, stmt := range stmts {
			stmt.Free()
		}
		return nil, false, moerr.NewInternalError(ctx, "prepared clone SQL must contain exactly one statement")
	}
	if _, ok := stmts[0].(*tree.CloneTable); !ok {
		stmts[0].Free()
		return nil, false, moerr.NewInternalError(ctx, "prepared clone SQL did not parse as CLONE TABLE")
	}
	return stmts[0], true, nil
}

func doDeallocate(ses *Session, execCtx *ExecCtx, st *tree.Deallocate) error {
	return doDeallocateInSession(ses, ses, execCtx, st)
}

func doDeallocateInSession(owner *Session, executionSes FeSession, execCtx *ExecCtx, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlanWithAuthorization(execCtx.reqCtx, executionSes, executionSes.GetTxnCompileCtx(), st)
	if err != nil {
		return err
	}
	name := deallocatePlan.GetDcl().GetDeallocate().GetName()
	if !owner.RemovePrepareStmt(name) {
		return moerr.NewUnknownStmtHandler(execCtx.reqCtx, name, "DEALLOCATE PREPARE")
	}
	return nil
}

func doReset(ctx context.Context, ses *Session, st *tree.Reset) error {
	prepareStmt, err := ses.GetPrepareStmt(ctx, string(st.Name))
	if err != nil {
		return err
	}
	prepareStmt.resetBinaryParamState()
	return nil
}

// handleDeallocate
func handleDeallocate(ses FeSession, execCtx *ExecCtx, st *tree.Deallocate) error {
	return doDeallocate(ses.(*Session), execCtx, st)
}

// handleReset
func handleReset(ses FeSession, execCtx *ExecCtx, st *tree.Reset) error {
	return doReset(execCtx.reqCtx, ses.(*Session), st)
}

func handleCreatePublication(ses FeSession, execCtx *ExecCtx, cp *tree.CreatePublication) error {
	return doCreatePublication(execCtx.reqCtx, ses.(*Session), cp)
}

func handleCreateSubscription(ses FeSession, execCtx *ExecCtx, cs *tree.CreateSubscription) error {
	return doCreateSubscription(execCtx.reqCtx, ses.(*Session), cs)
}

func handleAlterPublication(ses FeSession, execCtx *ExecCtx, ap *tree.AlterPublication) error {
	return doAlterPublication(execCtx.reqCtx, ses.(*Session), ap)
}

func handleDropPublication(ses FeSession, execCtx *ExecCtx, dp *tree.DropPublication) error {
	return doDropPublication(execCtx.reqCtx, ses.(*Session), dp)
}

func handleDropCcprSubscription(ses FeSession, execCtx *ExecCtx, dcs *tree.DropCcprSubscription) error {
	return doDropCcprSubscription(execCtx.reqCtx, ses.(*Session), dcs)
}

func handleResumeCcprSubscription(ses FeSession, execCtx *ExecCtx, rcs *tree.ResumeCcprSubscription) error {
	return doResumeCcprSubscription(execCtx.reqCtx, ses.(*Session), rcs)
}

func handlePauseCcprSubscription(ses FeSession, execCtx *ExecCtx, pcs *tree.PauseCcprSubscription) error {
	return doPauseCcprSubscription(execCtx.reqCtx, ses.(*Session), pcs)
}

func handleCreateStage(ses FeSession, execCtx *ExecCtx, cs *tree.CreateStage) error {
	return doCreateStage(execCtx.reqCtx, ses.(*Session), cs)
}

func handleAlterStage(ses FeSession, execCtx *ExecCtx, as *tree.AlterStage) error {
	return doAlterStage(execCtx.reqCtx, ses.(*Session), as)
}

func handleDropStage(ses FeSession, execCtx *ExecCtx, ds *tree.DropStage) error {
	return doDropStage(execCtx.reqCtx, ses.(*Session), ds)
}

func handleRemoveStageFiles(ses FeSession, execCtx *ExecCtx, rs *tree.RemoveStageFiles) error {
	return doRemoveStageFiles(execCtx.reqCtx, ses.(*Session), rs)
}

func handleCreateSnapshot(ses *Session, execCtx *ExecCtx, ct *tree.CreateSnapShot) error {
	return doCreateSnapshot(execCtx.reqCtx, ses, ct)
}

func handleDropSnapshot(ses *Session, execCtx *ExecCtx, ct *tree.DropSnapShot) error {
	return doDropSnapshot(execCtx.reqCtx, ses, ct)
}

func handleRestoreSnapshot(ses *Session, execCtx *ExecCtx, rs *tree.RestoreSnapShot) (statistic.StatsArray, error) {
	return doRestoreSnapshot(execCtx.reqCtx, ses, rs)
}

func handleCreatePitr(ses *Session, execCtx *ExecCtx, cp *tree.CreatePitr) error {
	return doCreatePitr(execCtx.reqCtx, ses, cp)
}

func handleDropPitr(ses *Session, execCtx *ExecCtx, dp *tree.DropPitr) error {
	return doDropPitr(execCtx.reqCtx, ses, dp)
}

func handleAlterPitr(ses *Session, execCtx *ExecCtx, ap *tree.AlterPitr) error {
	return doAlterPitr(execCtx.reqCtx, ses, ap)
}

func handleRestorePitr(ses *Session, execCtx *ExecCtx, rp *tree.RestorePitr) (statistic.StatsArray, error) {
	return doRestorePitr(execCtx.reqCtx, ses, rp)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ses FeSession, execCtx *ExecCtx, ca *tree.CreateAccount, proc *process.Process) error {
	//step1 : create new account.
	var err error
	create := &createAccount{
		IfNotExists:  ca.IfNotExists,
		IdentTyp:     ca.AuthOption.IdentifiedType.Typ,
		StatusOption: ca.StatusOption,
		Comment:      ca.Comment,
	}

	b := strParamBinder{
		ctx:    execCtx.reqCtx,
		params: proc.GetPrepareParams(),
	}
	create.Name = b.bind(ca.Name)
	create.AdminName = b.bind(ca.AuthOption.AdminName)
	create.IdentStr = b.bindIdentStr(&ca.AuthOption.IdentifiedType)
	if b.err != nil {
		return b.err
	}

	bh := ses.GetBackgroundExec(execCtx.reqCtx)
	defer bh.Close()

	err = bh.Exec(execCtx.reqCtx, "begin;")
	defer func() {
		err = finishTxn(execCtx.reqCtx, bh, err)
	}()
	if err != nil {
		return err
	}

	return InitGeneralTenant(execCtx.reqCtx, bh, ses.(*Session), create)
}

func handleDropAccount(ses FeSession, execCtx *ExecCtx, da *tree.DropAccount, proc *process.Process) error {
	var err error
	drop := &dropAccount{
		IfExists: da.IfExists,
	}

	b := strParamBinder{
		ctx:    execCtx.reqCtx,
		params: proc.GetPrepareParams(),
	}
	drop.Name = b.bind(da.Name)
	if b.err != nil {
		return b.err
	}

	bh := ses.GetBackgroundExec(
		execCtx.reqCtx,
		&BackgroundExecOption{forcePessimisticRC: true},
	)
	defer bh.Close()

	err = bh.Exec(execCtx.reqCtx, "begin;")
	defer func() {
		err = finishTxn(execCtx.reqCtx, bh, err)
	}()
	if err != nil {
		return err
	}

	return doDropAccount(execCtx.reqCtx, bh, ses.(*Session), drop)
}

// handleDropAccount drops a new user-level tenant
func handleAlterAccount(ses FeSession, execCtx *ExecCtx, st *tree.AlterAccount, proc *process.Process) error {
	aa := &alterAccount{
		IfExists:     st.IfExists,
		StatusOption: st.StatusOption,
		Comment:      st.Comment,
	}

	b := strParamBinder{
		ctx:    execCtx.reqCtx,
		params: proc.GetPrepareParams(),
	}

	aa.Name = b.bind(st.Name)
	if st.AuthOption.Exist {
		aa.AuthExist = true
		aa.AdminName = b.bind(st.AuthOption.AdminName)
		aa.IdentTyp = st.AuthOption.IdentifiedType.Typ
		aa.IdentStr = b.bindIdentStr(&st.AuthOption.IdentifiedType)
	}
	if b.err != nil {
		return b.err
	}

	return doAlterAccount(execCtx.reqCtx, ses.(*Session), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func handleAlterDataBaseConfig(ses FeSession, execCtx *ExecCtx, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(execCtx.reqCtx, ses.(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func handleAlterAccountConfig(ses FeSession, execCtx *ExecCtx, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(execCtx.reqCtx, ses.(*Session), st)
}

// handleCreateUser creates the user for the tenant
func handleCreateUser(ses FeSession, execCtx *ExecCtx, st *tree.CreateUser) error {
	tenant := ses.GetTenantInfo()

	cu := &createUser{
		IfNotExists:        st.IfNotExists,
		Role:               st.Role,
		Users:              make([]*user, 0, len(st.Users)),
		MiscOpt:            st.MiscOpt,
		CommentOrAttribute: st.CommentOrAttribute,
	}

	for _, u := range st.Users {
		v := user{
			Username: u.Username,
			Hostname: u.Hostname,
		}
		if u.AuthOption != nil {
			v.AuthExist = true
			v.IdentTyp = u.AuthOption.Typ
			switch v.IdentTyp {
			case tree.AccountIdentifiedByPassword,
				tree.AccountIdentifiedWithSSL:
				var err error
				v.IdentStr, err = unboxExprStr(execCtx.reqCtx, u.AuthOption.Str)
				if err != nil {
					return err
				}
			}
		}
		cu.Users = append(cu.Users, &v)
	}

	//step1 : create the user
	return InitUser(execCtx.reqCtx, ses.(*Session), tenant, cu)
}

// handleDropUser drops the user for the tenant
func handleDropUser(ses FeSession, execCtx *ExecCtx, du *tree.DropUser) error {
	return doDropUser(execCtx.reqCtx, ses.(*Session), du)
}

func handleAlterUser(ses FeSession, execCtx *ExecCtx, st *tree.AlterUser) error {
	au := &alterUser{
		IfExists: st.IfExists,
		Users:    make([]*user, 0, len(st.Users)),
		Role:     st.Role,
		MiscOpt:  st.MiscOpt,

		CommentOrAttribute: st.CommentOrAttribute,
	}

	for _, su := range st.Users {
		u := &user{
			Username: su.Username,
			Hostname: su.Hostname,
		}
		if su.AuthOption != nil {
			u.AuthExist = true
			u.IdentTyp = su.AuthOption.Typ
			switch u.IdentTyp {
			case tree.AccountIdentifiedByPassword,
				tree.AccountIdentifiedWithSSL:
				var err error
				u.IdentStr, err = unboxExprStr(execCtx.reqCtx, su.AuthOption.Str)
				if err != nil {
					return err
				}
			}
		}
		au.Users = append(au.Users, u)
	}
	return doAlterUser(execCtx.reqCtx, ses.(*Session), au)
}

// handleCreateRole creates the new role
func handleCreateRole(ses FeSession, execCtx *ExecCtx, cr *tree.CreateRole) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(execCtx.reqCtx, ses.(*Session), tenant, cr)
}

// handleDropRole drops the role
func handleDropRole(ses FeSession, execCtx *ExecCtx, dr *tree.DropRole) error {
	return doDropRole(execCtx.reqCtx, ses.(*Session), dr)
}

// handleAlterRole renames the role
func handleAlterRole(ses FeSession, execCtx *ExecCtx, ar *tree.AlterRole) error {
	return doAlterRole(execCtx.reqCtx, ses.(*Session), ar)
}

func handleCreateFunction(ses FeSession, execCtx *ExecCtx, cf *tree.CreateFunction) error {
	tenant := ses.GetTenantInfo()
	return InitFunction(ses.(*Session), execCtx, tenant, cf)
}

func handleDropFunction(ses FeSession, execCtx *ExecCtx, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(execCtx.reqCtx, ses.(*Session), df, func(path string) error {
		return proc.Base.FileService.Delete(execCtx.reqCtx, path)
	})
}
func handleCreateProcedure(ses FeSession, execCtx *ExecCtx, cp *tree.CreateProcedure) error {
	tenant := ses.GetTenantInfo()

	return InitProcedure(execCtx.reqCtx, ses.(*Session), tenant, cp)
}

func handleDropProcedure(ses FeSession, execCtx *ExecCtx, dp *tree.DropProcedure) error {
	return doDropProcedure(execCtx.reqCtx, ses.(*Session), dp)
}

func handleCallProcedure(ses FeSession, execCtx *ExecCtx, call *tree.CallStmt, bg bool) error {
	var affectedRows int64
	results, err := doInterpretCall(
		execCtx.reqCtx,
		ses,
		call,
		bg,
		procedureCallerAffectedRows(execCtx),
		&affectedRows,
	)
	if err != nil {
		return err
	}
	execCtx.runResult = &util.RunResult{AffectRows: normalizeProcedureAffectedRows(affectedRows)}

	ses.SetMysqlResultSet(nil)
	execCtx.results = results
	return nil
}

func procedureCallerAffectedRows(execCtx *ExecCtx) int64 {
	if execCtx.proc == nil {
		return 0
	}
	return execCtx.proc.GetAffectedRows()
}

func normalizeProcedureAffectedRows(affectedRows int64) uint64 {
	if affectedRows < 0 {
		return 0
	}
	return uint64(affectedRows)
}

// handleGrantRole grants the role
func handleGrantRole(ses FeSession, execCtx *ExecCtx, gr *tree.GrantRole) error {
	return doGrantRole(execCtx.reqCtx, ses.(*Session), gr)
}

// handleRevokeRole revokes the role
func handleRevokeRole(ses FeSession, execCtx *ExecCtx, rr *tree.RevokeRole) error {
	return doRevokeRole(execCtx.reqCtx, ses.(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func handleGrantPrivilege(ses FeSession, execCtx *ExecCtx, gp *tree.GrantPrivilege) (err error) {
	ctx := execCtx.reqCtx
	// Object lifecycle locks are part of GRANT's correctness contract. Force the
	// private transaction into the same pessimistic RC protocol as DROP even on
	// optimistic deployments; LockOp intentionally skips optimistic txns.
	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{forcePessimisticRC: true})
	defer bh.Close()

	// put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	return doGrantPrivilege(ctx, ses, gp, bh)
}

// handleRevokePrivilege revokes the privilege from the user or role
func handleRevokePrivilege(ses FeSession, execCtx *ExecCtx, rp *tree.RevokePrivilege) (err error) {
	ctx := execCtx.reqCtx
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// put it into the single transaction
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	return doRevokePrivilege(ctx, ses, rp, bh)
}

// handleSwitchRole switches the role to another role
func handleSwitchRole(ses FeSession, execCtx *ExecCtx, sr *tree.SetRole) error {
	return doSwitchRole(execCtx.reqCtx, ses.(*Session), sr)
}

func doKill(ses *Session, execCtx *ExecCtx, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = getRtMgr(ses.GetService()).kill(execCtx.reqCtx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = getRtMgr(ses.GetService()).kill(execCtx.reqCtx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func handleKill(ses *Session, execCtx *ExecCtx, k *tree.Kill) error {
	err := doKill(ses, execCtx, k)
	if err != nil {
		return err
	}
	return err
}

// handleShowAccounts lists the info of accounts
func handleShowAccounts(ses FeSession, execCtx *ExecCtx, sa *tree.ShowAccounts) error {
	err := doShowAccounts(execCtx.reqCtx, ses.(*Session), sa)
	if err != nil {
		return err
	}
	return err
}

// handleShowRecoveryWindow lists the info of recovery window
func handleShowRecoveryWindow(ses FeSession, execCtx *ExecCtx, srw *tree.ShowRecoveryWindow) error {
	err := doShowRecoveryWindow(execCtx.reqCtx, ses.(*Session), srw)
	if err != nil {
		return err
	}
	return err
}

// handleShowCollation lists the info of collation
func handleShowCollation(ses FeSession, execCtx *ExecCtx, sc *tree.ShowCollation) error {
	err := doShowCollation(ses.(*Session), execCtx, execCtx.proc, sc)
	if err != nil {
		return err
	}
	return err
}

func doShowCollation(ses *Session, execCtx *ExecCtx, proc *process.Process, sc *tree.ShowCollation) error {
	var err error
	var bat *batch.Batch
	// var outputBatches []*batch.Batch

	// Construct the columns.
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col1.SetName("Collation")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col2.SetName("Charset")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	col3.SetName("Id")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col4.SetName("Default")

	col5 := new(MysqlColumn)
	col5.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col5.SetName("Compiled")

	col6 := new(MysqlColumn)
	col6.SetColumnType(defines.MYSQL_TYPE_LONG)
	col6.SetName("Sortlen")

	col7 := new(MysqlColumn)
	col7.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col7.SetName("Pad_attribute")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)
	mrs.AddColumn(col7)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sc.Like != nil {
		hasLike = true
		if sc.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sc.Like.Right.String())
	}

	// Construct the rows.
	rows := make([][]interface{}, 0, len(Collations))
	for _, collation := range Collations {
		if hasLike {
			s := collation.collationName
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}
		row := make([]interface{}, 7)
		row[0] = collation.collationName
		row[1] = collation.charset
		row[2] = collation.id
		row[3] = collation.isDefault
		row[4] = collation.isCompiled
		row[5] = collation.sortLen
		row[6] = collation.padAttribute
		rows = append(rows, row)
	}

	bat, _, err = convertRowsIntoBatch(ses.GetMemPool(), mrs.Columns, rows)
	defer cleanBatch(ses.GetMemPool(), bat)
	if err != nil {
		return err
	}

	if sc.Where != nil {
		binder := plan2.NewDefaultBinder(execCtx.reqCtx, nil, nil, plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"collation", "charset", "id", "default", "compiled", "sortlen", "pad_attribute"})
		planExpr, err := binder.BindExpr(sc.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedColWithTypeCheck[bool](vec)
		sels := vector.GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels, false)
		vector.PutSels(sels)
		v0, area0 := vector.MustVarlenaRawData(bat.Vecs[0])
		v1, area1 := vector.MustVarlenaRawData(bat.Vecs[1])
		v2 := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])
		v3, area3 := vector.MustVarlenaRawData(bat.Vecs[3])
		v4, area4 := vector.MustVarlenaRawData(bat.Vecs[4])
		v5 := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[5])
		v6, area6 := vector.MustVarlenaRawData(bat.Vecs[6])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i].GetString(area0)
			rows[i][1] = v1[i].GetString(area1)
			rows[i][2] = v2[i]
			rows[i][3] = v3[i].GetString(area3)
			rows[i][4] = v4[i].GetString(area4)
			rows[i][5] = v5[i]
			rows[i][6] = v6[i].GetString(area6)
		}
	}

	//sort by name
	slices.SortFunc(rows, func(a, b []interface{}) int {
		return cmp.Compare(a[0].(string), b[0].(string))
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	ses.SetMysqlResultSet(mrs)

	if canSaveQueryResult(execCtx.reqCtx, ses) {
		//already have the batch
		ses.rs, _, _, err = mysqlColDef2PlanResultColDef(mrs.Columns)
		if err != nil {
			return err
		}

		// save query result
		err = saveQueryResult(execCtx.reqCtx, ses,
			func() ([]*batch.Batch, error) {
				return []*batch.Batch{bat}, nil
			},
			nil,
		)
		if err != nil {
			return err
		}
	}

	return err
}

func handleShowPublications(ses FeSession, execCtx *ExecCtx, sp *tree.ShowPublications) error {
	return doShowPublications(execCtx.reqCtx, ses.(*Session), sp)
}

func handleShowSubscriptions(ses FeSession, execCtx *ExecCtx, ss *tree.ShowSubscriptions) error {
	return doShowSubscriptions(execCtx.reqCtx, ses.(*Session), ss)
}

func handleShowPublicationCoverage(ses FeSession, execCtx *ExecCtx, spc *tree.ShowPublicationCoverage) error {
	return doShowPublicationCoverage(execCtx.reqCtx, ses.(*Session), spc)
}

func handleShowCcprSubscriptions(ses FeSession, execCtx *ExecCtx, scs *tree.ShowCcprSubscriptions) error {
	return doShowCcprSubscriptions(execCtx.reqCtx, ses.(*Session), scs)
}

func doShowBackendServers(ses *Session, execCtx *ExecCtx) error {
	// Construct the columns.
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("UUID")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Address")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3.SetName("Work State")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetName("Labels")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)

	var filterLabels = func(labels map[string]string) map[string]string {
		var reservedLabels = map[string]struct{}{
			"os_user":      {},
			"os_sudouser":  {},
			"program_name": {},
		}
		for k := range labels {
			if _, ok := reservedLabels[k]; ok || strings.HasPrefix(k, "_") {
				delete(labels, k)
			}
		}
		return labels
	}

	var appendFn = func(s *metadata.CNService) {
		row := make([]interface{}, 4)
		row[0] = s.ServiceID
		row[1] = s.SQLAddress
		row[2] = s.WorkState.String()
		var labelStr string
		for key, value := range s.Labels {
			labelStr += fmt.Sprintf("%s:%s;", key, strings.Join(value.Labels, ","))
		}
		row[3] = labelStr
		mrs.AddRow(row)
	}

	tenant := ses.GetTenantInfo().GetTenant()
	var se clusterservice.Selector
	labels, err := ParseLabel(getLabelPart(ses.GetUserName()))
	if err != nil {
		return err
	}
	labels["account"] = tenant
	se = clusterservice.NewSelector().SelectByLabel(
		filterLabels(labels), clusterservice.Contain)
	moc := clusterservice.GetMOCluster(ses.GetService())
	moc.ForceRefresh(true)
	if isSysTenant(tenant) {
		u := ses.GetTenantInfo().GetUser()
		// For super use dump and root, we should list all servers.
		if isSuperUser(u) {
			moc.GetCNService(
				clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
					appendFn(&s)
					return true
				})
		} else {
			route.RouteForSuperTenant(
				ses.GetService(),
				se,
				u,
				nil,
				appendFn,
			)
		}
	} else {
		route.RouteForCommonTenant(ses.GetService(), se, nil, appendFn)
	}

	return trySaveQueryResult(execCtx.reqCtx, ses, mrs)
}

func handleShowBackendServers(ses FeSession, execCtx *ExecCtx) error {
	var err error
	if err := doShowBackendServers(ses.(*Session), execCtx); err != nil {
		return err
	}
	return err
}

func handleEmptyStmt(ses FeSession, execCtx *ExecCtx, stmt *tree.EmptyStmt) error {
	var err error
	return err
}

func getExplainOption(reqCtx context.Context, options []tree.OptionElem) (*explain.ExplainOptions, error) {
	es := explain.NewExplainDefaultOptions()
	if options == nil {
		return es, nil
	} else {
		for _, v := range options {
			if strings.EqualFold(v.Name, tree.VerboseOption) {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, tree.AnalyzeOption) {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, tree.FormatOption) {
				if strings.EqualFold(v.Value, "TEXT") {
					es.Format = explain.EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					return nil, moerr.NewNotSupportedf(reqCtx, "Unsupport explain format '%s'", v.Value)
				} else if strings.EqualFold(v.Value, "DOT") {
					return nil, moerr.NewNotSupportedf(reqCtx, "Unsupport explain format '%s'", v.Value)
				} else {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, tree.CheckOption) {
				if err := json.Unmarshal([]byte(v.Value), &es.CheckExpr); err != nil {
					return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s': %s", v.Name, v.Value, err.Error())
				}
			} else {
				return nil, moerr.NewInvalidInputf(reqCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
			}
		}
		return es, nil
	}
}

func buildMoExplainQuery(execCtx *ExecCtx, explainColName string, buffer *explain.ExplainDataBuffer, session *Session, fill outputCallBackFunc) error {
	bat := batch.New([]string{explainColName})
	rs := buffer.Lines
	vs := make([][]byte, len(rs))

	count := 0
	for _, r := range rs {
		str := []byte(r)
		vs[count] = str
		count++
	}
	vs = vs[:count]
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(session.GetMemPool())
	vector.AppendBytesList(vec, vs, nil, session.GetMemPool())
	bat.Vecs[0] = vec
	bat.SetRowCount(count)

	err := fill(session, execCtx, bat, nil)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, execCtx, nil, nil)
	return err
}

func buildMoExplainPhyPlan(
	execCtx *ExecCtx,
	explainColName string,
	reader *bufio.Reader,
	session *Session,
	fill outputCallBackFunc,
	trace schedule.Trace,
) error {
	bat := batch.New([]string{explainColName})
	vs := make([][]byte, 0)
	count := 0
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF && len(line) > 0 {
			vs = append(vs, []byte(strings.TrimSuffix(line, "\n")))
			count++
			break
		}
		if err != nil {
			return moerr.NewInvalidInputf(execCtx.reqCtx, "Error when read explain phyplan buffer: %s", err.Error())
		}

		vs = append(vs, []byte(strings.TrimSuffix(line, "\n")))
		count++
	}
	for _, line := range schedule.ExplainLines(trace) {
		vs = append(vs, []byte(line))
		count++
	}

	vs = vs[:count]
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(session.GetMemPool())
	vector.AppendBytesList(vec, vs, nil, session.GetMemPool())
	bat.Vecs[0] = vec
	bat.SetRowCount(count)

	err := fill(session, execCtx, bat, nil)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, execCtx, nil, nil)
	return err
}

func buildPlan(
	reqCtx context.Context,
	ses FeSession,
	ctx plan2.CompilerContext,
	stmt tree.Statement,
) (*plan2.Plan, error) {
	return buildPlanWithPrepareMode(reqCtx, ses, ctx, stmt, false)
}

func buildPlanWithPrepareMode(
	reqCtx context.Context,
	ses FeSession,
	ctx plan2.CompilerContext,
	stmt tree.Statement,
	forcePrepare bool,
) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error

	// A later statement in a multi-statement packet can reuse a compiler
	// context whose process has already been released.  Planning does not
	// require a transaction operator, so keep the tracing setup optional
	// instead of dereferencing the missing process.
	var txnOp client.TxnOperator
	if proc := ctx.GetProcess(); proc != nil {
		txnOp = proc.GetTxnOperator()
	}
	start := time.Now()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
		txnTrace.GetService(ses.GetService()).AddTxnDurationAction(
			txnOp,
			client.BuildPlanEvent,
			seq,
			0,
			0,
			err)
	}

	defer func() {
		cost := time.Since(start)
		if txnOp != nil {
			txnTrace.GetService(ses.GetService()).AddTxnDurationAction(
				txnOp,
				client.BuildPlanEvent,
				seq,
				0,
				cost,
				err)
		}
		v2.TxnStatementBuildPlanDurationHistogram.Observe(cost.Seconds())
	}()

	// NOTE: The context used by buildPlan comes from the CompilerContext object.
	// A nested expression evaluation can temporarily replace that context with
	// an ExecCtx which is closed before a later statement in the same packet is
	// planned.  Keep planning and the tracing helpers on the current request
	// context instead of passing nil to context.WithValue.
	planContext := ctx.GetContext()
	if planContext == nil {
		planContext = reqCtx
	}
	if planContext == nil {
		planContext = context.Background()
	}
	stats := statistic.StatsInfoFromContext(planContext)
	stats.PlanStart()

	crs := new(perfcounter.CounterSet)
	planContext = perfcounter.AttachBuildPlanMarkKey(planContext, crs)
	ctx.SetContext(planContext)
	defer func() {
		stats.AddBuildPlanS3Request(statistic.S3Request{
			List:      crs.FileService.S3.List.Load(),
			Head:      crs.FileService.S3.Head.Load(),
			Put:       crs.FileService.S3.Put.Load(),
			Get:       crs.FileService.S3.Get.Load(),
			Delete:    crs.FileService.S3.Delete.Load(),
			DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
		})
		stats.PlanEnd()
	}()

	isPrepareStmt := forcePrepare
	if ses != nil {
		accId, err := defines.GetAccountId(reqCtx)
		if err != nil {
			return nil, err
		}
		ses.SetAccountId(accId)

		if len(ses.GetSql()) > 8 {
			prefix := strings.ToLower(ses.GetSql()[:8])
			isPrepareStmt = isPrepareStmt || prefix == "execute " || prefix == "prepare "
		}
	}
	// Handle specific statement types
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
			if err != nil {
				return nil, err
			}
		}
	}

	if ret != nil {
		ret.IsPrepare = isPrepareStmt
		if forcePrepare {
			err = plan2.NormalizePrepareParamRefs(reqCtx, ret)
		}
		return ret, err
	}

	// Default handling of various statements
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		*tree.Update, *tree.Delete, *tree.Insert, *tree.MultiInsert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns, *tree.ShowColumnNumber,
		*tree.ShowTableNumber, *tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze, *tree.ExplainPhyPlan:
		opt := plan2.NewBaseOptimizer(ctx)
		optimized, err := opt.Optimize(stmt, isPrepareStmt)
		if err != nil {
			return nil, err
		}

		ret = &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}
	default:
		ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
	}

	if ret != nil {
		ret.IsPrepare = isPrepareStmt
		if forcePrepare && err == nil {
			err = plan2.NormalizePrepareParamRefs(reqCtx, ret)
		}
	}
	return ret, err
}

// buildPlanWithAuthorization wraps the buildPlan function to perform permission checks
// after the plan has been successfully built.
var buildPlanWithAuthorization = func(reqCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	planContext := ctx.GetContext()
	stats := statistic.StatsInfoFromContext(planContext)

	// Step 1: Call buildPlan to construct the execution plan
	plan, err := buildPlan(reqCtx, ses, ctx, stmt)
	if err != nil {
		return nil, err
	}

	// Step 2: Perform permission check after the plan is built
	if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
		authStats, err := authenticateCanExecuteStatementAndPlan(reqCtx, ses.(*Session), stmt, plan)
		if err != nil {
			return nil, err
		}
		// record permission statistics.
		stats.PermissionAuth.Add(&authStats)
	}
	return plan, nil
}

func checkModify(plan0 *plan.Plan, resolveFn func(string, string, *plan2.Snapshot) (*plan2.ObjectRef, *plan2.TableDef, error)) (bool, error) {
	if plan0 == nil {
		return true, nil
	}

	checkCatalogObject := func(
		ref *plan.ObjectRef,
		name string,
		snapshot *plan2.Snapshot,
		version int64,
		tableID int64,
	) (bool, error) {
		if ref == nil {
			return true, nil
		}
		_, tableDef, err := resolveFn(plan2.DbNameOfObjRef(ref), name, snapshot)
		if err != nil {
			return true, err
		}
		if tableDef == nil {
			return true, nil
		}
		if int64(tableDef.Version) != version || int64(tableDef.TblId) != tableID {
			return true, nil
		}
		return false, nil
	}
	checkFn := func(ref *plan.ObjectRef, def *plan.TableDef) (bool, error) {
		if ref == nil || def == nil {
			return true, nil
		}
		return checkCatalogObject(ref, def.Name, nil, int64(def.Version), int64(def.TblId))
	}
	switch p := plan0.Plan.(type) {
	case *plan.Plan_Query:
		for i := range p.Query.Nodes {
			if def := p.Query.Nodes[i].TableDef; def != nil {
				flag, err := checkFn(p.Query.Nodes[i].ObjRef, def)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].InsertCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].DeleteCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				flag, err := checkFn(ctx.Ref, ctx.TableDef)
				if err != nil || flag {
					return true, err
				}
			}
		}
		for _, dependency := range p.Query.GetCatalogDependencies() {
			flag, err := checkCatalogObject(
				dependency,
				dependency.GetObjName(),
				dependency.GetSnapshot(),
				dependency.GetServer(),
				dependency.GetObj(),
			)
			if err != nil || flag {
				return true, err
			}
		}
	default:
	}
	return false, nil
}

func cachedPlanForInput(ses *Session, input *UserInput) *cachedPlan {
	if !input.canUsePlanCache() {
		return nil
	}
	if !reusablePlanGenerationSupported(ses.proc) {
		// Evict eagerly while the rollout gate is closed. Besides releasing the
		// owned AST, this prevents an entry from surviving an observed protocol
		// rollback and becoming eligible again after a later upgrade.
		ses.removeCachedPlan(input.getHash())
		return nil
	}
	cached := ses.getCachedPlan(input.getHash())
	// SELECT ... INTO @var changes the type of a session variable as part of
	// execution.  A cached SELECT-INTO plan can therefore never be reused: it
	// may have been bound against the variable's pre-assignment type, and the
	// assignment also invalidates the session cache.  Drop any stale entry so
	// it cannot be selected again after this request.
	if cached != nil && containsSelectInto(cached.stmts) {
		ses.removeCachedPlan(input.getHash())
		return nil
	}
	return cached
}

func containsSelectInto(stmts []tree.Statement) bool {
	for _, stmt := range stmts {
		if selectStmt, ok := stmt.(*tree.Select); ok && len(selectStmt.IntoVars) > 0 {
			return true
		}
	}
	return false
}

var GetComputationWrapper = func(execCtx *ExecCtx, db string, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
	// COM_QUERY carries the switch captured before its first statement. Other
	// protocols retain their existing session-level behavior.
	if execCtx.input.rewritePolicy != nil {
		execCtx.rewriteEnabled = execCtx.input.rewritePolicy.enabled
	} else {
		execCtx.rewriteEnabled = ses.rewriteEnabled.Load()
	}
	parserSQLMode := sessionSQLModeForParser(ses)
	// Reset the per-statement database remap; it is (re)populated below only when
	// the rewrite feature is enabled and a remapdb is configured.
	execCtx.remapDb = nil
	var cws []ComputationWrapper = nil
	var statementRemaps []map[string]string
	if preparePlan := execCtx.input.getPreparePlan(); preparePlan != nil {
		tcw := InitTxnComputationWrapper(ses, execCtx.input.stmt, proc)
		tcw.plan = preparePlan.GetDcl().GetPrepare().Plan
		tcw.binaryPrepare = execCtx.input.isBinaryProtExecute
		tcw.prepareName = execCtx.input.stmtName
		if tcw.binaryPrepare {
			// COM_STMT_EXECUTE borrows the AST retained by PrepareStmt. Mark it
			// before Compile so every early error path keeps the shared AST alive.
			tcw.stmtBorrowed = true
		}
		tcw.SetRemapDb(execCtx.input.remapDb)
		cws = append(cws, tcw)
		return cws, nil
	} else if cached := cachedPlanForInput(ses, execCtx.input); cached != nil {
		var remapErr error
		statementSchedulingSQL, schedulingErr := schedulingSQLByStatementWithSQLMode(
			execCtx.reqCtx, execCtx.input.getSql(), parserSQLMode)
		if schedulingErr != nil {
			return nil, schedulingErr
		}
		if len(statementSchedulingSQL) != len(cached.stmts) {
			return nil, moerr.NewInternalError(execCtx.reqCtx, "the count of scheduling policies is not equal to cached statements")
		}
		statementRemaps, remapErr = extractRemapDbByStatementWithSQLMode(execCtx.reqCtx, execCtx.input.getSql(), parserSQLMode)
		if remapErr != nil {
			return nil, remapErr
		}
		if len(statementRemaps) != len(cached.stmts) {
			return nil, moerr.NewInternalError(execCtx.reqCtx, "the count of remapdb policies is not equal to cached statements")
		}
		for i, stmt := range cached.stmts {
			tcw := InitTxnComputationWrapper(ses, stmt, proc)
			// The cache owns its ASTs until eviction. Wrappers only borrow them;
			// otherwise normal cleanup or stale-plan reset can return the same AST
			// to the parser pool while the cache still owns or already freed it.
			tcw.stmtBorrowed = true
			tcw.plan = cached.plans[i]
			tcw.cachedPlanSQL = execCtx.input.getHash()
			tcw.cachedPlanIndex = i
			tcw.cachedPlanGeneration = cached.plans[i]
			tcw.setPlanSnapshotTS(cached.planSnapshotTS[i])
			tcw.planGenerationReused = true
			tcw.protocolVersion = cached.protocolVersion
			tcw.SetRemapDb(statementRemaps[i])
			tcw.SetSchedulingSQL(statementSchedulingSQL[i])
			cws = append(cws, tcw)
		}

		return cws, nil
	}

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var cmdGetSnapshotTsStmt *InternalCmdGetSnapshotTs
	var cmdGetDatabasesStmt *InternalCmdGetDatabases
	var cmdGetMoIndexesStmt *InternalCmdGetMoIndexes
	var cmdGetDdlStmt *InternalCmdGetDdl
	var cmdGetObjectStmt *InternalCmdGetObject
	var cmdObjectListStmt *InternalCmdObjectList
	var err error
	// if the input is an option ast, we should use it directly
	if execCtx.input.getStmt() != nil {
		stmts = append(stmts, execCtx.input.getStmt())
	} else if isCmdFieldListSql(execCtx.input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else if isCmdGetSnapshotTsSql(execCtx.input.getSql()) {
		cmdGetSnapshotTsStmt, err = parseCmdGetSnapshotTs(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdGetSnapshotTsStmt)
	} else if isCmdGetDatabasesSql(execCtx.input.getSql()) {
		cmdGetDatabasesStmt, err = parseCmdGetDatabases(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdGetDatabasesStmt)
	} else if isCmdGetMoIndexesSql(execCtx.input.getSql()) {
		cmdGetMoIndexesStmt, err = parseCmdGetMoIndexes(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdGetMoIndexesStmt)
	} else if isCmdGetDdlSql(execCtx.input.getSql()) {
		cmdGetDdlStmt, err = parseCmdGetDdl(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdGetDdlStmt)
	} else if isCmdGetObjectSql(execCtx.input.getSql()) {
		cmdGetObjectStmt, err = parseCmdGetObject(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdGetObjectStmt)
	} else if isCmdObjectListSql(execCtx.input.getSql()) {
		cmdObjectListStmt, err = parseCmdObjectList(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdObjectListStmt)
	} else if isCmdCheckSnapshotFlushedSql(execCtx.input.getSql()) {
		cmdCheckSnapshotFlushedStmt, err := parseCmdCheckSnapshotFlushed(execCtx.reqCtx, execCtx.input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdCheckSnapshotFlushedStmt)
	} else {
		stmts, err = parseSql(execCtx, ses.GetMySQLParser())
		if err != nil {
			return nil, err
		}
		if execCtx.rewriteEnabled {
			err = parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
				execCtx.reqCtx, stmts, execCtx.input.getSql(), parserSQLMode, parserLowerCaseTableNames(ses))
			if err != nil {
				return nil, err
			}
			// Apply remapdb (database-name substitution) on the parsed AST
			// before privilege checks and planning resolve the original
			// database. The effective remapdb (role rules carry none, the
			// session variable and any inline hint are merged by rewriteSQL
			// into the leading hint) is read back from the statement text and
			// applied to qualified references in SELECT and INSERT/UPDATE/
			// DELETE alike. Only the remapdb field is decoded here, so layered
			// (array-form) rewrites in the merged hint do not interfere. Each map
			// travels with its computation wrapper and is installed on execCtx
			// before authorization/planning, so DefaultDatabase can remap the
			// current database for UNQUALIFIED references (USE is not remapped).
			statementRemaps, err = extractRemapDbByStatementWithSQLMode(execCtx.reqCtx, execCtx.input.getSql(), parserSQLMode)
			if err != nil {
				return nil, err
			}
			// Protocol callers may explicitly restore a remap captured with an
			// already prepared statement whose current SQL text has no hint.
			if len(execCtx.input.remapDb) > 0 && len(statementRemaps) == 1 {
				statementRemaps[0] = execCtx.input.remapDb
			}
			// Text EXECUTE similarly contains no original hint. Restore the policy
			// saved with the prepared statement before authorization/planning.
			for i, stmt := range stmts {
				if execute, ok := stmt.(*tree.Execute); ok {
					if prepared, getErr := ses.GetPrepareStmt(execCtx.reqCtx, string(execute.Name)); getErr == nil {
						statementRemaps[i] = prepared.remapDb
					}
				}
			}
			if err = applyRemapDbByStatement(
				execCtx.reqCtx, stmts, statementRemaps, parserLowerCaseTableNames(ses),
			); err != nil {
				return nil, err
			}
		}
	}

	var statementSchedulingSQL []string
	if execCtx.input.getStmt() != nil {
		statementSchedulingSQL = []string{execCtx.input.getSql()}
	} else {
		statementSchedulingSQL, err = schedulingSQLByStatementWithSQLMode(
			execCtx.reqCtx, execCtx.input.getSql(), parserSQLMode)
		if err != nil {
			return nil, err
		}
	}
	if len(statementSchedulingSQL) != len(stmts) {
		return nil, moerr.NewInternalError(execCtx.reqCtx, "the count of scheduling policies is not equal to statements")
	}
	for i, stmt := range stmts {
		tcw := InitTxnComputationWrapper(ses, stmt, proc)
		tcw.SetSchedulingSQL(statementSchedulingSQL[i])
		if len(statementRemaps) == len(stmts) {
			tcw.SetRemapDb(statementRemaps[i])
		}
		cws = append(cws, tcw)
	}
	return cws, nil
}

func parseSql(execCtx *ExecCtx, p *mysql.MySQLParser) (stmts []tree.Statement, err error) {
	return p.ParseWithSQLMode(
		execCtx.reqCtx,
		execCtx.input.getSql(),
		parserLowerCaseTableNames(execCtx.ses),
		sessionSQLModeForParser(execCtx.ses),
	)
}

func sessionSQLMode(ses FeSession) string {
	v, err := ses.GetSessionSysVar("sql_mode")
	if err != nil {
		return ""
	}
	mode, ok := v.(string)
	if !ok {
		return ""
	}
	return mode
}

func sessionSQLModeForParser(ses FeSession) string {
	mode := sessionSQLMode(ses)
	return mysql.SessionSQLModeForParser(mode)
}

func refreshStatementScopedSessionInfo(ses FeSession, proc *process.Process) {
	refreshStatementScopedSessionInfoWithSQLMode(sessionSQLMode(ses), proc)
}

func refreshStatementScopedSessionInfoWithSQLMode(sqlMode string, proc *process.Process) {
	refreshStatementScopedSessionInfoWithNativeMode(mysql.HasMatrixOneNativeSQLMode(sqlMode), proc)
}

func refreshStatementScopedSessionInfoWithNativeMode(nativeMode bool, proc *process.Process) {
	if proc == nil || proc.Base == nil {
		return
	}
	proc.Base.SessionInfo.MatrixOneNativeMode = nativeMode
}

func parserLowerCaseTableNames(ses FeSession) int64 {
	v, err := ses.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		return 1
	}
	lctn, ok := v.(int64)
	if !ok {
		return 1
	}
	return lctn
}

func isSessionSQLModeSet(stmt tree.Statement) bool {
	setVar, ok := stmt.(*tree.SetVar)
	if !ok {
		return false
	}
	for _, assignment := range setVar.Assignments {
		if assignment != nil && assignment.System && !assignment.Global && strings.EqualFold(assignment.Name, "sql_mode") {
			return true
		}
	}
	return false
}

func hasStatement(fragment string, parserSQLMode string) bool {
	scanner := mysql.NewScannerWithSQLMode(dialect.MYSQL, fragment, mysql.ParseSQLModeFlags(parserSQLMode))
	defer mysql.PutScanner(scanner)
	for {
		token, _ := scanner.Scan()
		switch token {
		case 0, mysql.EofChar():
			return false
		case int(';'):
			continue
		default:
			return true
		}
	}
}

func mayNeedSQLModeStaging(sql string) bool {
	return strings.Contains(sql, ";") && strings.Contains(strings.ToLower(sql), "sql_mode")
}

func prepareSQLModeStagedExecution(
	ctx context.Context,
	ses FeSession,
	p *mysql.MySQLParser,
	sql string,
) (first string, remaining string, staged bool, err error) {
	lctn := parserLowerCaseTableNames(ses)
	parserSQLMode := sessionSQLModeForParser(ses)
	stmts, parseErr := p.ParseWithSQLMode(ctx, sql, lctn, parserSQLMode)
	if parseErr == nil {
		stageAt := -1
		for i, stmt := range stmts {
			if isSessionSQLModeSet(stmt) {
				stageAt = i
				break
			}
		}
		shouldStage := stageAt >= 0 && stageAt < len(stmts)-1
		freeStatements(stmts)
		if !shouldStage {
			return "", "", false, nil
		}
	} else {
		probe := sql
		foundSQLModeSet := false
		for hasStatement(probe, parserSQLMode) {
			stmt, end, firstErr := p.ParseFirstWithSQLMode(ctx, probe, lctn, parserSQLMode)
			if firstErr != nil {
				return "", "", false, parseErr
			}
			foundSQLModeSet = isSessionSQLModeSet(stmt)
			stmt.Free()
			probe = probe[end:]
			if foundSQLModeSet {
				break
			}
		}
		if !foundSQLModeSet || !hasStatement(probe, parserSQLMode) {
			return "", "", false, parseErr
		}
	}

	stmt, end, err := p.ParseFirstWithSQLMode(ctx, sql, lctn, parserSQLMode)
	if err != nil {
		return "", "", false, err
	}
	stmt.Free()
	return sql[:end], sql[end:], true, nil
}

func nextSQLModeStatementInput(
	ctx context.Context,
	ses FeSession,
	p *mysql.MySQLParser,
	input *UserInput,
	sql string,
) (*UserInput, string, error) {
	stmt, end, err := p.ParseFirstWithSQLMode(
		ctx,
		sql,
		parserLowerCaseTableNames(ses),
		sessionSQLModeForParser(ses),
	)
	if err != nil {
		return nil, sql, err
	}
	stmt.Free()
	return newSQLStatementInput(input, ses, sql[:end]), sql[end:], nil
}

func newSQLStatementInput(input *UserInput, ses FeSession, sql string) *UserInput {
	statementInput := *input
	statementInput.sql = sql
	statementInput.hashedSql = ""
	statementInput.sqlSourceType = nil
	statementInput.genHash()
	statementInput.genSqlSourceType(ses)
	return &statementInput
}

func rewriteSQLStatementInput(ctx context.Context, ses *Session, input *UserInput) (*UserInput, error) {
	if input.rewritePolicy == nil || input.rewritePolicyMaterialized {
		return input, nil
	}
	rewritten, err := input.rewritePolicy.rewrite(ctx, input.getSql(), sessionSQLModeForParser(ses))
	if err != nil {
		return input, err
	}
	if rewritten == input.getSql() {
		return input, nil
	}
	return newSQLStatementInput(input, ses, rewritten), nil
}

func sqlForRecord(sql string) string {
	parts := parsers.HandleSqlForRecord(sql)
	if len(parts) == 0 {
		return strings.TrimSpace(sql)
	}
	return strings.Join(parts, "; ")
}

func freeStatements(stmts []tree.Statement) {
	for _, stmt := range stmts {
		if stmt != nil {
			stmt.Free()
		}
	}
}

func installStatementRemap(execCtx *ExecCtx, cw ComputationWrapper) {
	execCtx.remapDb = nil
	if carrier, ok := cw.(interface{ GetRemapDb() map[string]string }); ok {
		execCtx.remapDb = carrier.GetRemapDb()
	}
}

func incTransactionCounter(tenant string, tenantId uint32) {
	metric.TransactionCounter(tenant, tenantId).Inc()
}

func incTransactionErrorsCounter(tenant string, tenantId uint32, t metric.SQLType) {
	if t == metric.SQLTypeRollback {
		return
	}
	metric.TransactionErrorsCounter(tenant, tenantId, t).Inc()
}

func incStatementErrorsCounter(tenant string, tenantId uint32, stmt tree.Statement) {
	metric.StatementErrorsCounter(tenant, tenantId, getStatementType(stmt).GetQueryType()).Inc()
}

// authenticateUserCanExecuteStatement checks the user can execute the statement
func authenticateUserCanExecuteStatement(reqCtx context.Context, ses *Session, stmt tree.Statement) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	reqCtx, span := trace.Debug(reqCtx, "authenticateUserCanExecuteStatement")
	defer span.End()
	if getPu(ses.GetService()).SV.SkipCheckPrivilege {
		return stats, nil
	}

	if ses.skipAuthForSpecialUser() {
		return stats, nil
	}
	if ses.GetTenantInfo() != nil {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))
		if !canCreateMongoDBTableMapping(stmt, ses.GetTenantInfo()) {
			// The privilege model has no external-connection USAGE object yet.
			// Fail closed instead of letting any ordinary CREATE TABLE holder use
			// an administrator's connection against an arbitrary collection.
			return stats, moerr.NewInternalError(reqCtx, "MongoDB external table creation requires account admin until connection USAGE privileges are available")
		}

		// can or not execute in retricted status
		if ses.getRoutine() != nil && ses.getRoutine().isRestricted() && !ses.GetPrivilege().canExecInRestricted {
			return stats, moerr.NewInternalError(reqCtx, "do not have enough storage to execute the statement")
		}

		// can or not execute in password expired status
		if ses.getRoutine() != nil && ses.getRoutine().isExpired() && !ses.GetPrivilege().canExecInPasswordExpired {
			return stats, moerr.NewInternalError(reqCtx, "password has expired, please change the password")
		}

		havePrivilege, delta, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(reqCtx, ses, stmt)
		if err != nil {
			return stats, err
		}
		stats.Add(&delta)

		if !havePrivilege {
			err = moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
			return stats, err
		}

		havePrivilege, delta, err = authenticateUserCanExecuteStatementWithObjectTypeNone(reqCtx, ses, stmt)
		if err != nil {
			return stats, err
		}
		stats.Add(&delta)

		if !havePrivilege {
			err = moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
			return stats, err
		}

		//!!!note: clone table executed in the frontend.
		//handle privilege check here for it
		priv := ses.GetPrivilege()
		if priv.objectType() == objectTypeTable {
			if !checkProtectedDatabaseWriteByPrivilege(reqCtx, ses, priv) {
				return stats, moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
			}
		}
	}
	return stats, nil
}

func canCreateMongoDBTableMapping(stmt tree.Statement, tenant *TenantInfo) bool {
	create, ok := stmt.(*tree.CreateTable)
	if !ok || create.MongoDBParam == nil {
		return true
	}
	return tenant != nil && tenant.IsAdminRole()
}

// authenticateCanExecuteStatementAndPlan checks the user can execute the statement and its plan
func authenticateCanExecuteStatementAndPlan(reqCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	_, task := gotrace.NewTask(reqCtx, "frontend.authenticateCanExecuteStatementAndPlan")
	defer task.End()
	if getPu(ses.GetService()).SV.SkipCheckPrivilege {
		return stats, nil
	}

	if ses.skipAuthForSpecialUser() {
		return stats, nil
	}
	yes, delta, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(reqCtx, ses, stmt, p)
	if err != nil {
		return stats, err
	}
	stats.Add(&delta)

	if !yes {
		return stats, moerr.NewInternalError(reqCtx, "do not have privilege to execute the statement")
	}
	return stats, nil
}

func bindSessionDatabaseForStatement(ses *Session, defaultDatabase string) func() {
	if defaultDatabase == "" || defaultDatabase == ses.GetDatabaseName() {
		return func() {}
	}
	currentDatabase := ses.GetDatabaseName()
	ses.SetDatabaseName(defaultDatabase)
	return func() { ses.SetDatabaseName(currentDatabase) }
}

// authenticatePrivilegeOfPrepareAndExecute checks the user can execute the Prepare or Execute statement
func authenticateUserCanExecutePrepareOrExecute(
	reqCtx context.Context,
	ses *Session,
	stmt tree.Statement,
	p *plan.Plan,
	defaultDatabase string,
) (statistic.StatsArray, error) {
	var stats statistic.StatsArray
	stats.Reset()

	// Unqualified names in a prepared AST retain their PREPARE-time binding.
	// Authorization must resolve that same object rather than the database that
	// happens to be active when EXECUTE runs.
	restoreDatabase := bindSessionDatabaseForStatement(ses, defaultDatabase)
	defer restoreDatabase()

	_, task := gotrace.NewTask(reqCtx, "frontend.authenticateUserCanExecutePrepareOrExecute")
	defer task.End()
	if getPu(ses.GetService()).SV.SkipCheckPrivilege {
		return stats, nil
	}
	stmt = unwrapExecutableExplainStatement(stmt)
	delta, err := authenticateUserCanExecuteStatement(reqCtx, ses, stmt)
	if err != nil {
		return stats, err
	}
	stats.Add(&delta)

	delta, err = authenticateCanExecuteStatementAndPlan(reqCtx, ses, stmt, p)
	if err != nil {
		return stats, err
	}
	stats.Add(&delta)
	return stats, err
}

// unwrapExecutableExplainStatement returns the statement that an executable
// EXPLAIN wrapper will run. The wrapper itself has no table privileges, while
// the plan belongs to its inner query and must be checked against that query's
// privilege set at every prepared execution.
func unwrapExecutableExplainStatement(stmt tree.Statement) tree.Statement {
	for {
		switch explainStmt := stmt.(type) {
		case *tree.ExplainStmt:
			stmt = explainStmt.Statement
		case *tree.ExplainAnalyze:
			stmt = explainStmt.Statement
		case *tree.ExplainPhyPlan:
			stmt = explainStmt.Statement
		default:
			return stmt
		}
	}
}

// canExecuteStatementInUncommittedTxn checks the user can execute the statement in an uncommitted transaction
func canExecuteStatementInUncommittedTransaction(
	reqCtx context.Context,
	ses FeSession,
	stmt tree.Statement,
) error {

	can, err := statementCanBeExecutedInUncommittedTransaction(reqCtx, ses, stmt)
	if err != nil {
		return err
	}
	if !can {
		switch stmt.(type) {
		case *tree.DataBranchMerge, *tree.DataBranchPick:
			return moerr.NewInternalError(reqCtx, dataBranchMergePickTxnErrorInfo())
		}
		//is ddl statement
		if IsCreateDropDatabase(stmt) {
			return moerr.NewInternalError(reqCtx, createDropDatabaseErrorInfo())
		} else if IsDDL(stmt) {
			return moerr.NewInternalError(reqCtx, onlyCreateStatementErrorInfo())
		} else if IsAdministrativeStatement(stmt) {
			return moerr.NewInternalError(reqCtx, administrativeCommandIsUnsupportedInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(reqCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func removePrepareStmtForReplacement(ses *Session, stmt tree.Statement) {
	switch st := stmt.(type) {
	case *tree.PrepareStmt:
		ses.RemovePrepareStmt(string(st.Name))
	case *tree.PrepareString:
		ses.RemovePrepareStmt(string(st.Name))
	case *tree.PrepareVar:
		ses.RemovePrepareStmt(string(st.Name))
	}
}

func readThenWrite(ses FeSession, execCtx *ExecCtx, param *tree.ExternParam, writer *io.PipeWriter, mysqlRrWr MysqlRrWr, skipWrite bool, epoch uint64) (_ bool, _ time.Duration, _ time.Duration, err error) {
	var readTime, writeTime time.Duration
	var payload []byte
	start := time.Now()
	defer func() {
		if err != nil {
			mysqlRrWr.FreeLoadLocal()
		}
	}()
	payload, err = mysqlRrWr.ReadLoadLocalPacket()
	if err != nil {
		if errors.Is(err, errorInvalidLength0) {
			return skipWrite, readTime, writeTime, err
		}
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewInvalidInputf(execCtx.reqCtx, "cannot read '%s' from client,please check the file path, user privilege and if client start with --local-infile", param.Filepath)
		}
		return skipWrite, readTime, writeTime, err
	}
	readTime = time.Since(start)

	//empty packet means the file is over.
	size := len(payload)
	if size == 0 {
		return skipWrite, readTime, writeTime, errorInvalidLength0
	}
	ses.CountPayload(size)

	// If inner error occurs(unexpected or expected(ctrl-c)), proc.Base.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	start = time.Now()
	if !skipWrite {
		_, err = writer.Write(payload)
		if err != nil {
			ses.Errorf(execCtx.reqCtx, "Failed to load local file: epoch=%d, error=%v", epoch, err)
			skipWrite = true
		}
		writeTime = time.Since(start)

	}
	return skipWrite, readTime, writeTime, err
}

// processLoadLocal executes the load data local.
// load data local interaction: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_request.html
func processLoadLocal(ses FeSession, execCtx *ExecCtx, param *tree.ExternParam, writer *io.PipeWriter, reader *io.PipeReader) (err error) {
	//pipewriter may stick when there is no reader reading on the pipereader.
	//so we need to make sure the pipewriter.write returns.
	//issue3976
	quitC := make(chan int)
	go func(ctx context.Context, reader *io.PipeReader) {
		select {
		case <-ctx.Done():
			//close reader
			_ = reader.Close()
		case <-quitC:
		}
	}(execCtx.reqCtx, reader)
	defer func() {
		close(quitC)
	}()
	mysqlRwer := ses.GetResponser().MysqlRrWr()
	defer func() {
		err2 := writer.Close()
		if err == nil {
			err = err2
		}
		//free load local buffer anyway
		mysqlRwer.FreeLoadLocal()
	}()
	err = plan2.InitInfileParam(param)
	if err != nil {
		return
	}
	err = mysqlRwer.WriteLocalInfileRequest(param.Filepath)
	if err != nil {
		return
	}

	// handleNetworkTimeout checks if the error is a network timeout and disconnects the client
	handleNetworkTimeout := func(err error) error {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			ses.Errorf(execCtx.reqCtx, "load local file failed: network read timeout: %v, disconnecting client", err)
			if disconnectErr := mysqlRwer.Disconnect(); disconnectErr != nil {
				ses.Errorf(execCtx.reqCtx, "failed to disconnect client: %v", disconnectErr)
			}
			return moerr.NewInternalErrorf(execCtx.reqCtx,
				"load local file failed: network read timeout, client connection closed")
		}
		return nil
	}

	var skipWrite bool
	skipWrite = false
	var readTime, writeTime time.Duration
	var retError error
	start := time.Now()
	epoch, printTime := uint64(0), uint64(1024*60)
	minReadTime, maxReadTime, minWriteTime, maxWriteTime := 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond

	// updateTimeStats updates min/max time statistics
	updateTimeStats := func(readTime, writeTime time.Duration) {
		if readTime > maxReadTime {
			maxReadTime = readTime
		}
		if readTime < minReadTime {
			minReadTime = readTime
		}
		if writeTime > maxWriteTime {
			maxWriteTime = writeTime
		}
		if writeTime < minWriteTime {
			minWriteTime = writeTime
		}
	}

	checkLockTableBinds := func() error {
		if execCtx == nil || execCtx.proc == nil || execCtx.proc.GetTxnOperator() == nil {
			return nil
		}
		ctx := execCtx.reqCtx
		if ctx == nil && execCtx.proc.Ctx != nil {
			ctx = execCtx.proc.Ctx
		}
		if ctx == nil {
			ctx = context.Background()
		}
		return execCtx.proc.GetTxnOperator().CheckLockTableBinds(ctx)
	}

	if err = checkLockTableBinds(); err != nil {
		return
	}
	skipWrite, readTime, writeTime, err = readThenWrite(ses, execCtx, param, writer, mysqlRwer, skipWrite, epoch)
	if err != nil {
		if errors.Is(err, errorInvalidLength0) {
			return nil
		}
		if timeoutErr := handleNetworkTimeout(err); timeoutErr != nil {
			return timeoutErr
		}
		retError = err
	}
	updateTimeStats(readTime, writeTime)

	const maxRetries = 100               // Maximum number of consecutive errors
	const maxTotalTime = 3 * time.Minute // Maximum total consecutive processing time
	var consecutiveErrors int
	consecutiveLoopStartTime := time.Now()

	for {
		if err = checkLockTableBinds(); err != nil {
			return
		}
		skipWrite, readTime, writeTime, err = readThenWrite(ses, execCtx, param, writer, mysqlRwer, skipWrite, epoch)
		if err != nil {
			if errors.Is(err, errorInvalidLength0) {
				if retError != nil {
					err = retError
					break
				}
				err = nil
				break
			}

			if timeoutErr := handleNetworkTimeout(err); timeoutErr != nil {
				return timeoutErr
			}

			retError = err
			consecutiveErrors++
			ses.Errorf(execCtx.reqCtx, "readThenWrite error (attempt %d): %v", consecutiveErrors, err)
			time.Sleep(10 * time.Millisecond)

			if consecutiveErrors >= maxRetries || time.Since(consecutiveLoopStartTime) > maxTotalTime {
				return moerr.NewInternalErrorf(execCtx.reqCtx,
					"load local file failed: consecutive errors (%d), timeout after %v", maxRetries, maxTotalTime)
			}
		} else {
			consecutiveErrors = 0
			consecutiveLoopStartTime = time.Now()
		}

		updateTimeStats(readTime, writeTime)

		if epoch%printTime == 0 {
			if execCtx.isIssue3482 {
				ses.Infof(execCtx.reqCtx, "load local '%s', epoch: %d, skipWrite: %v, minReadTime: %s, maxReadTime: %s, minWriteTime: %s, maxWriteTime: %s,\n", param.Filepath, epoch, skipWrite, minReadTime.String(), maxReadTime.String(), minWriteTime.String(), maxWriteTime.String())
			}
			minReadTime, maxReadTime, minWriteTime, maxWriteTime = 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
		}
		epoch += 1
	}

	if execCtx.isIssue3482 {
		ses.Infof(execCtx.reqCtx, "load local '%s', read&write all data from client cost: %s\n", param.Filepath, time.Since(start))
	}
	return
}

func makeCompactTxnInfo(op TxnOperator) string {
	txn := op.Txn()
	var buf [128]byte
	b := buf[:0]
	b = append(b, hex.EncodeToString(txn.ID)...)
	b = append(b, ':')
	b = append(b, txn.SnapshotTS.DebugString()...)
	return string(b)
}

func executeStmtWithResponse(ses *Session,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPStmtWithResponse)
	defer ses.ExitFPrint(FPStmtWithResponse)
	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "executeStmtWithResponse",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(ses.GetTxnId(), ses.GetStmtId(), ses.GetSqlOfStmt()))
	defer func() {
		if execCtx.returning != nil {
			if closeErr := execCtx.returning.Close(execCtx); closeErr != nil {
				if err != nil {
					err = errors.Join(err, closeErr)
				} else {
					ses.Warn(execCtx.reqCtx, "failed to close committed DML RETURNING spool", zap.Error(closeErr))
				}
			}
			execCtx.returning = nil
		}
	}()

	ses.SetQueryInProgress(true)
	ses.SetQueryStart(time.Now())
	ses.SetQueryInExecute(true)
	defer ses.SetQueryEnd(time.Now())
	defer ses.SetQueryInProgress(false)

	// executeStmtWithMaxExecutionTime returns only after
	// executeStmtWithWorkspace has run its deferred transaction finalizer.
	// Cursor responses are staged by executeResultRowStmt and emitted by
	// RespPostMeta below, so a commit error can never follow an advertised
	// cursor on the wire.
	err = executeStmtWithMaxExecutionTime(ses, execCtx)
	// The WHOLE-statement terminal for deferred Kafka scan progress: every
	// pipeline (including downstream consumers on split scopes) has finished
	// by the time executeStmtWithMaxExecutionTime returns. Kafka progress is
	// deliberately statement/session state, not transaction state. Consumers
	// that need atomic data+offset commits store LAST_KAFKA_MESSAGE_ID() in a
	// separate MatrixOne table in the same explicit transaction.
	ses.FinalizeKafkaProgress(err == nil)
	if err != nil {
		return abortPreparedCursorQueryResult(execCtx, abortStagedReturning(execCtx, err))
	}

	// Record the rows affected by this statement so the ROW_COUNT() builtin in a
	// following statement (same proc for multi-statement COM_QUERY, or the next
	// COM_QUERY via the session) reads the correct value.
	recordLastAffectedRows(ses, execCtx)

	err = respClientWhenSuccess(ses, execCtx)
	if err != nil {
		return err
	}
	recordLastFoundRows(ses, execCtx)

	return
}

func abortStagedReturning(execCtx *ExecCtx, cause error) error {
	if cause == nil || execCtx == nil || execCtx.returning == nil || execCtx.returning.stagedSaver == nil {
		return cause
	}
	return errors.Join(cause, execCtx.returning.stagedSaver.Abort(execCtx))
}

func executeStmtWithTxn(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPExecStmtWithTxn)
	defer ses.ExitFPrint(FPExecStmtWithTxn)
	if !ses.IsDerivedStmt() {
		err = executeStmtWithWorkspace(ses, statsArr, execCtx)
	} else {

		txnOp := ses.GetTxnHandler().GetTxn()
		//refresh proc txnOp
		execCtx.proc.Base.TxnOperator = txnOp

		err = dispatchStmt(ses, statsArr, execCtx)
		recordSessionDDL(ses, execCtx, err)
	}
	return
}

func effectiveStatementForTxn(
	ctx context.Context,
	ses FeSession,
	stmt tree.Statement,
) (tree.Statement, string, error) {
	seen := make(map[string]struct{})
	defaultDatabase := ""
	for {
		execute, ok := stmt.(*tree.Execute)
		if !ok {
			return stmt, defaultDatabase, nil
		}
		name := strings.ToLower(string(execute.Name))
		if _, ok = seen[name]; ok {
			return nil, "", moerr.NewInternalError(ctx, "cyclic prepared EXECUTE reference")
		}
		seen[name] = struct{}{}
		prepared, err := ses.GetPrepareStmt(ctx, name)
		if err != nil {
			return nil, "", err
		}
		if prepared == nil || prepared.PrepareStmt == nil {
			return nil, "", moerr.NewInternalError(ctx, "prepared statement has no executable statement")
		}
		stmt = prepared.PrepareStmt
		// Unqualified names in the saved AST were bound against this database.
		// Admission must resolve them exactly like the prepared plan, regardless
		// of the session database when EXECUTE runs.
		defaultDatabase = prepared.defaultDatabase
	}
}

func executeStmtWithWorkspace(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPExecStmtWithWorkspace)
	defer ses.ExitFPrint(FPExecStmtWithWorkspace)
	if ses.IsDerivedStmt() {
		return
	}
	var autocommit bool
	//derived stmt shares the same txn with ancestor.
	//it only executes select statements.

	//7. pass or commit or rollback txn
	// Admission errors occur before StartStatement and must not roll back the
	// previous workspace statement. Enable transaction finalization only for an
	// explicit COMMIT/ROLLBACK or after transaction admission succeeds.
	finishTxnOnReturn := false
	defer func() {
		if e := recover(); e != nil {
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(execCtx.reqCtx, e))
			} else {
				err = errors.Join(err, moe)
			}

			ses.Error(execCtx.reqCtx, "recover from panic before finishTxnFunc", zap.Error(err))
		}
		if finishTxnOnReturn {
			err = finishTxnFunc(ses, err, execCtx)
		}
	}()

	_, _, _ = fault.TriggerFault("executeStmtWithWorkspace_panic")

	//1. start txn
	//special BEGIN,COMMIT,ROLLBACK
	beginStmt := false
	execCtx.txnOpt.Close()
	effectiveStmt, effectiveDefaultDatabase, err := effectiveStatementForTxn(
		execCtx.reqCtx, ses, execCtx.stmt,
	)
	if err != nil {
		return err
	}
	if effectiveDefaultDatabase == "" {
		// Binary execution and wrappers may already expose the prepared inner AST;
		// initExecuteStmtParam recorded its binding database before authorization.
		effectiveDefaultDatabase = execCtx.effectiveTxnDefaultDatabase
	}
	execCtx.effectiveTxnDefaultDatabase = effectiveDefaultDatabase
	execCtx.txnOpt.forcePessimisticObjectLifecycle = requiresPessimisticObjectLifecycleTxn(
		ses, effectiveStmt, effectiveDefaultDatabase,
	)
	execCtx.txnOpt.activeTxnAtStart = ses.GetTxnHandler().InActiveTxn()
	execCtx.txnOpt.activeTxnAtStartKnown = true
	switch execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		execCtx.txnOpt.byBegin = true
		beginStmt = true
	case *tree.CommitTransaction:
		execCtx.txnOpt.byCommit = true
		finishTxnOnReturn = true
		return nil
	case *tree.RollbackTransaction:
		execCtx.txnOpt.byRollback = true
		finishTxnOnReturn = true
		return nil
	case *tree.SavePoint, *tree.ReleaseSavePoint:
		return nil
	case *tree.RollbackToSavePoint:
		return moerr.NewInternalError(execCtx.reqCtx, "savepoint has not been implemented yet. please rollback the transaction.")
	}

	//in session migration, the txn forced to be autocommit.
	//then the txn can be committed.
	if execCtx.inMigration {
		autocommit = true
	} else {
		autocommit, err = autocommitValue(ses)
		if err != nil {
			return err
		}
	}

	execCtx.txnOpt.autoCommit = autocommit
	err = ses.GetTxnHandler().Create(execCtx)
	if err != nil {
		return err
	}
	finishTxnOnReturn = true

	//skip BEGIN stmt
	if beginStmt {
		return err
	}

	if ses.GetTxnHandler() == nil {
		panic("need txn handler")
	}

	txnOp := ses.GetTxnHandler().GetTxn()

	//refresh txn id
	ses.SetTxnId(txnOp.Txn().ID)
	ses.SetStaticTxnInfo(makeCompactTxnInfo(txnOp))

	//refresh proc txnOp
	execCtx.proc.Base.TxnOperator = txnOp

	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return err
	}

	ses.EnterFPrint(FPExecStmtWithWorkspaceBeforeStart)
	defer ses.ExitFPrint(FPExecStmtWithWorkspaceBeforeStart)
	//!!!NOTE!!!: statement management
	//2. start statement on workspace
	txnOp.GetWorkspace().StartStatement()
	//3. end statement on workspace
	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		if ses.GetTxnHandler() == nil {
			panic("need txn handler 2")
		}

		txnOp = ses.GetTxnHandler().GetTxn()
		if txnOp != nil {
			ses.EnterFPrint(FPExecStmtWithWorkspaceBeforeEnd)
			defer ses.ExitFPrint(FPExecStmtWithWorkspaceBeforeEnd)
			//most of the cases, txnOp will not nil except that "set autocommit = 1"
			//commit the txn immediately then the txnOp is nil.
			txnOp.GetWorkspace().EndStatement()
		}
	}()

	err = executeStmtWithIncrStmt(ses, statsArr, execCtx, txnOp)
	recordSessionDDL(ses, execCtx, err)

	return
}

func recordSessionDDL(ses FeSession, execCtx *ExecCtx, err error) {
	if err != nil || execCtx == nil {
		return
	}
	var queryPlan *plan.Plan
	if cw, ok := execCtx.cw.(*TxnComputationWrapper); ok {
		queryPlan = cw.Plan()
	}
	if !changesSessionCatalog(execCtx.stmt, queryPlan) {
		return
	}
	if session := upstreamUserSession(ses); session != nil {
		session.advanceDDLVersion()
	}
}

func upstreamUserSession(ses FeSession) *Session {
	for ses != nil {
		if session, ok := ses.(*Session); ok {
			return session
		}
		next := ses.GetUpstream()
		if next == ses {
			return nil
		}
		ses = next
	}
	return nil
}

func executeStmtWithIncrStmt(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx,
	txnOp TxnOperator,
) (err error) {
	var hasRecovered bool
	ses.EnterFPrint(FPExecStmtWithIncrStmt)
	defer ses.ExitFPrint(FPExecStmtWithIncrStmt)

	err = disttae.CheckTxnIsValid(txnOp)
	if err != nil {
		return err
	}

	if ses.IsDerivedStmt() {
		return
	}
	ses.EnterFPrint(FPExecStmtWithIncrStmtBeforeIncr)
	defer ses.ExitFPrint(FPExecStmtWithIncrStmtBeforeIncr)
	//3. increase statement id

	crs := new(perfcounter.CounterSet)
	newCtx := perfcounter.AttachS3RequestKey(execCtx.reqCtx, crs)
	err, hasRecovered = ExecuteFuncWithRecover(func() error {
		return txnOp.GetWorkspace().IncrStatementID(newCtx, false)
	})
	if err != nil || hasRecovered {
		return err
	}
	stats := statistic.StatsInfoFromContext(newCtx)
	stats.AddTxnIncrStatementS3Request(statistic.S3Request{
		List:      crs.FileService.S3.List.Load(),
		Head:      crs.FileService.S3.Head.Load(),
		Put:       crs.FileService.S3.Put.Load(),
		Get:       crs.FileService.S3.Get.Load(),
		Delete:    crs.FileService.S3.Delete.Load(),
		DeleteMul: crs.FileService.S3.DeleteMulti.Load(),
	})

	defer func() {
		if ses.GetTxnHandler() == nil {
			panic("need txn handler 3")
		}

		//!!!NOTE!!!: it does not work
		//_, txnOp = ses.GetTxnHandler().GetTxn()
		//if txnOp != nil {
		//	err = rollbackLastStmt(execCtx, txnOp, err)
		//}
	}()

	err = dispatchStmt(ses, statsArr, execCtx)
	return
}

func rebuildStaleCachedStatements(ses FeSession, execCtx *ExecCtx) (err error) {
	// Evict this stale entry before rebuilding so a successful replan replaces
	// it instead of paying the validation/rebuild cost forever.
	if session, ok := ses.(*Session); ok {
		session.removeCachedPlan(execCtx.input.getHash())
	}

	stmts, err := parseSql(execCtx, ses.GetMySQLParser())
	defer freeStmts(stmts)
	if err != nil {
		return err
	}
	if len(stmts) != len(execCtx.cws) {
		return moerr.NewInternalError(execCtx.reqCtx, "the count of stmts parsed from cached sql is not equal to cws length")
	}
	if execCtx.rewriteEnabled {
		if err = parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
			execCtx.reqCtx, stmts, execCtx.input.getSql(), sessionSQLModeForParser(ses),
			parserLowerCaseTableNames(ses)); err != nil {
			return err
		}
		remaps := make([]map[string]string, len(execCtx.cws))
		for i, cw := range execCtx.cws {
			if carrier, ok := cw.(interface{ GetRemapDb() map[string]string }); ok {
				remaps[i] = carrier.GetRemapDb()
			}
		}
		if err = applyRemapDbByStatement(
			execCtx.reqCtx, stmts, remaps, parserLowerCaseTableNames(ses),
		); err != nil {
			return err
		}
	}
	for i, cw := range execCtx.cws {
		cw.ResetPlanAndStmt(stmts[i])
		// ResetPlanAndStmt now owns the replacement AST. Keep the deferred
		// cleanup responsible only for statements that were not transferred.
		stmts[i] = nil
	}
	return nil
}

func dispatchStmt(ses FeSession,
	statsArr *statistic.StatsArray,
	execCtx *ExecCtx) (err error) {
	ses.EnterFPrint(FPDispatchStmt)
	defer ses.ExitFPrint(FPDispatchStmt)
	//5. check plan within txn
	if !execCtx.input.isBinaryProtExecute && execCtx.cw.Plan() != nil {
		flag, err := checkModify(execCtx.cw.Plan(), ses.GetTxnCompileCtx().Resolve)
		if err != nil {
			return err
		}
		if flag {
			if err = rebuildStaleCachedStatements(ses, execCtx); err != nil {
				return err
			}
		}
	}

	//6. execute stmt within txn
	switch sesImpl := ses.(type) {
	case *Session:
		return executeStmt(sesImpl, execCtx)
	case *backSession:
		return executeStmtInBack(sesImpl, statsArr, execCtx)
	default:
		return moerr.NewInternalError(execCtx.reqCtx, "no such session implementation")
	}
}

func executeStmt(ses *Session,
	execCtx *ExecCtx,
) (err error) {
	ses.EnterFPrint(FPExecStmt)
	defer ses.ExitFPrint(FPExecStmt)
	ses.GetTxnCompileCtx().tcw = execCtx.cw

	var cmpBegin time.Time
	var ret interface{}

	getExecLocation := func() tree.ExecLocation {
		// because when isBinaryProtExecute is true, execCtx.stmt is preparestmt, actually it's execute
		if execCtx.input.isBinaryProtExecute {
			return tree.EXEC_IN_ENGINE
		}
		return execCtx.stmt.StmtKind().ExecLocation()
	}
	switch getExecLocation() {
	case tree.EXEC_IN_FRONTEND:
		stats, err := execInFrontend(ses, execCtx)
		defer execCtx.cw.RecordCompoundStmt(execCtx.reqCtx, stats)
		return err
	case tree.EXEC_IN_ENGINE:
		//in the computation engine
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			if getPu(ses.GetService()).SV.DisableSelectInto {
				err = moerr.NewSyntaxError(execCtx.reqCtx, "Unsupport select statement")
				return
			}
			ses.InitExportConfig(st.Ep)
			defer func() {
				ses.ClearExportParam()
			}()
			err = doCheckFilePath(execCtx.reqCtx, ses, st.Ep)
			if err != nil {
				return
			}
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && ses.GetTenantInfo() != nil && !ses.GetTenantInfo().IsAdminRole() {
			err = moerr.NewInternalError(execCtx.reqCtx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.reqCtx, string(st.Name))
		if err != nil {
			return
		}
		ses.InvalidatePrivilegeCache()
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == ses.GetDatabaseName() {
			ses.SetDatabaseName("")
		}
	case *tree.ExplainAnalyze:
		ses.SetData(nil)
	case *tree.ExplainPhyPlan:
		ses.SetData(nil)
	case *tree.ShowTableStatus:
		ses.SetShowStmtType(ShowTableStatus)
		ses.SetData(nil)
	case *tree.Load:
		if st.Local {
			execCtx.proc.Base.LoadLocalReader, execCtx.loadLocalWriter = io.Pipe()
		}
	case *tree.ShowGrants:
		if len(st.Username) == 0 {
			st.Username = execCtx.userName
		}
		if len(st.Hostname) == 0 || st.Hostname == "%" {
			st.Hostname = rootHost
		}
	}

	cmpBegin = time.Now()

	ses.EnterFPrint(FPExecStmtBeforeCompile)
	defer ses.ExitFPrint(FPExecStmtBeforeCompile)
	if ret, err = execCtx.cw.Compile(execCtx, ses.GetOutputCallback(execCtx)); err != nil {
		return
	}

	defer func() {
		if c, ok := ret.(*compile.Compile); ok {
			var phyPlan *models.PhyPlan
			analyzeModule := c.GetAnalyzeModule()
			if analyzeModule != nil {
				phyPlan = analyzeModule.GetPhyPlan()
				execCtx.cw.SetExplainBuffer(analyzeModule.GetExplainPhyBuffer())
			}

			if txnCw, ok := execCtx.cw.(*TxnComputationWrapper); ok {
				txnCw.completeCompileExecution(c, err)
			}

			// Serialize the execution plan as json
			_ = execCtx.cw.RecordExecPlan(execCtx.reqCtx, phyPlan)
			c.Release()
		}
	}()

	// cw.Compile may rewrite the stmt in the EXECUTE statement, we fetch the latest version
	//need to check again.
	execCtx.stmt = execCtx.cw.GetAst()
	switch execCtx.stmt.StmtKind().ExecLocation() {
	case tree.EXEC_IN_FRONTEND:
		_, err = execInFrontend(ses, execCtx)
		return err
	case tree.EXEC_IN_ENGINE:

	}

	if execCtx.stmt.StmtKind().RespType() == tree.RESP_DEFERRED_RESULT_ROW {
		if ses.GetIsInternal() || ses.IsBackgroundSession() {
			return moerr.NewNotSupported(execCtx.reqCtx, "DML RETURNING does not support internal executor")
		}
		compiled, ok := ret.(*compile.Compile)
		if !ok {
			return moerr.NewInternalError(execCtx.reqCtx, "DML RETURNING requires engine compile")
		}
		execCtx.returning = &returningState{spool: &returningSpool{}}
		compiled.SetResultSink(execCtx.returning.spool)
	}

	execCtx.runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		ses.Infof(execCtx.reqCtx, "time of Exec.Build : %s", time.Since(cmpBegin).String())
	}

	//output result & status
	StmtKind := execCtx.stmt.StmtKind().OutputType()
	switch StmtKind {
	case tree.OUTPUT_RESULT_ROW:
		err = executeResultRowStmt(ses, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_STATUS:
		err = executeStatusStmt(ses, execCtx)
		if err != nil {
			return err
		}
	case tree.OUTPUT_UNDEFINED:
		if _, ok := execCtx.stmt.(*tree.Execute); !ok {
			return moerr.NewInternalErrorf(execCtx.reqCtx, "need set result type for %s", execCtx.sqlOfStmt)
		}
	}

	return
}

// execute query
func countUpdateChangedRows(ses *Session) bool {
	if ses.GetIsInternal() || ses.IsBackgroundSession() {
		return false
	}
	resper, ok := ses.GetResponser().(*MysqlResp)
	return ok && resper.GetU32(CAPABILITY)&CLIENT_FOUND_ROWS == 0
}

// rollbackWholeTxnOnPreExecutionError applies mo_rollback_txn_on_error to a
// failure that never reached the executor.
//
// A parse error or a privilege rejection returns from doComQuery long before
// finishTxnFunc, which is where the setting is otherwise honoured. Without this
// the setting would quietly mean "any error the executor produced", exempting
// the ones that never got that far: with it on,
// `BEGIN; INSERT ...; selec 1; COMMIT;` would still COMMIT the row.
//
// It is called from the defer every COM_QUERY error path converges on. A
// statement that already rolled back has left no active transaction, so the
// guard below makes this a no-op for the errors finishTxnFunc handled, rather
// than rolling back twice.
func rollbackWholeTxnOnPreExecutionError(ses FeSession, execCtx *ExecCtx, retErr error) {
	if !sessionRollsBackTxnOnError(ses, retErr) {
		return
	}
	txnHandler := ses.GetTxnHandler()
	if txnHandler == nil || !txnHandler.InMultiStmtTransactionMode() || !txnHandler.InActiveTxn() {
		return
	}
	if rbErr := txnHandler.Rollback(execCtx); rbErr != nil {
		// The statement's own error is what the client asked about; a failure
		// to roll back is logged, not substituted for it.
		ses.Error(execCtx.reqCtx, "rollback whole txn on error failed",
			zap.Error(rbErr), zap.Error(retErr))
	}
}

func doComQuery(ses *Session, execCtx *ExecCtx, input *UserInput) (retErr error) {
	ses.EnterFPrint(FPDoComQuery)
	defer ses.ExitFPrint(FPDoComQuery)
	defer ses.ClearDDLOwnerRoleID()
	ses.GetTxnCompileCtx().SetExecCtx(execCtx)
	beginInstant := time.Now()
	execCtx.reqCtx = appendStatementAt(execCtx.reqCtx, beginInstant)
	execCtx.reqCtx = defines.AttachDDLOwnerRoleIDProvider(execCtx.reqCtx, ses)
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	resper := ses.GetResponser()
	ses.SetSql(input.getSql())
	input.genHash()
	version := ses.GetCreateVersion()
	if len(version) == 0 {
		version = serverVersion.Load().(string)
	}

	sqlLen := len(input.getSql())
	if sqlLen != 0 {
		v2.TotalSQLLengthHistogram.Observe(float64(sqlLen))
		if strings.HasPrefix(input.sql, "LOAD DATA INLINE") {
			v2.LoadDataInlineSQLLengthHistogram.Observe(float64(sqlLen))
		} else {
			v2.OtherSQLLengthHistogram.Observe(float64(sqlLen))
		}
	}

	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName

	// case: exec `set @ t= 2;` will trigger an internal query, like: `select 1 from dual`, in the same session.
	defer func(stmt *motrace.StatementInfo) {
		if stmt != nil {
			ses.tStmt = stmt
		}
	}(ses.tStmt)
	ses.tStmt = nil

	proc := ses.proc
	proc.ReplaceTopCtx(execCtx.reqCtx)

	pu := getPu(ses.GetService())
	proc.Base.Id = ses.getNextProcessId()
	proc.Base.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Base.Lim.SpillSize = pu.SV.ProcessLimitationSpillSize
	proc.Base.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Base.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Base.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.Base.SessionInfo = process.SessionInfo{
		User:                   ses.GetUserName(),
		Host:                   pu.SV.Host,
		ConnectionID:           uint64(resper.GetU32(CONNID)),
		Database:               ses.GetDatabaseName(),
		Version:                makeServerVersion(pu, version),
		TimeZone:               ses.GetTimeZone(),
		StorageEngine:          pu.StorageEngine,
		LastInsertID:           ses.GetLastInsertID(),
		SqlHelper:              ses.GetSqlHelper(),
		Buf:                    ses.GetBuffer(),
		LogLevel:               zapcore.InfoLevel, //TODO: need set by session level config
		SessionId:              ses.GetSessId(),
		ApplySQLSelectLimit:    !ses.GetIsInternal() && !ses.IsBackgroundSession() && !ses.IsDerivedStmt(),
		CountUpdateChangedRows: countUpdateChangedRows(ses),
		FoundRows:              ses.GetLastFoundRows(),
	}
	proc.SetLastInsertID(ses.GetLastInsertID())
	// Carry the previous statement's affected rows into this proc so the
	// ROW_COUNT() builtin can read it.
	proc.SetAffectedRows(ses.GetLastAffectedRows())
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)
	proc.SetResolveVariableIsBinFunc(ses.txnCompileCtx.ResolveVariableIsBin)
	proc.SetResolveVariablePrepareParamKindFunc(ses.txnCompileCtx.ResolveVariablePrepareParamKind)
	refreshStatementScopedSessionInfo(ses, proc)
	// Frontend client SQL — session-bound resolver. Procs constructed
	// via pkg/sql/compile/sql_executor.go's NewTopProcess inherit
	// IsFrontend from opts.IsFrontend() (default false → background);
	// this proc is built inline here so we set the flag explicitly,
	// paired with the resolver bind above as the "I have a session"
	// signal.
	proc.Base.IsFrontend = true
	proc.InitSeq()
	// Copy curvalues stored in session to this proc.
	// Deep copy the map, takes some memory.
	ses.CopySeqToProc(proc)

	// MySQL semantics: when a statement fails (parse / compile / execution error),
	// ROW_COUNT() for the following statement must return -1. recordLastAffectedRows
	// only runs after a statement succeeds, so cover every error path of the main
	// COM_QUERY / COM_STMT_EXECUTE flow here. proc is the session's reused proc and
	// is reseeded from the session on the next query, so the session value drives
	// the next statement; the proc is updated too for completeness.
	defer func() {
		if retErr == nil {
			return
		}
		markRowCountFailed(ses, proc)

		rollbackWholeTxnOnPreExecutionError(ses, execCtx, retErr)
	}()

	if ses.GetTenantInfo() != nil {
		proc.Base.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.Base.SessionInfo.Role = ses.GetTenantInfo().GetDefaultRole()

		if len(ses.GetTenantInfo().GetVersion()) != 0 {
			proc.Base.SessionInfo.Version = ses.GetTenantInfo().GetVersion()
		}
		userNameOnly = ses.GetTenantInfo().GetUser()
	}
	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "doComQuery",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.Base.SessionInfo.User = userNameOnly
	proc.Base.SessionInfo.QueryId = ses.getQueryId(input.isInternal())

	statsInfo := statistic.NewStatsInfo()
	statsInfo.ParseStage.ParseStartTime = beginInstant

	execCtx.reqCtx = statistic.ContextWithStatsInfo(execCtx.reqCtx, statsInfo)
	execCtx.isIssue3482 = input.isIssue3482Sql()

	executionInput := input
	stagedSQLMode := false
	stagedRemaining := ""
	var err error
	if ses.GetCmd() == COM_QUERY && input.getStmt() == nil && !input.isInternal() && mayNeedSQLModeStaging(input.getSql()) {
		first, remaining, staged, stageErr := prepareSQLModeStagedExecution(
			execCtx.reqCtx,
			ses,
			ses.GetMySQLParser(),
			input.getSql(),
		)
		if stageErr != nil {
			err = stageErr
		} else if staged {
			stagedSQLMode = true
			stagedRemaining = remaining
			executionInput = newSQLStatementInput(input, ses, first)
		}
	}
	if err == nil {
		executionInput, err = rewriteSQLStatementInput(execCtx.reqCtx, ses, executionInput)
		if !stagedSQLMode {
			input = executionInput
		}
	}

	var cws []ComputationWrapper
	if err == nil {
		execCtx.input = executionInput
		cws, err = GetComputationWrapper(execCtx, ses.GetDatabaseName(),
			ses.GetUserName(),
			pu.StorageEngine,
			proc, ses)
	}

	ParseDuration := time.Since(beginInstant)
	recordParseError := func(errorInput *UserInput, parseErr error) error {
		if isTopLevelClientStatement(ses, execCtx, errorInput) {
			ses.resetDiagnostics()
		}
		statsInfo.ParseStage.ParseDuration = time.Since(beginInstant)
		diagnosticErr := redactStatementErrorForLogging(parseErr, errorInput.getSql())
		var recordErr error
		execCtx.reqCtx, recordErr = RecordParseErrorStatement(
			execCtx.reqCtx,
			ses,
			proc,
			beginInstant,
			parsers.HandleSqlForRecord(errorInput.getSql()),
			errorInput.getSqlSourceTypes(),
			diagnosticErr,
		)
		if recordErr != nil {
			return recordErr
		}
		if sqlmongodb.RedactSQLForDiagnostics(errorInput.getSql()) != errorInput.getSql() {
			parseErr = diagnosticErr
		} else if _, ok := parseErr.(*moerr.Error); !ok {
			parseErr = moerr.NewParseError(execCtx.reqCtx, parseErr.Error())
		}
		// Keep the terminal error log on the same diagnostic boundary as
		// RecordParseErrorStatement. Parse failures have no AST, so use the raw
		// text scanner and never pass the original selector to the logger.
		logStatementStringStatus(execCtx.reqCtx, ses, redactStatementTextForLogging(nil, errorInput.getSql()), fail, diagnosticErr)
		return parseErr
	}

	if err != nil {
		return recordParseError(input, err)
	}

	singleStatement := len(cws) == 1 && !stagedSQLMode
	if ses.GetCmd() == COM_STMT_PREPARE && !singleStatement {
		if len(cws) > 0 {
			resetDiagnosticsForStatement(ses, execCtx, input, cws[0].GetAst())
		}
		return moerr.NewNotSupported(execCtx.reqCtx, "prepare multi statements")
	}

	defer func() {
		ses.SetMysqlResultSet(nil)
		ses.rs = nil
		ses.p = nil
	}()

	canCache := !stagedSQLMode && input.canUsePlanCache() &&
		reusablePlanGenerationSupported(proc)
	Cached := false
	defer func() {
		execCtx.stmt = nil
		execCtx.cw = nil
		execCtx.cws = nil
		execCtx.runner = nil
		if !Cached {
			for i := 0; i < len(cws); i++ {
				cws[i].Free()
			}
		}
	}()
	var sqlRecord []string
	if !stagedSQLMode {
		sqlRecord, err = sqlForRecordByStatementWithSQLMode(execCtx.reqCtx, input.getSql(), sessionSQLModeForParser(ses))
		if err != nil {
			return err
		}
	}
	stagedInputs := make([]*UserInput, 0, 1)
	stagedSQLRecords := make([]string, 0, 1)
	if stagedSQLMode {
		stagedInputs = append(stagedInputs, executionInput)
		stagedSQLRecords = append(stagedSQLRecords, sqlForRecord(executionInput.getSql()))
	}

	for i := 0; i < len(cws); i++ {
		cw := cws[i]
		stmt := cw.GetAst()
		// The assignment performed by SELECT ... INTO @var changes the
		// variable's bind-time type.  Do not cache this statement after it has
		// run; setUserDefinedVarWithTypeAndKind clears the existing cache, but
		// the outer request must also avoid writing this just-executed plan back.
		if selectStmt, ok := stmt.(*tree.Select); ok && len(selectStmt.IntoVars) > 0 {
			canCache = false
		}
		currentInput := input
		currentSQLRecord := ""
		sqlType := input.getSqlSourceType(i)
		hasMoreStatements := i < len(cws)-1
		if stagedSQLMode {
			currentInput = stagedInputs[i]
			currentSQLRecord = stagedSQLRecords[i]
			sqlType = currentInput.getSqlSourceType(0)
			hasMoreStatements = hasStatement(stagedRemaining, sessionSQLModeForParser(ses))
		} else {
			currentSQLRecord = sqlRecord[i]
		}
		// ExecCtx spans the whole request, while these fields belong to one
		// statement generation. Reset before authorization/admission, then inject
		// binary PREPARE metadata captured before doComQuery.
		execCtx.beginStatementGeneration(currentInput)
		// Install the policy that belongs to this wrapper before authorization and
		// planning. In particular, DefaultDatabase uses it for unqualified names.
		installStatementRemap(execCtx, cw)
		if stmt.GetQueryType() == tree.QueryTypeDDL || stmt.GetQueryType() == tree.QueryTypeDCL ||
			stmt.GetQueryType() == tree.QueryTypeOth ||
			stmt.GetQueryType() == tree.QueryTypeTCL {
			if _, ok := stmt.(*tree.SetVar); !ok {
				ses.cleanCache()
			}
			canCache = false
		}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		ses.sentRows.Store(int64(0))
		resper.ResetStatistics() // move from getDataFromPipeline, for record column fields' data
		// ExecCtx is reused across statements in a multi-statement COM_QUERY;
		// clear the previous statement's run result so a statement that does not
		// set it (e.g. a status statement) does not inherit a stale AffectRows.
		execCtx.runResult = nil
		// The process is reused for every statement in a multi-statement
		// COM_QUERY.  The generated-key field belongs to this statement's OK
		// packet, so clear it before executing each statement while leaving the
		// session-visible LAST_INSERT_ID state in LastInsertID untouched.
		proc.SetStatementLastInsertID(0)
		resetDiagnosticsForStatement(ses, execCtx, currentInput, stmt)
		removePrepareStmtForReplacement(ses, stmt)
		var err2 error
		execCtx.reqCtx, err2 = RecordStatement(execCtx.reqCtx, ses, proc, cw, beginInstant, currentSQLRecord, sqlType, singleStatement)
		if err2 != nil {
			return err2
		}

		statsInfo.Reset()
		//average parse duration
		statsInfo.ParseStage.ParseStartTime = beginInstant
		statsInfo.ParseStage.ParseDuration = time.Duration(ParseDuration.Nanoseconds() / int64(len(cws)))

		tenant := ses.GetTenantNameWithStmt(stmt)
		//skip PREPARE statement here
		if ses.GetTenantInfo() != nil && !IsPrepareStatement(stmt) {
			ses.ClearDDLOwnerRoleID()
			authStats, authErr := func() (statistic.StatsArray, error) {
				restoreDatabase := bindSessionDatabaseForStatement(
					ses, execCtx.effectiveTxnDefaultDatabase,
				)
				defer restoreDatabase()
				return authenticateUserCanExecuteStatement(execCtx.reqCtx, ses, stmt)
			}()
			if authErr != nil {
				logStatementStatus(execCtx.reqCtx, ses, stmt, fail, authErr)
				return authErr
			}
			statsInfo.PermissionAuth.Add(&authStats)
		}

		/*
				if it is in an active or multi-statement transaction, we check the type of the statement.
				Then we decide that if we can execute the statement.

			If we check the active transaction, it will generate the case below.
			case:
			set autocommit = 0;  <- no active transaction
			                     <- no active transaction
			drop table test1;    <- no active transaction, no error
			                     <- has active transaction
			drop table test1;    <- has active transaction, error
			                     <- has active transaction
		*/
		if ses.GetTxnHandler().InActiveTxn() {
			err = canExecuteStatementInUncommittedTransaction(execCtx.reqCtx, ses, stmt)
			if err != nil {
				logStatementStatus(execCtx.reqCtx, ses, stmt, fail, err)
				return err
			}
		}

		// update UnixTime for new query, which is used for now() / CURRENT_TIMESTAMP
		proc.Base.UnixTime = time.Now().UnixNano()
		if ses.proc != nil {
			ses.proc.Base.UnixTime = proc.Base.UnixTime
		}
		execCtx.txnOpt.Close()
		execCtx.stmt = stmt
		execCtx.isLastStmt = !hasMoreStatements
		execCtx.singleStatementQuery = singleStatement
		execCtx.tenant = tenant
		execCtx.userName = userNameOnly
		execCtx.sqlOfStmt = currentSQLRecord
		execCtx.cw = cw
		execCtx.proc = proc
		execCtx.resper = resper
		execCtx.ses = ses
		if stagedSQLMode {
			execCtx.cws = []ComputationWrapper{cw}
		} else {
			execCtx.cws = cws
		}
		execCtx.input = currentInput

		err = executeStmtWithResponse(ses, execCtx)
		ses.ClearDDLOwnerRoleID()
		if err != nil {
			return err
		}

		if stagedSQLMode && hasMoreStatements {
			// SET expressions may execute an internal query, which temporarily
			// replaces the compiler context's ExecCtx. Restore this outer query
			// before planning the next staged statement.
			ses.GetTxnCompileCtx().SetExecCtx(execCtx)
			nextInput, remaining, nextErr := nextSQLModeStatementInput(
				execCtx.reqCtx,
				ses,
				ses.GetMySQLParser(),
				input,
				stagedRemaining,
			)
			if nextErr != nil {
				return recordParseError(newSQLStatementInput(input, ses, stagedRemaining), nextErr)
			}
			nextInput, nextErr = rewriteSQLStatementInput(execCtx.reqCtx, ses, nextInput)
			if nextErr != nil {
				return recordParseError(nextInput, nextErr)
			}
			execCtx.input = nextInput
			refreshStatementScopedSessionInfo(ses, proc)
			nextCWs, nextErr := GetComputationWrapper(execCtx, ses.GetDatabaseName(),
				ses.GetUserName(),
				pu.StorageEngine,
				proc, ses)
			if nextErr != nil {
				return recordParseError(nextInput, nextErr)
			}
			if len(nextCWs) != 1 {
				for _, nextCW := range nextCWs {
					nextCW.Free()
				}
				return moerr.NewInternalError(execCtx.reqCtx, "staged sql_mode execution parsed an unexpected statement count")
			}
			cws = append(cws, nextCWs[0])
			stagedInputs = append(stagedInputs, nextInput)
			stagedSQLRecords = append(stagedSQLRecords, sqlForRecord(nextInput.getSql()))
			stagedRemaining = remaining
		}

	} // end of for

	if !canCache {
		return nil
	}
	cacheKey := input.getHash()
	if ses.isCached(cacheKey) {
		return nil
	}
	for _, cw := range cws {
		if tcw, ok := cw.(*TxnComputationWrapper); ok && tcw.cachedPlanSQL == cacheKey {
			// A publication or failed generation replacement made the entry stale
			// while these wrappers still borrowed its AST. Do not republish the
			// just-executed old plan without rebuilding its statistics dependencies.
			// Wrapper cleanup runs first; the next lookup then evicts the stale owner.
			return nil
		}
	}

	cacheProtocolVersion := currentProtocolVersion(proc)
	planStatsVersions := make([]map[optimizerStatsTableKey]uint64, len(cws))
	planSnapshotTS := make([]timestamp.Timestamp, len(cws))
	for i, cw := range cws {
		tcw, ok := cw.(*TxnComputationWrapper)
		if !ok || tcw.protocolVersion != cacheProtocolVersion {
			return nil
		}
		var hasPlanSnapshotTS bool
		planSnapshotTS[i], hasPlanSnapshotTS = tcw.PlanSnapshotTS()
		if !hasPlanSnapshotTS {
			return nil
		}
		planStatsVersions[i] = tcw.optimizerStatsVersions
	}

	plans := make([]*plan.Plan, len(cws))
	stmts := make([]tree.Statement, len(cws))
	for i, cw := range cws {
		if checkNodeCanCache(cw.Plan()) {
			plans[i] = cw.Plan()
			stmts[i] = cw.GetAst()
		} else {
			return nil
		}
		cw.Clear()
	}
	Cached = true
	ses.cachePlanWithSnapshotsAndStatsVersions(
		cacheKey, stmts, plans, planSnapshotTS, planStatsVersions, cacheProtocolVersion)

	return nil
}

// sqlForRecordByStatement keeps the sanitized per-statement text aligned with
// the parser's AST list. HandleSqlForRecord intentionally preserves blank and
// comment-only semicolon fragments for its existing callers; execution skips
// those fragments, so filter them here before indexing by computation wrapper.
func sqlForRecordByStatement(ctx context.Context, sql string) ([]string, error) {
	return sqlForRecordByStatementWithSQLMode(ctx, sql, "")
}

func sqlForRecordByStatementWithSQLMode(ctx context.Context, sql string, sqlMode string) ([]string, error) {
	if isCmdFieldListSql(sql) || isCmdGetSnapshotTsSql(sql) ||
		isCmdGetDatabasesSql(sql) || isCmdGetMoIndexesSql(sql) ||
		isCmdGetDdlSql(sql) || isCmdGetObjectSql(sql) ||
		isCmdObjectListSql(sql) || isCmdCheckSnapshotFlushedSql(sql) {
		return parsers.HandleSqlForRecord(sql), nil
	}
	fragments, err := parsers.SplitSqlByStatementWithSQLMode(ctx, sql, sqlMode)
	if err != nil {
		return nil, err
	}
	records, err := parsers.HandleSqlForRecordByStatementWithSQLMode(ctx, sql, sqlMode)
	if err != nil {
		return nil, err
	}
	if len(fragments) == 1 {
		return records, nil
	}
	byStatement := make([]string, 0, len(records))
	for i, fragment := range fragments {
		if parsers.FragmentHasStatement(fragment) {
			byStatement = append(byStatement, records[i])
		}
	}
	if len(byStatement) == 0 {
		return []string{""}, nil
	}
	return byStatement, nil
}

// schedulingSQLByStatementWithSQLMode keeps raw statement text (including
// optimizer comments) aligned with the parser's AST list. Unlike sqlForRecord,
// this text is control-plane input and must never be sanitized first.
func schedulingSQLByStatementWithSQLMode(ctx context.Context, sql string, sqlMode string) ([]string, error) {
	if isCmdFieldListSql(sql) || isCmdGetSnapshotTsSql(sql) ||
		isCmdGetDatabasesSql(sql) || isCmdGetMoIndexesSql(sql) ||
		isCmdGetDdlSql(sql) || isCmdGetObjectSql(sql) ||
		isCmdObjectListSql(sql) || isCmdCheckSnapshotFlushedSql(sql) {
		return []string{sql}, nil
	}
	fragments, err := parsers.SplitSqlByStatementWithSQLMode(ctx, sql, sqlMode)
	if err != nil {
		return nil, err
	}
	byStatement := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		if parsers.FragmentHasStatement(fragment) {
			byStatement = append(byStatement, fragment)
		}
	}
	if len(byStatement) == 0 {
		return []string{sql}, nil
	}
	return byStatement, nil
}

func checkNodeCanCache(p *plan2.Plan) bool {
	if p == nil {
		return true
	}
	if q, ok := p.Plan.(*plan2.Plan_Query); ok {
		if q.Query.GetHasForeignKeyAction() {
			return false
		}
		for _, node := range q.Query.Nodes {
			if node.NotCacheable {
				return false
			}
			if node.ObjRef != nil && len(node.ObjRef.SubscriptionName) > 0 {
				return false
			}
		}
	}
	return true
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func wrapNativePrepareSQL(name, materializedSQL string) string {
	trimmed := strings.TrimLeft(materializedSQL, " \t\r\n\f")
	if strings.HasPrefix(trimmed, "/*+") || strings.HasPrefix(trimmed, "/*!+") {
		if end := strings.Index(trimmed, "*/"); end >= 0 {
			content, ok := leadingHintContent(trimmed)
			content = strings.TrimSpace(content)
			var policy map[string]json.RawMessage
			decodeErr := json.Unmarshal([]byte(content), &policy)
			isPolicy := policy["rewrites"] != nil || policy["remapdb"] != nil
			if ok && strings.HasPrefix(content, "{") && (decodeErr != nil || isPolicy) {
				end += 2
				return fmt.Sprintf("%s prepare %s from %s", trimmed[:end], quotePrepareStmtName(name),
					strings.TrimSpace(trimmed[end:]))
			}
		}
	}
	return fmt.Sprintf("prepare %s from %s", quotePrepareStmtName(name), materializedSQL)
}

func validateNativePrepareJSONHints(ctx context.Context, materializedSQL string, lowerCaseTableNames int64) error {
	rest := strings.TrimLeft(materializedSQL, " \t\r\n\f")
	for strings.HasPrefix(rest, "/*+") || strings.HasPrefix(rest, "/*!+") {
		contentStart := 3
		if strings.HasPrefix(rest, "/*!+") {
			contentStart = 4
		}
		end := strings.Index(rest[contentStart:], "*/")
		if end < 0 {
			break
		}
		end += contentStart
		content := strings.TrimSpace(rest[contentStart:end])
		if strings.HasPrefix(content, "{") {
			if _, _, err := parsers.DecodeRewriteHintWithLowerCaseTableNames(
				ctx, content, lowerCaseTableNames); err != nil {
				return err
			}
		}
		rest = strings.TrimLeft(rest[end+2:], " \t\r\n\f")
	}
	return nil
}

func newBinaryExecuteUserInput(sql string, prepareStmt *PrepareStmt, cursorRequested bool) *UserInput {
	return &UserInput{
		sql: sql, stmtName: prepareStmt.Name, stmt: prepareStmt.PrepareStmt,
		preparePlan: prepareStmt.PreparePlan, isBinaryProtExecute: true,
		preparedDefaultDatabase: prepareStmt.defaultDatabase,
		isCursorExecute:         cursorRequested,
		remapDb:                 prepareStmt.remapDb,
	}
}

func ExecRequest(ses *Session, execCtx *ExecCtx, req *Request) (resp *Response, err error) {
	defer func() {
		if e := recover(); e != nil {
			// A cursor callback may panic after reserving its batch budget. Close
			// the statement-owned spool before converting the panic to a client
			// error so retained rows and the session accounting are released.
			if execCtx != nil && execCtx.prepareStmt != nil {
				execCtx.prepareStmt.closeCursor()
			}
			markRowCountFailed(ses, ses.GetProc())
			var serverStatus uint16
			if txnHandler := ses.GetTxnHandler(); txnHandler != nil {
				serverStatus = txnHandler.GetServerStatus()
			}
			moe, ok := e.(*moerr.Error)
			if !ok {
				err = errors.Join(err, moerr.ConvertPanicError(execCtx.reqCtx, e))
				resp = NewGeneralErrorResponse(COM_QUERY, serverStatus, err)
			} else {
				err = errors.Join(err, moe)
				resp = NewGeneralErrorResponse(COM_QUERY, serverStatus, moe)
			}
			// log the query's statement and error info.
			logStatementStatus(execCtx.reqCtx, ses, execCtx.stmt, fail, err)
		}
	}()
	_, _, _ = fault.TriggerFaultInDomain(fault.DomainFrontend, "exec_request_panic")

	ses.EnterFPrint(FPExecRequest)
	defer ses.ExitFPrint(FPExecRequest)

	var span trace.Span
	execCtx.reqCtx, span = trace.Start(execCtx.reqCtx, "ExecRequest",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	var sql string
	ses.Debugf(execCtx.reqCtx, "cmd %v", req.GetCmd())
	ses.SetCmd(req.GetCmd())
	switch req.GetCmd() {
	case COM_QUIT:
		return resp, moerr.GetMysqlClientQuit()
	case COM_QUERY:
		var query = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		// SIDECAR is an explicit statement selector. Keep the raw request intact
		// so doComQuery can bind each hint to its own computation wrapper; a
		// request-scoped marker would leak into unhinted sibling statements.
		ses.addSqlCount(1)
		// Freeze the policy once, then let doComQuery materialize it under the
		// SQL mode current for each staged statement.
		rewritePolicy, rewriteErr := captureRewritePolicy(execCtx.reqCtx, ses)
		if rewriteErr != nil {
			ses.resetDiagnostics()
			markRowCountFailed(ses, ses.GetProc())
			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus(), rewriteErr)
			return resp, nil
		}
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(commonutil.Abbreviate(query, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))))
		input := &UserInput{sql: query, rewritePolicy: rewritePolicy}
		err = doComQuery(ses, execCtx, input)
		if err != nil {
			markRowCountFailed(ses, ses.GetProc())
			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus(), err)
			resp.isIssue3482 = input.isIssue3482Sql()
			if resp.isIssue3482 {
				resp.loadLocalFile = query
			}
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(ses, execCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, ses.GetTxnHandler().GetServerStatus(), err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(ses, execCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, ses.GetTxnHandler().GetServerStatus(), err)
		}

		return resp, nil
	case COM_PING:
		setRowCount(ses, ses.GetProc(), 0)
		resp = NewGeneralOkResponse(COM_PING, ses.GetTxnHandler().GetServerStatus())

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = commonutil.UnsafeBytesToString(req.GetData().([]byte))
		var preparedRemapDb map[string]string
		// Materialize rewrite rules on the protocol payload before it enters the
		// prepareable_stmt grammar. The resulting AST consumes the hint once.
		if ses.rewriteEnabled.Load() {
			var rewriteErr error
			sql, rewriteErr = rewriteSQL(execCtx.reqCtx, ses, sql)
			if rewriteErr != nil {
				ses.resetDiagnostics()
				markRowCountFailed(ses, ses.GetProc())
				resp = NewGeneralErrorResponse(COM_STMT_PREPARE, ses.GetTxnHandler().GetServerStatus(), rewriteErr)
				return resp, nil
			}
			preparedRemapDb = extractInlineRemapDb(sql)
		}
		if err = validateNativePrepareJSONHints(execCtx.reqCtx, sql, parserLowerCaseTableNames(ses)); err != nil {
			ses.resetDiagnostics()
			markRowCountFailed(ses, ses.GetProc())
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, ses.GetTxnHandler().GetServerStatus(), err)
			return resp, nil
		}
		ses.addSqlCount(1)

		// Keep the protocol acceptance boundary in prepareable_stmt. EXPLAIN is
		// admitted there explicitly; unsupported and empty payloads fail parsing
		// before planning.
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = wrapNativePrepareSQL(newStmtName, sql)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(sql))

		savedRowCount := ses.GetLastAffectedRows()
		err = doComQuery(ses, execCtx, &UserInput{sql: sql, remapDb: preparedRemapDb})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, ses.GetTxnHandler().GetServerStatus(), err)
		} else {
			restoreRowCount(ses, ses.GetProc(), savedRowCount)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		var prepareStmt *PrepareStmt
		sql, prepareStmt, err = parseStmtExecute(execCtx.reqCtx, ses, req.GetData().([]byte))
		if err != nil {
			ses.resetDiagnostics()
			if prepareStmt != nil {
				prepareStmt.closeCursor()
				prepareStmt.clearBinaryParamState(ses.GetProc())
			}
			// MySQL semantics: a failed statement makes the next ROW_COUNT() return -1.
			// This parse failure never reaches doComQuery, so the error defer there
			// does not run; set the state explicitly here.
			markRowCountFailed(ses, ses.GetProc())
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetTxnHandler().GetServerStatus(), err), nil
		}
		cursorRequested := prepareStmt.cursorRequested
		// A new execute invalidates any rows retained by the previous cursor,
		// including a normal (non-cursor) execute of the same statement.
		prepareStmt.closeCursor()
		if cursorRequested {
			if _, ok := prepareStmt.PrepareStmt.(*tree.Select); !ok {
				prepareStmt.clearBinaryParamState(ses.GetProc())
				markRowCountFailed(ses, ses.GetProc())
				return NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetTxnHandler().GetServerStatus(),
					moerr.NewNotSupported(execCtx.reqCtx, "server-side cursors require a SELECT statement")), nil
			}
		}
		if cursorRequested {
			prepareStmt.cursor = newPreparedStmtCursor(ses)
		}
		execCtx.prepareStmt = prepareStmt
		execCtx.prepareColDef = prepareStmt.ColDefData
		err = doComQuery(ses, execCtx, newBinaryExecuteUserInput(sql, prepareStmt, cursorRequested))
		if err != nil {
			prepareStmt.closeCursor()
			markRowCountFailed(ses, ses.GetProc())
			resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetTxnHandler().GetServerStatus(), err)
		} else if cursorRequested && (prepareStmt.cursor == nil || prepareStmt.cursor.result == nil || prepareStmt.cursor.result.GetColumnCount() == 0) {
			// Defensive cleanup for a statement shape that does not use the
			// streaming result-row response path.
			prepareStmt.closeCursor()
		}
		prepareStmt.clearBinaryParamState(ses.GetProc())
		return resp, nil

	case COM_STMT_FETCH:
		ses.SetCmd(COM_STMT_FETCH)
		resp, err = executeStmtFetch(execCtx.reqCtx, ses, req.GetData().([]byte))
		if err != nil || resp != nil && resp.category == ErrorResponse {
			markRowCountFailed(ses, ses.GetProc())
		}
		return resp, err

	case COM_STMT_SEND_LONG_DATA:
		ses.SetCmd(COM_STMT_SEND_LONG_DATA)
		err = parseStmtSendLongData(execCtx.reqCtx, ses, req.GetData().([]byte))
		if err != nil {
			markRowCountFailed(ses, ses.GetProc())
			resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, ses.GetTxnHandler().GetServerStatus(), err)
			return resp, nil
		}
		return nil, nil

	case COM_STMT_CLOSE:
		// rewrite to "deallocate Prepare stmt_name"
		savedRowCount := ses.GetLastAffectedRows()
		data := req.GetData().([]byte)
		if len(data) < 4 {
			restoreRowCount(ses, ses.GetProc(), savedRowCount)
			return NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(),
				moerr.NewInternalError(execCtx.reqCtx, "invalid COM_STMT_CLOSE packet")), nil
		}
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		var preStmt *PrepareStmt
		stmtName := getPrepareStmtName(stmtID)
		preStmt, err = ses.GetPrepareStmt(execCtx.reqCtx, stmtName)
		if err != nil {
			restoreRowCount(ses, ses.GetProc(), savedRowCount)
			return NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(), err), nil
		}
		preStmt.closeCursor()
		prefix := ""
		if preStmt.IsCloudNonuser {
			prefix = "/* cloud_nonuser */"
		}
		sql = fmt.Sprintf("%sdeallocate prepare %s", prefix, stmtName)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(sql))

		// COM_STMT_CLOSE never changes ROW_COUNT(), including deallocation errors.
		err = doComQuery(ses, execCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetTxnHandler().GetServerStatus(), err)
		}
		restoreRowCount(ses, ses.GetProc(), savedRowCount)
		return resp, nil

	case COM_STMT_RESET:
		//Payload of COM_STMT_RESET
		data := req.GetData().([]byte)
		if len(data) < 4 {
			// A malformed (too short) packet is a failed statement: reset
			// ROW_COUNT() to -1 and return an error instead of panicking on the slice.
			markRowCountFailed(ses, ses.GetProc())
			return NewGeneralErrorResponse(COM_STMT_RESET, ses.GetTxnHandler().GetServerStatus(),
				moerr.NewInternalError(execCtx.reqCtx, "invalid COM_STMT_RESET packet")), nil
		}
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		var preStmt *PrepareStmt
		preStmt, err = ses.GetPrepareStmt(execCtx.reqCtx, stmtName)
		if err != nil {
			// MySQL semantics: a failed statement makes the next ROW_COUNT() return -1.
			// This early return never reaches doComQuery, so set the state here.
			markRowCountFailed(ses, ses.GetProc())
			return NewGeneralErrorResponse(COM_STMT_RESET, ses.GetTxnHandler().GetServerStatus(), err), nil
		}
		preStmt.closeCursor()
		prefix := ""
		if preStmt.IsCloudNonuser {
			prefix = "/* cloud_nonuser */"
		}
		sql = fmt.Sprintf("%sreset prepare %s", prefix, stmtName)
		ses.Debug(execCtx.reqCtx, "query trace", logutil.QueryField(sql))
		err = doComQuery(ses, execCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, ses.GetTxnHandler().GetServerStatus(), err)
		} else {
			setRowCount(ses, ses.GetProc(), 0)
		}
		return resp, nil

	case COM_SET_OPTION:
		err = handleSetOption(ses, execCtx, req.GetData().([]byte))
		setRowCount(ses, ses.GetProc(), -1)
		if err != nil {
			return NewGeneralErrorResponse(COM_SET_OPTION, ses.GetTxnHandler().GetServerStatus(), err), nil
		}
		return NewGeneralOkResponse(COM_SET_OPTION, ses.GetTxnHandler().GetServerStatus()), nil

	default:
		markRowCountFailed(ses, ses.GetProc())
		resp = NewGeneralErrorResponse(req.GetCmd(), ses.GetTxnHandler().GetServerStatus(), moerr.NewInternalErrorf(execCtx.reqCtx, "unsupported command. 0x%x", int64(req.GetCmd())))
	}
	return resp, nil
}

func parseStmtExecute(reqCtx context.Context, ses *Session, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(reqCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := getPrepareStmtName(stmtID)
	preStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return "", nil, err
	}

	var sql string
	if preStmt.IsCloudNonuser {
		var buf [128]byte
		b := append(buf[:0], "/* cloud_nonuser */execute "...)
		b = append(b, stmtName...)
		sql = string(b)
	} else {
		var buf [64]byte
		b := append(buf[:0], "execute "...)
		b = append(b, stmtName...)
		sql = string(b)
	}

	ses.Debug(reqCtx, "query trace", logutil.QueryField(sql))
	err = ses.GetResponser().MysqlRrWr().ParseExecuteData(reqCtx, ses.GetProc(), preStmt, data, pos)
	if err != nil {
		return "", preStmt, err
	}
	return sql, preStmt, nil
}

// executeStmtFetch sends the next batch from a read-only prepared cursor.
// COM_STMT_FETCH carries only the statement id and requested row count; result
// metadata was sent by the preceding COM_STMT_EXECUTE response.
func executeStmtFetch(ctx context.Context, ses *Session, data []byte) (*Response, error) {
	if len(data) < 8 {
		return NewGeneralErrorResponse(COM_STMT_FETCH, ses.GetTxnHandler().GetServerStatus(),
			moerr.NewInvalidInput(ctx, "invalid COM_STMT_FETCH packet")), nil
	}
	stmtID := binary.LittleEndian.Uint32(data[:4])
	fetchRows := uint64(binary.LittleEndian.Uint32(data[4:8]))
	stmt, err := ses.GetPrepareStmt(ctx, getPrepareStmtName(stmtID))
	if err != nil {
		return NewGeneralErrorResponse(COM_STMT_FETCH, ses.GetTxnHandler().GetServerStatus(), err), nil
	}
	if stmt.cursor == nil || stmt.cursor.result == nil {
		return NewGeneralErrorResponse(COM_STMT_FETCH, ses.GetTxnHandler().GetServerStatus(),
			moerr.NewInvalidState(ctx, "prepared statement has no active cursor")), nil
	}

	cursor := stmt.cursor
	total := cursor.result.GetRowCount()
	start := cursor.offset
	if start > total {
		start = total
	}
	end := total
	if fetchRows < total-start {
		end = start + fetchRows
	}
	rows := &MysqlResultSet{
		Columns: cursor.result.Columns,
		Data:    cursor.result.Data[start:end],
	}
	if end > start {
		if err = ses.GetResponser().MysqlRrWr().WriteResultSetRow(rows, end-start); err != nil {
			stmt.closeCursor()
			return nil, err
		}
	}
	cursor.offset = end

	status := checkMoreResultSet(ses.getStatusAfterTxnIsEnded(), true)
	status &^= SERVER_STATUS_CURSOR_EXISTS | SERVER_STATUS_LAST_ROW_SENT
	if end >= total {
		status |= SERVER_STATUS_LAST_ROW_SENT
		stmt.closeCursor()
	} else {
		status |= SERVER_STATUS_CURSOR_EXISTS
	}
	if err = ses.GetResponser().MysqlRrWr().WriteEOFOrOK(0, status); err != nil {
		stmt.closeCursor()
		return nil, err
	}
	setRowCount(ses, ses.GetProc(), -1)
	return nil, nil
}

func parseStmtSendLongData(reqCtx context.Context, ses *Session, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(reqCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := getPrepareStmtName(stmtID)
	preStmt, err := ses.GetPrepareStmt(reqCtx, stmtName)
	if err != nil {
		return err
	}

	var sql string
	if preStmt.IsCloudNonuser {
		var buf [128]byte
		b := append(buf[:0], "/* cloud_nonuser */send long data for stmt "...)
		b = append(b, stmtName...)
		sql = string(b)
	} else {
		var buf [64]byte
		b := append(buf[:0], "send long data for stmt "...)
		b = append(b, stmtName...)
		sql = string(b)
	}

	ses.Debug(reqCtx, "query trace", logutil.QueryField(sql))

	err = ses.GetResponser().MysqlRrWr().ParseSendLongData(reqCtx, ses.GetProc(), preStmt, data, pos)
	if err != nil {
		return err
	}
	return nil
}

/*
convert the type in computation engine to the type in mysql.
*/
func convertEngineTypeToMysqlType(ctx context.Context, engineType types.T, col *MysqlColumn) error {
	switch engineType {
	case types.T_any:
		col.SetColumnType(defines.MYSQL_TYPE_NULL)
	case types.T_json:
		col.SetColumnType(defines.MYSQL_TYPE_JSON)
	case types.T_bool:
		col.SetColumnType(defines.MYSQL_TYPE_BOOL)
	case types.T_bit:
		col.SetColumnType(defines.MYSQL_TYPE_BIT)
		col.SetSigned(false)
	case types.T_int8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
	case types.T_uint8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
		col.SetSigned(false)
	case types.T_int16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
	case types.T_uint16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
		col.SetSigned(false)
	case types.T_int32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
	case types.T_uint32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
		col.SetSigned(false)
	case types.T_int64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	case types.T_uint64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		col.SetSigned(false)
	case types.T_float32:
		col.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	case types.T_float64:
		col.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	case types.T_char:
		col.SetColumnType(defines.MYSQL_TYPE_STRING)
	case types.T_varchar:
		col.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	case types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_datalink:
		col.SetColumnType(defines.MYSQL_TYPE_TEXT)
	case types.T_binary:
		col.SetColumnType(defines.MYSQL_TYPE_STRING)
	case types.T_varbinary:
		col.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	case types.T_date:
		col.SetColumnType(defines.MYSQL_TYPE_DATE)
	case types.T_datetime:
		col.SetColumnType(defines.MYSQL_TYPE_DATETIME)
	case types.T_time:
		col.SetColumnType(defines.MYSQL_TYPE_TIME)
	case types.T_timestamp:
		col.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	case types.T_year:
		col.SetColumnType(defines.MYSQL_TYPE_YEAR)
	case types.T_decimal64:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_decimal128:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_decimal256:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_blob:
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	case types.T_text:
		col.SetColumnType(defines.MYSQL_TYPE_TEXT)
	case types.T_geometry, types.T_geometry32:
		col.SetColumnType(defines.MYSQL_TYPE_GEOMETRY)
	case types.T_uuid:
		// Downgrade to string for client compatibility (e.g. Go MySQL driver).
		col.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	case types.T_TS:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_Blockid:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_enum:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_Rowid:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	default:
		return moerr.NewInternalErrorf(ctx, "RunWhileSend : unsupported type %d", engineType)
	}
	return nil
}

func convertMysqlTextTypeToBlobType(col *MysqlColumn) {
	if col.ColumnType() == defines.MYSQL_TYPE_TEXT {
		// MySQL sends the TEXT family using the corresponding BLOB protocol
		// type while retaining the text charset. The length determines which
		// family member the client should expose.
		switch {
		case col.Length() <= types.MaxTinyTextLen:
			col.SetColumnType(defines.MYSQL_TYPE_TINY_BLOB)
		case col.Length() <= types.MaxStringSize:
			col.SetColumnType(defines.MYSQL_TYPE_BLOB)
		case col.Length() <= types.MaxMediumTextLen:
			col.SetColumnType(defines.MYSQL_TYPE_MEDIUM_BLOB)
		default:
			col.SetColumnType(defines.MYSQL_TYPE_LONG_BLOB)
		}
		col.SetFlag(col.Flag() | uint16(defines.BLOB_FLAG))
	}
}

// build plan json when marhal plan error
func buildErrorJsonPlan(buffer *bytes.Buffer, uuid uuid.UUID, errcode uint16, msg string) []byte {
	var bytes [36]byte
	commonutil.EncodeUUIDHex(bytes[:], uuid[:])
	explainData := models.ExplainData{
		Code:    errcode,
		Message: msg,
		Uuid:    commonutil.UnsafeBytesToString(bytes[:]),
	}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(explainData)
	return buffer.Bytes()
}

type jsonPlanHandler struct {
	jsonBytes              []byte
	statsBytes             statistic.StatsArray
	stats                  motrace.Statistic
	buffer                 *bytes.Buffer
	persistSchedulingTrace bool
	marshalHandler         *marshalPlanHandler
}

func NewJsonPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, ses FeSession, plan *plan2.Plan, phyPlan *models.PhyPlan, opts ...marshalPlanOptions) *jsonPlanHandler {
	h := NewMarshalPlanHandler(ctx, stmt, plan, phyPlan, opts...)
	statsBytes, stats := h.Stats(ctx, ses)
	var staticJSON []byte
	if h.marshalPlan == nil && (h.schedulingTrace == nil || !h.schedulingTrace.PersistStandalone()) {
		if h.query != nil {
			staticJSON = sqlQueryIgnoreExecPlan
		} else {
			staticJSON = sqlQueryNoRecordExecPlan
		}
	}
	// The terminal resource refresh only needs the materialized ExplainData.
	// Do not retain the statement or logical plan while the record waits for
	// asynchronous export.
	h.stmt = nil
	h.query = nil
	return &jsonPlanHandler{
		jsonBytes:              staticJSON,
		statsBytes:             statsBytes,
		stats:                  stats,
		persistSchedulingTrace: h.persistSchedulingTrace,
		marshalHandler:         h,
	}
}

func newSchedulingTracePlanHandler(ctx context.Context, trace schedule.Trace) *jsonPlanHandler {
	trace = trace.Clone()
	h := &marshalPlanHandler{
		marshalPlanConfig: marshalPlanConfig{
			schedulingTrace: &trace,
		},
	}
	return &jsonPlanHandler{
		statsBytes:     statistic.DefaultStatsArray,
		marshalHandler: h,
	}
}

// SetResourceSummary forwards the sealed summary into the retained marshal
// handler. Marshal remains lazy so the final explain payload is encoded once.
func (h *jsonPlanHandler) SetResourceSummary(summary resource.StatementResourceSummary) {
	if h == nil || h.marshalHandler == nil {
		return
	}
	h.marshalHandler.SetResourceSummary(summary)
}

func (h *jsonPlanHandler) Stats(ctx context.Context) (statistic.StatsArray, motrace.Statistic) {
	return h.statsBytes, h.stats
}

func (h *jsonPlanHandler) Marshal(ctx context.Context) []byte {
	if h.jsonBytes == nil && h.marshalHandler != nil {
		h.jsonBytes = h.marshalHandler.Marshal(ctx)
		h.buffer = h.marshalHandler.handoverBuffer()
	}
	return h.jsonBytes
}

func (h *jsonPlanHandler) Free() {
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
		h.jsonBytes = nil
	}
	h.marshalHandler = nil
}

type marshalPlanConfig struct {
	waitActiveCost          time.Duration
	schedulingTrace         *schedule.Trace
	schedulingTraceRecorder *schedule.TraceRecorder
}

type marshalPlanOptions func(*marshalPlanConfig)

func WithWaitActiveCost(cost time.Duration) marshalPlanOptions {
	return func(h *marshalPlanConfig) {
		h.waitActiveCost = cost
	}
}

func WithSchedulingTrace(trace schedule.Trace) marshalPlanOptions {
	return func(h *marshalPlanConfig) {
		if trace.Empty() {
			return
		}
		cloned := trace.Clone()
		h.schedulingTrace = &cloned
	}
}

func withSchedulingTraceRecorder(recorder *schedule.TraceRecorder) marshalPlanOptions {
	return func(h *marshalPlanConfig) {
		h.schedulingTraceRecorder = recorder
	}
}

type marshalPlanHandler struct {
	query       *plan.Query
	marshalPlan *models.ExplainData
	stmt        *motrace.StatementInfo
	uuid        uuid.UUID
	buffer      *bytes.Buffer
	// internal sub statements, such as sub statements of compound statements,
	// are not user SQL requests and should not emit top-level diagnostics.
	isInternalSubStmt bool

	persistSchedulingTrace bool

	marshalPlanConfig
}

// NewMarshalPlanHandlerCompositeSubStmt builds the legacy diagnostic handler
// used by BackgroundExec projections for child statements.  The handler only
// projects plan statistics; it never publishes them to the statement
// resource root.
func NewMarshalPlanHandlerCompositeSubStmt(ctx context.Context, p *plan.Plan, opts ...marshalPlanOptions) *marshalPlanHandler {
	h := &marshalPlanHandler{isInternalSubStmt: true}
	if p != nil {
		h.query = p.GetQuery()
	}
	for _, opt := range opts {
		opt(&h.marshalPlanConfig)
	}
	return h
}

// SetResourceSummary injects an already sealed summary into a materialized
// explain plan. It intentionally does not touch the live resource root.
func (h *marshalPlanHandler) SetResourceSummary(summary resource.StatementResourceSummary) {
	if h == nil || h.marshalPlan == nil {
		return
	}
	h.marshalPlan.PhyPlan.Resource = &summary
}

func NewMarshalPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan, phyPlan *models.PhyPlan, opts ...marshalPlanOptions) *marshalPlanHandler {
	// TODO: need mem improvement
	uuid := uuid.UUID(stmt.StatementID)
	stmt.MarkResponseAt()
	h := &marshalPlanHandler{
		stmt: stmt,
		uuid: uuid,
	}
	for _, opt := range opts {
		opt(&h.marshalPlanConfig)
	}
	if plan != nil && plan.GetQuery() != nil {
		h.query = plan.GetQuery()
	}
	needFullPlan := h.query != nil && h.needMarshalPlan()
	h.resolveSchedulingTrace(needFullPlan)

	if needFullPlan {
		h.marshalPlan = explain.BuildJsonPlan(ctx, h.uuid, &explain.MarshalPlanOptions, h.query)
		h.marshalPlan.NewPlanStats.SetWaitActiveCost(h.waitActiveCost)
		if phyPlan != nil {
			h.marshalPlan.PhyPlan = *phyPlan.CloneForExport()
		}
		if h.schedulingTrace != nil {
			h.marshalPlan.Scheduling = h.schedulingTrace
		}
	}
	return h
}

func (h *marshalPlanHandler) resolveSchedulingTrace(includeNormalLocal bool) {
	if h.schedulingTrace == nil && h.schedulingTraceRecorder != nil {
		trace := h.schedulingTraceRecorder.SnapshotForExport(includeNormalLocal)
		if !trace.Empty() {
			h.schedulingTrace = &trace
		}
	}
	if h.schedulingTrace != nil {
		h.persistSchedulingTrace = h.schedulingTrace.PersistStandalone()
	}
	// The recorder is only needed while constructing the synchronous handler.
	h.schedulingTraceRecorder = nil
}

// needMarshalPlan return true if statement.duration - waitActive > longQueryTime && NOT mo_logger query
// check longQueryTime, need after StatementInfo.MarkResponseAt
// MoLogger NOT record ExecPlan
func (h *marshalPlanHandler) needMarshalPlan() bool {
	return (h.stmt.Duration-h.waitActiveCost) > motrace.GetLongQueryTime() &&
		!h.stmt.IsMoLogger()
}

func (h *marshalPlanHandler) Free() {
	h.stmt = nil
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
	}
}

func (h *marshalPlanHandler) handoverBuffer() *bytes.Buffer {
	b := h.buffer
	h.buffer = nil
	return b
}

var marshalPlanBufferPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, 8192))
}}

// get buffer from marshalPlanBufferPool
func getMarshalPlanBufferPool() *bytes.Buffer {
	return marshalPlanBufferPool.Get().(*bytes.Buffer)
}

func releaseMarshalPlanBufferPool(b *bytes.Buffer) {
	marshalPlanBufferPool.Put(b)
}

// allocBufferIfNeeded should call just right before needed.
// It will reuse buffer from pool if possible.
func (h *marshalPlanHandler) allocBufferIfNeeded() {
	if h.buffer == nil {
		h.buffer = getMarshalPlanBufferPool()
	}
}

func (h *marshalPlanHandler) Marshal(ctx context.Context) (jsonBytes []byte) {
	var err error
	if h.marshalPlan != nil {
		sanitizeNonFiniteFloatValues(h.marshalPlan)
		h.allocBufferIfNeeded()
		h.buffer.Reset()
		var jsonBytesLen = 0
		// XXX, `buffer` can be used repeatedly as a global variable in the future
		// Provide a relatively balanced initial capacity [8192] for byte slice to prevent multiple memory requests
		encoder := json.NewEncoder(h.buffer)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(h.marshalPlan)
		if err != nil {
			moError := moerr.NewInternalErrorf(ctx, "serialize plan to json error: %s", err.Error())
			h.buffer.Reset()
			jsonBytes = buildErrorJsonPlan(h.buffer, h.uuid, moError.ErrorCode(), moError.Error())
		} else {
			jsonBytesLen = h.buffer.Len()
		}
		// BG: bytes.Buffer maintain buf []byte.
		// if buf[off:] not enough but len(buf) is enough place, then it will reset off = 0.
		// So, in here, we need call Next(...) after all data has been written
		if jsonBytesLen > 0 {
			jsonBytes = h.buffer.Next(jsonBytesLen)
		}
	} else if h.schedulingTrace != nil && h.schedulingTrace.PersistStandalone() {
		h.allocBufferIfNeeded()
		h.buffer.Reset()
		encoder := json.NewEncoder(h.buffer)
		encoder.SetEscapeHTML(false)
		if err = encoder.Encode(struct {
			Scheduling *schedule.Trace `json:"scheduling"`
		}{Scheduling: h.schedulingTrace}); err != nil {
			h.buffer.Reset()
			return sqlQueryIgnoreExecPlan
		}
		return h.buffer.Next(h.buffer.Len())
	} else if h.query != nil {
		// DO NOT use h.buffer
		return sqlQueryIgnoreExecPlan
	} else {
		// DO NOT use h.buffer
		return sqlQueryNoRecordExecPlan
	}
	return
}

func sanitizeNonFiniteFloatValues(v any) {
	sanitizeNonFiniteFloatValue(reflect.ValueOf(v), make(map[uintptr]struct{}))
}

func sanitizeNonFiniteFloatValue(v reflect.Value, seen map[uintptr]struct{}) {
	if !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Interface:
		if !v.IsNil() {
			sanitizeNonFiniteFloatValue(v.Elem(), seen)
		}
	case reflect.Ptr:
		if v.IsNil() {
			return
		}
		ptr := v.Pointer()
		if _, ok := seen[ptr]; ok {
			return
		}
		seen[ptr] = struct{}{}
		sanitizeNonFiniteFloatValue(v.Elem(), seen)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			sanitizeNonFiniteFloatValue(v.Field(i), seen)
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			sanitizeNonFiniteFloatValue(v.Index(i), seen)
		}
	case reflect.Map:
		if v.IsNil() {
			return
		}
		for _, key := range v.MapKeys() {
			value := v.MapIndex(key)
			if !value.IsValid() {
				continue
			}
			copied := reflect.New(value.Type()).Elem()
			copied.Set(value)
			sanitizeNonFiniteFloatValue(copied, seen)
			v.SetMapIndex(key, copied)
		}
	case reflect.Float32, reflect.Float64:
		if !v.CanSet() {
			return
		}
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			v.SetFloat(0)
		}
	}
}

var sqlQueryIgnoreExecPlan = []byte(`{}`)
var sqlQueryNoRecordExecPlan = []byte(`{"code":200,"message":"sql query no record execution plan"}`)

func (h *marshalPlanHandler) Stats(ctx context.Context, ses FeSession) (statsByte statistic.StatsArray, stats motrace.Statistic) {
	statsByte.Reset()
	if h.query != nil {
		options := &explain.MarshalPlanOptions
		for _, node := range h.query.Nodes {
			if h.isInternalSubStmt {
				s := explain.GetStatistic4Trace(ctx, node, options)
				statsByte.Add(&s)
			}
			if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN {
				rows, bytes := explain.GetInputRowsAndInputSize(ctx, node, options)
				stats.RowsRead += rows
				stats.BytesScan += bytes
			}
		}
	} else if h.isInternalSubStmt {
		statsByte = statistic.DefaultStatsArray
	}
	// Top-level statements use the sealed ResourceRoot. Only rows/bytes are
	// projected from the plan; computing the legacy resource formula here would
	// be a shadow accounting path whose result is discarded by ExecPlan2Stats.
	if !h.isInternalSubStmt {
		return
	}
	statsInfo := statistic.StatsInfoFromContext(ctx)
	if statsInfo == nil {
		return
	}
	operatorTimeConsumed := int64(statsByte.GetTimeConsumed())
	totalTime := operatorTimeConsumed +
		int64(statsInfo.ParseStage.ParseDuration) +
		int64(statsInfo.PlanStage.PlanDuration) +
		int64(statsInfo.CompileStage.CompileDuration) +
		statsInfo.PrepareRunStage.ScopePrepareDuration +
		statsInfo.PrepareRunStage.CompilePreRunOnceDuration -
		statsInfo.PrepareRunStage.CompilePreRunOnceWaitLock -
		statsInfo.PlanStage.BuildPlanStatsIOConsumption -
		(statsInfo.IOAccessTimeConsumption + statsInfo.S3FSPrefetchFileIOMergerTimeConsumption)
	if totalTime < 0 {
		if !h.isInternalSubStmt && ses != nil && h.stmt != nil {
			ses.Infof(ctx, "negative cpu statement_id:%s, statement_type:%s", uuid.UUID(h.stmt.StatementID).String(), h.stmt.StatementType)
		}
		v2.GetTraceNegativeCUCounter("cpu").Inc()
	} else {
		statsByte.WithTimeConsumed(float64(totalTime))
	}

	planS3Input := statsInfo.PlanStage.BuildPlanS3Request.CountPUT()
	planS3Output := statsInfo.PlanStage.BuildPlanS3Request.CountGET()
	planS3List := statsInfo.PlanStage.BuildPlanS3Request.CountLIST()
	planS3Delete := statsInfo.PlanStage.BuildPlanS3Request.CountDELETE()
	compileS3Input := statsInfo.CompileStage.CompileS3Request.CountPUT()
	compileS3Output := statsInfo.CompileStage.CompileS3Request.CountGET()
	compileS3List := statsInfo.CompileStage.CompileS3Request.CountLIST()
	compileS3Delete := statsInfo.CompileStage.CompileS3Request.CountDELETE()
	preRunS3Input := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountPUT()
	preRunS3Output := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountGET()
	preRunS3List := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountLIST()
	preRunS3Delete := statsInfo.PrepareRunStage.ScopePrepareS3Request.CountDELETE()
	statsByte.WithS3IOInputCount(statsByte.GetS3IOInputCount() + float64(planS3Input+compileS3Input+preRunS3Input))
	statsByte.WithS3IOOutputCount(statsByte.GetS3IOOutputCount() + float64(planS3Output+compileS3Output+preRunS3Output))
	statsByte.WithS3IOListCount(statsByte.GetS3IOListCount() + float64(planS3List+compileS3List+preRunS3List))
	statsByte.WithS3IODeleteCount(statsByte.GetS3IODeleteCount() + float64(planS3Delete+compileS3Delete+preRunS3Delete))
	statsByte.Add(&statsInfo.PermissionAuth)
	return
}

func handleSetOption(ses *Session, execCtx *ExecCtx, data []byte) (err error) {
	if len(data) < 2 {
		return moerr.NewInternalError(execCtx.reqCtx, "invalid cmd_set_option data length")
	}
	cap := ses.GetResponser().MysqlRrWr().GetU32(CAPABILITY)
	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		// MO do not support CLIENT_MULTI_STATEMENTS in prepare, so do nothing here(Like MySQL)
		// cap |= CLIENT_MULTI_STATEMENTS
		// GetSession().GetMysqlProtocol().SetCapability(cap)

	case 1:
		cap &^= CLIENT_MULTI_STATEMENTS
		ses.GetResponser().MysqlRrWr().SetU32(CAPABILITY, cap)

	default:
		return moerr.NewInternalError(execCtx.reqCtx, "invalid cmd_set_option data")
	}

	return nil
}

func handleExecUpgrade(ses *Session, execCtx *ExecCtx, st *tree.UpgradeStatement) error {
	retryCount := st.Retry
	if st.Retry <= 0 {
		retryCount = 1
	}
	err := ses.UpgradeTenant(execCtx.reqCtx, st.Target.AccountName, uint32(retryCount), st.Target.IsALLAccount)
	if err != nil {
		return err
	}

	return nil
}
