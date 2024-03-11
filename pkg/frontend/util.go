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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

type CloseFlag struct {
	//closed flag
	closed uint32
}

// 1 for closed
// 0 for others
func (cf *CloseFlag) setClosed(value uint32) {
	atomic.StoreUint32(&cf.closed, value)
}

func (cf *CloseFlag) Open() {
	cf.setClosed(0)
}

func (cf *CloseFlag) Close() {
	cf.setClosed(1)
}

func (cf *CloseFlag) IsClosed() bool {
	return atomic.LoadUint32(&cf.closed) != 0
}

func (cf *CloseFlag) IsOpened() bool {
	return atomic.LoadUint32(&cf.closed) == 0
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

// GetRoutineId gets the routine id
func GetRoutineId() uint64 {
	data := make([]byte, 64)
	data = data[:runtime.Stack(data, false)]
	data = bytes.TrimPrefix(data, []byte("goroutine "))
	data = data[:bytes.IndexByte(data, ' ')]
	id, _ := strconv.ParseUint(string(data), 10, 64)
	return id
}

type Timeout struct {
	//last record of the time
	lastTime atomic.Value //time.Time

	//period
	timeGap time.Duration

	//auto update
	autoUpdate bool
}

func NewTimeout(tg time.Duration, autoUpdateWhenChecked bool) *Timeout {
	ret := &Timeout{
		timeGap:    tg,
		autoUpdate: autoUpdateWhenChecked,
	}
	ret.lastTime.Store(time.Now())
	return ret
}

func (t *Timeout) UpdateTime(tn time.Time) {
	t.lastTime.Store(tn)
}

/*
----------+---------+------------------+--------

	lastTime     Now         lastTime + timeGap

return true  :  is timeout. the lastTime has been updated.
return false :  is not timeout. the lastTime has not been updated.
*/
func (t *Timeout) isTimeout() bool {
	if time.Since(t.lastTime.Load().(time.Time)) <= t.timeGap {
		return false
	}

	if t.autoUpdate {
		t.lastTime.Store(time.Now())
	}
	return true
}

/*
length:
-1, complete string.
0, empty string
>0 , length of characters at the header of the string.
*/
func SubStringFromBegin(str string, length int) string {
	if length == 0 || length < -1 {
		return ""
	}

	if length == -1 {
		return str
	}

	l := Min(len(str), length)
	if l != len(str) {
		return str[:l] + "..."
	}
	return str[:l]
}

/*
path exists in the system
return:
true/false - exists or not.
true/false - file or directory
error
*/
var PathExists = func(path string) (bool, bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return true, !fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, false, err
	}

	return false, false, err
}

/*
MakeDebugInfo prints bytes in multi-lines.
*/
func MakeDebugInfo(data []byte, bytesCount int, bytesPerLine int) string {
	if len(data) == 0 || bytesCount == 0 || bytesPerLine == 0 {
		return ""
	}
	pl := Min(bytesCount, len(data))
	ps := ""
	for i := 0; i < pl; i++ {
		if i > 0 && (i%bytesPerLine == 0) {
			ps += "\n"
		}
		if i%bytesPerLine == 0 {
			ps += fmt.Sprintf("%d", i/bytesPerLine) + " : "
		}
		ps += fmt.Sprintf("%02x ", data[i])
	}
	return ps
}

func getSystemVariables(configFile string) (*mo_config.FrontendParameters, error) {
	sv := &mo_config.FrontendParameters{}
	var err error
	_, err = toml.DecodeFile(configFile, sv)
	if err != nil {
		return nil, err
	}
	return sv, err
}

func getParameterUnit(configFile string, eng engine.Engine, txnClient TxnClient) (*mo_config.ParameterUnit, error) {
	sv, err := getSystemVariables(configFile)
	if err != nil {
		return nil, err
	}
	logutil.Info("Using Dump Storage Engine and Cluster Nodes.")
	pu := mo_config.NewParameterUnit(sv, eng, txnClient, engine.Nodes{})

	return pu, nil
}

// WildcardMatch implements wildcard pattern match algorithm.
// pattern and target are ascii characters
// TODO: add \_ and \%
func WildcardMatch(pattern, target string) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := len(pattern)
	tlen := len(target)
	for t < tlen {
		//%
		if p < plen && pattern[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pattern[p] == '_' || pattern[p] == target[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pattern[p] == '%' {
		p++
	}
	return p >= plen
}

// getExprValue executes the expression and returns the value.
func getExprValue(e tree.Expr, mce *MysqlCmdExecutor, ses *Session) (interface{}, error) {
	/*
		CORNER CASE:
			SET character_set_results = utf8; // e = tree.UnresolvedName{'utf8'}.

			tree.UnresolvedName{'utf8'} can not be resolved as the column of some table.
	*/
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		return v.Parts[0], nil
	}

	var err error

	table := &tree.TableName{}
	table.ObjectName = "dual"

	//1.composite the 'select (expr) from dual'
	compositedSelect := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				tree.SelectExpr{
					Expr: e,
				},
			},
			From: &tree.From{
				Tables: tree.TableExprs{
					&tree.JoinTableExpr{
						JoinType: tree.JOIN_TYPE_CROSS,
						Left: &tree.AliasedTableExpr{
							Expr: table,
						},
					},
				},
			},
		},
	}

	//2.run the select
	ctx := ses.GetRequestContext()

	//run the statement in the same session
	ses.ClearResultBatches()
	err = executeStmtInSameSession(ctx, mce, ses, compositedSelect)
	if err != nil {
		return nil, err
	}

	batches := ses.GetResultBatches()
	if len(batches) == 0 {
		return nil, moerr.NewInternalError(ctx, "the expr %s does not generate a value", e.String())
	}

	if batches[0].VectorCount() > 1 {
		return nil, moerr.NewInternalError(ctx, "the expr %s generates multi columns value", e.String())
	}

	//evaluate the count of rows, the count of columns
	count := 0
	var resultVec *vector.Vector
	for _, b := range batches {
		if b.RowCount() == 0 {
			continue
		}
		count += b.RowCount()
		if count > 1 {
			return nil, moerr.NewInternalError(ctx, "the expr %s generates multi rows value", e.String())
		}
		if resultVec == nil && b.GetVector(0).Length() != 0 {
			resultVec = b.GetVector(0)
		}
	}

	if resultVec == nil {
		return nil, moerr.NewInternalError(ctx, "the expr %s does not generate a value", e.String())
	}

	// for the decimal type, we need the type of expr
	//!!!NOTE: the type here may be different from the one in the result vector.
	var planExpr *plan.Expr
	oid := resultVec.GetType().Oid
	if oid == types.T_decimal64 || oid == types.T_decimal128 {
		builder := plan2.NewQueryBuilder(plan.Query_SELECT, ses.GetTxnCompileCtx(), false)
		bindContext := plan2.NewBindContext(builder, nil)
		binder := plan2.NewSetVarBinder(builder, bindContext)
		planExpr, err = binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
	}

	return getValueFromVector(resultVec, ses, planExpr)
}

// only support single value and unary minus
func GetSimpleExprValue(e tree.Expr, ses *Session) (interface{}, error) {
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		return v.Parts[0], nil
	default:
		builder := plan2.NewQueryBuilder(plan.Query_SELECT, ses.GetTxnCompileCtx(), false)
		bindContext := plan2.NewBindContext(builder, nil)
		binder := plan2.NewSetVarBinder(builder, bindContext)
		planExpr, err := binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
		// set @a = 'on', type of a is bool. And mo cast rule does not fit set variable rule so delay to convert type.
		// Here the evalExpr may execute some function that needs engine.Engine.
		ses.txnCompileCtx.GetProcess().Ctx = context.WithValue(ses.txnCompileCtx.GetProcess().Ctx, defines.EngineKey{}, ses.storage)

		vec, err := colexec.EvalExpressionOnce(ses.txnCompileCtx.GetProcess(), planExpr, []*batch.Batch{batch.EmptyForConstFoldBatch})
		if err != nil {
			return nil, err
		}

		value, err := getValueFromVector(vec, ses, planExpr)
		vec.Free(ses.txnCompileCtx.GetProcess().Mp())
		return value, err
	}
}

func getValueFromVector(vec *vector.Vector, ses *Session, expr *plan2.Expr) (interface{}, error) {
	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return nil, nil
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.MustFixedCol[bool](vec)[0], nil
	case types.T_bit:
		return vector.MustFixedCol[uint64](vec)[0], nil
	case types.T_int8:
		return vector.MustFixedCol[int8](vec)[0], nil
	case types.T_int16:
		return vector.MustFixedCol[int16](vec)[0], nil
	case types.T_int32:
		return vector.MustFixedCol[int32](vec)[0], nil
	case types.T_int64:
		return vector.MustFixedCol[int64](vec)[0], nil
	case types.T_uint8:
		return vector.MustFixedCol[uint8](vec)[0], nil
	case types.T_uint16:
		return vector.MustFixedCol[uint16](vec)[0], nil
	case types.T_uint32:
		return vector.MustFixedCol[uint32](vec)[0], nil
	case types.T_uint64:
		return vector.MustFixedCol[uint64](vec)[0], nil
	case types.T_float32:
		return vector.MustFixedCol[float32](vec)[0], nil
	case types.T_float64:
		return vector.MustFixedCol[float64](vec)[0], nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob:
		return vec.GetStringAt(0), nil
	case types.T_array_float32:
		return vector.GetArrayAt[float32](vec, 0), nil
	case types.T_array_float64:
		return vector.GetArrayAt[float64](vec, 0), nil
	case types.T_decimal64:
		val := vector.GetFixedAt[types.Decimal64](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_decimal128:
		val := vector.GetFixedAt[types.Decimal128](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_json:
		val := vec.GetBytesAt(0)
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.MustFixedCol[types.Uuid](vec)[0]
		return val.ToString(), nil
	case types.T_date:
		val := vector.MustFixedCol[types.Date](vec)[0]
		return val.String(), nil
	case types.T_time:
		val := vector.MustFixedCol[types.Time](vec)[0]
		return val.String(), nil
	case types.T_datetime:
		val := vector.MustFixedCol[types.Datetime](vec)[0]
		return val.String(), nil
	case types.T_timestamp:
		val := vector.MustFixedCol[types.Timestamp](vec)[0]
		return val.String2(ses.GetTimeZone(), vec.GetType().Scale), nil
	case types.T_enum:
		return vector.MustFixedCol[types.Enum](vec)[0], nil
	default:
		return nil, moerr.NewInvalidArg(ses.GetRequestContext(), "variable type", vec.GetType().Oid.String())
	}
}

type statementStatus int

const (
	success statementStatus = iota
	fail
	sessionId = "session_id"

	txnId       = "txn_id"
	statementId = "statement_id"
)

func (s statementStatus) String() string {
	switch s {
	case success:
		return "success"
	case fail:
		return "fail"
	}
	return "running"
}

// logStatementStatus prints the status of the statement into the log.
func logStatementStatus(ctx context.Context, ses *Session, stmt tree.Statement, status statementStatus, err error) {
	var stmtStr string
	stm := motrace.StatementFromContext(ctx)
	if stm == nil {
		fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
		stmt.Format(fmtCtx)
		stmtStr = fmtCtx.String()
	} else {
		stmtStr = stm.Statement
	}
	logStatementStringStatus(ctx, ses, stmtStr, status, err)
}

func logStatementStringStatus(ctx context.Context, ses *Session, stmtStr string, status statementStatus, err error) {
	str := SubStringFromBegin(stmtStr, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
	outBytes, outPacket := ses.GetMysqlProtocol().CalculateOutTrafficBytes(true)
	if status == success {
		logDebug(ses, ses.GetDebugString(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), trace.ContextField(ctx))
		err = nil // make sure: it is nil for EndStatement
	} else {
		logError(ses, ses.GetDebugString(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), logutil.ErrorField(err), trace.ContextField(ctx))
	}
	// pls make sure: NO ONE use the ses.tStmt after EndStatement
	motrace.EndStatement(ctx, err, ses.sentRows.Load(), outBytes, outPacket)
	// need just below EndStatement
	ses.SetTStmt(nil)
}

var logger *log.MOLogger
var loggerOnce sync.Once

func getLogger() *log.MOLogger {
	loggerOnce.Do(initLogger)
	return logger
}

func initLogger() {
	rt := moruntime.ProcessLevelRuntime()
	if rt == nil {
		rt = moruntime.DefaultRuntime()
	}
	logger = rt.Logger().Named("frontend")
}

func appendSessionField(fields []zap.Field, ses *Session) []zap.Field {
	if ses != nil {
		if ses.tStmt != nil {
			fields = append(fields, zap.String(sessionId, uuid.UUID(ses.tStmt.SessionID).String()))
			fields = append(fields, zap.String(statementId, uuid.UUID(ses.tStmt.StatementID).String()))
			txnInfo := ses.GetTxnInfo()
			if txnInfo != "" {
				fields = append(fields, zap.String(txnId, txnInfo))
			}
		} else {
			fields = append(fields, zap.String(sessionId, uuid.UUID(ses.GetUUID()).String()))
		}
	}
	return fields
}

func logInfo(ses *Session, info string, msg string, fields ...zap.Field) {
	if ses != nil && ses.tenant != nil && ses.tenant.User == db_holder.MOLoggerUser {
		return
	}
	fields = append(fields, zap.String("session_info", info))
	fields = appendSessionField(fields, ses)
	getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(1), fields...)
}

func logInfof(info string, msg string, fields ...zap.Field) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.InfoLevel) {
		fields = append(fields, zap.String("session_info", info))
		getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.InfoLevel).AddCallerSkip(1), fields...)
	}
}

func logDebug(ses *Session, info string, msg string, fields ...zap.Field) {
	if ses != nil && ses.tenant != nil && ses.tenant.User == db_holder.MOLoggerUser {
		return
	}
	fields = append(fields, zap.String("session_info", info))
	fields = appendSessionField(fields, ses)
	getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.DebugLevel).AddCallerSkip(1), fields...)
}

func logError(ses *Session, info string, msg string, fields ...zap.Field) {
	if ses != nil && ses.tenant != nil && ses.tenant.User == db_holder.MOLoggerUser {
		return
	}
	fields = append(fields, zap.String("session_info", info))
	fields = appendSessionField(fields, ses)
	getLogger().Log(msg, log.DefaultLogOptions().WithLevel(zap.ErrorLevel).AddCallerSkip(1), fields...)
}

// todo: remove this function after all the logDebugf are replaced by logDebug
func logDebugf(info string, msg string, fields ...interface{}) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		fields = append(fields, info)
		logutil.Debugf(msg+" %s", fields...)
	}
}

// isCmdFieldListSql checks the sql is the cmdFieldListSql or not.
func isCmdFieldListSql(sql string) bool {
	if len(sql) < cmdFieldListSqlLen {
		return false
	}
	prefix := sql[:cmdFieldListSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdFieldListSql) == 0
}

// makeCmdFieldListSql makes the internal CMD_FIELD_LIST sql
func makeCmdFieldListSql(query string) string {
	nullIdx := strings.IndexRune(query, rune(0))
	if nullIdx != -1 {
		query = query[:nullIdx]
	}
	return cmdFieldListSql + " " + query
}

// parseCmdFieldList parses the internal cmd field list
func parseCmdFieldList(ctx context.Context, sql string) (*InternalCmdFieldList, error) {
	if !isCmdFieldListSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the CMD_FIELD_LIST")
	}
	tableName := strings.TrimSpace(sql[len(cmdFieldListSql):])
	return &InternalCmdFieldList{tableName: tableName}, nil
}

func getVariableValue(varDefault interface{}) string {
	switch val := varDefault.(type) {
	case int64:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case float64:
		// 0.1 => 0.100000
		// 0.0000001 -> 1.000000e-7
		if val >= 1e-6 {
			return fmt.Sprintf("%.6f", val)
		} else {
			return fmt.Sprintf("%.6e", val)
		}
	case string:
		return val
	default:
		return ""
	}
}

func makeServerVersion(pu *mo_config.ParameterUnit, version string) string {
	return pu.SV.ServerVersionPrefix + version
}

func copyBytes(src []byte, needCopy bool) []byte {
	if needCopy {
		if len(src) > 0 {
			dst := make([]byte, len(src))
			copy(dst, src)
			return dst
		} else {
			return []byte{}
		}
	}
	return src
}

// getUserProfile returns the account, user, role of the account
func getUserProfile(account *TenantInfo) (string, string, string) {
	var (
		accountName string
		userName    string
		roleName    string
	)

	if account != nil {
		accountName = account.GetTenant()
		userName = account.GetUser()
		roleName = account.GetDefaultRole()
	} else {
		accountName = sysAccountName
		userName = rootName
		roleName = moAdminRoleName
	}
	return accountName, userName, roleName
}

// RewriteError rewrites the error info
func RewriteError(err error, username string) (uint16, string, string) {
	if err == nil {
		return moerr.ER_INTERNAL_ERROR, "", ""
	}
	var errorCode uint16
	var sqlState string
	var msg string

	errMsg := strings.ToLower(err.Error())
	if needConvertedToAccessDeniedError(errMsg) {
		failed := moerr.MysqlErrorMsgRefer[moerr.ER_ACCESS_DENIED_ERROR]
		if len(username) > 0 {
			tipsFormat := "Access denied for user %s. %s"
			msg = fmt.Sprintf(tipsFormat, getUserPart(username), err.Error())
		} else {
			msg = err.Error()
		}
		errorCode = failed.ErrorCode
		sqlState = failed.SqlStates[0]
	} else {
		//Reference To : https://github.com/matrixorigin/matrixone/pull/12396/files#r1374443578
		switch errImpl := err.(type) {
		case *moerr.Error:
			if errImpl.MySQLCode() != moerr.ER_UNKNOWN_ERROR {
				errorCode = errImpl.MySQLCode()
			} else {
				errorCode = errImpl.ErrorCode()
			}
			msg = err.Error()
			sqlState = errImpl.SqlState()
		default:
			failed := moerr.MysqlErrorMsgRefer[moerr.ER_INTERNAL_ERROR]
			msg = err.Error()
			errorCode = failed.ErrorCode
			sqlState = failed.SqlStates[0]
		}

	}
	return errorCode, sqlState, msg
}

func needConvertedToAccessDeniedError(errMsg string) bool {
	if strings.Contains(errMsg, "check password failed") ||
		/*
			following two cases are suggested by the peers from the mo cloud team.
			we keep the consensus with them.
		*/
		strings.Contains(errMsg, "suspended") ||
		strings.Contains(errMsg, "source address") &&
			strings.Contains(errMsg, "is not authorized") {
		return true
	}
	return false
}

const (
	quitStr = "!!!COM_QUIT!!!"
)

// makeExecuteSql appends the PREPARE sql and its values of parameters for the EXECUTE statement.
// Format 1: execute ... using ...
// execute.... // prepare stmt1 from .... ; set var1 = val1 ; set var2 = val2 ;
// Format 2: COM_STMT_EXECUTE
// execute.... // prepare stmt1 from .... ; param0 ; param1 ...
func makeExecuteSql(ses *Session, stmt tree.Statement) string {
	if ses == nil || stmt == nil {
		return ""
	}
	preSql := ""
	bb := &strings.Builder{}
	//fill prepare parameters
	switch t := stmt.(type) {
	case *tree.Execute:
		name := string(t.Name)
		prepareStmt, err := ses.GetPrepareStmt(name)
		if err != nil || prepareStmt == nil {
			break
		}
		preSql = strings.TrimSpace(prepareStmt.Sql)
		bb.WriteString(preSql)
		bb.WriteString(" ; ")
		if len(t.Variables) != 0 {
			//for EXECUTE ... USING statement. append variables if there is.
			//get SET VAR sql
			setVarSqls := make([]string, len(t.Variables))
			for i, v := range t.Variables {
				_, userVal, err := ses.GetUserDefinedVar(v.Name)
				if err == nil && userVal != nil && len(userVal.Sql) != 0 {
					setVarSqls[i] = userVal.Sql
				}
			}
			bb.WriteString(strings.Join(setVarSqls, " ; "))
		} else if prepareStmt.params != nil {
			//for COM_STMT_EXECUTE
			//get value of parameters
			paramCnt := prepareStmt.params.Length()
			paramValues := make([]string, paramCnt)
			vs := vector.MustFixedCol[types.Varlena](prepareStmt.params)
			for i := 0; i < paramCnt; i++ {
				isNull := prepareStmt.params.GetNulls().Contains(uint64(i))
				if isNull {
					paramValues[i] = "NULL"
				} else {
					paramValues[i] = vs[i].GetString(prepareStmt.params.GetArea())
				}
			}
			bb.WriteString(strings.Join(paramValues, " ; "))
		}
	default:
		return ""
	}
	return bb.String()
}

func mysqlColDef2PlanResultColDef(mr *MysqlResultSet) *plan.ResultColDef {
	if mr == nil {
		return nil
	}

	resultCols := make([]*plan.ColDef, len(mr.Columns))
	for i, col := range mr.Columns {
		resultCols[i] = &plan.ColDef{
			Name: col.Name(),
		}
		switch col.ColumnType() {
		case defines.MYSQL_TYPE_VAR_STRING:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_varchar),
			}
		case defines.MYSQL_TYPE_LONG:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_int32),
			}
		case defines.MYSQL_TYPE_LONGLONG:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_int64),
			}
		case defines.MYSQL_TYPE_DOUBLE:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_float64),
			}
		case defines.MYSQL_TYPE_FLOAT:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_float32),
			}
		case defines.MYSQL_TYPE_DATE:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_date),
			}
		case defines.MYSQL_TYPE_TIME:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_time),
			}
		case defines.MYSQL_TYPE_DATETIME:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_datetime),
			}
		case defines.MYSQL_TYPE_TIMESTAMP:
			resultCols[i].Typ = plan.Type{
				Id: int32(types.T_timestamp),
			}
		default:
			panic(fmt.Sprintf("unsupported mysql type %d", col.ColumnType()))
		}
	}
	return &plan.ResultColDef{
		ResultCols: resultCols,
	}
}

// errCodeRollbackWholeTxn denotes that the error code
// that should rollback the whole txn
var errCodeRollbackWholeTxn = map[uint16]bool{
	moerr.ErrDeadLockDetected:     false,
	moerr.ErrLockTableBindChanged: false,
	moerr.ErrLockTableNotFound:    false,
	moerr.ErrDeadlockCheckBusy:    false,
	moerr.ErrLockConflict:         false,
}

func isErrorRollbackWholeTxn(inputErr error) bool {
	if inputErr == nil {
		return false
	}
	me, ok := inputErr.(*moerr.Error)
	if !ok {
		// This is not a moerr
		return false
	}
	if _, has := errCodeRollbackWholeTxn[me.ErrorCode()]; has {
		return true
	}
	return false
}

func getRandomErrorRollbackWholeTxn() error {
	rand.NewSource(time.Now().UnixNano())
	x := rand.Intn(len(errCodeRollbackWholeTxn))
	arr := make([]uint16, 0, len(errCodeRollbackWholeTxn))
	for k := range errCodeRollbackWholeTxn {
		arr = append(arr, k)
	}
	switch arr[x] {
	case moerr.ErrDeadLockDetected:
		return moerr.NewDeadLockDetectedNoCtx()
	case moerr.ErrLockTableBindChanged:
		return moerr.NewLockTableBindChangedNoCtx()
	case moerr.ErrLockTableNotFound:
		return moerr.NewLockTableNotFoundNoCtx()
	case moerr.ErrDeadlockCheckBusy:
		return moerr.NewDeadlockCheckBusyNoCtx()
	case moerr.ErrLockConflict:
		return moerr.NewLockConflictNoCtx()
	default:
		panic(fmt.Sprintf("usp error code %d", arr[x]))
	}
}
