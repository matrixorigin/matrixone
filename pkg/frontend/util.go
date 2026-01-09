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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"github.com/petermattis/goid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

const (
	invalidGoroutineId = math.MaxInt64
)

// GetRoutineId gets the routine id
func GetRoutineId() uint64 {
	id := goid.Get()
	if id == 0 {
		id = invalidGoroutineId
	}
	return uint64(id)
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
	sv.SetDefaultValues()
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
func getExprValue(e tree.Expr, ses *Session, execCtx *ExecCtx) (interface{}, error) {
	/*
		CORNER CASE:
			SET character_set_results = utf8; // e = tree.UnresolvedName{'utf8'}.

			tree.UnresolvedName{'utf8'} can not be resolved as the column of some table.
	*/
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		return v.ColName(), nil
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

	//run the statement in the same session
	ses.ClearResultBatches()
	//!!!different ExecCtx
	tempExecCtx := ExecCtx{
		reqCtx: execCtx.reqCtx,
		ses:    ses,
	}
	defer tempExecCtx.Close()
	err = executeStmtInSameSession(tempExecCtx.reqCtx, ses, &tempExecCtx, compositedSelect)
	if err != nil {
		return nil, err
	}

	batches := ses.GetResultBatches()
	if len(batches) == 0 {
		return nil, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s does not generate a value", e.String())
	}

	if batches[0].VectorCount() > 1 {
		return nil, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s generates multi columns value", e.String())
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
			return nil, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s generates multi rows value", e.String())
		}
		if resultVec == nil && b.GetVector(0).Length() != 0 {
			resultVec = b.GetVector(0)
		}
	}

	if resultVec == nil {
		return nil, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s does not generate a value", e.String())
	}

	// for the decimal type, we need the type of expr
	//!!!NOTE: the type here may be different from the one in the result vector.
	var planExpr *plan.Expr
	oid := resultVec.GetType().Oid
	if oid == types.T_decimal64 || oid == types.T_decimal128 {
		builder := plan2.NewQueryBuilder(plan.Query_SELECT, ses.GetTxnCompileCtx(), false, false)
		bindContext := plan2.NewBindContext(builder, nil)
		binder := plan2.NewSetVarBinder(builder, bindContext)
		planExpr, err = binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
	}

	return getValueFromVector(execCtx.reqCtx, resultVec, ses, planExpr)
}

// only support single value and unary minus
func GetSimpleExprValue(ctx context.Context, e tree.Expr, feSes FeSession) (interface{}, error) {
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		return v.ColName(), nil
	default:
		builder := plan2.NewQueryBuilder(plan.Query_SELECT, feSes.GetTxnCompileCtx(), false, false)
		bindContext := plan2.NewBindContext(builder, nil)
		binder := plan2.NewSetVarBinder(builder, bindContext)
		planExpr, err := binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}

		txnCompileCtx := feSes.GetTxnCompileCtx()
		// set @a = 'on', type of a is bool. And mo cast rule does not fit set variable rule so delay to convert type.
		// Here the evalExpr may execute some function that needs engine.Engine.
		txnCompileCtx.GetProcess().ReplaceTopCtx(
			attachValue(txnCompileCtx.GetProcess().GetTopContext(),
				defines.EngineKey{},
				feSes.GetTxnHandler().GetStorage()))

		vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(txnCompileCtx.GetProcess(), planExpr)
		if err != nil {
			return nil, err
		}

		value, err := getValueFromVector(ctx, vec, feSes, planExpr)
		free()
		return value, err
	}
}

func getValueFromVector(ctx context.Context, vec *vector.Vector, feSes FeSession, expr *plan2.Expr) (interface{}, error) {
	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return nil, nil
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.MustFixedColNoTypeCheck[bool](vec)[0], nil
	case types.T_bit:
		return vector.MustFixedColNoTypeCheck[uint64](vec)[0], nil
	case types.T_int8:
		return vector.MustFixedColNoTypeCheck[int8](vec)[0], nil
	case types.T_int16:
		return vector.MustFixedColNoTypeCheck[int16](vec)[0], nil
	case types.T_int32:
		return vector.MustFixedColNoTypeCheck[int32](vec)[0], nil
	case types.T_int64:
		return vector.MustFixedColNoTypeCheck[int64](vec)[0], nil
	case types.T_uint8:
		return vector.MustFixedColNoTypeCheck[uint8](vec)[0], nil
	case types.T_uint16:
		return vector.MustFixedColNoTypeCheck[uint16](vec)[0], nil
	case types.T_uint32:
		return vector.MustFixedColNoTypeCheck[uint32](vec)[0], nil
	case types.T_uint64:
		return vector.MustFixedColNoTypeCheck[uint64](vec)[0], nil
	case types.T_float32:
		return vector.MustFixedColNoTypeCheck[float32](vec)[0], nil
	case types.T_float64:
		return vector.MustFixedColNoTypeCheck[float64](vec)[0], nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		return vec.GetStringAt(0), nil
	case types.T_array_float32:
		return vector.GetArrayAt[float32](vec, 0), nil
	case types.T_array_float64:
		return vector.GetArrayAt[float64](vec, 0), nil
	case types.T_decimal64:
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_decimal128:
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_json:
		val := vec.GetBytesAt(0)
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.MustFixedColNoTypeCheck[types.Uuid](vec)[0]
		return val.String(), nil
	case types.T_date:
		val := vector.MustFixedColNoTypeCheck[types.Date](vec)[0]
		return val.String(), nil
	case types.T_time:
		val := vector.MustFixedColNoTypeCheck[types.Time](vec)[0]
		return val.String(), nil
	case types.T_datetime:
		val := vector.MustFixedColNoTypeCheck[types.Datetime](vec)[0]
		return val.String(), nil
	case types.T_timestamp:
		val := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[0]
		return val.String2(feSes.GetTimeZone(), vec.GetType().Scale), nil
	case types.T_year:
		val := vector.MustFixedColNoTypeCheck[types.MoYear](vec)[0]
		return val.String(), nil
	case types.T_enum:
		return vector.MustFixedColNoTypeCheck[types.Enum](vec)[0], nil
	default:
		return nil, moerr.NewInvalidArg(ctx, "variable type", vec.GetType().Oid.String())
	}
}

type statementStatus int

const (
	success statementStatus = iota
	fail
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
func logStatementStatus(
	ctx context.Context,
	ses FeSession,
	_ tree.Statement,
	status statementStatus,
	err error,
) {
	logStatementStringStatus(ctx, ses, "", status, err)
}

// logStatementStringStatus
// if stmtStr == "", get the query statement from FeSession or motrace.StatementInfo
// (which migrate from logStatementStatus).
// This op is aim to avoid string copy in 'status == success' case.
func logStatementStringStatus(
	ctx context.Context,
	ses FeSession,
	stmtStr string,
	status statementStatus,
	err error,
) {
	var outBytes, outPacket int64
	var getFormatedSqlStr = func() string {
		var str = stmtStr
		if len(stmtStr) == 0 {
			if stm := ses.GetStmtInfo(); stm == nil {
				str = ses.GetSqlOfStmt()
			} else {
				// case `execute __prepared_stmt_id__;`: this value holds the raw prepare statement and raw args.
				str = stm.CopyStatementInfo()
			}
		}
		str = commonutil.Abbreviate(str, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
		return str
	}
	switch resper := ses.GetResponser().(type) {
	case *MysqlResp:
		outBytes, outPacket = resper.mysqlRrWr.CalculateOutTrafficBytes(true)
	default:
	}

	if status == success {
		if ses.LogDebug() {
			str := getFormatedSqlStr()
			ses.Debug(ctx, "query trace status", logutil.StatementField(str), logutil.StatusField(status.String()))
		}
		err = nil // make sure: it is nil for EndStatement
	} else {
		str := getFormatedSqlStr()
		ses.Error(
			ctx,
			"query trace status",
			logutil.StatementField(str),
			logutil.StatusField(status.String()),
			logutil.ErrorField(err),
			logutil.TxnInfoField(ses.GetStaticTxnInfo()),
		)
	}

	// pls make sure: NO ONE use the ses.tStmt after EndStatement
	if !ses.IsBackgroundSession() {
		if stmt := ses.GetStmtInfo(); stmt != nil {
			stmt.EndStatement(ctx, err, ses.SendRows(), outBytes, outPacket)
		}
	}
	// need just below EndStatement
	ses.SetTStmt(nil)
}

func getLogger(sid string) *log.MOLogger {
	return moruntime.GetLogger(sid)
}

// appendSessionField append session id, transaction id and statement id to the fields
// history:
// #15877, discard ses.GetTxnInfo(), it need ses.Lock(). may cause deadlock: locked by itself.
// #16028, depend on ses.GetStmtProfile() itself do the log. get rid of StatementInfo.
func appendSessionField(fields []zap.Field, ses FeSession) []zap.Field {
	if ses != nil {
		fields = append(fields, logutil.SessionIdField(uuid.UUID(ses.GetUUID()).String()))
		p := ses.GetStmtProfile()
		if p.GetStmtId() != dumpUUID {
			fields = append(fields, logutil.StatementIdField(uuid.UUID(p.GetStmtId()).String()))
		}
		if txnId := p.GetTxnId(); txnId != dumpUUID {
			fields = append(fields, logutil.TxnIdField(hex.EncodeToString(txnId[:])))
		}
	}
	return fields
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
	quitStr = "MysqlClientQuit"
)

// makeExecuteSql appends the PREPARE sql and its values of parameters for the EXECUTE statement.
// Format 1: execute ... using ...
// execute.... // prepare stmt1 from .... ; set var1 = val1 ; set var2 = val2 ;
// Format 2: COM_STMT_EXECUTE
// execute.... // prepare stmt1 from .... ; param0 ; param1 ...
func makeExecuteSql(ctx context.Context, ses *Session, stmt tree.Statement, binExec bool, prepareName string) string {
	if ses == nil || stmt == nil {
		return ""
	}
	isExec := false
	name := ""
	var Variables []*tree.VarExpr
	if binExec {
		isExec = true
		name = prepareName
	} else if t, ok := stmt.(*tree.Execute); ok {
		isExec = true
		name = string(t.Name)
		Variables = t.Variables
	}
	preSql := ""
	bb := &strings.Builder{}
	//fill prepare parameters
	if isExec {
		prepareStmt, err := ses.GetPrepareStmt(ctx, name)
		if err != nil || prepareStmt == nil {
			return ""
		}
		preSql = strings.TrimSpace(prepareStmt.Sql)
		bb.WriteString(preSql)
		bb.WriteString(" ; ")
		if len(Variables) != 0 {
			//for EXECUTE ... USING statement. append variables if there is.
			//get SET VAR sql
			setVarSqls := make([]string, len(Variables))
			for i, v := range Variables {
				userVal, err := ses.GetUserDefinedVar(v.Name)
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
			vs := vector.MustFixedColNoTypeCheck[types.Varlena](prepareStmt.params)
			for i := 0; i < paramCnt; i++ {
				isNull := prepareStmt.params.GetNulls().Contains(uint64(i))
				if isNull {
					paramValues[i] = "NULL"
				} else {
					paramValues[i] = vs[i].UnsafeGetString(prepareStmt.params.GetArea())
				}
			}
			bb.WriteString(strings.Join(paramValues, " ; "))
		}
	}
	return bb.String()
}

func convertRowsIntoBatch(pool *mpool.MPool, cols []Column, rows [][]any) (*batch.Batch, *plan.ResultColDef, error) {
	planColDefs, colTyps, colNames, err := mysqlColDef2PlanResultColDef(cols)
	if err != nil {
		return nil, nil, err
	}
	//1. make vector type
	bat := batch.New(colNames)
	//2. make batch
	cnt := len(rows)
	bat.SetRowCount(cnt)
	for colIndex, typ := range colTyps {
		bat.Vecs[colIndex] = vector.NewVec(typ)
		nsp := nulls.NewWithSize(cnt)

		switch typ.Oid {
		case types.T_varchar:
			for rowIdx, row := range rows {
				var val string
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					strVal, ok := row[colIndex].(string)
					if ok {
						val = strVal
					} else {
						val = fmt.Sprintf("%v", row[colIndex])
					}
				}
				err := vector.AppendBytes(bat.Vecs[colIndex], []byte(val), false, pool)
				if err != nil {
					return nil, nil, err
				}
			}

		case types.T_text:
			for rowIdx, row := range rows {
				var val string
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					strVal, ok := row[colIndex].(string)
					if ok {
						val = strVal
					} else {
						val = fmt.Sprintf("%v", row[colIndex])
					}
				}

				err := vector.AppendBytes(bat.Vecs[colIndex], []byte(val), false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int8:
			for rowIdx, row := range rows {
				var val int8
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int8)
				}
				err := vector.AppendFixed[int8](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int16:
			for rowIdx, row := range rows {
				var val int16
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int16)
				}

				err := vector.AppendFixed[int16](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int32:
			for rowIdx, row := range rows {
				var val int32
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int32)
				}

				err := vector.AppendFixed[int32](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int64:
			for rowIdx, row := range rows {
				var val int64
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int64)
				}

				err := vector.AppendFixed[int64](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_float64:
			for rowIdx, row := range rows {
				var val float64
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(float64)
				}

				err := vector.AppendFixed[float64](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_float32:
			for rowIdx, row := range rows {
				var val float32
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(float32)
				}

				err := vector.AppendFixed[float32](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_date:
			for rowIdx, row := range rows {
				var val types.Date
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Date)
				}

				err := vector.AppendFixed[types.Date](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_time:
			for rowIdx, row := range rows {
				var val types.Time
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Time)
				}

				err := vector.AppendFixed[types.Time](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_datetime:
			for rowIdx, row := range rows {
				var val types.Datetime
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Datetime)
				}

				err := vector.AppendFixed[types.Datetime](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_timestamp:
			for rowIdx, row := range rows {
				var val types.Timestamp
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					timeStampRowVal := row[colIndex]
					switch v := timeStampRowVal.(type) {
					case types.Timestamp:
						val = v
					case string:
						val, err = types.ParseTimestamp(time.Local, v, typ.Scale)
						if err != nil {
							return nil, nil, err
						}
					default:
						return nil, nil, moerr.NewInternalErrorNoCtxf("%v can't convert to timestamp type", v)
					}
				}
				err := vector.AppendFixed[types.Timestamp](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_enum:
			for rowIdx, row := range rows {
				var val types.Enum
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Enum)
				}

				err := vector.AppendFixed[types.Enum](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		default:
			return nil, nil, moerr.NewInternalErrorNoCtxf("unsupported type %d", typ.Oid)
		}

		bat.Vecs[colIndex].SetNulls(nsp)
	}
	return bat, planColDefs, nil
}

func cleanBatch(pool *mpool.MPool, data ...*batch.Batch) {
	for _, item := range data {
		if item != nil {
			item.Clean(pool)
		}
	}
}

func mysqlColDef2PlanResultColDef(cols []Column) (*plan.ResultColDef, []types.Type, []string, error) {
	if len(cols) == 0 {
		return nil, nil, nil, nil
	}

	resultCols := make([]*plan.ColDef, len(cols))
	resultColTypes := make([]types.Type, len(cols))
	resultColNames := make([]string, len(cols))
	for i, col := range cols {
		resultColNames[i] = col.Name()
		resultCols[i] = &plan.ColDef{
			Name: col.Name(),
		}
		var pType plan.Type
		var tType types.Type
		switch col.ColumnType() {
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			pType = plan.Type{
				Id: int32(types.T_varchar),
			}
			tType = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		case defines.MYSQL_TYPE_TEXT:
			pType = plan.Type{
				Id: int32(types.T_text),
			}
			tType = types.New(types.T_text, types.MaxVarcharLen, 0)
		case defines.MYSQL_TYPE_TINY:
			pType = plan.Type{
				Id: int32(types.T_int8),
			}
			tType = types.New(types.T_int8, 0, 0)
		case defines.MYSQL_TYPE_SHORT:
			pType = plan.Type{
				Id: int32(types.T_int16),
			}
			tType = types.New(types.T_int16, 0, 0)
		case defines.MYSQL_TYPE_LONG:
			pType = plan.Type{
				Id: int32(types.T_int32),
			}
			tType = types.New(types.T_int32, 0, 0)
		case defines.MYSQL_TYPE_LONGLONG:
			pType = plan.Type{
				Id: int32(types.T_int64),
			}
			tType = types.New(types.T_int64, 0, 0)
		case defines.MYSQL_TYPE_DOUBLE:
			pType = plan.Type{
				Id: int32(types.T_float64),
			}
			tType = types.New(types.T_float64, 0, 0)
		case defines.MYSQL_TYPE_FLOAT:
			pType = plan.Type{
				Id: int32(types.T_float32),
			}
			tType = types.New(types.T_float32, 0, 0)
		case defines.MYSQL_TYPE_DATE:
			pType = plan.Type{
				Id: int32(types.T_date),
			}
			tType = types.New(types.T_date, 0, 0)
		case defines.MYSQL_TYPE_TIME:
			pType = plan.Type{
				Id: int32(types.T_time),
			}
			tType = types.New(types.T_time, 0, 0)
		case defines.MYSQL_TYPE_DATETIME:
			pType = plan.Type{
				Id: int32(types.T_datetime),
			}
			tType = types.New(types.T_datetime, 0, 0)
		case defines.MYSQL_TYPE_TIMESTAMP:
			pType = plan.Type{
				Id: int32(types.T_timestamp),
			}
			tType = types.New(types.T_timestamp, 0, 0)
		case defines.MYSQL_TYPE_ENUM:
			pType = plan.Type{
				Id: int32(types.T_enum),
			}
			tType = types.New(types.T_enum, 0, 0)
		default:
			return nil, nil, nil, moerr.NewInternalErrorNoCtxf("unsupported mysql type %d", col.ColumnType())
		}
		resultCols[i].Typ = pType
		resultColTypes[i] = tType
	}
	return &plan.ResultColDef{
		ResultCols: resultCols,
	}, resultColTypes, resultColNames, nil
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

func skipClientQuit(info string) bool {
	return strings.Contains(info, quitStr)
}

// UserInput
// normally, just use the sql.
// for some special statement, like 'set_var', we need to use the stmt.
// if the stmt is not nil, we neglect the sql.
type UserInput struct {
	sql                 string
	hashedSql           string
	stmtName            string
	stmt                tree.Statement
	preparePlan         *plan.Plan // binary protocol execute
	sqlSourceType       []string
	isRestore           bool
	isBinaryProtExecute bool
	// isInternalInput mark this UserInput is come from mo internal.
	// replace old logic: (stmt != nil)
	// cc isInternal()
	isInternalInput bool
	// operator account, the account executes restoration
	// e.g. sys takes a snapshot sn1 for acc1, then restores acc1 from snapshot sn1. In this scenario, sys is the operator account
	isRestoreByTs bool
	opAccount     uint32
	toAccount     uint32
}

func (ui *UserInput) getSql() string {
	return ui.sql
}

func (ui *UserInput) genHash() {
	ui.hashedSql = hashString(ui.sql)
}

func (ui *UserInput) getHash() string {
	return ui.hashedSql
}

func (ui *UserInput) getPreparePlan() *plan.Plan {
	return ui.preparePlan
}

// getStmt if the stmt is not nil, we neglect the sql.
func (ui *UserInput) getStmt() tree.Statement {
	return ui.stmt
}

func (ui *UserInput) getSqlSourceTypes() []string {
	return ui.sqlSourceType
}

// isInternal return true if the stmt is not nil.
// it means the statement is not from any client.
// currently, we use it to handle the 'set_var' statement.
func (ui *UserInput) isInternal() bool {
	return ui.isInternalInput
}

func (ui *UserInput) genSqlSourceType(ses FeSession) {
	sql := ui.getSql()
	ui.sqlSourceType = nil
	if ui.isInternal() {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	tenant := ses.GetTenantInfo()
	if tenant == nil || strings.HasPrefix(sql, cmdFieldListSql) {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	flag, _, _ := isSpecialUser(tenant.GetUser())
	if flag {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	if tenant.GetTenant() == sysAccountName && tenant.GetUser() == "internal" {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	for len(sql) > 0 {
		p1 := strings.Index(sql, "/*")
		p2 := strings.Index(sql, "*/")
		if p1 < 0 || p2 < 0 || p2 <= p1+1 {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.ExternSql)
			return
		}
		source := strings.TrimSpace(sql[p1+2 : p2])
		if source == cloudUserTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudUserSql)
		} else if source == cloudNoUserTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudNoUserSql)
		} else if source == saveResultTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudUserSql)
		} else {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.ExternSql)
		}
		sql = sql[p2+2:]
	}
}

func (ui *UserInput) getSqlSourceType(i int) string {
	sqlType := constant.ExternSql
	if i < len(ui.sqlSourceType) {
		sqlType = ui.sqlSourceType[i]
	}
	return sqlType
}

const (
	issue3482SqlPrefix    = "load data local infile"
	issue3482SqlPrefixLen = len(issue3482SqlPrefix)
)

// !!!NOTE!!! For debug
// https://github.com/matrixorigin/MO-Cloud/issues/3482
// TODO: remove it in the future
func (ui *UserInput) isIssue3482Sql() bool {
	if ui == nil {
		return false
	}
	sql := ui.getSql()
	sqlLen := len(sql)
	if sqlLen <= issue3482SqlPrefixLen {
		return false
	}
	return strings.HasPrefix(strings.ToLower(sql), issue3482SqlPrefix)
}

func unboxExprStr(ctx context.Context, expr tree.Expr) (string, error) {
	if e, ok := expr.(*tree.NumVal); ok && e.ValType == tree.P_char {
		return e.String(), nil
	}
	return "", moerr.NewInternalError(ctx, "invalid expr type")
}

type strParamBinder struct {
	ctx    context.Context
	params *vector.Vector
	err    error
}

func (b *strParamBinder) bind(e tree.Expr) string {
	if b.err != nil {
		return ""
	}

	switch val := e.(type) {
	case *tree.NumVal:
		return val.String()
	case *tree.ParamExpr:
		return b.params.GetStringAt(val.Offset - 1)
	default:
		b.err = moerr.NewInternalErrorf(b.ctx, "invalid params type %T", e)
		return ""
	}
}

func (b *strParamBinder) bindIdentStr(ident *tree.AccountIdentified) string {
	if b.err != nil {
		return ""
	}

	switch ident.Typ {
	case tree.AccountIdentifiedByPassword,
		tree.AccountIdentifiedWithSSL:
		return b.bind(ident.Str)
	default:
		return ""
	}
}

func resetBits(t *uint32, val uint32) {
	if t == nil {
		return
	}
	*t = val
}

func setBits(t *uint32, bit uint32) {
	if t == nil {
		return
	}
	*t |= bit
}

func clearBits(t *uint32, bit uint32) {
	if t == nil {
		return
	}
	*t &= ^bit
}

func bitsIsSet(t uint32, bit uint32) bool {
	return t&bit != 0
}

func attachValue(ctx context.Context, key, val any) context.Context {
	if ctx == nil {
		panic("context is nil")
	}

	return context.WithValue(ctx, key, val)
}

const KeySep = "#"

func genKey(dbName, tblName string) string {
	return fmt.Sprintf("%s%s%s", dbName, KeySep, tblName)
}

func splitKey(key string) (string, string) {
	parts := strings.Split(key, KeySep)
	if len(parts) >= 2 {
		return parts[0], parts[1]
	}
	return parts[0], ""
}

type toposort struct {
	next map[string][]string
}

func (g *toposort) addVertex(v string) {
	if _, ok := g.next[v]; ok {
		return
	}
	g.next[v] = make([]string, 0)
}

func (g *toposort) addEdge(from, to string) {
	if _, ok := g.next[from]; !ok {
		g.next[from] = make([]string, 0)
	}
	g.next[from] = append(g.next[from], to)
}

func (g *toposort) sort() (ans []string, err error) {
	inDegree := make(map[string]uint)
	for u := range g.next {
		inDegree[u] = 0
	}
	for _, nextVertices := range g.next {
		for _, v := range nextVertices {
			inDegree[v] += 1
		}
	}

	var noPreVertices []string
	for v, deg := range inDegree {
		if deg == 0 {
			noPreVertices = append(noPreVertices, v)
		}
	}

	for len(noPreVertices) > 0 {
		// find vertex whose inDegree = 0
		v := noPreVertices[0]
		noPreVertices = noPreVertices[1:]
		ans = append(ans, v)

		// update the next vertices from v
		for _, to := range g.next[v] {
			inDegree[to] -= 1
			if inDegree[to] == 0 {
				noPreVertices = append(noPreVertices, to)
			}
		}
	}

	if len(ans) != len(inDegree) {
		err = moerr.NewInternalErrorNoCtx("There is a cycle in dependency graph")
	}
	return
}

func ToRequest(payload []byte) *Request {
	req := &Request{
		cmd:  CommandType(payload[0]),
		data: payload[1:],
	}

	return req
}

// CancelCheck checks if the given context has been canceled.
// If the context is canceled, it returns the context's error.
func CancelCheck(Ctx context.Context) error {
	select {
	case <-Ctx.Done():
		return Ctx.Err()
	default:
		return nil
	}
}

func checkMoreResultSet(status uint16, isLastStmt bool) uint16 {
	if !isLastStmt {
		status |= SERVER_MORE_RESULTS_EXISTS
	}
	return status
}

func Copy[T any](src []T) []T {
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		return []T{}
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

func hashString(s string) string {
	hash := sha256.New()
	hash.Write(commonutil.UnsafeStringToBytes(s))
	hashBytes := hash.Sum(nil)
	return hex.EncodeToString(hashBytes)
}

func colDef2MysqlColumn(ctx context.Context, col *plan.ColDef) (*MysqlColumn, error) {
	var err error
	c := new(MysqlColumn)
	c.SetName(col.Name)
	c.SetOrgName(col.GetOriginCaseName())
	c.SetTable(col.TblName)
	c.SetOrgTable(col.TblName)
	c.SetAutoIncr(col.Typ.AutoIncr)
	c.SetSchema(col.DbName)
	err = convertEngineTypeToMysqlType(ctx, types.T(col.Typ.Id), c)
	if err != nil {
		return nil, err
	}
	setColFlag(c)
	setColLength(c, col.Typ.Width)
	setCharacter(c)

	// For binary/varbinary with mysql_type_varchar.Change the charset.
	if types.T(col.Typ.Id) == types.T_binary || types.T(col.Typ.Id) == types.T_varbinary {
		c.SetCharset(0x3f)
	}

	c.SetDecimal(col.Typ.Scale)

	// For TIMESTAMPADD function compatibility with MySQL:
	// GetResultColumnsFromPlan sets the return type based on input type and unit:
	// - DATE input + date unit → DATE type (MYSQL_TYPE_DATE)
	// - DATE input + time unit → DATETIME type (MYSQL_TYPE_DATETIME)
	// - DATETIME input → DATETIME type (MYSQL_TYPE_DATETIME)

	convertMysqlTextTypeToBlobType(c)
	return c, nil
}

// isLegal checks if the sqls are legal parsed by the mo parser.
// if there is at least one sql can be parsed, it returns true
func isLegal(name string, sqls []string) bool {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(sqls) == 0 {
		return false
	}
	for _, sql := range sqls {
		if len(sql) == 0 {
			return false
		}
	}
	yes := false
	for _, sql := range sqls {
		_, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			continue
		}
		yes = true
		break
	}
	return yes
}

// hasSpecialChars checks the string have special characters (','  '.'  ':' '`')
func hasSpecialChars(s string) bool {
	return strings.ContainsAny(s, ",.:`")
}

/*
accountNameIsLegal checks the account name legal or not.
rule:

	if create account name or create account `name` can succeed,
	it is legal.

	it means all most all string can be legal.
*/
func accountNameIsLegal(name string) bool {
	if hasSpecialChars(name) {
		return false
	}
	name = strings.TrimSpace(name)
	createAccountSqls := []string{
		"create account " + name + " ADMIN_NAME 'admin' IDENTIFIED BY '111'",
		"create account `" + name + "` ADMIN_NAME 'admin' IDENTIFIED BY '111'",
	}
	return isLegal(name, createAccountSqls)
}

/*
dbNameIsLegal checks the database name legal or not.
rule:

	if create database name or create database `name` can succeed,
	it is legal.

	it means all most all string can be legal.
*/
func dbNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == cdc.CDCPitrGranularity_All {
		return true
	}

	createDBSqls := []string{
		"create database " + name,
		"create database `" + name + "`",
	}
	return isLegal(name, createDBSqls)
}

/*
tableNameIsLegal checks the table name legal or not.
rule:

	if create table name or create table `name` can succeed,
	it is legal.

	it means all most all string can be legal.
*/
func tableNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == cdc.CDCPitrGranularity_All {
		return true
	}

	createTableSqls := []string{
		"create table " + name + "(a int)",
		"create table `" + name + "`(a int)",
	}
	return isLegal(name, createTableSqls)
}

//func tableNameIsRegexpr(s string) bool {
//	if len(s) < 2 {
//		return false
//	}
//	if strings.HasPrefix(s, "/") && strings.HasSuffix(s, "/") {
//		_, err := regexp.Compile(s)
//		if err != nil {
//			return false
//		}
//		return true
//	}
//	return false
//}

// replaceStr replaces s[start:end] by s2
func replaceStr(s string, start, end int, s2 string) string {
	if start >= end || start < 0 || end < 0 {
		return s
	}
	if end <= len(s) {
		return s[:start] + s2 + s[end:]
	}
	return s
}

func buildTableDefFromMoColumns(ctx context.Context, accountId uint64, dbName, table string, ses FeSession) (*plan.TableDef, error) {
	bh := NewShareTxnBackgroundExec(ctx, ses, false)
	defer bh.Close()
	var (
		sql     string
		erArray []ExecResult
		err     error
	)

	sql, err = getTableColumnDefSql(accountId, dbName, table)
	if err != nil {
		return nil, err
	}

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(erArray) {
		return nil, moerr.NewNoSuchTable(ctx, dbName, table)
	}

	cols, err := extractTableDefColumns(erArray, ctx, dbName, table)
	if err != nil {
		return nil, err
	}

	return &plan.TableDef{
		Name:   table,
		DbName: dbName,
		Cols:   cols,
	}, nil
}

func extractTableDefColumns(erArray []ExecResult, ctx context.Context, dbName, table string) ([]*plan.ColDef, error) {
	cols := make([]*plan.ColDef, 0)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			colName, err := result.GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}

			colType, err := result.GetString(ctx, i, 1)
			if err != nil {
				return nil, err
			}

			typ := new(types.Type)
			err = typ.Unmarshal([]byte(colType))
			if err != nil {
				return nil, err
			}

			colNum, err := result.GetUint64(ctx, i, 2)
			if err != nil {
				return nil, err
			}

			attDefault, err := result.GetString(ctx, i, 4)
			if err != nil {
				return nil, err
			}
			def := new(plan.Default)
			err = types.Decode([]byte(attDefault), def)
			if err != nil {
				return nil, err
			}

			isHidden, err := result.GetInt64(ctx, i, 6)
			if err != nil {
				return nil, err
			}

			cols = append(cols, &plan.ColDef{
				TblName:    table,
				DbName:     dbName,
				ColId:      colNum,
				Name:       strings.ToLower(colName),
				OriginName: colName,
				Hidden:     isHidden == 1,
				Typ: plan.Type{
					Id:          int32(typ.Oid),
					Width:       typ.Width,
					Scale:       typ.Scale,
					Table:       table,
					NotNullable: !def.NullAbility,
				},
				Default: def,
			})
		}
	}
	return cols, nil
}

var _ Allocator = new(LeakCheckAllocator)

const (
	leakCheckAllocatorModeNormal = iota
	leakCheckAllocatorModeAllocReturnErr
	leakCheckAllocatorModeAllocPanic
)

type LeakCheckAllocator struct {
	sync.Mutex
	allocated uint64
	freed     uint64
	records   map[unsafe.Pointer]int
	mod       int
}

func NewLeakCheckAllocator() *LeakCheckAllocator {
	return &LeakCheckAllocator{
		records: make(map[unsafe.Pointer]int),
	}
}

func (lca *LeakCheckAllocator) Alloc(capacity int) ([]byte, error) {
	lca.Lock()
	defer lca.Unlock()
	if lca.mod == leakCheckAllocatorModeAllocReturnErr {
		return nil, moerr.NewInternalErrorNoCtx("leak check allocator returns eror")
	} else if lca.mod == leakCheckAllocatorModeAllocPanic {
		panic("leak check allocator panic")
	}
	buf := make([]byte, capacity)
	lca.allocated += uint64(len(buf))
	lca.records[unsafe.Pointer(&buf[0])] = capacity
	return buf, nil
}

func (lca *LeakCheckAllocator) Free(bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	lca.Lock()
	defer lca.Unlock()
	if _, ok := lca.records[unsafe.Pointer(&bytes[0])]; ok {
		delete(lca.records, unsafe.Pointer(&bytes[0]))
	} else {
		panic(fmt.Sprintf("no such ptr %v", unsafe.Pointer(&bytes[0])))
	}
	lca.freed += uint64(len(bytes))
}

func (lca *LeakCheckAllocator) CheckBalance() bool {
	lca.Lock()
	defer lca.Unlock()
	return lca.allocated == lca.freed && len(lca.records) == 0
}
