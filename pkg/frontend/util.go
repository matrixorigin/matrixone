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
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"

	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

// only support single value and unary minus
func GetSimpleExprValue(e tree.Expr, ses *Session) (interface{}, error) {
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		return v.Parts[0], nil
	default:
		builder := plan2.NewQueryBuilder(plan.Query_SELECT, ses.GetTxnCompileCtx())
		bindContext := plan2.NewBindContext(builder, nil)
		binder := plan2.NewSetVarBinder(builder, bindContext)
		planExpr, err := binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
		// set @a = 'on', type of a is bool. And mo cast rule does not fit set variable rule so delay to convert type.
		bat := batch.NewWithSize(0)
		bat.Zs = []int64{1}
		// Here the evalExpr may execute some function that needs engine.Engine.
		ses.txnCompileCtx.GetProcess().Ctx = context.WithValue(ses.txnCompileCtx.GetProcess().Ctx, defines.EngineKey{}, ses.storage)

		vec, err := colexec.EvalExpressionOnce(ses.txnCompileCtx.GetProcess(), planExpr, []*batch.Batch{bat})
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
	case types.T_decimal64:
		val := vector.GetFixedAt[types.Decimal64](vec, 0)
		return plan2.MakePlan2Decimal64ExprWithType(val, plan2.DeepCopyType(expr.Typ)), nil
	case types.T_decimal128:
		val := vector.GetFixedAt[types.Decimal128](vec, 0)
		return plan2.MakePlan2Decimal128ExprWithType(val, plan2.DeepCopyType(expr.Typ)), nil
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
	default:
		return nil, moerr.NewInvalidArg(ses.GetRequestContext(), "variable type", vec.GetType().Oid.String())
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
	if status == success {
		motrace.EndStatement(ctx, nil, ses.sentRows.Load())
		logInfo(ses.GetDebugString(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), trace.ContextField(ctx))
	} else {
		motrace.EndStatement(ctx, err, ses.sentRows.Load())
		logError(ses.GetDebugString(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), logutil.ErrorField(err), trace.ContextField(ctx))
	}
}

func logInfo(info string, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("session_info", info))
	logutil.Info(msg, fields...)
}

//func logDebug(info string, msg string, fields ...zap.Field) {
//	fields = append(fields, zap.String("session_info", info))
//	logutil.Debug(msg, fields...)
//}

func logError(info string, msg string, fields ...zap.Field) {
	fields = append(fields, zap.String("session_info", info))
	logutil.Error(msg, fields...)
}

func logInfof(info string, msg string, fields ...interface{}) {
	fields = append(fields, info)
	logutil.Infof(msg+" %s", fields...)
}

func logDebugf(info string, msg string, fields ...interface{}) {
	fields = append(fields, info)
	logutil.Debugf(msg+" %s", fields...)
}

func logErrorf(info string, msg string, fields ...interface{}) {
	fields = append(fields, info)
	logutil.Errorf(msg+" %s", fields...)
}

// isCmdFieldListSql checks the sql is the cmdFieldListSql or not.
func isCmdFieldListSql(sql string) bool {
	return strings.HasPrefix(strings.ToLower(sql), cmdFieldListSql)
}

// makeCmdFieldListSql makes the internal CMD_FIELD_LIST sql
func makeCmdFieldListSql(query string) string {
	return cmdFieldListSql + " " + query
}

// parseCmdFieldList parses the internal cmd field list
func parseCmdFieldList(ctx context.Context, sql string) (*InternalCmdFieldList, error) {
	if !isCmdFieldListSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the CMD_FIELD_LIST")
	}
	rest := strings.TrimSpace(sql[len(cmdFieldListSql):])
	//find null
	nullIdx := strings.IndexRune(rest, rune(0))
	var tableName string
	if nullIdx < len(rest) {
		tableName = rest[:nullIdx]
		//neglect wildcard
		//wildcard := payload[nullIdx+1:]
		return &InternalCmdFieldList{tableName: tableName}, nil
	} else {
		return nil, moerr.NewInternalError(ctx, "wrong format for COM_FIELD_LIST")
	}
}

//func getAccount(ctx context.Context) (uint32, uint32, uint32) {
//	var accountId, userId, roleId uint32
//
//	if v := ctx.Value(defines.TenantIDKey{}); v != nil {
//		accountId = v.(uint32)
//	}
//	if v := ctx.Value(defines.UserIDKey{}); v != nil {
//		userId = v.(uint32)
//	}
//	if v := ctx.Value(defines.RoleIDKey{}); v != nil {
//		roleId = v.(uint32)
//	}
//	return accountId, userId, roleId
//}

func rewriteExpr(
	ctx context.Context,
	proc *process.Process,
	compileCtx plan2.CompilerContext,
	emptyBatch *batch.Batch,
	params []*plan.Expr,
	e *plan.Expr) (*plan.Expr, error) {

	var err error
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		needResetFunction := false
		for i, arg := range exprImpl.F.Args {
			if _, ok := arg.Expr.(*plan.Expr_P); ok {
				needResetFunction = true
			}
			if _, ok := arg.Expr.(*plan.Expr_V); ok {
				needResetFunction = true
			}
			exprImpl.F.Args[i], err = rewriteExpr(ctx, proc, compileCtx, emptyBatch, params, arg)
			if err != nil {
				return nil, err
			}
		}

		// reset function
		if needResetFunction {
			e, err = plan2.BindFuncExprImplByPlanExpr(ctx, exprImpl.F.Func.GetObjName(), exprImpl.F.Args)
			if err != nil {
				return nil, err
			}
		}
		return e, nil

	case *plan.Expr_P:
		return plan2.GetVarValue(ctx, compileCtx, proc, emptyBatch, params[int(exprImpl.P.Pos)])

	case *plan.Expr_V:
		return plan2.GetVarValue(ctx, compileCtx, proc, emptyBatch, e)
	}

	return e, nil
}

func rowsetDataToVector(
	ctx context.Context,
	proc *process.Process,
	compileCtx plan2.CompilerContext,
	exprs []*plan.Expr,
	tarVec *vector.Vector,
	params []*plan.Expr,
	uf func(*vector.Vector, *vector.Vector, int64) error,
) error {
	var exprImpl *plan.Expr
	var typ = plan2.MakePlan2Type(tarVec.GetType())
	var err error

	for _, e := range exprs {
		if expr, ok := e.Expr.(*plan.Expr_F); ok {
			if expr.F.Func.ObjName == "cast" {
				castTyp := expr.F.Args[1].Typ
				if typ.Id == castTyp.Id && typ.Width == castTyp.Width && typ.Scale == castTyp.Scale {
					e = expr.F.Args[0]
				}
			}
		}

		if expr, ok := e.Expr.(*plan.Expr_P); ok {
			exprImpl, err = plan2.GetVarValue(ctx, compileCtx, proc, batch.EmptyForConstFoldBatch, params[int(expr.P.Pos)])
			if err != nil {
				return err
			}
		} else if _, ok := e.Expr.(*plan.Expr_V); ok {
			exprImpl, err = plan2.GetVarValue(ctx, compileCtx, proc, batch.EmptyForConstFoldBatch, e)
			if err != nil {
				return err
			}
		} else if _, ok := e.Expr.(*plan.Expr_C); ok {
			exprImpl = e
		} else {
			exprImpl = plan2.DeepCopyExpr(e)
			exprImpl, err = rewriteExpr(ctx, proc, compileCtx, batch.EmptyForConstFoldBatch, params, exprImpl)
			if err != nil {
				return err
			}
		}

		exprImpl, err = plan2.ForceCastExpr(ctx, exprImpl, typ)
		if err != nil {
			return err
		}

		var vec *vector.Vector
		vec, err = colexec.EvalExpressionOnce(proc, exprImpl, []*batch.Batch{batch.EmptyForConstFoldBatch})
		if err != nil {
			return err
		}

		if err = uf(tarVec, vec, 0); err != nil {
			vec.Free(proc.Mp())
			return err
		}
		vec.Free(proc.Mp())
	}

	return nil
}

func getVariableValue(varDefault interface{}) string {
	switch val := varDefault.(type) {
	case int64:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case string:
		return val
	default:
		return ""
	}
}

func makeServerVersion(pu *mo_config.ParameterUnit, version string) string {
	return pu.SV.ServerVersionPrefix + version
}
