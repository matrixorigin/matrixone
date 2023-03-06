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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	dumpUtils "github.com/matrixorigin/matrixone/pkg/vectorize/dump"

	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
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
		binder := plan2.NewDefaultBinder(ses.GetRequestContext(), nil, nil, nil, nil)
		planExpr, err := binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
		// set @a = 'on', type of a is bool. And mo cast rule does not fit set variable rule so delay to convert type.
		bat := batch.NewWithSize(0)
		bat.Zs = []int64{1}
		vec, err := colexec.EvalExpr(bat, ses.txnCompileCtx.GetProcess(), planExpr)
		if err != nil {
			return nil, err
		}
		return getValueFromVector(vec, ses, planExpr)
	}
}

func getValueFromVector(vec *vector.Vector, ses *Session, expr *plan2.Expr) (interface{}, error) {
	if nulls.Any(vec.Nsp) {
		return nil, nil
	}
	switch vec.Typ.Oid {
	case types.T_bool:
		return vector.GetValueAt[bool](vec, 0), nil
	case types.T_int8:
		return vector.GetValueAt[int8](vec, 0), nil
	case types.T_int16:
		return vector.GetValueAt[int16](vec, 0), nil
	case types.T_int32:
		return vector.GetValueAt[int32](vec, 0), nil
	case types.T_int64:
		return vector.GetValueAt[int64](vec, 0), nil
	case types.T_uint8:
		return vector.GetValueAt[uint8](vec, 0), nil
	case types.T_uint16:
		return vector.GetValueAt[uint16](vec, 0), nil
	case types.T_uint32:
		return vector.GetValueAt[uint32](vec, 0), nil
	case types.T_uint64:
		return vector.GetValueAt[uint64](vec, 0), nil
	case types.T_float32:
		return vector.GetValueAt[float32](vec, 0), nil
	case types.T_float64:
		return vector.GetValueAt[float64](vec, 0), nil
	case types.T_char, types.T_varchar, types.T_binary,
		types.T_varbinary, types.T_text, types.T_blob:
		return vec.GetString(0), nil
	case types.T_decimal64:
		val := vector.GetValueAt[types.Decimal64](vec, 0)
		return plan2.MakePlan2Decimal64ExprWithType(val, plan2.DeepCopyTyp(expr.Typ)), nil
	case types.T_decimal128:
		val := vector.GetValueAt[types.Decimal128](vec, 0)
		return plan2.MakePlan2Decimal128ExprWithType(val, plan2.DeepCopyTyp(expr.Typ)), nil
	case types.T_json:
		val := vec.GetBytes(0)
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.GetValueAt[types.Uuid](vec, 0)
		return val.ToString(), nil
	case types.T_date:
		val := vector.GetValueAt[types.Date](vec, 0)
		return val.String(), nil
	case types.T_time:
		val := vector.GetValueAt[types.Time](vec, 0)
		return val.String(), nil
	case types.T_datetime:
		val := vector.GetValueAt[types.Datetime](vec, 0)
		return val.String(), nil
	case types.T_timestamp:
		val := vector.GetValueAt[types.Timestamp](vec, 0)
		return val.String2(ses.GetTimeZone(), vec.Typ.Scale), nil
	default:
		return nil, moerr.NewInvalidArg(ses.GetRequestContext(), "variable type", vec.Typ.Oid.String())
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
		motrace.EndStatement(ctx, nil)
		logInfo(ses.GetConciseProfile(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), trace.ContextField(ctx))
	} else {
		motrace.EndStatement(ctx, err)
		logError(ses.GetConciseProfile(), "query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), logutil.ErrorField(err), trace.ContextField(ctx))
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

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getAttrFromTableDef(defs []engine.TableDef) ([]string, bool, error) {
	attrs := make([]string, 0, len(defs))
	isView := false
	for _, tblDef := range defs {
		switch def := tblDef.(type) {
		case *engine.AttributeDef:
			if def.Attr.IsHidden || def.Attr.IsRowId {
				continue
			}
			attrs = append(attrs, def.Attr.Name)
		case *engine.ViewDef:
			isView = true
		}
	}
	return attrs, isView, nil
}

func getDDL(bh BackgroundExec, ctx context.Context, sql string) (string, error) {
	bh.ClearExecResultSet()
	err := bh.Exec(ctx, sql)
	if err != nil {
		return "", err
	}
	ret := string(bh.GetExecResultSet()[0].(*MysqlResultSet).Data[0][1].([]byte))
	if !strings.HasSuffix(ret, ";") {
		ret += ";"
	}
	return ret, nil
}

func convertValueBat2Str(ctx context.Context, bat *batch.Batch, mp *mpool.MPool, loc *time.Location) (*batch.Batch, error) {
	var err error
	rbat := batch.NewWithSize(bat.VectorCount())
	rbat.InitZsOne(bat.Length())
	for i := 0; i < rbat.VectorCount(); i++ {
		rbat.Vecs[i] = vector.New(types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen}) //TODO: check size
		rs := make([]string, bat.Length())
		switch bat.Vecs[i].Typ.Oid {
		case types.T_bool:
			xs := vector.MustTCols[bool](bat.Vecs[i])
			rs, err = dumpUtils.ParseBool(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_int8:
			xs := vector.MustTCols[int8](bat.Vecs[i])
			rs, err = dumpUtils.ParseSigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_int16:
			xs := vector.MustTCols[int16](bat.Vecs[i])
			rs, err = dumpUtils.ParseSigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_int32:
			xs := vector.MustTCols[int32](bat.Vecs[i])
			rs, err = dumpUtils.ParseSigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_int64:
			xs := vector.MustTCols[int64](bat.Vecs[i])
			rs, err = dumpUtils.ParseSigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)

		case types.T_uint8:
			xs := vector.MustTCols[uint8](bat.Vecs[i])
			rs, err = dumpUtils.ParseUnsigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_uint16:
			xs := vector.MustTCols[uint16](bat.Vecs[i])
			rs, err = dumpUtils.ParseUnsigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_uint32:
			xs := vector.MustTCols[uint32](bat.Vecs[i])
			rs, err = dumpUtils.ParseUnsigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)

		case types.T_uint64:
			xs := vector.MustTCols[uint64](bat.Vecs[i])
			rs, err = dumpUtils.ParseUnsigned(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		case types.T_float32:
			xs := vector.MustTCols[float32](bat.Vecs[i])
			rs, err = dumpUtils.ParseFloats(xs, bat.GetVector(int32(i)).GetNulls(), rs, 32)
		case types.T_float64:
			xs := vector.MustTCols[float64](bat.Vecs[i])
			rs, err = dumpUtils.ParseFloats(xs, bat.GetVector(int32(i)).GetNulls(), rs, 64)
		case types.T_decimal64:
			xs := vector.MustTCols[types.Decimal64](bat.Vecs[i])
			rs, err = dumpUtils.ParseQuoted(xs, bat.GetVector(int32(i)).GetNulls(), rs, dumpUtils.DefaultParser[types.Decimal64])
		case types.T_decimal128:
			xs := vector.MustTCols[types.Decimal128](bat.Vecs[i])
			rs, err = dumpUtils.ParseQuoted(xs, bat.GetVector(int32(i)).GetNulls(), rs, dumpUtils.DefaultParser[types.Decimal128])
		case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
			xs := vector.MustStrCols(bat.Vecs[i])
			rs, err = dumpUtils.ParseQuoted(xs, bat.GetVector(int32(i)).GetNulls(), rs, dumpUtils.DefaultParser[string])
		case types.T_json:
			xs := vector.MustBytesCols(bat.Vecs[i])
			rs, err = dumpUtils.ParseQuoted(xs, bat.GetVector(int32(i)).GetNulls(), rs, dumpUtils.JsonParser)
		case types.T_timestamp:
			xs := vector.MustTCols[types.Timestamp](bat.Vecs[i])
			rs, err = dumpUtils.ParseTimeStamp(xs, bat.GetVector(int32(i)).GetNulls(), rs, loc, bat.GetVector(int32(i)).Typ.Scale)
		case types.T_datetime:
			xs := vector.MustTCols[types.Datetime](bat.Vecs[i])
			rs, err = dumpUtils.ParseQuoted(xs, bat.GetVector(int32(i)).GetNulls(), rs, dumpUtils.DefaultParser[types.Datetime])
		case types.T_date:
			xs := vector.MustTCols[types.Date](bat.Vecs[i])
			rs, err = dumpUtils.ParseQuoted(xs, bat.GetVector(int32(i)).GetNulls(), rs, dumpUtils.DefaultParser[types.Date])
		case types.T_uuid:
			xs := vector.MustTCols[types.Uuid](bat.Vecs[i])
			rs, err = dumpUtils.ParseUuid(xs, bat.GetVector(int32(i)).GetNulls(), rs)
		default:
			err = moerr.NewNotSupported(ctx, "type %v", bat.Vecs[i].Typ.String())
		}
		if err != nil {
			return nil, err
		}
		for j := 0; j < len(rs); j++ {
			err = rbat.Vecs[i].Append([]byte(rs[j]), false, mp)
			if err != nil {
				return nil, err
			}
		}
	}
	rbat.InitZsOne(bat.Length())
	return rbat, nil
}

func genDumpFileName(outfile string, idx int64) string {
	path := filepath.Dir(outfile)
	filename := strings.Split(filepath.Base(outfile), ".")
	base, extend := strings.Join(filename[:len(filename)-1], ""), filename[len(filename)-1]
	return filepath.Join(path, fmt.Sprintf("%s_%d.%s", base, idx, extend))
}

func createDumpFile(ctx context.Context, filename string) (*os.File, error) {
	exists, err := fileExists(filename)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, moerr.NewFileAlreadyExists(ctx, filename)
	}

	ret, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func writeDump2File(ctx context.Context, buf *bytes.Buffer, dump *tree.MoDump, f *os.File, curFileIdx, curFileSize int64) (ret *os.File, newFileIdx, newFileSize int64, err error) {
	if dump.MaxFileSize > 0 && int64(buf.Len()) > dump.MaxFileSize {
		err = moerr.NewInternalError(ctx, "dump: data in db is too large,please set a larger max_file_size")
		return
	}
	if dump.MaxFileSize > 0 && curFileSize+int64(buf.Len()) > dump.MaxFileSize {
		f.Close()
		if curFileIdx == 1 {
			os.Rename(dump.OutFile, genDumpFileName(dump.OutFile, curFileIdx))
		}
		newFileIdx = curFileIdx + 1
		newFileSize = int64(buf.Len())
		ret, err = createDumpFile(ctx, genDumpFileName(dump.OutFile, newFileIdx))
		if err != nil {
			return
		}
		_, err = buf.WriteTo(ret)
		if err != nil {
			return
		}
		buf.Reset()
		return
	}
	newFileSize = curFileSize + int64(buf.Len())
	_, err = buf.WriteTo(f)
	if err != nil {
		return
	}
	buf.Reset()
	return f, curFileIdx, newFileSize, nil
}

func maybeAppendExtension(s string) string {
	path := filepath.Dir(s)
	filename := strings.Split(filepath.Base(s), ".")
	if len(filename) == 1 {
		filename = append(filename, "sql")
	}
	base, extend := strings.Join(filename[:len(filename)-1], ""), filename[len(filename)-1]
	return filepath.Join(path, base+"."+extend)
}

func removeFile(s string, idx int64) {
	if idx == 1 {
		os.RemoveAll(s)
		return
	}
	path := filepath.Dir(s)
	filename := strings.Split(filepath.Base(s), ".")
	base, extend := strings.Join(filename[:len(filename)-1], ""), filename[len(filename)-1]
	for i := int64(1); i <= idx; i++ {
		os.RemoveAll(filepath.Join(path, fmt.Sprintf("%s_%d.%s", base, i, extend)))
	}
}

func isInvalidConfigInput(config string) bool {
	// first verify if the input string can parse as a josn type data
	_, err := types.ParseStringToByteJson(config)
	return err != nil
}
