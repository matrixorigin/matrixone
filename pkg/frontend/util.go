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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"

	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
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

func MinInt64(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MinUint64(a uint64, b uint64) uint64 {
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

func MaxInt64(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func MaxUint64(a uint64, b uint64) uint64 {
	if a < b {
		return b
	} else {
		return a
	}
}

type Uint64List []uint64

func (ul Uint64List) Len() int {
	return len(ul)
}

func (ul Uint64List) Less(i, j int) bool {
	return ul[i] < ul[j]
}

func (ul Uint64List) Swap(i, j int) {
	ul[i], ul[j] = ul[j], ul[i]
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

type DebugCounter struct {
	length  int
	counter []uint64
	Cf      CloseFlag
}

func NewDebugCounter(l int) *DebugCounter {
	return &DebugCounter{
		length:  l,
		counter: make([]uint64, l),
	}
}

func (dc *DebugCounter) Add(i int, v uint64) {
	atomic.AddUint64(&dc.counter[i], v)
}

func (dc *DebugCounter) Set(i int, v uint64) {
	atomic.StoreUint64(&dc.counter[i], v)
}

func (dc *DebugCounter) Get(i int) uint64 {
	return atomic.LoadUint64(&dc.counter[i])
}

func (dc *DebugCounter) Len() int {
	return dc.length
}

func (dc *DebugCounter) DCRoutine() {
	dc.Cf.Open()

	for dc.Cf.IsOpened() {
		for i := 0; i < dc.length; i++ {
			if i != 0 && i%8 == 0 {
				logutil.Infof("")
			}
			v := dc.Get(i)
			logutil.Infof("[%4d %4d]", i, v)
			dc.Set(i, 0)
		}
		logutil.Infof("")
		time.Sleep(5 * time.Second)
	}
}

const (
	TIMEOUT_TYPE_SECOND int = iota
	TIMEOUT_TYPE_MILLISECOND
)

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
func GetSimpleExprValue(e tree.Expr) (interface{}, error) {
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		return v.Parts[0], nil
	default:
		binder := plan2.NewDefaultBinder(nil, nil, nil, nil)
		planExpr, err := binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
		// set @a = 'on', type of a is bool. And mo cast rule does not fit set variable rule so delay to convert type.
		bat := batch.NewWithSize(0)
		bat.Zs = []int64{1}
		vec, err := colexec.EvalExpr(bat, nil, planExpr)
		if err != nil {
			return nil, err
		}
		return getValueFromVector(vec)
	}
}

func getValueFromVector(vec *vector.Vector) (interface{}, error) {
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
	case types.T_char, types.T_varchar:
		return vec.GetString(0), nil
	case types.T_decimal64:
		val := vector.GetValueAt[types.Decimal64](vec, 0)
		return val.String(), nil
	case types.T_decimal128:
		val := vector.GetValueAt[types.Decimal128](vec, 0)
		return val.String(), nil
	default:
		return nil, moerr.NewInvalidArg("variable type", vec.Typ.Oid.String())
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
	stm := trace.StatementFromContext(ctx)
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
		logutil.Info("query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()))
	} else {
		logutil.Error("query trace status", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.StatementField(str), logutil.StatusField(status.String()), logutil.ErrorField(err))
	}
}
