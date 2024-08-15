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
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func builtInDateDiff(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v1-v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCurrentTimestamp(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)

	// TODO: not a good way to solve this problem. and will be fixed by file `specialRule.go`
	scale := int32(6)
	if len(ivecs) == 1 && !ivecs[0].IsConstNull() {
		scale = int32(vector.MustFixedCol[int64](ivecs[0])[0])
	}
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	resultValue := types.UnixNanoToTimestamp(proc.GetUnixTime())
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(resultValue, false); err != nil {
			return err
		}
	}

	return nil
}

func builtInSysdate(ivecs []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)

	scale := int32(6)
	if len(ivecs) == 1 && !ivecs[0].IsConstNull() {
		scale = int32(vector.MustFixedCol[int64](ivecs[0])[0])
	}
	rs.TempSetType(types.New(types.T_timestamp, 0, scale))

	resultValue := types.UnixNanoToTimestamp(time.Now().UnixNano())
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(resultValue, false); err != nil {
			return err
		}
	}

	return nil
}

const (
	onUpdateExpr = iota
	defaultExpr
	typNormal
	typWithLen
)

func builtInMoShowVisibleBin(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[uint8](parameters[1])

	tp, null := p2.GetValue(0)
	if null {
		return moerr.NewNotSupported(proc.Ctx, "show visible bin, the second argument must be in [0, 3], but got NULL")
	}
	if tp > 3 {
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("show visible bin, the second argument must be in [0, 3], but got %d", tp))
	}

	var f func(s []byte) ([]byte, error)
	rs := vector.MustFunctionResult[types.Varlena](result)
	switch tp {
	case onUpdateExpr:
		f = func(s []byte) ([]byte, error) {
			update := new(plan.OnUpdate)
			err := types.Decode(s, update)
			if err != nil {
				return nil, err
			}
			return functionUtil.QuickStrToBytes(update.OriginString), nil
		}
	case defaultExpr:
		f = func(s []byte) ([]byte, error) {
			def := new(plan.Default)
			err := types.Decode(s, def)
			if err != nil {
				return nil, err
			}
			return functionUtil.QuickStrToBytes(def.OriginString), nil
		}
	case typNormal:
		f = func(s []byte) ([]byte, error) {
			typ := new(types.Type)
			err := types.Decode(s, typ)
			if err != nil {
				return nil, err
			}
			ts := typ.String()
			// after decimal fix, remove this
			if typ.Oid.IsDecimal() {
				ts = "DECIMAL"
			}

			return functionUtil.QuickStrToBytes(ts), nil
		}
	case typWithLen:
		f = func(s []byte) ([]byte, error) {
			typ := new(types.Type)
			err := types.Decode(s, typ)
			if err != nil {
				return nil, err
			}

			ts := typ.String()
			// after decimal fix, remove this
			if typ.Oid.IsDecimal() {
				ts = "DECIMAL"
			}

			ret := fmt.Sprintf("%s(%d)", ts, typ.Width)
			return functionUtil.QuickStrToBytes(ret), nil
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 || len(v1) == 0 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			b, err := f(v1)
			if err != nil {
				return err
			}
			if b == nil {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				if err = rs.AppendBytes(b, false); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func builtInMoShowVisibleBinEnum(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	enumVal := vector.GenerateFunctionStrParameter(parameters[1])

	var f func([]byte, string) ([]byte, error)
	rs := vector.MustFunctionResult[types.Varlena](result)
	f = func(s []byte, enumStr string) ([]byte, error) {
		typ := new(types.Type)
		err := types.Decode(s, typ)
		if err != nil {
			return nil, err
		}
		if typ.Oid != types.T_enum {
			return nil, moerr.NewNotSupported(proc.Ctx, "show visible bin enum, the type must be enum, but got %s", typ.String())
		}

		// get enum values
		enums := strings.Split(enumStr, ",")
		enumVal := ""
		for i, e := range enums {
			enumVal += fmt.Sprintf("'%s'", e)
			if i < len(enums)-1 {
				enumVal += ","
			}
		}
		ret := fmt.Sprintf("%s(%s)", typ.String(), enumVal)
		return functionUtil.QuickStrToBytes(ret), nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		enumStr, null2 := enumVal.GetStrValue(i)
		if null1 || null2 || len(v1) == 0 || len(enumStr) == 0 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			enumString := functionUtil.QuickBytesToStr(enumStr)
			b, err := f(v1, enumString)
			if err != nil {
				return err
			}
			if b == nil {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				if err = rs.AppendBytes(b, false); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func builtInInternalCharLength(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsMySQLString() {
				if err := rs.Append(int64(typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalCharSize(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsMySQLString() {
				if err := rs.Append(int64(typ.GetSize()*typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalNumericPrecision(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsDecimal() {
				if err := rs.Append(int64(typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalNumericScale(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsDecimal() {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalDatetimeScale(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid == types.T_datetime {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalCharacterSet(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
				typ.Oid == types.T_blob || typ.Oid == types.T_text || typ.Oid == types.T_datalink {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInConcatCheck(_ []overload, inputs []types.Type) checkResult {
	if len(inputs) > 1 {
		shouldCast := false

		ret := make([]types.Type, len(inputs))
		for i, source := range inputs {
			if !source.Oid.IsMySQLString() {
				c, _ := tryToMatch([]types.Type{source}, []types.T{types.T_varchar})
				if c == matchFailed {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
				if c == matchByCast {
					shouldCast = true
					ret[i] = types.T_varchar.ToType()
				}
			} else {
				ret[i] = source
			}
		}
		if shouldCast {
			return newCheckResultWithCast(0, ret)
		}
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func builtInConcat(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ps := make([]vector.FunctionParameterWrapper[types.Varlena], len(parameters))
	for i := range ps {
		ps[i] = vector.GenerateFunctionStrParameter(parameters[i])
	}

	for i := uint64(0); i < uint64(length); i++ {
		var vs string
		apv := true

		for _, p := range ps {
			v, null := p.GetStrValue(i)
			if null {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				apv = false
				break
			} else {
				vs += string(v)
			}
		}
		if apv {
			if err := rs.AppendBytes([]byte(vs), false); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	formatMask = "%Y/%m/%d"
	regexpMask = `\d{1,4}/\d{1,2}/\d{1,2}`
)

// MOLogDate parse 'YYYY/MM/DD' date from input string.
// return '0001-01-01' if input string not container 'YYYY/MM/DD' substr, until DateParse Function support return NULL for invalid date string.
func builtInMoLogDate(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Date](result)
	p1 := vector.GenerateFunctionStrParameter(parameters[0])

	op := newOpBuiltInRegexp()
	generalTime := NewGeneralTime()
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			expr := functionUtil.QuickBytesToStr(v)
			match, parsedInput, err := op.regMap.regularSubstr(regexpMask, expr, 1, 1)
			if err != nil {
				return err
			}
			if !match {
				if err = rs.Append(0, true); err != nil {
					return err
				}
			} else {
				generalTime.ResetTime()
				success := coreStrToDate(proc.Ctx, generalTime, parsedInput, formatMask)
				if success && types.ValidDate(int32(generalTime.year), generalTime.month, generalTime.day) {
					val := types.DateFromCalendar(int32(generalTime.year), generalTime.month, generalTime.day)
					if err = rs.Append(val, false); err != nil {
						return err
					}
				} else {
					if err = rs.Append(0, true); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// builtInPurgeLog act like `select mo_purge_log('rawlog,statement_info,metric', '2023-06-27')`
// moc#3199
// - Not Support TXN
// - Not Support Multi-table in one cmd
// - 2 way to do purge: diff_hours = {now}-{target_date}; if diff_hours <= 24h, exec delete from ; else exec prune
func builtInPurgeLog(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[1])

	if proc.GetSessionInfo().AccountId != sysAccountID {
		return moerr.NewNotSupported(proc.Ctx, "only support sys account")
	}

	v, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.InternalSQLExecutor)
	if !ok {
		return moerr.NewNotSupported(proc.Ctx, "no implement sqlExecutor")
	}
	exec := v.(executor.SQLExecutor)

	deleteTable := func(tbl *table.Table, dateStr string) error {
		sql := fmt.Sprintf("delete from `%s`.`%s` where `%s` < %q",
			tbl.Database, tbl.Table, tbl.TimestampColumn.Name, dateStr)
		opts := executor.Options{}.WithDatabase(tbl.Database).
			WithTxn(proc.GetTxnOperator()).
			WithTimeZone(proc.GetSessionInfo().TimeZone)
		if proc.GetTxnOperator() != nil {
			opts = opts.WithDisableIncrStatement() // this option always with WithTxn()
		}
		res, err := exec.Exec(proc.Ctx, sql, opts)
		if err != nil {
			return err
		}
		res.Close()
		return nil
	}
	pruneObj := func(tbl *table.Table, hours time.Duration) (string, error) {
		var result string
		// Tips: NO Txn guarantee
		opts := executor.Options{}.WithDatabase(tbl.Database).
			WithTimeZone(proc.GetSessionInfo().TimeZone)
		// fixme: hours should > 24 * time.Hour
		runPruneSql := fmt.Sprintf(`select mo_ctl('dn', 'inspect', 'objprune -t %s.%s -d %s -f')`, tbl.Database, tbl.Table, hours)
		res, err := exec.Exec(proc.Ctx, runPruneSql, opts)
		if err != nil {
			return "", err
		}
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				result += executor.GetStringRows(cols[0])[i]
			}
			return true
		})
		res.Close()
		return result, nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		// fixme: should we need to support null date?
		if null1 || null2 {
			rs.AppendBytes(nil, true)
			continue
		}

		tblName := strings.TrimSpace(util.UnsafeBytesToString(v1))
		// not allow purge multi table in one call.
		if strings.Contains(tblName, ",") {
			return moerr.NewNotSupported(proc.Ctx, "table name contains comma.")
		}

		now := time.Now()
		found := false
		tables := table.GetAllTables()
		for _, tbl := range tables {
			if tbl.TimestampColumn == nil || tblName != tbl.Table {
				continue
			}

			found = true
			targetTime := v2.ToDatetime().ConvertToGoTime(time.Local)
			if d := now.Sub(targetTime); d > rpc.AllowPruneDuration {
				d = d / time.Second * time.Second
				result, err := pruneObj(tbl, d)
				if err != nil {
					return err
				}
				rs.AppendMustBytesValue(util.UnsafeStringToBytes(result))
			} else {
				// try prune obj 24 hours before
				_, err := pruneObj(tbl, rpc.AllowPruneDuration)
				if err != nil {
					return err
				}
				// do the delete job
				if err := deleteTable(tbl, v2.String()); err != nil {
					return err
				}
				rs.AppendMustBytesValue([]byte("success"))
			}
			break
		}
		if !found {
			return moerr.NewNotSupported(proc.Ctx, "purge '%s'", tblName)
		}

	}

	return nil
}

func builtInDatabase(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		db := proc.GetSessionInfo().GetDatabase()
		if err := rs.AppendBytes(functionUtil.QuickStrToBytes(db), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentRole(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.GetSessionInfo().GetRole()), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentAccountID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[uint32](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(proc.GetSessionInfo().AccountId, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentAccountName(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.GetSessionInfo().Account), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentRoleID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[uint32](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(proc.GetSessionInfo().RoleId, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentRoleName(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.GetSessionInfo().Role), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentUserID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[uint32](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(proc.GetSessionInfo().UserId, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInCurrentUserName(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes([]byte(proc.GetSessionInfo().User), false); err != nil {
			return err
		}
	}
	return nil
}

const MaxTgtLen = int64(16 * 1024 * 1024)

func doLpad(src string, tgtLen int64, pad string) (string, bool) {
	srcRune, padRune := []rune(src), []rune(pad)
	srcLen, padLen := len(srcRune), len(padRune)

	if tgtLen < 0 || tgtLen > MaxTgtLen {
		return "", true
	} else if int(tgtLen) < srcLen {
		return string(srcRune[:tgtLen]), false
	} else if int(tgtLen) == srcLen {
		return src, false
	} else if padLen == 0 {
		return "", false
	} else {
		r := int(tgtLen) - srcLen
		p, m := r/padLen, r%padLen
		return strings.Repeat(pad, p) + string(padRune[:m]) + src, false
	}
}

func doRpad(src string, tgtLen int64, pad string) (string, bool) {
	srcRune, padRune := []rune(src), []rune(pad)
	srcLen, padLen := len(srcRune), len(padRune)

	if tgtLen < 0 || tgtLen > MaxTgtLen {
		return "", true
	} else if int(tgtLen) < srcLen {
		return string(srcRune[:tgtLen]), false
	} else if int(tgtLen) == srcLen {
		return src, false
	} else if padLen == 0 {
		return "", false
	} else {
		r := int(tgtLen) - srcLen
		p, m := r/padLen, r%padLen
		return src + strings.Repeat(pad, p) + string(padRune[:m]), false
	}
}

func builtInRepeat(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// repeat the string n times.
	repeatNTimes := func(base string, n int64) (r string, null bool) {
		if n <= 0 {
			return "", false
		}

		// return null if result is too long.
		// I'm not sure if this is the right thing to do, MySql can repeat string with the result length at least 1,000,000.
		// and there is no documentation about the limit of the result length.
		sourceLen := int64(len(base))
		if sourceLen*n > MaxTgtLen {
			return "", true
		}
		return strings.Repeat(base, int(n)), false
	}

	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)

	var err error
	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			err = rs.AppendMustNullForBytesResult()
		} else {
			r, null := repeatNTimes(functionUtil.QuickBytesToStr(v1), v2)
			if null {
				err = rs.AppendMustNullForBytesResult()
			} else {
				err = rs.AppendBytes([]byte(r), false)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func builtInLpad(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	p3 := vector.GenerateFunctionStrParameter(parameters[2])

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		v3, null3 := p3.GetStrValue(i)
		if !(null1 || null2 || null3) {
			rval, shouldNull := doLpad(string(v1), v2, string(v3))
			if !shouldNull {
				if err := rs.AppendBytes([]byte(rval), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInRpad(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	p3 := vector.GenerateFunctionStrParameter(parameters[2])

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		v3, null3 := p3.GetStrValue(i)
		if !(null1 || null2 || null3) {
			rval, shouldNull := doRpad(string(v1), v2, string(v3))
			if !shouldNull {
				if err := rs.AppendBytes([]byte(rval), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.AppendBytes(nil, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInUUID(_ []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Uuid](result)
	for i := uint64(0); i < uint64(length); i++ {
		val, err := uuid.NewV7()
		if err != nil {
			return moerr.NewInternalError(proc.Ctx, "newuuid failed")
		}
		if err = rs.Append(types.Uuid(val), false); err != nil {
			return err
		}
	}
	return nil
}

func builtInUnixTimestamp(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	if len(parameters) == 0 {
		val := types.CurrentTimestamp().Unix()
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(val, false); err != nil {
				return nil
			}
		}
		return nil
	}

	p1 := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](parameters[0])
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		val := v1.Unix()
		if val < 0 || null1 {
			// XXX v1 < 0 need to raise error here.
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func mustTimestamp(loc *time.Location, s string) types.Timestamp {
	ts, err := types.ParseTimestamp(loc, s, 6)
	if err != nil {
		ts = 0
	}
	return ts
}

func builtInUnixTimestampVarcharToInt64(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val := mustTimestamp(proc.GetSessionInfo().TimeZone, string(v1)).Unix()
			if val < 0 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			if err := rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

var _ = builtInUnixTimestampVarcharToFloat64

func builtInUnixTimestampVarcharToFloat64(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[float64](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			val := mustTimestamp(proc.GetSessionInfo().TimeZone, string(v1))
			if err := rs.Append(val.UnixToFloat(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInUnixTimestampVarcharToDecimal128(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Decimal128](result)

	var d types.Decimal128
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.Append(d, true); err != nil {
				return err
			}
		} else {
			val, err := mustTimestamp(proc.GetSessionInfo().TimeZone, string(v1)).UnixToDecimal128()
			if err != nil {
				return err
			}
			if val.Compare(types.Decimal128{B0_63: 0, B64_127: 0}) <= 0 {
				if err := rs.Append(d, true); err != nil {
					return err
				}
			}
			if err = rs.Append(val, false); err != nil {
				return err
			}
		}
	}
	return nil
}

// XXX I just copy this function.
func builtInHash(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	fillStringGroupStr := func(keys [][]byte, vec *vector.Vector, n int, start int) {
		area := vec.GetArea()
		vs := vector.MustFixedCol[types.Varlena](vec)
		if !vec.GetNulls().Any() {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(0))
				keys[i] = append(keys[i], vs[i+start].GetByteSlice(area)...)
			}
		} else {
			nsp := vec.GetNulls()
			for i := 0; i < n; i++ {
				hasNull := nsp.Contains(uint64(i + start))
				if hasNull {
					keys[i] = append(keys[i], byte(1))
				} else {
					keys[i] = append(keys[i], byte(0))
					keys[i] = append(keys[i], vs[i+start].GetByteSlice(area)...)
				}
			}
		}
	}

	fillGroupStr := func(keys [][]byte, vec *vector.Vector, n int, sz int, start int) {
		data := unsafe.Slice(vector.GetPtrAt[byte](vec, 0), (n+start)*sz)
		if !vec.GetNulls().Any() {
			for i := 0; i < n; i++ {
				keys[i] = append(keys[i], byte(0))
				keys[i] = append(keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
			}
		} else {
			nsp := vec.GetNulls()
			for i := 0; i < n; i++ {
				isNull := nsp.Contains(uint64(i + start))
				if isNull {
					keys[i] = append(keys[i], byte(1))
				} else {
					keys[i] = append(keys[i], byte(0))
					keys[i] = append(keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
				}
			}
		}
	}

	encodeHashKeys := func(keys [][]byte, vecs []*vector.Vector, start, count int) {
		for _, vec := range vecs {
			if vec.GetType().IsFixedLen() {
				fillGroupStr(keys, vec, count, vec.GetType().TypeSize(), start)
			} else {
				fillStringGroupStr(keys, vec, count, start)
			}
		}
		for i := 0; i < count; i++ {
			if l := len(keys[i]); l < 16 {
				keys[i] = append(keys[i], hashtable.StrKeyPadding[l:]...)
			}
		}
	}

	rs := vector.MustFunctionResult[int64](result)

	keys := make([][]byte, hashmap.UnitLimit)
	states := make([][3]uint64, hashmap.UnitLimit)
	for i := 0; i < length; i += hashmap.UnitLimit {
		n := length - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		for j := 0; j < n; j++ {
			keys[j] = keys[j][:0]
		}
		encodeHashKeys(keys, parameters, i, n)

		hashtable.BytesBatchGenHashStates(&keys[0], &states[0], n)
		for j := 0; j < n; j++ {
			rs.AppendMustValue(int64(states[j][0]))
		}
	}
	return nil
}

// BuiltInSerial have a similar function named SerialWithCompacted in the index_util
// Serial func is used by users, the function make true when input vec have ten
// rows, the output vec is ten rows, when the vectors have null value, the output
// vec will set the row null
// for example:
// input vec is [[1, 1, 1], [2, 2, null], [3, 3, 3]]
// result vec is [serial(1, 2, 3), serial(1, 2, 3), null]
func (op *opSerial) BuiltInSerial(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	op.tryExpand(length, proc.Mp())

	bitMap := new(nulls.Nulls)

	for _, v := range parameters {
		if v.IsConstNull() {
			nulls.AddRange(rs.GetResultVector().GetNulls(), 0, uint64(length))
			return nil
		}
		SerialHelper(v, bitMap, op.ps, false)
	}

	//NOTE: make sure to use uint64(length) instead of len(op.ps[i])
	// as length of packer array could be larger than length of input vectors
	for i := uint64(0); i < uint64(length); i++ {
		if bitMap.Contains(i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if err := rs.AppendBytes(op.ps[i].GetBuf(), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *opSerial) BuiltInSerialFull(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {

	rs := vector.MustFunctionResult[types.Varlena](result)
	op.tryExpand(length, proc.Mp())

	for _, v := range parameters {
		if v.IsConstNull() {
			for i := 0; i < v.Length(); i++ {
				op.ps[i].EncodeNull()
			}
			continue
		}

		SerialHelper(v, nil, op.ps, true)
	}

	//NOTE: make sure to use uint64(length) instead of len(op.ps[i])
	// as length of packer array could be larger than length of input vectors
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.AppendBytes(op.ps[i].GetBuf(), false); err != nil {
			return err
		}
	}
	return nil
}

// SerialHelper is unified function used in builtInSerial and BuiltInSerialFull
// To use it inside builtInSerial, pass the bitMap pointer and set isFull false
// To use it inside BuiltInSerialFull, pass the bitMap as nil and set isFull to true
func SerialHelper(v *vector.Vector, bitMap *nulls.Nulls, ps []*types.Packer, isFull bool) {

	if !isFull && bitMap == nil {
		// if you are using it inside the builtInSerial then, you should pass bitMap
		panic("for builtInSerial(), bitmap should not be nil")
	}
	hasNull := v.HasNull()
	switch v.GetType().Oid {
	case types.T_bool:
		s := vector.ExpandFixedCol[bool](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeBool(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeBool(b)
			}
		}
	case types.T_bit:
		s := vector.ExpandFixedCol[uint64](v)
		for i, b := range s {
			if v.IsNull(uint64(i)) {
				if isFull {
					ps[i].EncodeNull()
				} else {
					nulls.Add(bitMap, uint64(i))
				}
			} else {
				ps[i].EncodeUint64(b)
			}
		}
	case types.T_int8:
		s := vector.ExpandFixedCol[int8](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeInt8(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeInt8(b)
			}
		}
	case types.T_int16:
		s := vector.ExpandFixedCol[int16](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeInt16(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeInt16(b)
			}
		}
	case types.T_int32:
		s := vector.ExpandFixedCol[int32](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeInt32(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeInt32(b)
			}
		}
	case types.T_int64:
		s := vector.ExpandFixedCol[int64](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeInt64(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeInt64(b)
			}
		}
	case types.T_uint8:
		s := vector.ExpandFixedCol[uint8](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeUint8(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeUint8(b)
			}
		}
	case types.T_uint16:
		s := vector.ExpandFixedCol[uint16](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeUint16(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeUint16(b)
			}
		}
	case types.T_uint32:
		s := vector.ExpandFixedCol[uint32](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeUint32(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeUint32(b)
			}
		}
	case types.T_uint64:
		s := vector.ExpandFixedCol[uint64](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeUint64(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeUint64(b)
			}
		}
	case types.T_float32:
		s := vector.ExpandFixedCol[float32](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeFloat32(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeFloat32(b)
			}
		}
	case types.T_float64:
		s := vector.ExpandFixedCol[float64](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeFloat64(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeFloat64(b)
			}
		}
	case types.T_date:
		s := vector.ExpandFixedCol[types.Date](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeDate(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeDate(b)
			}
		}
	case types.T_time:
		s := vector.ExpandFixedCol[types.Time](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeTime(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeTime(b)
			}
		}
	case types.T_datetime:
		s := vector.ExpandFixedCol[types.Datetime](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeDatetime(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeDatetime(b)
			}
		}
	case types.T_timestamp:
		s := vector.ExpandFixedCol[types.Timestamp](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeTimestamp(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeTimestamp(b)
			}
		}
	case types.T_enum:
		s := vector.MustFixedCol[types.Enum](v)
		if hasNull {
			for i, b := range s {
				if nulls.Contains(v.GetNulls(), uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeEnum(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeEnum(b)
			}
		}
	case types.T_decimal64:
		s := vector.ExpandFixedCol[types.Decimal64](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeDecimal64(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeDecimal64(b)
			}
		}
	case types.T_decimal128:
		s := vector.ExpandFixedCol[types.Decimal128](v)
		if hasNull {
			for i, b := range s {
				if v.IsNull(uint64(i)) {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, uint64(i))
					}
				} else {
					ps[i].EncodeDecimal128(b)
				}
			}
		} else {
			for i, b := range s {
				ps[i].EncodeDecimal128(b)
			}
		}
	case types.T_json, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		if hasNull {
			fv := vector.GenerateFunctionStrParameter(v)
			for i, j := uint64(0), uint64(v.Length()); i < j; i++ {
				value, null := fv.GetStrValue(i)
				if null {
					if isFull {
						ps[i].EncodeNull()
					} else {
						nulls.Add(bitMap, i)
					}
					continue
				}
				ps[i].EncodeStringType(value)
			}
		} else {
			vs := vector.ExpandBytesCol(v)
			for i := range vs {
				ps[i].EncodeStringType(vs[i])
			}
		}
	}
}

// builtInSerialExtract is used to extract a tupleElement from the serial vector.
// For example:
//
//	serial_col = serial(floatCol, varchar3Col)
//	serial_extract(serial_col, 1, varchar(3)) will return 2
func builtInSerialExtract(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])
	resTyp := parameters[2].GetType()

	switch resTyp.Oid {
	case types.T_bit:
		rs := vector.MustFunctionResult[uint64](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_int8:
		rs := vector.MustFunctionResult[int8](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_int16:
		rs := vector.MustFunctionResult[int16](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_int32:
		rs := vector.MustFunctionResult[int32](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_int64:
		rs := vector.MustFunctionResult[int64](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_uint8:
		rs := vector.MustFunctionResult[uint8](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_uint16:
		rs := vector.MustFunctionResult[uint16](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_uint32:
		rs := vector.MustFunctionResult[uint32](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_uint64:
		rs := vector.MustFunctionResult[uint64](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_float32:
		rs := vector.MustFunctionResult[float32](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_float64:
		rs := vector.MustFunctionResult[float64](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_decimal64:
		rs := vector.MustFunctionResult[types.Decimal64](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_decimal128:
		rs := vector.MustFunctionResult[types.Decimal128](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_bool:
		rs := vector.MustFunctionResult[bool](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_date:
		rs := vector.MustFunctionResult[types.Date](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_datetime:
		rs := vector.MustFunctionResult[types.Datetime](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_time:
		rs := vector.MustFunctionResult[types.Time](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)
	case types.T_timestamp:
		rs := vector.MustFunctionResult[types.Timestamp](result)
		return serialExtractExceptStrings(p1, p2, rs, proc, length, selectList)

	case types.T_json, types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_array_float32, types.T_array_float64, types.T_datalink:
		rs := vector.MustFunctionResult[types.Varlena](result)
		return serialExtractForString(p1, p2, rs, proc, length, selectList)
	}
	return moerr.NewInternalError(proc.Ctx, "not supported type %s", resTyp.String())

}

func serialExtractExceptStrings[T types.Number | bool | types.Date | types.Datetime | types.Time | types.Timestamp](
	p1 vector.FunctionParameterWrapper[types.Varlena],
	p2 vector.FunctionParameterWrapper[int64],
	result *vector.FunctionResult[T], proc *process.Process, length int, selectList *FunctionSelectList) error {

	for i := uint64(0); i < uint64(length); i++ {
		v1, null := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		if null || null2 {
			var nilVal T
			if err := result.Append(nilVal, true); err != nil {
				return err
			}
			continue
		}

		tuple, schema, err := types.UnpackWithSchema(v1)
		if err != nil {
			return err
		}

		if int(v2) >= len(tuple) {
			return moerr.NewInternalError(proc.Ctx, "index out of range")
		}

		if schema[v2] == types.T_any {
			var nilVal T
			if err = result.Append(nilVal, true); err != nil {
				return err
			}
			continue
		}

		if value, ok := tuple[v2].(T); ok {
			if err := result.Append(value, false); err != nil {
				return err
			}
		} else {
			return moerr.NewInternalError(proc.Ctx, "provided type did not match the expected type")
		}
	}

	return nil
}

func serialExtractForString(p1 vector.FunctionParameterWrapper[types.Varlena],
	p2 vector.FunctionParameterWrapper[int64],
	result *vector.FunctionResult[types.Varlena], proc *process.Process, length int, selectList *FunctionSelectList) error {
	for i := uint64(0); i < uint64(length); i++ {
		v1, null := p1.GetStrValue(i)
		v2, null2 := p2.GetValue(i)
		if null || null2 {
			if err := result.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		tuple, schema, err := types.UnpackWithSchema(v1)
		if err != nil {
			return err
		}

		if int(v2) >= len(tuple) {
			return moerr.NewInternalError(proc.Ctx, "index out of range")
		}

		if schema[v2] == types.T_any {
			if err = result.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if value, ok := tuple[v2].([]byte); ok {
			if err := result.AppendBytes(value, false); err != nil {
				return err
			}
		} else {
			return moerr.NewInternalError(proc.Ctx, "provided type did not match the expected type")
		}
	}

	return nil
}

// 24-hour seconds
const SecondsIn24Hours = 86400

// The number of days in the year 0000 AD
const ADZeroDays = 366

const (
	intervalUnitYEAR      = "YEAR"
	intervalUnitQUARTER   = "QUARTER"
	intervalUnitMONTH     = "MONTH"
	intervalUnitWEEK      = "WEEK"
	intervalUnitDAY       = "DAY"
	intervalUnitHOUR      = "HOUR"
	intervalUnitMINUTE    = "MINUTE"
	intervalUnitSECOND    = "SECOND"
	intervalUnitMICSECOND = "MICROSECOND"
)

// ToDays: InMySQL: Given a date data, returns a day number (the number of days since year 0). Returns NULL if date is NULL.
// note:  but Matrxone think the date of the first year of the year is 0001-01-01, this function selects compatibility with MySQL
// reference linking: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days
func builtInToDays(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	dateParams := vector.GenerateFunctionFixedTypeParameter[types.Datetime](parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		datetimeValue, isNull := dateParams.GetValue(i)
		if isNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rs.Append(DateTimeDiff(intervalUnitDAY, types.ZeroDatetime, datetimeValue)+ADZeroDays, false)
	}
	return nil
}

// DateTimeDiff returns t2 - t1 where t1 and t2 are datetime expressions.
// The unit for the result is given by the unit argument.
// The values for interval unit are "QUARTER","YEAR","MONTH", "DAY", "HOUR", "SECOND", "MICROSECOND"
func DateTimeDiff(intervalUnit string, t1 types.Datetime, t2 types.Datetime) int64 {
	seconds, microseconds, negative := calcDateTimeInterval(t2, t1, 1)
	months := uint(0)
	if intervalUnit == intervalUnitYEAR || intervalUnit == intervalUnitQUARTER ||
		intervalUnit == intervalUnitMONTH {
		var (
			yearBegin, yearEnd, monthBegin, monthEnd, dayBegin, dayEnd uint
			secondBegin, secondEnd, microsecondBegin, microsecondEnd   uint
		)

		if negative {
			yearBegin = uint(t2.Year())
			yearEnd = uint(t1.Year())
			monthBegin = uint(t2.Month())
			monthEnd = uint(t1.Month())
			dayBegin = uint(t2.Day())
			dayEnd = uint(t1.Day())
			secondBegin = uint(int(t2.Hour())*3600 + int(t2.Minute())*60 + int(t2.Sec()))
			secondEnd = uint(int(t1.Hour())*3600 + int(t1.Minute())*60 + int(t1.Sec()))
			microsecondBegin = uint(t2.MicroSec())
			microsecondEnd = uint(t1.MicroSec())
		} else {
			yearBegin = uint(t1.Year())
			yearEnd = uint(t2.Year())
			monthBegin = uint(t1.Month())
			monthEnd = uint(t2.Month())
			dayBegin = uint(t1.Day())
			dayEnd = uint(t2.Day())
			secondBegin = uint(int(t1.Hour())*3600 + int(t1.Minute())*60 + int(t1.Sec()))
			secondEnd = uint(int(t2.Hour())*3600 + int(t2.Minute())*60 + int(t2.Sec()))
			microsecondBegin = uint(t1.MicroSec())
			microsecondEnd = uint(t2.MicroSec())
		}

		// calculate years
		years := yearEnd - yearBegin
		if monthEnd < monthBegin ||
			(monthEnd == monthBegin && dayEnd < dayBegin) {
			years--
		}

		// calculate months
		months = 12 * years
		if monthEnd < monthBegin ||
			(monthEnd == monthBegin && dayEnd < dayBegin) {
			months += 12 - (monthBegin - monthEnd)
		} else {
			months += monthEnd - monthBegin
		}

		if dayEnd < dayBegin {
			months--
		} else if (dayEnd == dayBegin) &&
			((secondEnd < secondBegin) ||
				(secondEnd == secondBegin && microsecondEnd < microsecondBegin)) {
			months--
		}
	}

	// negative
	negV := int64(1)
	if negative {
		negV = -1
	}
	switch intervalUnit {
	case intervalUnitYEAR:
		return int64(months) / 12 * negV
	case intervalUnitQUARTER:
		return int64(months) / 3 * negV
	case intervalUnitMONTH:
		return int64(months) * negV
	case intervalUnitWEEK:
		return int64(seconds) / SecondsIn24Hours / 7 * negV
	case intervalUnitDAY:
		return int64(seconds) / SecondsIn24Hours * negV
	case intervalUnitHOUR:
		return int64(seconds) / 3600 * negV
	case intervalUnitMINUTE:
		return int64(seconds) / 60 * negV
	case intervalUnitSECOND:
		return int64(seconds) * negV
	case intervalUnitMICSECOND:
		return int64(seconds*1000000+microseconds) * negV
	}
	return 0
}

// calcDateTimeInterval: calculates time interval between two datetime values
func calcDateTimeInterval(t1 types.Datetime, t2 types.Datetime, sign int) (seconds, microseconds int, neg bool) {
	// Obtain the year, month, day, hour, minute, and second of the t2 datetime
	year := int(t2.Year())
	month := int(t2.Month())
	day := int(t2.Day())
	hour := int(t2.Hour())
	minute := int(t2.Minute())
	second := int(t2.Sec())
	microsecond := int(t2.MicroSec())

	days1 := calcDaysSinceZero(int(t1.Year()), int(t1.Month()), int(t1.Day()))
	days2 := calcDaysSinceZero(year, month, day)
	days1 -= sign * days2

	tmp := (int64(days1)*SecondsIn24Hours+
		int64(t1.Hour())*3600+int64(t1.Minute())*60+
		int64(t1.Sec())-
		int64(sign)*(int64(hour)*3600+
			int64(minute)*60+
			int64(second)))*1e6 +
		t1.MicroSec() - int64(sign)*int64(microsecond)

	if tmp < 0 {
		tmp = -tmp
		neg = true
	}
	seconds = int(tmp / 1e6)
	microseconds = int(tmp % 1e6)
	return
}

// calcDaynr calculates days since 0000-00-00.
func calcDaysSinceZero(year int, month int, day int) int {
	if year == 0 && month == 0 {
		return 0
	}

	delsum := 365*year + 31*(month-1) + day
	if month <= 2 {
		year--
	} else {
		delsum -= (month*4 + 23) / 10
	}
	temp := ((year/100 + 1) * 3) / 4
	return delsum + year/4 - temp
}

// Seconds in 0000 AD
const ADZeroSeconds = 31622400

// ToSeconds: InMySQL: Given a date date, returns a day number (the number of days since year 0000). Returns NULL if date is NULL.
// note:  but Matrxone think the date of the first year of the year is 0001-01-01, this function selects compatibility with MySQL
// reference linking: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-seconds
func builtInToSeconds(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	dateParams := vector.GenerateFunctionFixedTypeParameter[types.Datetime](parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		datetimeValue, isNull := dateParams.GetValue(i)
		if isNull {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		rs.Append(DateTimeDiff(intervalUnitSECOND, types.ZeroDatetime, datetimeValue)+ADZeroSeconds, false)
	}
	return nil
}

// CalcToSeconds: CalcToDays is used to return a day number (the number of days since year 0)
func CalcToSeconds(ctx context.Context, datetimes []types.Datetime, ns *nulls.Nulls) ([]int64, error) {
	res := make([]int64, len(datetimes))
	for idx, datetime := range datetimes {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		res[idx] = DateTimeDiff(intervalUnitSECOND, types.ZeroDatetime, datetime) + ADZeroSeconds
	}
	return res, nil
}

func builtInSin(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Sin(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInSinh(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Sinh(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCos(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Cos(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCot(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Cot(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInTan(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Tan(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInExp(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Exp(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInSqrt(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryFixedToFixedWithErrorCheck[float64, float64](parameters, result, proc, length, func(v float64) (float64, error) {
		return momath.Sqrt(v)
	}, selectList)
}

func builtInSqrtArray[T types.RealNumbers](parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytesWithErrorCheck(parameters, result, proc, length, func(in []byte) (out []byte, err error) {
		_in := types.BytesToArray[T](in)

		_out, err := moarray.Sqrt(_in)
		if err != nil {
			return nil, err
		}
		return types.ArrayToBytes[float64](_out), nil

	}, selectList)
}

func builtInACos(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Acos(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInATan(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Atan(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInATan2(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[1])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v1 == 0 {
				return moerr.NewInvalidArg(proc.Ctx, "Atan first input", 0)
			}
			if err := rs.Append(math.Atan(v2/v1), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInLn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Ln(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInLog(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[1])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v1 == float64(1) {
				return moerr.NewInvalidArg(proc.Ctx, "log base", 1)
			}
			tempV1, err := momath.Ln(v1)
			if err != nil {
				return moerr.NewInvalidArg(proc.Ctx, "log input", "<= 0")
			}
			tempV2, err := momath.Ln(v2)
			if err != nil {
				return moerr.NewInvalidArg(proc.Ctx, "log input", "<= 0")
			}
			if err = rs.Append(tempV2/tempV1, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInLog2(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			log2Value, err := momath.Log2(v)
			if err != nil {
				return err
			}
			if err = rs.Append(log2Value, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInLog10(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			log10Value, err := momath.Lg(v)
			if err != nil {
				return err
			}
			if err = rs.Append(log10Value, false); err != nil {
				return err
			}
		}
	}
	return nil
}

type opBuiltInRand struct {
	seed *rand.Rand
}

var _ = newOpBuiltInRand().builtInRand

func newOpBuiltInRand() *opBuiltInRand {
	return new(opBuiltInRand)
}

func builtInRand(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v := rand.Float64()
		if err := rs.Append(v, false); err != nil {
			return err
		}
	}
	return nil
}

func (op *opBuiltInRand) builtInRand(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !parameters[0].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "parameter of rand", "column")
	}
	if parameters[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "parameter of rand", "null")
	}

	p1 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)

	if op.seed == nil {
		seedNumber, _ := p1.GetValue(0)
		op.seed = rand.New(rand.NewSource(seedNumber))
	}

	for i := uint64(0); i < uint64(length); i++ {
		v := op.seed.Float64()
		if err := rs.Append(v, false); err != nil {
			return err
		}
	}
	return nil
}

func builtInConvertFake(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// ignore the second parameter and just set result the same to the first parameter.
	return opUnaryBytesToBytes(parameters, result, proc, length, func(v []byte) []byte {
		return v
	}, selectList)
}

func builtInToUpper(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytes(parameters, result, proc, length, func(v []byte) []byte {
		return bytes.ToUpper(v)
	}, selectList)
}

func builtInToLower(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opUnaryBytesToBytes(parameters, result, proc, length, func(v []byte) []byte {
		return bytes.ToLower(v)
	}, selectList)
}

// buildInMOCU extract cu or calculate cu from parameters
// example:
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123)
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123, 'total')
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123, 'cpu')
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123, 'mem')
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123, 'ioin')
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123, 'ioout')
// - select mo_cu('[1,2,3,4,5,6,7,8]', 134123, 'network')
func buildInMOCU(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return buildInMOCUWithCfg(parameters, result, proc, length, nil)
}

func buildInMOCUv1(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	cfg := motrace.GetCUConfigV1()
	return buildInMOCUWithCfg(parameters, result, proc, length, cfg)
}

func buildInMOCUWithCfg(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, cfg *config.OBCUConfig) error {
	var (
		cu     float64
		p3     vector.FunctionParameterWrapper[types.Varlena]
		stats  statistic.StatsArray
		target []byte
		null3  bool
	)
	rs := vector.MustFunctionResult[float64](result)

	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[1])

	if len(parameters) == 3 {
		p3 = vector.GenerateFunctionStrParameter(parameters[2])
	}

	for i := uint64(0); i < uint64(length); i++ {
		statsJsonArrayStr, null1 := p1.GetStrValue(i) /* stats json array */
		durationNS, null2 := p2.GetValue(i)           /* duration_ns */
		if p3 == nil {
			target, null3 = []byte("total"), false
		} else {
			target, null3 = p3.GetStrValue(i)
		}

		if null1 || null2 || null3 {
			rs.Append(float64(0), true)
			continue
		}

		if len(statsJsonArrayStr) == 0 {
			rs.Append(float64(0), true)
			continue
		}

		if err := json.Unmarshal(statsJsonArrayStr, &stats); err != nil {
			rs.Append(float64(0), true)
			//return moerr.NewInternalError(proc.Ctx, "failed to parse json arr: %v", err)
		}

		switch util.UnsafeBytesToString(target) {
		case "cpu":
			cu = motrace.CalculateCUCpu(int64(stats.GetTimeConsumed()), cfg)
		case "mem":
			cu = motrace.CalculateCUMem(int64(stats.GetMemorySize()), durationNS, cfg)
		case "ioin":
			cu = motrace.CalculateCUIOIn(int64(stats.GetS3IOInputCount()), cfg)
		case "ioout":
			cu = motrace.CalculateCUIOOut(int64(stats.GetS3IOOutputCount()), cfg)
		case "network":
			cu = motrace.CalculateCUTraffic(int64(stats.GetOutTrafficBytes()), stats.GetConnType(), cfg)
		case "total":
			cu = motrace.CalculateCUWithCfg(stats, durationNS, cfg)
		default:
			rs.Append(float64(0), true)
			continue
		}
		rs.Append(cu, false)
	}

	return nil
}
