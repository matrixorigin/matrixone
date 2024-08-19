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

package function

import (
	"fmt"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// sequence functions
// XXX current impl of sequence starts its own transaction, which is debatable.
// We really should have done this the sameway as auto incr, but well, we did not.
// We may fix this in the future. For now, all code are moved from old seq without
// any change.

// seq function tests are not ported.  mock table and txn are simply too much.
// we rely on bvt for sequence function tests.

var setEdge = true

// Retrieve values of this sequence.
// Set curval,lastval of current session.
// Set is_called to true if it is false, if is_]called is true Advance last_seq_num.
// Return advanced last_seq_num.

func Nextval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])

	// Here is the transaction
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.GetTxnOperator()
	if txn == nil {
		return moerr.NewInternalError(proc.Ctx, "Nextval: txn operator is nil")
	}

	// nextval is the real implementation of nextval function.
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err = rs.AppendBytes(nil, true); err != nil {
				return
			}
		} else {
			var res string
			res, err = nextval(string(v), proc, e, txn)
			if err == nil {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false)
			}
			if err != nil {
				return
			}
			// set last val
			if res != "" {
				proc.GetSessionInfo().SeqLastValue[0] = res
			}
		}
	}
	return nil
}

func nextval(tblname string, proc *process.Process, e engine.Engine, txn client.TxnOperator) (string, error) {
	db := proc.GetSessionInfo().Database
	dbHandler, err := e.Database(proc.Ctx, db, txn)
	if err != nil {
		return "", err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname, nil)
	if err != nil {
		return "", err
	}

	// Check is sequence table.
	td, err := rel.TableDefs(proc.Ctx)
	if err != nil {
		return "", err
	}
	if td[len(td)-1].(*engine.PropertiesDef).Properties[0].Value != catalog.SystemSequenceRel {
		return "", moerr.NewInternalError(proc.Ctx, "Table input is not a sequence")
	}

	_values, err := proc.GetSessionInfo().SqlHelper.ExecSql(fmt.Sprintf("select * from `%s`.`%s`", db, tblname))
	if err != nil {
		return "", err
	}
	values := _values[0]
	if values == nil {
		return "", moerr.NewInternalError(proc.Ctx, "Failed to get sequence meta data.")
	}

	switch values[0].(type) {
	case int16:
		// Get values store in sequence table.
		lsn, minv, maxv, _, incrv, cycle, isCalled := values[0].(int16), values[1].(int16), values[2].(int16),
			values[3].(int16), values[4].(int64), values[5].(bool), values[6].(bool)
		// When iscalled is not set, set it and do not advance sequence number.
		if !isCalled {
			return setIsCalled(proc, rel, lsn, db, tblname)
		}
		// When incr is over the range of this datatype.
		if incrv > math.MaxInt16 || incrv < math.MinInt16 {
			if cycle {
				return advanceSeq(lsn, minv, maxv, int16(incrv), cycle, incrv < 0, setEdge, rel, proc, db, tblname)
			} else {
				return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
			}
		}
		// Tranforming incrv to this datatype and make it positive for generic use.
		return advanceSeq(lsn, minv, maxv, makePosIncr[int16](incrv), cycle, incrv < 0, !setEdge, rel, proc, db, tblname)
	case int32:
		lsn, minv, maxv, _, incrv, cycle, isCalled := values[0].(int32), values[1].(int32), values[2].(int32),
			values[3].(int32), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			return setIsCalled(proc, rel, lsn, db, tblname)
		}
		/*
			no incrv can be bigger than maxint64 or smaller than minint64, therefore the following is noop

			if incrv > math.MaxInt64 || incrv < math.MinInt64 {
				if cycle {
					return advanceSeq(lsn, minv, maxv, int32(incrv), cycle, incrv < 0, setEdge, rel, proc, db, tblname)
				} else {
					return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
		*/
		return advanceSeq(lsn, minv, maxv, makePosIncr[int32](incrv), cycle, incrv < 0, !setEdge, rel, proc, db, tblname)
	case int64:
		lsn, minv, maxv, _, incrv, cycle, isCalled := values[0].(int64), values[1].(int64), values[2].(int64),
			values[3].(int64), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			return setIsCalled(proc, rel, lsn, db, tblname)
		}
		return advanceSeq(lsn, minv, maxv, makePosIncr[int64](incrv), cycle, incrv < 0, !setEdge, rel, proc, db, tblname)
	case uint16:
		lsn, minv, maxv, _, incrv, cycle, isCalled := values[0].(uint16), values[1].(uint16), values[2].(uint16),
			values[3].(uint16), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			return setIsCalled(proc, rel, lsn, db, tblname)
		}
		if incrv > math.MaxUint16 || -incrv > math.MaxUint16 {
			if cycle {
				return advanceSeq(lsn, minv, maxv, uint16(incrv), cycle, incrv < 0, setEdge, rel, proc, db, tblname)
			} else {
				return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
			}
		}
		return advanceSeq(lsn, minv, maxv, makePosIncr[uint16](incrv), cycle, incrv < 0, !setEdge, rel, proc, db, tblname)
	case uint32:
		lsn, minv, maxv, _, incrv, cycle, isCalled := values[0].(uint32), values[1].(uint32), values[2].(uint32),
			values[3].(uint32), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			return setIsCalled(proc, rel, lsn, db, tblname)
		}
		if incrv > math.MaxUint32 || -incrv > math.MaxUint32 {
			if cycle {
				return advanceSeq(lsn, minv, maxv, uint32(incrv), cycle, incrv < 0, setEdge, rel, proc, db, tblname)
			} else {
				return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
			}
		}
		return advanceSeq(lsn, minv, maxv, makePosIncr[uint32](incrv), cycle, incrv < 0, !setEdge, rel, proc, db, tblname)
	case uint64:
		lsn, minv, maxv, _, incrv, cycle, isCalled := values[0].(uint64), values[1].(uint64), values[2].(uint64),
			values[3].(uint64), values[4].(int64), values[5].(bool), values[6].(bool)
		if !isCalled {
			return setIsCalled(proc, rel, lsn, db, tblname)
		}
		return advanceSeq(lsn, minv, maxv, makePosIncr[uint64](incrv), cycle, incrv < 0, !setEdge, rel, proc, db, tblname)
	}

	return "", moerr.NewInternalError(proc.Ctx, "Wrong types of sequence number or failed to read the sequence table")
}

func makePosIncr[T constraints.Integer](incr int64) T {
	if incr < 0 {
		return T(-incr)
	}
	return T(incr)
}

func advanceSeq[T constraints.Integer](lsn, minv, maxv, incrv T,
	cycle, minus, setEdge bool, rel engine.Relation, proc *process.Process, db, tblname string) (string, error) {
	if setEdge {
		// Set lastseqnum to maxv when this is a descending sequence.
		if minus {
			return setSeq(proc, maxv, rel, db, tblname)
		}
		// Set lastseqnum to minv
		return setSeq(proc, minv, rel, db, tblname)
	}
	var adseq T
	if minus {
		adseq = lsn - incrv
	} else {
		adseq = lsn + incrv
	}

	// check descending sequence and reach edge
	if minus && (adseq < minv || adseq > lsn) {
		if !cycle {
			return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
		}
		return setSeq(proc, maxv, rel, db, tblname)
	}

	// checkout ascending sequence and reach edge
	if !minus && (adseq > maxv || adseq < lsn) {
		if !cycle {
			return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
		}
		return setSeq(proc, minv, rel, db, tblname)
	}

	// Otherwise set to adseq.
	return setSeq(proc, adseq, rel, db, tblname)
}

func setSeq[T constraints.Integer](proc *process.Process, setv T, rel engine.Relation, db, tbl string) (string, error) {
	_, err := proc.GetSessionInfo().SqlHelper.ExecSql(fmt.Sprintf("update `%s`.`%s` set last_seq_num = %d", db, tbl, setv))
	if err != nil {
		return "", err
	}

	tblId := rel.GetTableID(proc.Ctx)
	ress := fmt.Sprint(setv)

	// Set Curvalues here. Add new slot to proc's related field.
	proc.GetSessionInfo().SeqAddValues[tblId] = ress

	return ress, nil
}

func setIsCalled[T constraints.Integer](proc *process.Process, rel engine.Relation, lsn T, db, tbl string) (string, error) {
	// Set is called to true.
	_, err := proc.GetSessionInfo().SqlHelper.ExecSql(fmt.Sprintf("update `%s`.`%s` set is_called = true", db, tbl))
	if err != nil {
		return "", err
	}

	tblId := rel.GetTableID(proc.Ctx)
	ress := fmt.Sprint(lsn)

	// Set Curvalues here. Add new slot to proc's related field.
	proc.GetSessionInfo().SeqAddValues[tblId] = ress

	return ress, nil
}

func Setval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	tblnames := vector.GenerateFunctionStrParameter(ivecs[0])
	setnums := vector.GenerateFunctionStrParameter(ivecs[1])
	var iscalled vector.FunctionParameterWrapper[bool]
	if len(ivecs) > 2 {
		iscalled = vector.GenerateFunctionFixedTypeParameter[bool](ivecs[2])
	}

	// Txn
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.GetTxnOperator()
	if txn == nil {
		return moerr.NewInternalError(proc.Ctx, "Setval: txn operator is nil")
	}

	for i := uint64(0); i < uint64(length); i++ {
		tn, tnNull := tblnames.GetStrValue(i)
		sn, snNull := setnums.GetStrValue(i)
		isc := true
		iscNull := false
		if iscalled != nil {
			isc, iscNull = iscalled.GetValue(i)
		}

		if tnNull || snNull || iscNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return
			}
		} else {
			var res string
			res, err = setval(string(tn), string(sn), isc, proc, txn, e)
			if err == nil {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false)
			}
			if err != nil {
				return
			}
		}
	}
	return
}

func setval(tblname, setnum string, iscalled bool, proc *process.Process, txn client.TxnOperator, e engine.Engine) (string, error) {
	db := proc.GetSessionInfo().Database
	dbHandler, err := e.Database(proc.Ctx, db, txn)
	if err != nil {
		return "", err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname, nil)
	if err != nil {
		return "", err
	}

	_values, err := proc.GetSessionInfo().SqlHelper.ExecSql(fmt.Sprintf("select * from `%s`.`%s`", db, tblname))
	if err != nil {
		return "", err
	}
	values := _values[0]
	if values == nil {
		return "", moerr.NewInternalError(proc.Ctx, "Failed to get sequence meta data.")
	}

	switch values[0].(type) {
	case int16:
		minv, maxv := values[1].(int16), values[2].(int16)
		// Parse and compare.
		setnum, err := strconv.ParseInt(setnum, 10, 64)
		if err != nil {
			return "", err
		}
		snum := int16(setnum)
		if snum < minv || snum > maxv {
			return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
		}
		return setVal(proc, snum, iscalled, rel, db, tblname)
	case int32:
		minv, maxv := values[1].(int32), values[2].(int32)
		setnum, err := strconv.ParseInt(setnum, 10, 64)
		if err != nil {
			return "", err
		}
		snum := int32(setnum)
		if snum < minv || snum > maxv {
			return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
		}
		return setVal(proc, snum, iscalled, rel, db, tblname)
	case int64:
		minv, maxv := values[1].(int64), values[2].(int64)
		setnum, err := strconv.ParseInt(setnum, 10, 64)
		if err != nil {
			return "", err
		}
		snum := setnum
		if snum < minv || snum > maxv {
			return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
		}
		return setVal(proc, snum, iscalled, rel, db, tblname)
	case uint16:
		minv, maxv := values[1].(uint16), values[2].(uint16)
		setnum, err := strconv.ParseUint(setnum, 10, 64)
		if err != nil {
			return "", err
		}
		snum := uint16(setnum)
		if snum < minv || snum > maxv {
			return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
		}
		return setVal(proc, snum, iscalled, rel, db, tblname)
	case uint32:
		minv, maxv := values[1].(uint32), values[2].(uint32)
		setnum, err := strconv.ParseUint(setnum, 10, 64)
		if err != nil {
			return "", err
		}
		snum := uint32(setnum)
		if snum < minv || snum > maxv {
			return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
		}
		return setVal(proc, snum, iscalled, rel, db, tblname)
	case uint64:
		minv, maxv := values[1].(uint64), values[2].(uint64)
		setnum, err := strconv.ParseUint(setnum, 10, 64)
		if err != nil {
			return "", err
		}
		snum := uint64(setnum)
		if snum < minv || snum > maxv {
			return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
		}
		return setVal(proc, snum, iscalled, rel, db, tblname)
	}
	return "", moerr.NewInternalError(proc.Ctx, "Wrong types of sequence number or failed to read the sequence table")
}

func setVal[T constraints.Integer](proc *process.Process, setv T, setisCalled bool, rel engine.Relation, db, tbl string) (string, error) {
	_, err := proc.GetSessionInfo().SqlHelper.ExecSql(fmt.Sprintf("update `%s`.`%s` set last_seq_num = %d", db, tbl, setv))
	if err != nil {
		return "", err
	}

	ress := fmt.Sprint(setv)
	if setisCalled {
		tblId := rel.GetTableID(proc.Ctx)

		proc.GetSessionInfo().SeqAddValues[tblId] = ress

		// Only set lastvalue when it is already initialized.
		if proc.GetSessionInfo().SeqLastValue[0] != "" {
			proc.GetSessionInfo().SeqLastValue[0] = ress
		}
	}

	return ress, nil
}

func Currval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])

	// Here is the transaction
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.GetTxnOperator()
	if txn == nil {
		return moerr.NewInternalError(proc.Ctx, "Currval: txn operator is nil")
	}

	dbHandler, err := e.Database(proc.Ctx, proc.GetSessionInfo().Database, txn)
	if err != nil {
		return
	}

	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		if null {
			if err = rs.AppendBytes(nil, true); err != nil {
				return
			}
		} else {
			var rel engine.Relation
			rel, err = dbHandler.Relation(proc.Ctx, string(v), nil)
			if err != nil {
				return
			}
			tblId := rel.GetTableID(proc.Ctx)
			// Get cur values here.
			ss, exists := proc.GetSessionInfo().SeqCurValues[tblId]
			// If nextval called before this currval.Check add values
			ss1, existsAdd := proc.GetSessionInfo().SeqAddValues[tblId]
			if !exists && !existsAdd {
				err = moerr.NewInternalError(proc.Ctx, "Currvalue of %s in current session is not initialized", v)
				return
			}
			// Assign the values of SeqAddValues first, cause this values is the updated curvals.
			if existsAdd {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(ss1), false)
			} else {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(ss), false)
			}
			if err != nil {
				return
			}
		}
	}
	return
}

func Lastval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	// Get last value
	lastv := proc.GetSessionInfo().SeqLastValue[0]
	if lastv == "" {
		return moerr.NewInternalError(proc.Ctx, "Last value of current session is not initialized.")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err = rs.AppendBytes(functionUtil.QuickStrToBytes(lastv), false); err != nil {
			return
		}
	}
	return
}
