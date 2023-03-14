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

package seq

import (
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// TODO: Requires usage or update privilege on the sequence.

// Retrieve values of this sequence.
// Set curval,lastval of current session.
// Set is_called to true if it is false, if is_called is true Advance last_seq_num.
// Return advanced last_seq_num.
func Nextval(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := NewTxn(e, proc, proc.Ctx)
	if err != nil {
		return nil, err
	}
	// nextval is the implementation of nextval function.
	tblnames := vector.MustStrCols(vecs[0])
	restrings := make([]string, len(tblnames))

	res, err := proc.AllocVectorOfRows(types.T_varchar.ToType(), 0, nil)
	if err != nil {
		if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
			return nil, err1
		}
		return nil, err
	}

	for i := 0; i < vecs[0].Length(); i++ {
		if nulls.Contains(vecs[0].Nsp, uint64(i)) {
			nulls.Add(res.Nsp, uint64(i))
			continue
		}
		s, err := nextval(tblnames[i], proc, e, txn)
		if err != nil {
			if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
				return nil, err1
			}
			return nil, err
		}
		restrings[i] = s
	}

	vector.AppendString(res, restrings, proc.Mp())

	for i := len(restrings) - 1; i >= 0; i-- {
		if restrings[i] != "" {
			proc.SessionInfo.ValueSetter.SetSeqLastValue(restrings[i])
			break
		}
	}

	if err = CommitTxn(e, txn, proc.Ctx); err != nil {
		return nil, err
	}
	return res, nil
}

func nextval(tblname string, proc *process.Process, e engine.Engine, txn client.TxnOperator) (string, error) {
	var rds []engine.Reader

	dbHandler, err := e.Database(proc.Ctx, proc.SessionInfo.Database, txn)
	if err != nil {
		return "", err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname)
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

	// Read blocks of the sequence table.
	expr := &plan.Expr{}
	ret, err := rel.Ranges(proc.Ctx, expr)
	if err != nil {
		return "", err
	}
	switch {
	case len(ret) == 0:
		if rds, err = rel.NewReader(proc.Ctx, 1, expr, nil); err != nil {
			return "", err
		}
	case len(ret) == 1 && len(ret[0]) == 0:
		if rds, err = rel.NewReader(proc.Ctx, 1, expr, nil); err != nil {
			return "", err
		}
	case len(ret[0]) == 0:
		rds0, err := rel.NewReader(proc.Ctx, 1, expr, nil)
		if err != nil {
			return "", err
		}
		rds1, err := rel.NewReader(proc.Ctx, 1, expr, ret[1:])
		if err != nil {
			return "", err
		}
		rds = append(rds, rds0...)
		rds = append(rds, rds1...)
	default:
		rds, _ = rel.NewReader(proc.Ctx, 1, expr, ret)
	}

	for len(rds) > 0 {
		bat, err := rds[0].Read(proc.Ctx, Sequence_cols_name, expr, proc.Mp())
		defer func() {
			if bat != nil {
				bat.Clean(proc.Mp())
			}
		}()

		if err != nil {
			return "", moerr.NewInvalidInput(proc.Ctx, "Can not find the sequence")
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}
		if len(bat.Vecs) < 8 {
			return "", moerr.NewInternalError(proc.Ctx, "Wrong sequence cols num")
		}

		attrs := make([]string, len(Sequence_cols_name))
		for i := range attrs {
			attrs[i] = Sequence_cols_name[i]
		}
		bat.Attrs = attrs

		switch bat.Vecs[0].Typ.Oid {
		case types.T_int16:
			// Get values store in sequence table.
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[int16](bat.Vecs)
			// When iscalled is not set, set it and do not advance sequence number.
			if !isCalled {
				return setIsCalled(proc, bat, rel, lastSeqNum)
			}
			// When incr is over the range of this datatype.
			if incrv > math.MaxInt64 || incrv < math.MinInt64 {
				if cycle {
					return advanceSeq(lastSeqNum, minv, maxv, int16(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			// Tranforming incrv to this datatype and make it positive for generic use.
			return advanceSeq(lastSeqNum, minv, maxv, makePosIncr[int16](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_int32:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[int32](bat.Vecs)
			if !isCalled {
				return setIsCalled(proc, bat, rel, lastSeqNum)
			}
			if incrv > math.MaxInt32 || incrv < math.MinInt32 {
				if cycle {
					return advanceSeq(lastSeqNum, minv, maxv, int32(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			return advanceSeq(lastSeqNum, minv, maxv, makePosIncr[int32](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_int64:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[int64](bat.Vecs)
			if !isCalled {
				return setIsCalled(proc, bat, rel, lastSeqNum)
			}
			return advanceSeq(lastSeqNum, minv, maxv, makePosIncr[int64](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_uint16:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[uint16](bat.Vecs)
			if !isCalled {
				return setIsCalled(proc, bat, rel, lastSeqNum)
			}
			if incrv > math.MaxUint16 || -incrv > math.MaxUint16 {
				if cycle {
					return advanceSeq(lastSeqNum, minv, maxv, uint16(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			return advanceSeq(lastSeqNum, minv, maxv, makePosIncr[uint16](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_uint32:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[uint32](bat.Vecs)
			if !isCalled {
				return setIsCalled(proc, bat, rel, lastSeqNum)
			}
			if incrv > math.MaxUint32 || -incrv > math.MaxUint32 {
				if cycle {
					return advanceSeq(lastSeqNum, minv, maxv, uint32(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			return advanceSeq(lastSeqNum, minv, maxv, makePosIncr[uint32](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_uint64:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[uint64](bat.Vecs)
			if !isCalled {
				return setIsCalled(proc, bat, rel, lastSeqNum)
			}
			return advanceSeq(lastSeqNum, minv, maxv, makePosIncr[uint64](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		}
	}
	return "", moerr.NewInternalError(proc.Ctx, "Wrong types of sequence number or failed to read the sequence table")
}

func makePosIncr[T constraints.Integer](incr int64) T {
	if incr < 0 {
		return T(-incr)
	}
	return T(incr)
}

func advanceSeq[T constraints.Integer](lastsn, minv, maxv, incrv T,
	cycle, minus, setEdge bool, bat *batch.Batch, rel engine.Relation, proc *process.Process, tblname string) (string, error) {
	if setEdge {
		// Set lastseqnum to maxv when this is a descending sequence.
		if minus {
			return setSeq(proc, maxv, bat, rel)
		}
		// Set lastseqnum to minv
		return setSeq(proc, minv, bat, rel)
	}
	var adseq T
	if minus {
		adseq = lastsn - incrv
	} else {
		adseq = lastsn + incrv
	}

	// check descending sequence and reach edge
	if minus && (adseq < minv || adseq > lastsn) {
		if !cycle {
			return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
		}
		return setSeq(proc, maxv, bat, rel)
	}

	// checkout ascending sequence and reach edge
	if !minus && (adseq > maxv || adseq < lastsn) {
		if !cycle {
			return "", moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
		}
		return setSeq(proc, minv, bat, rel)
	}

	// Otherwise set to adseq.
	return setSeq(proc, adseq, bat, rel)
}

func setSeq[T constraints.Integer](proc *process.Process, setv T, bat *batch.Batch, rel engine.Relation) (string, error) {
	// Made the bat to update batch.
	bat.Vecs[0].CleanOnlyData()
	err := bat.Vecs[0].Append(setv, false, proc.Mp())
	if err != nil {
		return "", err
	}

	simpleUpdate(proc, bat, rel)

	tblId := rel.GetTableID(proc.Ctx)
	ress := fmt.Sprint(setv)
	proc.SessionInfo.ValueSetter.SetSeqCurValues(tblId, ress)

	return ress, nil
}

func setIsCalled[T constraints.Integer](proc *process.Process, bat *batch.Batch, rel engine.Relation, lastSeqNum T) (string, error) {
	// Here made the bat to update batch.
	// Set is called to true.
	bat.Vecs[6].CleanOnlyData()
	err := bat.Vecs[6].Append(true, false, proc.Mp())
	if err != nil {
		return "", err
	}

	simpleUpdate(proc, bat, rel)

	tblId := rel.GetTableID(proc.Ctx)
	ress := fmt.Sprint(lastSeqNum)
	proc.SessionInfo.ValueSetter.SetSeqCurValues(tblId, ress)

	return ress, nil
}
