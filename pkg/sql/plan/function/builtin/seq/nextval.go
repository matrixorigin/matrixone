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
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// Requires usage or update privilege on the sequence.
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
	res, err := nextval(vecs, proc, e, txn)
	if err != nil {
		if err1 := RolllbackTxn(e, txn, proc.Ctx); err1 != nil {
			return nil, err1
		}
		return nil, err
	}
	if err = CommitTxn(e, txn, proc.Ctx); err != nil {
		return nil, err
	}
	return res, nil
}

func nextval(vecs []*vector.Vector, proc *process.Process, e engine.Engine, txn client.TxnOperator) (*vector.Vector, error) {
	var rds []engine.Reader
	tblname := vector.MustStrCols(vecs[0])[0]

	dbHandler, err := e.Database(proc.Ctx, proc.SessionInfo.Database, txn)
	if err != nil {
		return nil, err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname)
	if err != nil {
		return nil, err
	}

	// Check is sequence table.
	td, err := rel.TableDefs(proc.Ctx)
	if err != nil {
		return nil, err
	}
	if td[len(td)-1].(*engine.PropertiesDef).Properties[0].Value != catalog.SystemSequenceRel {
		return nil, moerr.NewInternalError(proc.Ctx, "Table input is not a sequence")
	}

	expr := &plan.Expr{}

	// Read blocks of the sequence table.
	ret, err := rel.Ranges(proc.Ctx, expr)
	if err != nil {
		return nil, err
	}
	switch {
	case len(ret) == 0:
		if rds, err = rel.NewReader(proc.Ctx, 1, expr, nil); err != nil {
			return nil, err
		}
	case len(ret) == 1 && len(ret[0]) == 0:
		if rds, err = rel.NewReader(proc.Ctx, 1, expr, nil); err != nil {
			return nil, err
		}
	case len(ret[0]) == 0:
		rds0, err := rel.NewReader(proc.Ctx, 1, expr, nil)
		if err != nil {
			return nil, err
		}
		rds1, err := rel.NewReader(proc.Ctx, 1, expr, ret[1:])
		if err != nil {
			return nil, err
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
			return nil, moerr.NewInvalidInput(proc.Ctx, "Can not find the sequence")
		}
		if bat == nil {
			rds[0].Close()
			rds = rds[1:]
			continue
		}
		if len(bat.Vecs) < 8 {
			return nil, moerr.NewInternalError(proc.Ctx, "Wrong sequence cols num")
		}

		switch bat.Vecs[0].Typ.Oid {
		case types.T_int16:
			// Get values store in sequence table.
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[int16](bat.Vecs)
			resvec, err := proc.AllocVector(types.T_int16.ToType(), 0)
			if err != nil {
				return nil, err
			}
			// When iscalled is not set, set it and do not advance sequence number.
			if !isCalled {
				return setIsCalled(resvec, proc, bat, rel, lastSeqNum)
			}

			// When incr is over the range of this datatype.
			if incrv > math.MaxInt64 || incrv < math.MinInt64 {
				if cycle {
					return advanceSeq(resvec, lastSeqNum, minv, maxv, int16(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return nil, moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			// Tranforming incrv to this datatype and make it positive for generic use.
			return advanceSeq(resvec, lastSeqNum, minv, maxv, makePosIncr[int16](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_int32:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[int32](bat.Vecs)
			resvec, err := proc.AllocVector(types.T_int32.ToType(), 0)
			if err != nil {
				return nil, err
			}
			if !isCalled {
				return setIsCalled(resvec, proc, bat, rel, lastSeqNum)
			}
			if incrv > math.MaxInt32 || incrv < math.MinInt32 {
				if cycle {
					return advanceSeq(resvec, lastSeqNum, minv, maxv, int32(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return nil, moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			return advanceSeq(resvec, lastSeqNum, minv, maxv, makePosIncr[int32](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_int64:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[int64](bat.Vecs)
			resvec, err := proc.AllocVector(types.T_int64.ToType(), 0)
			if err != nil {
				return nil, err
			}
			if !isCalled {
				return setIsCalled(resvec, proc, bat, rel, lastSeqNum)
			}
			return advanceSeq(resvec, lastSeqNum, minv, maxv, makePosIncr[int64](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_uint16:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[uint16](bat.Vecs)
			resvec, err := proc.AllocVector(types.T_uint16.ToType(), 0)
			if err != nil {
				return nil, err
			}
			if !isCalled {
				return setIsCalled(resvec, proc, bat, rel, lastSeqNum)
			}
			if incrv > math.MaxUint16 || -incrv > math.MaxUint16 {
				if cycle {
					return advanceSeq(resvec, lastSeqNum, minv, maxv, uint16(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return nil, moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			return advanceSeq(resvec, lastSeqNum, minv, maxv, makePosIncr[uint16](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_uint32:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[uint32](bat.Vecs)
			resvec, err := proc.AllocVector(types.T_uint32.ToType(), 0)
			if err != nil {
				return nil, err
			}
			if !isCalled {
				return setIsCalled(resvec, proc, bat, rel, lastSeqNum)
			}
			if incrv > math.MaxUint32 || -incrv > math.MaxUint32 {
				if cycle {
					return advanceSeq(resvec, lastSeqNum, minv, maxv, uint32(incrv), cycle, incrv < 0, setEdge, bat, rel, proc, tblname)
				} else {
					return nil, moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
				}
			}
			return advanceSeq(resvec, lastSeqNum, minv, maxv, makePosIncr[uint32](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		case types.T_uint64:
			lastSeqNum, minv, maxv, _, incrv, cycle, isCalled := getValues[uint64](bat.Vecs)
			resvec, err := proc.AllocVector(types.T_uint64.ToType(), 0)
			if err != nil {
				return nil, err
			}
			if !isCalled {
				return setIsCalled(resvec, proc, bat, rel, lastSeqNum)
			}
			return advanceSeq(resvec, lastSeqNum, minv, maxv, makePosIncr[uint64](incrv), cycle, incrv < 0, !setEdge, bat, rel, proc, tblname)
		}
	}
	return nil, moerr.NewInternalError(proc.Ctx, "Wrong types of sequence number or failed to read the sequence table")
}

func makePosIncr[T constraints.Integer](incr int64) T {
	if incr < 0 {
		return T(-incr)
	}
	return T(incr)
}

func advanceSeq[T constraints.Integer](res *vector.Vector, lastsn, minv, maxv, incrv T,
	cycle, minus, setEdge bool, bat *batch.Batch, rel engine.Relation, proc *process.Process, tblname string) (*vector.Vector, error) {
	if setEdge {
		// Set lastseqnum to maxv when this is a descending sequence.
		if minus {
			return setSeq(res, proc, maxv, bat, rel)
		}
		// Set lastseqnum to minv
		return setSeq(res, proc, minv, bat, rel)
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
			return nil, moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
		}
		return setSeq(res, proc, maxv, bat, rel)
	}

	// checkout ascending sequence and reach edge
	if !minus && (adseq > maxv || adseq < lastsn) {
		if !cycle {
			return nil, moerr.NewInternalError(proc.Ctx, "Reached maximum value of sequence %s", tblname)
		}
		return setSeq(res, proc, minv, bat, rel)
	}

	// Otherwise set to adseq.
	return setSeq(res, proc, adseq, bat, rel)
}

func setSeq[T constraints.Integer](res *vector.Vector, proc *process.Process, setv T, bat *batch.Batch, rel engine.Relation) (*vector.Vector, error) {
	res.Append(setv, false, proc.Mp())
	// Made the bat to update batch.
	bat.Vecs[0].CleanOnlyData()
	err := bat.Vecs[0].Append(setv, false, proc.Mp())
	if err != nil {
		return nil, err
	}
	simpleUpdate(proc, bat, rel)
	return res, nil
}

func setIsCalled[T constraints.Integer](res *vector.Vector, proc *process.Process, bat *batch.Batch, rel engine.Relation, lastSeqNum T) (*vector.Vector, error) {
	// Return just lastSeqNum.
	res.Append(lastSeqNum, false, proc.Mp())

	// Here made the bat to update batch.
	// Set is called to true.
	bat.Vecs[6].CleanOnlyData()
	err := bat.Vecs[6].Append(true, false, proc.Mp())
	if err != nil {
		return nil, err
	}

	simpleUpdate(proc, bat, rel)
	return res, nil
}
