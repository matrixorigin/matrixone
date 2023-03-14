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
	"strconv"

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

// TODO: Require update privileges on the sequence.

// Sets sequence's last_seq_num, and can optionally set is_called.
// Setvalue can change lastval when 1.lastval is initialized by nextval 2. setval got true param.
// Setvalue can change currval when 1. setval got true param.

// First vec is sequence names.
func Setval(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn, err := NewTxn(e, proc, proc.Ctx)
	if err != nil {
		return nil, err
	}

	resultType := types.T_varchar.ToType()
	if vecs[0].IsConstNull() || vecs[1].IsConstNull() {
		if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
			return nil, err1
		}
		return vector.NewConstNull(resultType, vecs[0].Length(), proc.Mp()), nil
	}

	tblnames := vector.MustStrCol(vecs[0])
	setnums := vector.MustStrCol(vecs[1])
	iscalled := make([]bool, 1)
	iscalled[0] = true

	var v3 *vector.Vector
	v3 = nil
	if len(vecs) == 3 {
		if vecs[2].IsConstNull() {
			if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
				return nil, err1
			}
			return vector.NewConstNull(resultType, vecs[0].Length(), proc.Mp()), nil
		}
		v3 = vecs[2]
		iscalled = vector.MustFixedCol[bool](vecs[2])
	}

	maxLen := vecs[0].Length()
	for _, v := range vecs {
		if v.Length() > maxLen {
			maxLen = v.Length()
		}
	}

	res, err := proc.AllocVectorOfRows(types.T_varchar.ToType(), 0, nil)
	if err != nil {
		if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
			return nil, err1
		}
		return nil, err
	}

	ress := make([]string, len(tblnames))
	isNulls := make([]bool, len(tblnames))
	if len(tblnames) == 1 {
		// When len(tblnames) == 1, left 2 params are 1 too.
		for i := range tblnames {
			if checkNulls(vecs[0], vecs[1], v3, 0, 0, 0) {
				isNulls[i] = true
				continue
			}
			s, err := setval(tblnames[0], setnums[0], iscalled[0], proc, txn, e)
			if err != nil {
				if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
					return nil, err1
				}
				return nil, err
			}
			ress[i] = s
		}
	} else if len(setnums) == 1 && len(iscalled) == 1 {
		for i := range tblnames {
			if checkNulls(vecs[0], vecs[1], v3, uint64(i), 0, 0) {
				isNulls[i] = true
				continue
			}
			s, err := setval(tblnames[i], setnums[0], iscalled[0], proc, txn, e)
			if err != nil {
				if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
					return nil, err1
				}
				return nil, err
			}
			ress[i] = s
		}
	} else if len(setnums) == 1 {
		for i := range tblnames {
			if checkNulls(vecs[0], vecs[1], v3, uint64(i), 0, uint64(i)) {
				isNulls[i] = true
				continue
			}
			s, err := setval(tblnames[i], setnums[0], iscalled[i], proc, txn, e)
			if err != nil {
				if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
					return nil, err1
				}
				return nil, err
			}
			ress[i] = s
		}
	} else if len(iscalled) == 1 {
		for i := range tblnames {
			if checkNulls(vecs[0], vecs[1], v3, uint64(i), uint64(i), 0) {
				isNulls[i] = true
				continue
			}
			s, err := setval(tblnames[i], setnums[i], iscalled[0], proc, txn, e)
			if err != nil {
				if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
					return nil, err1
				}
				return nil, err
			}
			ress[i] = s
		}
	} else {
		for i := range tblnames {
			if checkNulls(vecs[0], vecs[1], v3, uint64(i), uint64(i), uint64(i)) {
				isNulls[i] = true
				continue
			}
			s, err := setval(tblnames[i], setnums[i], iscalled[i], proc, txn, e)
			if err != nil {
				if err1 := RollbackTxn(e, txn, proc.Ctx); err1 != nil {
					return nil, err1
				}
				return nil, err
			}
			ress[i] = s
		}
	}

	if err = CommitTxn(e, txn, proc.Ctx); err != nil {
		return nil, err
	}
	vector.AppendStringList(res, ress, isNulls, proc.Mp())
	return res, nil
}

func setval(tblname, setnum string, iscalled bool, proc *process.Process, txn client.TxnOperator, e engine.Engine) (string, error) {
	var rds []engine.Reader

	dbHandler, err := e.Database(proc.Ctx, proc.SessionInfo.Database, txn)
	if err != nil {
		return "", err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname)
	if err != nil {
		return "", err
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

		switch bat.Vecs[0].GetType().Oid {
		case types.T_int16:
			_, minv, maxv, _, _, _, _ := getValues[int16](bat.Vecs)
			// Parse and compare.
			setnum, err := strconv.ParseInt(setnum, 10, 64)
			if err != nil {
				return "", err
			}
			snum := int16(setnum)
			if snum < minv || snum > maxv {
				return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
			}
			return setVal(proc, snum, iscalled, bat, rel)
		case types.T_int32:
			_, minv, maxv, _, _, _, _ := getValues[int32](bat.Vecs)
			setnum, err := strconv.ParseInt(setnum, 10, 64)
			if err != nil {
				return "", err
			}
			snum := int32(setnum)
			if snum < minv || snum > maxv {
				return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
			}
			return setVal(proc, snum, iscalled, bat, rel)
		case types.T_int64:
			_, minv, maxv, _, _, _, _ := getValues[int64](bat.Vecs)
			setnum, err := strconv.ParseInt(setnum, 10, 64)
			if err != nil {
				return "", err
			}
			snum := setnum
			if snum < minv || snum > maxv {
				return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
			}
			return setVal(proc, snum, iscalled, bat, rel)
		case types.T_uint16:
			_, minv, maxv, _, _, _, _ := getValues[uint16](bat.Vecs)
			setnum, err := strconv.ParseUint(setnum, 10, 64)
			if err != nil {
				return "", err
			}
			snum := uint16(setnum)
			if snum < minv || snum > maxv {
				return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
			}
			return setVal(proc, snum, iscalled, bat, rel)
		case types.T_uint32:
			_, minv, maxv, _, _, _, _ := getValues[uint32](bat.Vecs)
			setnum, err := strconv.ParseUint(setnum, 10, 64)
			if err != nil {
				return "", err
			}
			snum := uint32(setnum)
			if snum < minv || snum > maxv {
				return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
			}
			return setVal(proc, snum, iscalled, bat, rel)
		case types.T_uint64:
			_, minv, maxv, _, _, _, _ := getValues[uint64](bat.Vecs)
			setnum, err := strconv.ParseUint(setnum, 10, 64)
			if err != nil {
				return "", err
			}
			snum := uint64(setnum)
			if snum < minv || snum > maxv {
				return "", moerr.NewInternalError(proc.Ctx, "Set value is not in range (minvalue, maxvlue).")
			}
			return setVal(proc, snum, iscalled, bat, rel)
		}
	}
	return "", moerr.NewInternalError(proc.Ctx, "Wrong types of sequence number or failed to read the sequence table")
}

func setVal[T constraints.Integer](proc *process.Process, setv T, setisCalled bool, bat *batch.Batch, rel engine.Relation) (string, error) {
	// Made the bat to update batch.
	bat.Vecs[0].CleanOnlyData()
	err := vector.AppendAny(bat.Vecs[0], setv, false, proc.Mp())
	if err != nil {
		return "", err
	}
	bat.Vecs[6].CleanOnlyData()
	err = vector.AppendAny(bat.Vecs[6], setisCalled, false, proc.Mp())
	if err != nil {
		return "", err
	}

	simpleUpdate(proc, bat, rel)

	ress := fmt.Sprint(setv)

	if setisCalled {
		tblId := rel.GetTableID(proc.Ctx)
		proc.SessionInfo.ValueSetter.SetSeqCurValues(tblId, ress)
		// Only set lastvalue when it is already initialized.
		if s := proc.SessionInfo.ValueSetter.GetSeqLastValue(); s != "" {
			proc.SessionInfo.ValueSetter.SetSeqLastValue(ress)
		}
	}

	return ress, nil
}

func checkNulls(v1, v2, v3 *vector.Vector, i1, i2, i3 uint64) bool {
	if v3 == nil {
		return nulls.Contains(v1.GetNulls(), i1) || nulls.Contains(v2.GetNulls(), i2)
	}
	return nulls.Contains(v1.GetNulls(), i1) || nulls.Contains(v2.GetNulls(), i2) || nulls.Contains(v3.GetNulls(), i3)
}
