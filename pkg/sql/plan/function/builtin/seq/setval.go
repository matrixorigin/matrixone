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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	txn := proc.TxnOperator
	if txn == nil {
		return nil, moerr.NewInternalError(proc.Ctx, "Setval: txn operator is nil")
	}

	resultType := types.T_varchar.ToType()
	if vecs[0].IsConstNull() || vecs[1].IsConstNull() {
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
				return nil, err
			}
			ress[i] = s
		}
	}
	vector.AppendStringList(res, ress, isNulls, proc.Mp())
	return res, nil
}

func setval(tblname, setnum string, iscalled bool, proc *process.Process, txn client.TxnOperator, e engine.Engine) (string, error) {
	db := proc.SessionInfo.Database
	dbHandler, err := e.Database(proc.Ctx, db, txn)
	if err != nil {
		return "", err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname)
	if err != nil {
		return "", err
	}

	values, err := proc.SessionInfo.SqlHelper.ExecSql(fmt.Sprintf("select * from `%s`.`%s`", db, tblname))
	if err != nil {
		return "", err
	}
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
	_, err := proc.SessionInfo.SqlHelper.ExecSql(fmt.Sprintf("update `%s`.`%s` set last_seq_num = %d", db, tbl, setv))
	if err != nil {
		return "", err
	}

	ress := fmt.Sprint(setv)
	if setisCalled {
		tblId := rel.GetTableID(proc.Ctx)

		proc.SessionInfo.SeqAddValues[tblId] = ress

		// Only set lastvalue when it is already initialized.
		if proc.SessionInfo.SeqLastValue[0] != "" {
			proc.SessionInfo.SeqLastValue[0] = ress
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
