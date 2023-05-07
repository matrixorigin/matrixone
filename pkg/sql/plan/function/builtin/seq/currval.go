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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TODO: Require usage or select privilege on the sequence.

// Currval will be init by nextval() or setval().
// When third arg or nextval is true it will set or init the currval of this session.
func Currval(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// Get the table ids, and make key to retrieve from values stored in session.
	tblnames := vector.MustStrCol(vecs[0])

	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.TxnOperator
	if txn == nil {
		return nil, moerr.NewInternalError(proc.Ctx, "Currval: txn operator is nil")
	}
	dbHandler, err := e.Database(proc.Ctx, proc.SessionInfo.Database, txn)
	if err != nil {
		return nil, err
	}
	res, err := proc.AllocVectorOfRows(types.T_varchar.ToType(), 0, nil)
	if err != nil {
		return nil, err
	}

	ress := make([]string, len(tblnames))
	isNulls := make([]bool, len(tblnames))
	for i := range tblnames {
		if tblnames[i] == "" {
			isNulls[i] = true
			continue
		}
		rel, err := dbHandler.Relation(proc.Ctx, tblnames[i])
		if err != nil {
			return nil, err
		}
		tblId := rel.GetTableID(proc.Ctx)
		// Get cur values here.
		ss, exists := proc.SessionInfo.SeqCurValues[tblId]
		// If nextval called before this currval.Check add values
		ss1, existsAdd := proc.SessionInfo.SeqAddValues[tblId]
		if !exists && !existsAdd {
			return nil, moerr.NewInternalError(proc.Ctx, "Currvalue of %s in current session is not initialized", tblnames[i])
		}
		// Assign the values of SeqAddValues first, cause this values is the updated curvals.
		if existsAdd {
			ress[i] = ss1
		} else {
			ress[i] = ss
		}
	}

	if err := vector.AppendStringList(res, ress, isNulls, proc.Mp()); err != nil {
		return nil, err
	}
	return res, nil
}
