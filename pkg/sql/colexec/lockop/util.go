// Copyright 2023 Matrix Origin
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

package lockop

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func getRelFromMoCatalog(ctx context.Context, eng engine.Engine, txnOp client.TxnOperator, tblName string) (engine.Relation, error) {
	dbSource, err := eng.Database(ctx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return nil, err
	}

	rel, err := dbSource.Relation(ctx, tblName, nil)
	if err != nil {
		return nil, err
	}

	return rel, nil
}

func getLockVector(proc *process.Process, accountId uint32, names []string) (*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(names)+1)
	defer func() {
		for _, v := range vecs {
			if v != nil {
				proc.PutVector(v)
			}
		}
	}()

	// append account_id
	accountIdVec := proc.GetVector(types.T_uint32.ToType())
	err := vector.AppendFixed(accountIdVec, accountId, false, proc.GetMPool())
	if err != nil {
		return nil, err
	}
	vecs[0] = accountIdVec
	// append names
	for i, name := range names {
		nameVec := proc.GetVector(types.T_varchar.ToType())
		err := vector.AppendBytes(nameVec, []byte(name), false, proc.GetMPool())
		if err != nil {
			return nil, err
		}
		vecs[i+1] = nameVec
	}

	vec, err := function.RunFunctionDirectly(proc, function.SerialFunctionEncodeID, vecs, 1)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func LockMoDatabase(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	dbName string) error {
	dbRel, err := getRelFromMoCatalog(ctx, eng, proc.TxnOperator, catalog.MO_DATABASE)
	if err != nil {
		return err
	}
	vec, err := getLockVector(proc, proc.SessionInfo.AccountId, []string{dbName})
	if err != nil {
		return err
	}
	if err := lockRows(eng, proc, dbRel, vec, lock.LockMode_Exclusive); err != nil {
		return err
	}
	return nil
}

func LockMoTable(
	ctx context.Context,
	proc *process.Process,
	eng engine.Engine,
	dbName string,
	tblName string,
	lockMode lock.LockMode) error {
	dbRel, err := getRelFromMoCatalog(ctx, eng, proc.TxnOperator, catalog.MO_TABLES)
	if err != nil {
		return err
	}
	vec, err := getLockVector(proc, proc.SessionInfo.AccountId, []string{dbName, tblName})
	if err != nil {
		return err
	}
	defer vec.Free(proc.Mp())
	if err := lockRows(eng, proc, dbRel, vec, lockMode); err != nil {
		return err
	}
	return nil
}

func lockRows(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	vec *vector.Vector,
	lockMode lock.LockMode) error {

	if vec == nil || vec.Length() == 0 {
		panic("lock rows is empty")
	}

	id := rel.GetTableID(proc.Ctx)

	err := LockRows(
		eng,
		proc,
		id,
		vec,
		*vec.GetType(),
		lockMode)
	return err
}
