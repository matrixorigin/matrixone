package lockop

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

func getRelFromMoCatalog(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	tblName string,
) (engine.Relation, error) {
	dbSource, err := eng.Database(ctx, catalog.MO_CATALOG, proc.TxnOperator)
	if err != nil {
		return nil, err
	}

	rel, err := dbSource.Relation(ctx, tblName, nil)
	if err != nil {
		return nil, err
	}

	return rel, nil
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

func doLockTable(
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	defChanged bool) error {
	id := rel.GetTableID(proc.Ctx)
	defs, err := rel.GetPrimaryKeys(proc.Ctx)
	if err != nil {
		return err
	}

	if len(defs) != 1 {
		panic("invalid primary keys")
	}

	err = lockTable(
		eng,
		proc,
		id,
		defs[0].Type,
		defChanged)

	return err
}

func LockTable(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	rel engine.Relation,
	dbName string,
	partitionTableNames []string,
	defChanged bool) error {

	if len(partitionTableNames) == 0 {
		return doLockTable(eng, proc, rel, defChanged)
	}

	dbSource, err := eng.Database(ctx, dbName, proc.TxnOperator)
	if err != nil {
		return err
	}

	for _, tableName := range partitionTableNames {
		pRel, pErr := dbSource.Relation(ctx, tableName, nil)
		if pErr != nil {
			return pErr
		}
		err = doLockTable(eng, proc, pRel, defChanged)
		if err != nil {
			return err
		}
	}
	return nil
}

func LockMoTable(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	dbName string,
	tblName string,
	lockMode lock.LockMode,
) error {
	dbRel, err := getRelFromMoCatalog(ctx, eng, proc, catalog.MO_TABLES)
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

func LockMoDatabase(
	ctx context.Context,
	eng engine.Engine,
	proc *process.Process,
	dbName string,
) error {
	dbRel, err := getRelFromMoCatalog(ctx, eng, proc, catalog.MO_DATABASE)
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
