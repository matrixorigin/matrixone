// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// LockCloneTargetNames uses the same synthetic catalog primary keys as
// lockMoTable. Holding every candidate name lets the clone inspect incoming FK
// references once before admitting independent table producers.
func LockCloneTargetNames(proc *process.Process, account uint32, database string, names []string) error {
	if len(names) == 0 {
		return nil
	}
	rel, err := colexec.GetNewRelation(proc.GetSessionInfo().StorageEngine, catalog.MO_CATALOG, catalog.MO_TABLES, proc.GetTxnOperator(), proc.Ctx)
	if err != nil {
		return err
	}
	accounts, err := vector.NewConstFixed(types.T_uint32.ToType(), account, len(names), proc.Mp())
	if err != nil {
		return err
	}
	defer accounts.Free(proc.Mp())
	databases, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(database), len(names), proc.Mp())
	if err != nil {
		return err
	}
	defer databases.Free(proc.Mp())
	tables := vector.NewVec(types.T_varchar.ToType())
	defer tables.Free(proc.Mp())
	for _, name := range names {
		if err := vector.AppendBytes(tables, []byte(name), false, proc.Mp()); err != nil {
			return err
		}
	}
	keys, err := function.RunFunctionDirectly(proc, function.SerialFunctionEncodeID, []*vector.Vector{accounts, databases, tables}, len(names))
	if err != nil {
		return err
	}
	defer keys.Free(proc.Mp())
	rows := batch.NewWithSize(1)
	rows.SetVector(0, keys)
	rows.SetRowCount(len(names))
	return lockop.LockRows(proc.GetSessionInfo().StorageEngine, proc, rel, catalog.MO_TABLES_ID, rows, 0,
		*keys.GetType(), lock.LockMode_Exclusive, lock.Sharding_None, account)
}
