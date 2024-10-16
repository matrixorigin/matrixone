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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	mu                sync.Mutex
	internalProcesses = map[string]*process.Process{}
)

// LockTableWithUniqueID is similar to LockTable, but used to lock a table directly based on
// a unique identifier, without using an external transaction.
func LockTableWithUniqueID(
	ctx context.Context,
	sid string,
	uniqueID string,
	tableID uint64,
	txnClient client.TxnClient,
	pkType types.Type,
	eng engine.Engine,
	mp *mpool.MPool,
	mode lock.LockMode,
) error {
	proc, err := getInternalProcessByUniqueID(
		ctx,
		sid,
		uniqueID,
		mp,
		txnClient,
	)
	if err != nil {
		return err
	}

	parker := types.NewPacker()
	defer parker.Close()

	opts := DefaultLockOptions(parker).
		WithLockMode(mode).
		WithLockTable(true, true).
		WithHasNewVersionInRangeFunc(func(
			proc *process.Process,
			rel engine.Relation,
			tableID uint64,
			eng engine.Engine,
			vec *vector.Vector,
			from, to timestamp.Timestamp) (bool, error) {
			return false, nil
		}).
		WithFetchLockRowsFunc(GetFetchRowsFunc(pkType))
	_, _, _, err = doLock(
		proc.Ctx,
		eng,
		nil,
		nil,
		tableID,
		proc,
		nil,
		pkType,
		opts)
	return err
}

// UnlockWithUniqueID unlock all locks by uniqueID
func UnlockWithUniqueID(
	ctx context.Context,
	uniqueID string) error {
	proc := mustGetProcessByUniqueID(uniqueID)
	return proc.GetTxnOperator().Commit(ctx)
}

func getInternalProcessByUniqueID(
	ctx context.Context,
	sid string,
	id string,
	mp *mpool.MPool,
	txnClient client.TxnClient,
) (*process.Process, error) {

	mu.Lock()
	defer mu.Unlock()

	if proc, ok := internalProcesses[id]; ok {
		return proc, nil
	}
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"getInternalProcessByUniqueID",
		0)
	op, err := txnClient.New(ctx, timestamp.Timestamp{}, createByOpt)
	if err != nil {
		return nil, err
	}
	v, _ := runtime.ServiceRuntime(sid).GetGlobalVariables(runtime.LockService)
	proc := process.NewTopProcess(
		ctx,
		mp,
		txnClient,
		op,
		nil,
		v.(lockservice.LockService),
		nil,
		nil,
		nil,
		nil,
	)
	internalProcesses[id] = proc
	return proc, nil
}

func mustGetProcessByUniqueID(id string) *process.Process {
	mu.Lock()
	defer mu.Unlock()

	if proc, ok := internalProcesses[id]; ok {
		delete(internalProcesses, id)
		return proc
	}
	panic("LockTableWithUniqueID not called or call UnlockWithUniqueID multi times")
}
