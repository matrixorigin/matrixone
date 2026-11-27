// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iscp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var errIterationNeedsRecovery = errors.New("iscp iteration needs durable recovery")

// CollectChanges consumes an inclusive interval starting after the last safe
// watermark. Publishing From on failure would skip the first failed timestamp.
func previousWatermark(from types.TS) types.TS {
	if from.IsEmpty() {
		return from
	}
	return from.Prev()
}

// unfinishedIteration drops independently completed or superseded members of a
// shared iteration. A Running row alone does not prove this runner owns it.
func unfinishedIteration(iter *IterationContext, specs []*JobSpec, statuses []*JobStatus, states []int8) (*IterationContext, []*JobSpec, []*JobStatus, error) {
	keep := make([]int, 0, len(statuses))
	for i, status := range statuses {
		if states[i] == ISCPJobState_Error || states[i] == ISCPJobState_Canceled || status.LSN > iter.lsn[i] || (status.LSN == iter.lsn[i] && states[i] != ISCPJobState_Running) {
			continue
		}
		if status.LSN != iter.lsn[i]-1 || states[i] != ISCPJobState_Completed {
			return nil, nil, nil, errIterationNeedsRecovery
		}
		keep = append(keep, i)
	}
	if len(keep) == len(statuses) {
		return iter, specs, statuses, nil
	}
	if len(keep) == 0 {
		return nil, nil, nil, nil
	}
	out := NewIterationContext(iter.accountID, iter.tableID, nil, nil, nil, iter.fromTS, iter.toTS)
	keptSpecs := make([]*JobSpec, 0, len(keep))
	keptStatuses := make([]*JobStatus, 0, len(keep))
	for _, i := range keep {
		out.jobNames = append(out.jobNames, iter.jobNames[i])
		out.jobIDs = append(out.jobIDs, iter.jobIDs[i])
		out.lsn = append(out.lsn, iter.lsn[i])
		keptSpecs = append(keptSpecs, specs[i])
		keptStatuses = append(keptStatuses, statuses[i])
	}
	return out, keptSpecs, keptStatuses, nil
}

// readIterationJob reads the exact active generation, never a same-name successor.
func readIterationJob(ctx context.Context, cnUUID string, txn client.TxnOperator, accountID uint32, tableID uint64, name string, id uint64) (status *JobStatus, state int8, watermark types.TS, err error) {
	query := fmt.Sprintf("SELECT job_status, job_state, watermark FROM mo_catalog.mo_iscp_log WHERE account_id = %d AND table_id = %d AND job_name = %s AND job_id = %d AND drop_at IS NULL", accountID, tableID, sqlquote.String(name), id)
	result, err := ExecWithResult(ctx, query, cnUUID, txn)
	if err != nil {
		return
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			if status != nil {
				err = moerr.NewInternalErrorNoCtx("duplicate active ISCP generation")
				return false
			}
			status, err = UnmarshalJobStatus([]byte(cols[0].GetStringAt(i)))
			if err != nil {
				return false
			}
			state = vector.GetFixedAtNoTypeCheck[int8](cols[1], i)
			watermark = types.StringToTS(cols[2].GetStringAt(i))
		}
		return true
	})
	return
}

// A failed acknowledgement is not evidence of rollback. Read in a new
// transaction after releasing the failed writer, and preserve completed or
// superseding durable outcomes. Running generations still require recovery.
func reconcileIterationPublication(ctx context.Context, cnUUID string, eng engine.Engine, tc client.TxnClient, accountID uint32, tableID uint64, names []string, ids, lsns []uint64) (resolved bool, err error) {
	txn, err := getTxn(ctx, eng, tc, "iscp reconcile publication")
	if err != nil {
		return false, err
	}
	defer func() {
		err = finishISCPTransaction(ctx, txn, err)
		if err != nil {
			resolved = false
		}
	}()
	for i, name := range names {
		status, state, _, e := readIterationJob(ctx, cnUUID, txn, accountID, tableID, name, ids[i])
		if e != nil {
			return false, e
		}
		if status != nil && state != ISCPJobState_Error && state != ISCPJobState_Canceled && (status.LSN < lsns[i] || (status.LSN == lsns[i] && state == ISCPJobState_Running)) {
			return false, nil
		}
	}
	return true, nil
}

// failIteration releases only unfinished jobs owned by this admitted iteration.
// Completed siblings and retired/successor generations retain their outcomes.
func (w *worker) failIteration(iter *IterationContext, failure error) (err error) {
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(w.ctx, catalog.System_Account), 5*time.Minute)
	defer cancel()
	txn, err := getTxn(ctx, w.cnEngine, w.cnTxnClient, "iscp recover iteration")
	if err != nil {
		return err
	}
	defer func() { err = finishISCPTransaction(ctx, txn, err) }()
	for i, name := range iter.jobNames {
		status, state, watermark, e := readIterationJob(ctx, w.cnUUID, txn, iter.accountID, iter.tableID, name, iter.jobIDs[i])
		if e != nil {
			return e
		}
		if status == nil || state == ISCPJobState_Error || state == ISCPJobState_Canceled || status.LSN > iter.lsn[i] || (status.LSN == iter.lsn[i] && state != ISCPJobState_Running) {
			continue
		}
		if !((status.LSN == iter.lsn[i] && state == ISCPJobState_Running) || (status.LSN+1 == iter.lsn[i] && state == ISCPJobState_Completed)) {
			return errIterationNeedsRecovery
		}
		priorLSN := status.LSN
		status.LSN = iter.lsn[i]
		status.SetError(failure)
		if err = FlushStatus(ctx, w.cnUUID, txn, iter.accountID, iter.tableID, name, iter.jobIDs[i], status, watermark, ISCPJobState_Completed, priorLSN); err != nil {
			return err
		}
	}
	return nil
}
