// Copyright 2021 Matrix Origin
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

package function

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

// Committed sequence generations allocate through a fresh autonomous
// transaction; nested SQL never retries the caller transaction's statement:
// only the outer compiler owns its workspace rollback and cancellation of
// running SQL. Transaction-local generations keep allocating in the caller
// transaction under the pass-scoped obligation protocol.

// sequenceSQL runs sequence metadata SQL inside the caller's transaction (the
// local route and the transaction-local generation path). The autonomous
// route in seqfunc_autonomous.go uses its own committed transaction instead.
func sequenceSQL(proc *process.Process, db, sql string) ([][]interface{}, error) {
	// The internal executor may finish a lock wait after the client request has
	// been cancelled.  Check the owning statement context at both sides of the
	// SQL call so a sequence update cannot start after cancellation, and an
	// update that raced cancellation is reported to the outer transaction for
	// rollback.  The outer transaction remains the single owner of cleanup.
	if err := sequenceContextErr(proc); err != nil {
		return nil, err
	}
	env, err := seqExecutorEnv(proc.Ctx, proc, db)
	if err != nil {
		return nil, err
	}
	opts := env.opts.WithTxn(proc.GetTxnOperator())
	if proc.GetSessionInfo().LockWaitTimeoutSet {
		opts = opts.WithLockWaitTimeout(time.Duration(proc.GetSessionInfo().LockWaitTimeout) * time.Second)
	}
	res, err := env.exec.Exec(env.ctx, sql, opts)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	if err := sequenceContextErr(proc); err != nil {
		return nil, err
	}
	row, err := readSeqRow(proc.Ctx, res)
	if err != nil || row == nil {
		return nil, err
	}
	return [][]interface{}{row}, nil
}

// readSeqRow parses a Result holding at most one 7-column sequence metadata
// row (last_seq_num, min_value, max_value, start_value, increment_value,
// cycle, is_called) into typed slots, failing closed on any shape, null or
// type deviation.
func readSeqRow(ctx context.Context, res executor.Result) ([]interface{}, error) {
	var row []interface{}
	var err error
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		if rows != 1 || row != nil || len(cols) != 7 {
			err = moerr.NewInternalError(ctx, "invalid sequence metadata shape")
			return false
		}
		row = make([]interface{}, 7)
		for i, col := range cols {
			if col == nil || col.Length() != 1 || col.IsConstNull() || col.GetNulls().Contains(0) {
				err = moerr.NewInternalError(ctx, "null sequence metadata")
				return false
			}
			if (i < 4 && col.GetType().Oid != cols[0].GetType().Oid) ||
				(i == 4 && col.GetType().Oid != types.T_int64) ||
				(i >= 5 && col.GetType().Oid != types.T_bool) ||
				(i < 4 && col.GetType().Oid == types.T_bool) {
				err = moerr.NewInternalError(ctx, "invalid sequence metadata type")
				return false
			}
			switch col.GetType().Oid {
			case types.T_int16:
				row[i] = vector.GetFixedAtNoTypeCheck[int16](col, 0)
			case types.T_int32:
				row[i] = vector.GetFixedAtNoTypeCheck[int32](col, 0)
			case types.T_int64:
				row[i] = vector.GetFixedAtNoTypeCheck[int64](col, 0)
			case types.T_uint16:
				row[i] = vector.GetFixedAtNoTypeCheck[uint16](col, 0)
			case types.T_uint32:
				row[i] = vector.GetFixedAtNoTypeCheck[uint32](col, 0)
			case types.T_uint64:
				row[i] = vector.GetFixedAtNoTypeCheck[uint64](col, 0)
			case types.T_bool:
				row[i] = vector.GetFixedAtNoTypeCheck[bool](col, 0)
			default:
				err = moerr.NewInternalError(ctx, "invalid sequence metadata type")
				return false
			}
		}
		return true
	})
	return row, err
}

func sequenceContextErr(proc *process.Process) error {
	if proc == nil || proc.Ctx == nil {
		return nil
	}
	return proc.Ctx.Err()
}

func sequenceTableName(db, table string) string {
	return "`" + strings.ReplaceAll(db, "`", "``") + "`.`" + strings.ReplaceAll(table, "`", "``") + "`"
}

// requireSequence verifies the catalog-owned relation kind before a sequence
// function reads or writes the relation's sequence-shaped row. The property
// list is metadata, not an API with a fixed order, so inspect the relkind key
// explicitly and fail closed on missing or conflicting markers.
func requireSequence(ctx context.Context, rel engine.Relation) error {
	td, err := rel.TableDefs(ctx)
	if err != nil {
		return err
	}

	var foundSequence, foundOther bool
	for _, def := range td {
		properties, ok := def.(*engine.PropertiesDef)
		if !ok || properties == nil {
			continue
		}
		for _, property := range properties.Properties {
			if property.Key != catalog.SystemRelAttr_Kind {
				continue
			}
			if property.Value == catalog.SystemSequenceRel {
				foundSequence = true
			} else {
				foundOther = true
			}
		}
	}
	if !foundSequence || foundOther {
		return moerr.NewInternalError(ctx, "Table input is not a sequence")
	}
	return nil
}

// Retrieve values of this sequence.
// Set curval,lastval of current session.
// Set is_called to true if it is false, if is_]called is true Advance last_seq_num.
// Return advanced last_seq_num.

func Nextval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if err = rejectSequenceMutationInReadOnly(proc); err != nil {
		return err
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	var databases vector.FunctionParameterWrapper[types.Varlena]
	if len(ivecs) > 1 {
		databases = vector.GenerateFunctionStrParameter(ivecs[1])
	}

	// Here is the transaction
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.GetTxnOperator()
	if txn == nil {
		return moerr.NewInternalError(proc.Ctx, "Nextval: txn operator is nil")
	}

	// Rows resolving to the same identity are allocated in ONE burst (one
	// committed transaction on the committed route): the sequence row lock
	// serializes the burst exactly as serialized transactions would, so the
	// emitted values and final counter are identical while a bulk default
	// insert pays one commit instead of one per row (design §6 bulk gate).
	vals := make([]string, length)
	nulls := make([]bool, length)
	var run *nextvalBatch
	flush := func() error {
		if run == nil {
			return nil
		}
		pending := run
		run = nil
		release, gateErr := proc.AcquireSequence(proc.Ctx)
		if gateErr != nil {
			return gateErr
		}
		defer release()
		// The gate covers the complete read/compute/write interval and
		// publication of LASTVAL. A same-transaction sibling must not
		// observe the row before this operation's UPDATE is complete.
		outs, err := nextvalRun(pending.id, len(pending.rows), proc, e)
		if err != nil {
			return err
		}
		for k, rowIdx := range pending.rows {
			vals[rowIdx] = outs[k]
			if outs[k] != "" {
				proc.GetSessionInfo().SeqLastValue[0] = outs[k]
			}
		}
		return nil
	}

	// nextval is the real implementation of nextval function.
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		db, dbNull := sequenceDatabase(proc.GetSessionInfo().Database, databases, i)
		if null || dbNull {
			nulls[i] = true
			continue
		}
		id, resolveErr := resolveNextval(string(v), db, proc, e, txn)
		if resolveErr != nil {
			// Preserve row order on failure: earlier rows allocate and
			// publish first, then the call reports this row's error.
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			return resolveErr
		}
		if run == nil || run.id != id {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			run = &nextvalBatch{id: id}
		}
		run.rows = append(run.rows, int(i))
	}
	if err := flush(); err != nil {
		return err
	}
	for i := uint64(0); i < uint64(length); i++ {
		if nulls[i] {
			err = rs.AppendBytes(nil, true)
		} else {
			err = rs.AppendBytes(functionUtil.QuickStrToBytes(vals[i]), false)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// rejectSequenceMutationInReadOnly enforces the READ ONLY transaction access
// mode at the sequence mutation boundary. Frontend admission cannot see every
// carrier of a sequence builtin — a temporary-target INSERT keeps its target
// exemption even when NEXTVAL hides in its SELECT arm, and stored views
// expand only at binding — so the frontend gate stamps the effective caller
// access mode (active transaction's declared mode, else the session
// transaction_read_only default) onto the shared process Base for every
// user statement. A stamped read-only mode rejects the mutation with
// ERROR 1792 before any counter write; an unstamped (zero-value) mode —
// internal, background or remote execution — keeps its previous behavior,
// and session-local CURRVAL/LASTVAL do not pass through here.
func rejectSequenceMutationInReadOnly(proc *process.Process) error {
	if proc == nil || proc.Ctx == nil {
		return nil
	}
	if proc.ReadOnlyTxnAccessMode() {
		return moerr.NewCantExecuteInReadOnlyTransaction(proc.Ctx)
	}
	return nil
}

func sequenceDatabase(sessionDatabase string, databases vector.FunctionParameterWrapper[types.Varlena], row uint64) (string, bool) {
	if databases == nil {
		return sessionDatabase, false
	}
	database, null := databases.GetStrValue(row)
	return string(database), null
}

// nextvalIdentity is one row's sequence identity resolved inside the caller
// transaction. Allocation routes and batches by identity, not name: the
// autonomous attempt must prove the caller's (database, relation) generation
// is the committed one before allocating, so an uncommitted shadow
// recreation cannot redirect the call into the stale committed generation.
type nextvalIdentity struct {
	tblname string
	db      string
	wantDB  string
	wantRel uint64
	rel     engine.Relation
}

// nextvalBatch is an open run of result rows sharing one identity; the run
// is flushed as one allocation burst.
type nextvalBatch struct {
	id   nextvalIdentity
	rows []int
}

// resolveNextval resolves and validates the sequence relation inside the
// caller's transaction (preserving caller-visible errors) and captures the
// generation identity the allocation route must match.
func resolveNextval(tblname, db string, proc *process.Process, e engine.Engine, txn client.TxnOperator) (nextvalIdentity, error) {
	dbHandler, err := e.Database(proc.Ctx, db, txn)
	if err != nil {
		return nextvalIdentity{}, err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname, nil)
	if err != nil {
		return nextvalIdentity{}, err
	}

	// Check is sequence table.
	if err := requireSequence(proc.Ctx, rel); err != nil {
		return nextvalIdentity{}, err
	}
	return nextvalIdentity{
		tblname: tblname,
		db:      db,
		wantDB:  dbHandler.GetDatabaseId(proc.Ctx),
		wantRel: rel.GetTableID(proc.Ctx),
		rel:     rel,
	}, nil
}

func makePosIncr[T constraints.Integer](incr int64) T {
	if incr < 0 {
		return T(-incr)
	}
	return T(incr)
}

func Setval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	if err = rejectSequenceMutationInReadOnly(proc); err != nil {
		return err
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	tblnames := vector.GenerateFunctionStrParameter(ivecs[0])
	setnums := vector.GenerateFunctionStrParameter(ivecs[1])
	var iscalled vector.FunctionParameterWrapper[bool]
	if len(ivecs) > 2 {
		iscalled = vector.GenerateFunctionFixedTypeParameter[bool](ivecs[2])
	}
	var databases vector.FunctionParameterWrapper[types.Varlena]
	if len(ivecs) > 3 {
		databases = vector.GenerateFunctionStrParameter(ivecs[3])
	}

	// Txn
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.GetTxnOperator()
	if txn == nil {
		return moerr.NewInternalError(proc.Ctx, "Setval: txn operator is nil")
	}

	for i := uint64(0); i < uint64(length); i++ {
		tn, tnNull := tblnames.GetStrValue(i)
		sn, snNull := setnums.GetStrValue(i)
		isc := true
		iscNull := false
		if iscalled != nil {
			isc, iscNull = iscalled.GetValue(i)
		}
		db, dbNull := sequenceDatabase(proc.GetSessionInfo().Database, databases, i)

		if tnNull || snNull || iscNull || dbNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return
			}
		} else {
			var res string
			release, gateErr := proc.AcquireSequence(proc.Ctx)
			if gateErr != nil {
				return gateErr
			}
			func() {
				defer release()
				// SETVAL has the same caller-transaction read/compute/write
				// interval as NEXTVAL and updates session sequence state inside
				// that interval.
				res, err = setval(string(tn), string(sn), isc, db, proc, txn, e)
			}()
			if err == nil {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), false)
			}
			if err != nil {
				return
			}
		}
	}
	return
}

// setval performs one complete sequence assignment. Callers must hold the
// process sequence gate for the whole call, just like nextval. Routing
// follows the same committed-visibility rule as nextval: committed
// generations are assigned in a fresh autonomous transaction, while
// transaction-local generations stay in the caller transaction and a
// successful assignment clears the pass-scoped obligation (a deliberate
// SETVAL boundary).
func setval(tblname, setnum string, iscalled bool, db string, proc *process.Process, txn client.TxnOperator, e engine.Engine) (string, error) {
	dbHandler, err := e.Database(proc.Ctx, db, txn)
	if err != nil {
		return "", err
	}
	rel, err := dbHandler.Relation(proc.Ctx, tblname, nil)
	if err != nil {
		return "", err
	}
	if err := requireSequence(proc.Ctx, rel); err != nil {
		return "", err
	}

	res, err := setvalAutonomous(tblname, setnum, iscalled, db, proc, e,
		dbHandler.GetDatabaseId(proc.Ctx), rel.GetTableID(proc.Ctx))
	if err == nil {
		return res, nil
	}
	if !errors.Is(err, errSeqLocalGeneration) {
		return "", err
	}
	return setvalLocal(tblname, setnum, iscalled, db, proc, rel)
}

func Currval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ivec := vector.GenerateFunctionStrParameter(ivecs[0])
	var databases vector.FunctionParameterWrapper[types.Varlena]
	if len(ivecs) > 1 {
		databases = vector.GenerateFunctionStrParameter(ivecs[1])
	}

	// Here is the transaction
	e := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	txn := proc.GetTxnOperator()
	if txn == nil {
		return moerr.NewInternalError(proc.Ctx, "Currval: txn operator is nil")
	}
	release, err := proc.AcquireSequence(proc.Ctx)
	if err != nil {
		return err
	}
	defer release()

	// A vector may contain repeated sequence names. Resolve each effective
	// database once per batch; hidden view arguments can vary by row, so cache
	// by database name rather than assuming one database for the whole vector.
	databasesByName := make(map[string]engine.Database, 1)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := ivec.GetStrValue(i)
		db, dbNull := sequenceDatabase(proc.GetSessionInfo().Database, databases, i)
		if null || dbNull {
			if err = rs.AppendBytes(nil, true); err != nil {
				return
			}
		} else {
			dbHandler, ok := databasesByName[db]
			if !ok {
				var dbErr error
				dbHandler, dbErr = e.Database(proc.Ctx, db, txn)
				if dbErr != nil {
					return dbErr
				}
				databasesByName[db] = dbHandler
			}
			var rel engine.Relation
			rel, err = dbHandler.Relation(proc.Ctx, string(v), nil)
			if err != nil {
				return
			}
			tblId := rel.GetTableID(proc.Ctx)
			// Get cur values here.
			ss, exists := proc.GetSessionInfo().SeqCurValues[tblId]
			// If nextval called before this currval.Check add values
			ss1, existsAdd := proc.GetSessionInfo().SeqAddValues[tblId]
			if !exists && !existsAdd {
				err = moerr.NewInternalErrorf(proc.Ctx, "Currvalue of %s in current session is not initialized", v)
				return
			}
			// Assign the values of SeqAddValues first, cause this values is the updated curvals.
			if existsAdd {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(ss1), false)
			} else {
				err = rs.AppendBytes(functionUtil.QuickStrToBytes(ss), false)
			}
			if err != nil {
				return
			}
		}
	}
	return
}

func Lastval(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) (err error) {
	release, err := proc.AcquireSequence(proc.Ctx)
	if err != nil {
		return err
	}
	defer release()

	// Get last value
	lastv := proc.GetSessionInfo().SeqLastValue[0]
	if lastv == "" {
		return moerr.NewInternalError(proc.Ctx, "Last value of current session is not initialized.")
	}

	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if err = rs.AppendBytes(functionUtil.QuickStrToBytes(lastv), false); err != nil {
			return
		}
	}
	return
}
