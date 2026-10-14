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

package ttl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const CronExpr = "0 * * * * *"

func TaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{ID: "table_ttl", Executor: task.TaskCode_TableTTL, Options: task.TaskOptions{Concurrency: 1}}
}

type WorkerOptions struct {
	BatchSize          int
	MaxBatchesPerTable int
	TableTimeout       time.Duration
	RunTimeout         time.Duration
}

func DefaultWorkerOptions() WorkerOptions {
	return WorkerOptions{BatchSize: 1024, MaxBatchesPerTable: 8, TableTimeout: 5 * time.Second, RunTimeout: 50 * time.Second}
}

type Candidate struct {
	AccountID  uint32
	TableID    uint64
	Version    uint32
	Database   string
	Table      string
	Constraint []byte
	Policy     Policy
}

// PolicyFromConstraint reads the existing legacy constraint stream with checked
// lengths, skipping unrelated index/FK protobufs. The generic legacy decoder
// allocates from unchecked counts; maintenance must not trust those counts.
func PolicyFromConstraint(data []byte) (*Policy, error) {
	var props []*plan.Property
	offset := 0
	readLength := func() (uint64, error) {
		if len(data)-offset < 8 {
			return 0, moerr.NewInternalErrorNoCtxf("truncated TTL constraint length")
		}
		n := binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		return n, nil
	}
	for offset < len(data) {
		kind := engine.ConstraintType(data[offset])
		offset++
		n, err := readLength()
		if err != nil {
			return nil, err
		}
		switch kind {
		case engine.PrimaryKey:
			if n > uint64(len(data)-offset) {
				return nil, moerr.NewInternalErrorNoCtxf("truncated primary key constraint")
			}
			offset += int(n)
		case engine.RefChildTable:
			if n > uint64((len(data)-offset)/8) {
				return nil, moerr.NewInternalErrorNoCtxf("truncated child table constraint")
			}
			offset += int(n) * 8
		case engine.Index, engine.ForeignKey, engine.StreamConfig:
			if n > uint64((len(data)-offset)/8) {
				return nil, moerr.NewInternalErrorNoCtxf("invalid constraint item count")
			}
			for i := uint64(0); i < n; i++ {
				size, err := readLength()
				if err != nil {
					return nil, err
				}
				if size > uint64(len(data)-offset) {
					return nil, moerr.NewInternalErrorNoCtxf("truncated constraint item")
				}
				if kind == engine.StreamConfig {
					prop := new(plan.Property)
					if err := prop.Unmarshal(data[offset : offset+int(size)]); err != nil {
						return nil, err
					}
					props = append(props, prop)
				}
				offset += int(size)
			}
		default:
			return nil, moerr.NewInternalErrorNoCtxf("unsupported TTL constraint kind %d", kind)
		}
	}
	return FromProperties(props)
}

// Discover is provisional. DeleteBatch validates the physical table identity and
// the entire policy-bearing constraint again under a table lock in tenant context.
func Discover(ctx context.Context, exec executor.SQLExecutor) ([]Candidate, error) {
	res, err := exec.Exec(ctx, "SELECT account_id, rel_id, rel_version, reldatabase, relname, `constraint` FROM mo_catalog.mo_tables WHERE relkind = 'r' AND reldatabase NOT IN ('mo_catalog','information_schema','mysql','system','system_metrics','mo_task','mo_debug') AND partitioned = 0 ORDER BY account_id, rel_id", executor.Options{}.WithAccountID(catalog.System_Account).WithTimeZone(time.UTC))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var candidates []Candidate
	var decodeErr error
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		accounts := vector.MustFixedColNoTypeCheck[uint32](cols[0])
		ids := vector.MustFixedColNoTypeCheck[uint64](cols[1])
		versions := vector.MustFixedColNoTypeCheck[uint32](cols[2])
		for i := 0; i < rows; i++ {
			data := cols[5].GetBytesAt(i)
			p, e := PolicyFromConstraint(data)
			if e != nil {
				decodeErr = errors.Join(decodeErr, moerr.NewInternalErrorNoCtxf("TTL metadata for account %d table %d: %v", accounts[i], ids[i], e))
				continue
			}
			if p == nil {
				continue
			}
			candidates = append(candidates, Candidate{AccountID: accounts[i], TableID: ids[i], Version: versions[i], Database: cols[3].GetStringAt(i), Table: cols[4].GetStringAt(i), Constraint: bytes.Clone(data), Policy: *p})
		}
		return true
	})
	return candidates, decodeErr
}

// DeleteBatch takes the same exclusive table lock as DDL before revalidation.
// RC lock acquisition refreshes the snapshot; an ABA drop/recreate, ALTER policy,
// rename, or source-column change cannot apply a stale deletion predicate.
func DeleteBatch(ctx context.Context, exec executor.SQLExecutor, c Candidate, now time.Time, batchSize int) (uint64, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		affected, err := deleteBatch(ctx, exec, c, now, batchSize)
		if err == nil || attempt >= 7 || !isRetryable(err) {
			return affected, err
		}
		// Restart the whole transaction, including identity checks. A stale RC
		// lock snapshot must never retry only DELETE under old metadata.
		timer := time.NewTimer(time.Millisecond * time.Duration(1<<attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}
func isRetryable(err error) bool {
	// ExecTxn joins the original failure with rollback's result. Match the
	// underlying MatrixOne error instead of asserting the wrapper's type.
	var underlying *moerr.Error
	if !errors.As(err, &underlying) {
		return false
	}
	err = underlying
	return moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) ||
		moerr.IsMoErrCode(err, moerr.ErrLockConflict) ||
		moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrRemoteLockWaitTimeout)
}
func deleteBatch(ctx context.Context, exec executor.SQLExecutor, c Candidate, now time.Time, batchSize int) (uint64, error) {
	storedPolicy, err := PolicyFromConstraint(c.Constraint)
	if err != nil {
		return 0, err
	}
	if storedPolicy == nil || *storedPolicy != c.Policy {
		return 0, moerr.NewInternalErrorNoCtxf("TTL candidate policy does not match its durable metadata")
	}
	sql, err := DeleteSQL(c.Database, c.Table, &c.Policy, now, batchSize)
	if err != nil {
		return 0, err
	}
	var affected uint64
	err = exec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		if err := tx.LockTable(c.Table); err != nil {
			return err
		}
		opts := executor.StatementOption{}.WithAccountID(c.AccountID)
		checkSQL := fmt.Sprintf("SELECT rel_id, rel_version, `constraint` FROM mo_catalog.mo_tables WHERE account_id = %d AND reldatabase = %s AND relname = %s AND relkind = 'r' AND partitioned = 0", c.AccountID, sqlquote.String(c.Database), sqlquote.String(c.Table))
		res, err := tx.Exec(checkSQL, opts)
		if err != nil {
			return err
		}
		matches := 0
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				if vector.MustFixedColNoTypeCheck[uint64](cols[0])[i] == c.TableID && vector.MustFixedColNoTypeCheck[uint32](cols[1])[i] == c.Version && bytes.Equal(cols[2].GetBytesAt(i), c.Constraint) {
					matches++
				}
			}
			return true
		})
		res.Close()
		if matches != 1 {
			return nil
		}
		// Verify the stable sequence number and type as well, including policies
		// restored or authored through PROPERTIES by an older client.
		columnSQL := fmt.Sprintf("SELECT attr_seqnum, atttyp, att_is_hidden FROM mo_catalog.mo_columns WHERE account_id = %d AND att_relname_id = %d AND lower(attname) = %s", c.AccountID, c.TableID, sqlquote.String(c.Policy.Column))
		res, err = tx.Exec(columnSQL, opts)
		if err != nil {
			return err
		}
		valid := 0
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				var typ types.Type
				e := types.Decode(cols[1].GetBytesAt(i), &typ)
				if e == nil && vector.MustFixedColNoTypeCheck[uint16](cols[0])[i] == uint16(c.Policy.Seqnum) && vector.MustFixedColNoTypeCheck[int8](cols[2])[i] == 0 {
					switch typ.Oid {
					case types.T_date, types.T_datetime, types.T_timestamp:
						valid++
					}
				}
			}
			return true
		})
		res.Close()
		if valid != 1 {
			return moerr.NewInternalErrorNoCtxf("TTL source column identity changed for table %d", c.TableID)
		}
		res, err = tx.Exec(sql, opts)
		if err != nil {
			res.Close()
			return err
		}
		affected = res.AffectedRows
		res.Close()
		return nil
	}, executor.Options{}.WithAccountID(c.AccountID).WithDatabase(c.Database).WithTimeZone(time.UTC).
		WithTxnMode(txnpb.TxnMode_Pessimistic).WithTxnIsolation(txnpb.TxnIsolation_RC).WithLockWaitTimeout(time.Second).WithWaitCommittedLogApplied())
	if err != nil {
		return 0, err
	}
	return affected, nil
}
func Run(ctx context.Context, exec executor.SQLExecutor, now time.Time, options WorkerOptions) error {
	if options.BatchSize <= 0 || options.MaxBatchesPerTable <= 0 || options.TableTimeout <= 0 || options.RunTimeout <= 0 {
		return moerr.NewInternalErrorNoCtxf("invalid TTL worker options")
	}
	runCtx, cancel := context.WithTimeout(ctx, options.RunTimeout)
	defer cancel()
	candidates, result := Discover(runCtx, exec)
	if len(candidates) == 0 {
		return result
	}
	// Rotate the starting point across ticks so a busy first table cannot starve
	// later tables when the run's wall-clock budget is exhausted.
	start := int(uint64(now.Unix()/60) % uint64(len(candidates)))
	for i := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		if runCtx.Err() != nil {
			break
		}
		c := candidates[(start+i)%len(candidates)]
		tableCtx, tableCancel := context.WithTimeout(runCtx, options.TableTimeout)
		for batch := 0; batch < options.MaxBatchesPerTable; batch++ {
			affected, err := DeleteBatch(tableCtx, exec, c, now, options.BatchSize)
			if err != nil {
				result = errors.Join(result, moerr.NewInternalErrorNoCtxf("TTL account %d table %d", c.AccountID, c.TableID), err)
				break
			}
			if affected < uint64(options.BatchSize) {
				break
			}
		}
		tableCancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if runCtx.Err() != nil {
		result = errors.Join(result, runCtx.Err())
	}
	return result
}
func TaskExecutor(exec executor.SQLExecutor) taskservice.TaskExecutor {
	return func(ctx context.Context, _ task.Task) error {
		return Run(ctx, exec, time.Now().UTC(), DefaultWorkerOptions())
	}
}
