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
	Discovery          DiscoveryOptions
}

func DefaultWorkerOptions() WorkerOptions {
	return WorkerOptions{BatchSize: 1024, MaxBatchesPerTable: 8, TableTimeout: 5 * time.Second, RunTimeout: 50 * time.Second, Discovery: DefaultDiscoveryOptions()}
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
	return policyFromConstraint(context.Background(), data)
}

func policyFromConstraint(ctx context.Context, data []byte) (*Policy, error) {
	var result *Policy
	offset := 0
	readLength := func() (uint64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
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
					var prop plan.Property
					if err := prop.Unmarshal(data[offset : offset+int(size)]); err != nil {
						return nil, err
					}
					if prop.Key == PropertyKey {
						if result != nil {
							return nil, moerr.NewInvalidInputNoCtx("TTL specified more than once")
						}
						p, err := Decode(prop.Value)
						if err != nil {
							return nil, err
						}
						result = p
					}
				}
				offset += int(size)
			}
		default:
			return nil, moerr.NewInternalErrorNoCtxf("unsupported TTL constraint kind %d", kind)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
		checkSQL := fmt.Sprintf("SELECT rel_id, rel_version, CASE WHEN LENGTH(`constraint`) = %d THEN `constraint` ELSE NULL END FROM mo_catalog.mo_tables WHERE account_id = %d AND reldatabase = %s AND relname = %s AND relkind = 'r' AND partitioned = 0", len(c.Constraint), c.AccountID, sqlquote.String(c.Database), sqlquote.String(c.Table))
		res, err := tx.Exec(checkSQL, opts)
		if err != nil {
			return err
		}
		matches := 0
		res.ReadRows(func(rows int, cols []*vector.Vector) bool {
			for i := 0; i < rows; i++ {
				if !cols[2].IsNull(uint64(i)) && vector.MustFixedColNoTypeCheck[uint64](cols[0])[i] == c.TableID && vector.MustFixedColNoTypeCheck[uint32](cols[1])[i] == c.Version && bytes.Equal(cols[2].GetBytesAt(i), c.Constraint) {
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
	var cursor discoveryCursor
	return run(ctx, exec, now, options, &cursor)
}

func run(ctx context.Context, exec executor.SQLExecutor, now time.Time, options WorkerOptions, cursor *discoveryCursor) error {
	if options.BatchSize <= 0 || options.MaxBatchesPerTable <= 0 || options.TableTimeout <= 0 || options.RunTimeout <= 0 {
		return moerr.NewInternalErrorNoCtx("invalid TTL worker options")
	}
	limits := options.Discovery
	if limits == (DiscoveryOptions{}) {
		limits = DefaultDiscoveryOptions()
	}
	if err := limits.validate(); err != nil {
		return err
	}
	runCtx, cancel := context.WithTimeout(ctx, options.RunTimeout)
	defer cancel()
	var diagnostics discoveryDiagnostics
	window, err := discoverWindow(runCtx, exec, *cursor, limits, &diagnostics)
	if err != nil {
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
		diagnostics.add(0, 0, err.Error())
		return diagnostics.err()
	}
	for _, c := range window.candidates {
		if err := runCtx.Err(); err != nil {
			return err
		}
		tableCtx, tableCancel := context.WithTimeout(runCtx, options.TableTimeout)
		for batch := 0; batch < options.MaxBatchesPerTable; batch++ {
			affected, err := DeleteBatch(tableCtx, exec, c, now, options.BatchSize)
			if err != nil {
				diagnostics.add(c.AccountID, c.TableID, err.Error())
				break
			}
			if affected < uint64(options.BatchSize) {
				break
			}
		}
		tableCancel()
		if err := runCtx.Err(); err != nil {
			return err
		}
		// Publish only a completed table attempt. A canceled invocation leaves
		// this candidate and all later candidates for the next invocation.
		*cursor = discoveryCursor{after: discoveryKey{c.AccountID, c.TableID}, upper: window.upper, active: true}
	}
	if err := runCtx.Err(); err != nil {
		return err
	}
	*cursor = discoveryCursor{after: window.last, upper: window.upper, active: true}
	if window.exhausted {
		*cursor = discoveryCursor{}
	}
	return diagnostics.err()
}

func TaskExecutor(exec executor.SQLExecutor) taskservice.TaskExecutor {
	var cursor discoveryCursor
	admission := make(chan struct{}, 1)
	return func(ctx context.Context, _ task.Task) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case admission <- struct{}{}:
			defer func() { <-admission }()
		default:
			return moerr.NewInternalError(ctx, "TTL worker invocation already running")
		}
		return run(ctx, exec, time.Now().UTC(), DefaultWorkerOptions(), &cursor)
	}
}
