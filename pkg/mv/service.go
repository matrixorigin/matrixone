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

package mv

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const RuntimeKey = "materialized-view-service"
const Timeout = 300 * time.Second

type Key struct {
	Account uint32
	ID      uint64
}
type State struct {
	Key
	Database, Name               string
	Definition                   catalog.MVDefinition
	Epoch, Requested, Successful uint64
	Populated                    bool
	LastRun                      string
	SourceTS                     timestamp.Timestamp
	Interval                     int64
}

// Service owns bounded per-CN admission, not cluster-wide publication locks.
// Fixed account buckets conservatively serialize collisions without a growing
// tenant-to-semaphore map. Transaction conflict detection fences publishers.
type Service struct {
	SQL      executor.SQLExecutor
	slots    chan struct{}
	accounts [256]chan struct{}
	pending  [256]chan struct{}
}

func NewService(sql executor.SQLExecutor) *Service {
	s := &Service{SQL: sql, slots: make(chan struct{}, 2)}
	for i := range s.accounts {
		s.accounts[i] = make(chan struct{}, 1)
		s.pending[i] = make(chan struct{}, 2)
	}
	return s
}
func Quote(s string) string   { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func literal(s string) string { return "'" + sqlquote.EscapeString(s) + "'" }
func system() executor.StatementOption {
	return executor.StatementOption{}.WithAccountID(catalog.System_Account).WithDisableLog()
}
func consume(tx executor.TxnExecutor, sql string) error {
	r, e := tx.Exec(sql, system())
	if e == nil {
		r.Close()
	}
	return e
}

func Load(tx executor.TxnExecutor, key Key) (*State, error) {
	r, err := tx.Exec(fmt.Sprintf("select database_name,name,definition,epoch,requested_sequence,successful_sequence,populated,last_run,source_physical,source_logical,interval_seconds from mo_catalog.mo_materialized_views where account_id=%d and mv_id=%d", key.Account, key.ID), system())
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var st *State
	var decodeErr error
	r.ReadRows(func(rows int, c []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			if st != nil {
				decodeErr = moerr.NewInternalErrorNoCtx("duplicate materialized view metadata")
				return false
			}
			st = &State{Key: key, Database: c[0].GetStringAt(i), Name: c[1].GetStringAt(i), Epoch: vector.GetFixedAtWithTypeCheck[uint64](c[3], i), Requested: vector.GetFixedAtWithTypeCheck[uint64](c[4], i), Successful: vector.GetFixedAtWithTypeCheck[uint64](c[5], i), Populated: vector.GetFixedAtWithTypeCheck[bool](c[6], i), LastRun: c[7].GetStringAt(i), SourceTS: timestamp.Timestamp{PhysicalTime: vector.GetFixedAtWithTypeCheck[int64](c[8], i), LogicalTime: vector.GetFixedAtWithTypeCheck[uint32](c[9], i)}, Interval: vector.GetFixedAtWithTypeCheck[int64](c[10], i)}
			definition, err := catalog.DecodeMaterializedViewDefinition(c[2].GetStringAt(i))
			decodeErr = err
			if err == nil {
				st.Definition = *definition
			}
		}
		return decodeErr == nil
	})
	if decodeErr != nil {
		return nil, decodeErr
	}
	if st == nil {
		return nil, moerr.NewInvalidInputNoCtx("materialized view metadata does not exist")
	}
	return st, nil
}

func ValidateSources(tx executor.TxnExecutor, st *State) error {
	for _, d := range st.Definition.Sources {
		if d.AccountID != st.Account || d.Database != st.Database {
			return moerr.NewInvalidInputNoCtx("invalid materialized view source account/database")
		}
		r, err := tx.Exec(fmt.Sprintf("select rel_id,rel_logical_id,rel_version from mo_catalog.mo_tables where account_id=%d and reldatabase_id=%d and relname=%s", d.AccountID, d.DatabaseID, literal(d.Table)), system())
		if err != nil {
			return err
		}
		valid, rows := false, 0
		r.ReadRows(func(n int, c []*vector.Vector) bool {
			rows += n
			for i := 0; i < n; i++ {
				valid = vector.GetFixedAtWithTypeCheck[uint64](c[0], i) == d.TableID && vector.GetFixedAtWithTypeCheck[uint64](c[1], i) == d.LogicalID && vector.GetFixedAtWithTypeCheck[uint32](c[2], i) == d.Version
			}
			return true
		})
		r.Close()
		if rows != 1 || !valid {
			return moerr.NewInvalidInputNoCtx("MV_INVALID_DEFINITION: materialized view source identity/schema changed")
		}
	}
	return nil
}

// Register is called by CREATE in the same transaction as native relation creation.
func Register(tx executor.TxnExecutor, key Key, databaseID uint64, name string, definition *catalog.MVDefinition) error {
	if err := DependencyGuard(tx, key.Account); err != nil {
		return err
	}
	r, err := tx.Exec(fmt.Sprintf("select count(*) from mo_catalog.mo_materialized_views where account_id=%d", key.Account), system())
	if err != nil {
		return err
	}
	var count int64
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		if n > 0 {
			count = vector.GetFixedAtWithTypeCheck[int64](c[0], 0)
		}
		return true
	})
	r.Close()
	if count >= 128 {
		return moerr.NewInvalidInputNoCtx("materialized view account limit (128) exceeded")
	}
	st := &State{Key: key, Database: definition.Database, Name: name, Definition: *definition}
	if err := ValidateSources(tx, st); err != nil {
		return err
	}
	data, err := json.Marshal(definition)
	if err != nil {
		return err
	}
	err = consume(tx, fmt.Sprintf("insert into mo_catalog.mo_materialized_views (account_id,mv_id,database_id,database_name,name,definition,interval_seconds,next_due) values (%d,%d,%d,%s,%s,%s,%d,%s)", key.Account, key.ID, databaseID, literal(definition.Database), literal(name), literal(string(data)), definition.IntervalSeconds, nextDueSQL(definition.IntervalSeconds)))
	if err != nil {
		return err
	}
	for _, d := range definition.Sources {
		if err := consume(tx, fmt.Sprintf("insert into mo_catalog.mo_mv_dependencies (account_id,mv_id,source_database_id,source_table_id,source_logical_id,source_version) values (%d,%d,%d,%d,%d,%d)", key.Account, key.ID, d.DatabaseID, d.TableID, d.LogicalID, d.Version)); err != nil {
			return err
		}
	}
	return nil
}
func nextDueSQL(seconds int64) string {
	if seconds == 0 {
		return "null"
	}
	return fmt.Sprintf("date_add(current_timestamp(),interval %d second)", seconds)
}

// CatalogReadinessSQL uses only the pre-existing system metadata catalog. All
// four global MV tables must be installed in the caller's transaction snapshot;
// a registered CN service or a session feature flag is not migration readiness.
const CatalogReadinessSQL = "select count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname in ('mo_materialized_views','mo_mv_dependencies','mo_mv_refresh_runs','mo_mv_account_state')"

func CatalogReady(tx executor.TxnExecutor) (bool, error) {
	r, err := tx.Exec(CatalogReadinessSQL, system())
	if err != nil {
		return false, err
	}
	defer r.Close()
	var count int64
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		if n > 0 {
			count = vector.GetFixedAtWithTypeCheck[int64](c[0], 0)
		}
		return false
	})
	if count != 0 && count != 4 {
		return false, moerr.NewInternalErrorNoCtx("incomplete materialized view catalog migration")
	}
	return count == 4, nil
}

func DependencyGuard(tx executor.TxnExecutor, account uint32) error {
	if err := consume(tx, fmt.Sprintf("insert ignore into mo_catalog.mo_mv_account_state (account_id,revision) values (%d,0)", account)); err != nil {
		return err
	}
	return consume(tx, fmt.Sprintf("update mo_catalog.mo_mv_account_state set revision=revision+1 where account_id=%d", account))
}

// Refresh publishes stored rows and their exact source snapshot together.
// Callers authorize the current user/owner and require protocol admission first.
func (s *Service) Refresh(ctx context.Context, key Key, expectedEpoch uint64) error {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Bound both active and waiting requests without a growing per-tenant map.
	// One active request and one waiter may occupy an account hash bucket.
	pending := s.pending[key.Account%uint32(len(s.pending))]
	select {
	case pending <- struct{}{}:
		defer func() { <-pending }()
	default:
		return moerr.NewInvalidInput(ctx, "MV_REFRESH_BUSY: refresh admission is full")
	}
	bucket := s.accounts[key.Account%uint32(len(s.accounts))]
	select {
	case bucket <- struct{}{}:
		defer func() { <-bucket }()
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	runID := uuid.NewString()
	if background, ok := defines.GetMaterializedViewTask(ctx); ok {
		if background.Account != key.Account || background.Table != key.ID || background.Epoch != expectedEpoch || background.RunID == "" {
			return moerr.NewInvalidInputNoCtx("invalid materialized view task identity")
		}
		runID = background.RunID
	}
	var sequence, epoch uint64
	var definition catalog.MVDefinition
	opts := executor.Options{}.WithAccountID(key.Account).WithTxnIsolation(txn.TxnIsolation_SI).WithTxnMode(txn.TxnMode_Optimistic).WithWaitCommittedLogApplied()
	err := s.retryTxn(ctx, func(tx executor.TxnExecutor) error {
		st, err := Load(tx, key)
		if err != nil {
			return err
		}
		if expectedEpoch != 0 && expectedEpoch != st.Epoch {
			return moerr.NewInvalidInputNoCtx("obsolete materialized view refresh task")
		}
		if prior, err := loadRun(tx, key, runID); err != nil {
			return err
		} else if prior != nil {
			sequence = prior.sequence
			epoch = prior.epoch
			definition = st.Definition
			if epoch != st.Epoch {
				return moerr.NewInvalidInputNoCtx("obsolete materialized view refresh attempt")
			}
			if st.Successful < sequence {
				return consume(tx, fmt.Sprintf("update mo_catalog.mo_mv_refresh_runs set status='RUNNING',started_at=current_timestamp(),finished_at=null,error_message='' where account_id=%d and mv_id=%d and run_id=%s", key.Account, key.ID, literal(runID)))
			}
			return nil
		}
		if st.Requested == ^uint64(0) {
			return moerr.NewInvalidInputNoCtx("materialized view refresh sequence exhausted")
		}
		sequence = st.Requested + 1
		epoch = st.Epoch
		definition = st.Definition
		if err := consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set requested_sequence=%d where account_id=%d and mv_id=%d and epoch=%d", sequence, key.Account, key.ID, epoch)); err != nil {
			return err
		}
		return consume(tx, fmt.Sprintf("insert into mo_catalog.mo_mv_refresh_runs (account_id,mv_id,run_id,sequence,epoch,status,started_at) values (%d,%d,%s,%d,%d,'RUNNING',current_timestamp())", key.Account, key.ID, literal(runID), sequence, epoch))
	}, opts)
	if err != nil {
		return err
	}
	zone, err := time.LoadLocation(definition.TimeZone)
	if err != nil || definition.TimeZone == "Local" {
		zone = time.FixedZone(definition.TimeZone, int(definition.TimeZoneOffset))
	}
	// Only the persisted source SQL inherits authoring context. Control-plane SQL
	// uses the executor's default literal escaping, including source names with
	// backslashes, and must keep its own parser mode.
	sourceOpts := opts.WithDatabase(definition.Database).WithTimeZone(zone).WithLowerCaseTableNames(&definition.LowerCaseTableNames).WithParserSQLMode(definition.SQLMode).WithResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return definition.SQLMode, nil
		}
		if executor.DefaultResolveVariable != nil {
			return executor.DefaultResolveVariable(name, system, global)
		}
		return nil, moerr.NewInvalidInputNoCtx("unsupported refresh session variable")
	})
	err = s.retryTxn(ctx, func(tx executor.TxnExecutor) error {
		st, err := Load(tx, key)
		if err != nil {
			return err
		}
		if st.LastRun == runID {
			return nil
		}
		if st.Successful >= sequence {
			return consume(tx, fmt.Sprintf("update mo_catalog.mo_mv_refresh_runs set status='SUPERSEDED',finished_at=current_timestamp() where account_id=%d and mv_id=%d and run_id=%s and status='RUNNING'", key.Account, key.ID, literal(runID)))
		}
		if st.Epoch != epoch {
			return moerr.NewInvalidInputNoCtx("materialized view definition changed during refresh")
		}
		if err := ValidateSources(tx, st); err != nil {
			return err
		}
		if err := consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set successful_sequence=%d where account_id=%d and mv_id=%d and epoch=%d", sequence, key.Account, key.ID, epoch)); err != nil {
			return err
		}
		ts := tx.Txn().SnapshotTS()
		writeCtx := defines.WithMaterializedViewBudget(defines.WithMaterializedViewWrite(ctx, key.Account, key.ID, tx.Txn().Txn().ID), key.ID, 512<<20)
		writeOpts := sourceOpts.WithTxn(tx.Txn())
		target := Quote(st.Database) + "." + Quote(st.Name)
		for _, sql := range []string{"delete from " + target, "insert into " + target + " " + definition.Query} {
			r, err := s.SQL.Exec(writeCtx, sql, writeOpts)
			if err != nil {
				return err
			}
			r.Close()
		}
		if !tx.Txn().SnapshotTS().Equal(ts) {
			return moerr.NewInternalErrorNoCtx("materialized view refresh source snapshot changed")
		}
		if err := consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set populated=true,last_run=%s,source_physical=%d,source_logical=%d,last_success=current_timestamp(),next_due=%s where account_id=%d and mv_id=%d and epoch=%d", literal(runID), ts.PhysicalTime, ts.LogicalTime, nextDueSQL(st.Interval), key.Account, key.ID, epoch)); err != nil {
			return err
		}
		return consume(tx, fmt.Sprintf("update mo_catalog.mo_mv_refresh_runs set status='SUCCEEDED',finished_at=current_timestamp(),source_physical=%d,source_logical=%d where account_id=%d and mv_id=%d and run_id=%s", ts.PhysicalTime, ts.LogicalTime, key.Account, key.ID, literal(runID)))
	}, opts)
	if err != nil {
		// Diagnostic failure recording never changes the last-good result. A
		// conditional RUNNING update cannot overwrite a committed success.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cleanupCancel()
		// Resolve a lost commit response before reporting failure.
		if committed, checkErr := s.State(cleanupCtx, key); checkErr == nil && committed.LastRun == runID {
			return nil
		}
		message := "refresh failed" // Do not persist raw SQL/data/connection error strings.
		r, recordErr := s.SQL.Exec(cleanupCtx, fmt.Sprintf("update mo_catalog.mo_mv_refresh_runs set status='FAILED',finished_at=current_timestamp(),error_message=%s where account_id=%d and mv_id=%d and run_id=%s and status='RUNNING'", literal(message), key.Account, key.ID, literal(runID)), executor.Options{}.WithAccountID(catalog.System_Account))
		if recordErr == nil {
			r.Close()
		}
		r, scheduleErr := s.SQL.Exec(cleanupCtx, fmt.Sprintf("update mo_catalog.mo_materialized_views set next_due=date_add(current_timestamp(),interval 60 second) where account_id=%d and mv_id=%d and epoch=%d and interval_seconds>0 and last_run<>%s", key.Account, key.ID, epoch, literal(runID)), executor.Options{}.WithAccountID(catalog.System_Account))
		if scheduleErr == nil {
			r.Close()
		}
	}
	return err
}

func (s *Service) State(ctx context.Context, key Key) (*State, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var st *State
	err := s.SQL.ExecTxn(ctx, func(tx executor.TxnExecutor) error { var err error; st, err = Load(tx, key); return err }, executor.Options{}.WithAccountID(key.Account))
	return st, err
}

type refreshRun struct {
	sequence, epoch uint64
	status          string
}

func loadRun(tx executor.TxnExecutor, key Key, id string) (*refreshRun, error) {
	r, err := tx.Exec(fmt.Sprintf("select sequence,epoch,status from mo_catalog.mo_mv_refresh_runs where account_id=%d and mv_id=%d and run_id=%s", key.Account, key.ID, literal(id)), system())
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var run *refreshRun
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		if n > 0 {
			run = &refreshRun{vector.GetFixedAtWithTypeCheck[uint64](c[0], 0), vector.GetFixedAtWithTypeCheck[uint64](c[1], 0), c[2].GetStringAt(0)}
		}
		return false
	})
	return run, nil
}
func (s *Service) retryTxn(ctx context.Context, fn func(executor.TxnExecutor) error, opts executor.Options) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = s.SQL.ExecTxn(ctx, fn, opts)
		if !moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) && !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) && !moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged) {
			return err
		}
	}
	return err
}
