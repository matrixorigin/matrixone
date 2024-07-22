// Copyright 2024 Matrix Origin
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

package trace

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func (s *service) TxnCreated(op client.TxnOperator) {
	if s.atomic.closed.Load() {
		return
	}

	if !s.atomic.txnEventEnabled.Load() &&
		!s.atomic.txnActionEventEnabled.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	register := false
	if s.atomic.txnEventEnabled.Load() {
		register = true
		s.txnC <- event{
			csv: newTxnCreated(op.Txn()),
		}
	}

	if s.atomic.txnActionEventEnabled.Load() {
		register = true
		s.doAddTxnAction(
			op.Txn().ID,
			client.OpenEvent.Name,
			0,
			0,
			0,
			"",
			nil)
	}

	if register {
		op.AppendEventCallback(client.WaitActiveEvent, s.handleTxnActive)
		op.AppendEventCallback(client.UpdateSnapshotEvent, s.handleTxnUpdateSnapshot)
		op.AppendEventCallback(client.CommitEvent, s.handleTxnCommit)
		op.AppendEventCallback(client.RollbackEvent, s.handleTxnRollback)

		op.AppendEventCallback(client.CommitResponseEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.CommitWaitApplyEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.UnlockEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.RangesEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.BuildPlanEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.ExecuteSQLEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.CompileEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.TableScanEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.WorkspaceWriteEvent, s.handleTxnActionEvent)
		op.AppendEventCallback(client.WorkspaceAdjustEvent, s.handleTxnActionEvent)
	}
}

func (s *service) TxnExecSQL(
	op client.TxnOperator,
	sql string) {
	if !s.Enabled(FeatureTraceTxn) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	sql = truncateSQL(sql)
	s.txnC <- event{
		csv: newTxnInfoEvent(op.Txn(), txnExecuteEvent, sql),
	}
}

func (s *service) TxnConflictChanged(
	op client.TxnOperator,
	tableID uint64,
	lastCommitAt timestamp.Timestamp,
) {
	if !s.Enabled(FeatureTraceTxn) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	table := buf.writeUint(tableID)
	ts := buf.writeTimestamp(lastCommitAt)

	idx := buf.buf.GetWriteIndex()
	buf.buf.WriteString("table:")
	buf.buf.WriteString(table)
	buf.buf.WriteString(", new-min-snapshot-ts: ")
	buf.buf.WriteString(ts)
	info := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
	s.txnC <- event{
		csv: newTxnInfoEvent(
			op.Txn(),
			txnConflictChanged,
			util.UnsafeBytesToString(info),
		),
	}
	s.txnC <- event{
		buffer: buf,
	}
}

func (s *service) TxnNoConflictChanged(
	op client.TxnOperator,
	tableID uint64,
	lockedAt, newSnapshotTS timestamp.Timestamp,
) {
	if !s.Enabled(FeatureTraceTxn) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	table := buf.writeUint(tableID)
	locked := buf.writeTimestamp(lockedAt)
	newSnapshot := buf.writeTimestamp(newSnapshotTS)

	idx := buf.buf.GetWriteIndex()
	buf.buf.WriteString("table:")
	buf.buf.WriteString(table)
	buf.buf.WriteString(", locked-ts: ")
	buf.buf.WriteString(locked)
	buf.buf.WriteString(", new-min-snapshot-ts: ")
	buf.buf.WriteString(newSnapshot)
	info := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
	s.txnC <- event{
		csv: newTxnInfoEvent(
			op.Txn(),
			txnNoConflictChanged,
			util.UnsafeBytesToString(info),
		),
	}
	s.txnC <- event{
		buffer: buf,
	}
}

func (s *service) TxnUpdateSnapshot(
	op client.TxnOperator,
	tableID uint64,
	why string) {
	if !s.Enabled(FeatureTraceTxn) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	table := buf.writeUint(tableID)

	idx := buf.buf.GetWriteIndex()
	buf.buf.WriteString(why)
	buf.buf.WriteString(" table:")
	buf.buf.WriteString(table)
	info := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
	s.txnC <- event{
		csv: newTxnInfoEvent(
			op.Txn(),
			txnUpdateSnapshotReasonEvent,
			util.UnsafeBytesToString(info),
		),
	}
	s.txnC <- event{
		buffer: buf,
	}
}

func (s *service) TxnCommit(
	op client.TxnOperator,
	entries []*api.Entry) {
	if !s.Enabled(FeatureTraceTxn) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	txnFilters := s.atomic.txnFilters.Load()
	if skipped := txnFilters.filter(op); skipped {
		return
	}

	ts := time.Now().UnixNano()
	buf := reuse.Alloc[buffer](nil)
	var entryData *EntryData
	defer func() {
		entryData.close()
	}()

	tableFilters := s.atomic.tableFilters.Load()

	n := 0
	for _, entry := range entries {
		if entryData == nil {
			entryData = newEntryData(entry, -1, ts)
		} else {
			entryData.reset()
			entryData.init(entry, -1, ts)
		}

		if skipped := tableFilters.filter(entryData); skipped {
			continue
		}

		entryData.createCommit(
			op.Txn().ID,
			buf,
			func(e dataEvent) {
				s.entryC <- event{
					csv: e,
				}
			},
			&s.atomic.complexPKTables)
		n++
	}

	if n == 0 {
		buf.close()
		return
	}
	s.entryC <- event{
		buffer: buf,
	}
}

func (s *service) TxnRead(
	op client.TxnOperator,
	snapshotTS timestamp.Timestamp,
	tableID uint64,
	columns []string,
	bat *batch.Batch) {
	if !s.Enabled(FeatureTraceData) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	entryData := newReadEntryData(tableID, snapshotTS, bat, columns, time.Now().UnixNano())
	defer func() {
		entryData.close()
	}()

	tableFilters := s.atomic.tableFilters.Load()
	if skipped := tableFilters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	entryData.createRead(
		op.Txn().ID,
		buf,
		func(e dataEvent) {
			s.entryC <- event{
				csv: e,
			}
		},
		&s.atomic.complexPKTables)
	s.entryC <- event{
		buffer: buf,
	}
}

func (s *service) TxnReadBlock(
	op client.TxnOperator,
	tableID uint64,
	block []byte) {
	if !s.Enabled(FeatureTraceTxn) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	entryData := newTableOnlyEntryData(tableID)
	defer func() {
		entryData.close()
	}()

	tableFilters := s.atomic.tableFilters.Load()
	if skipped := tableFilters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	s.entryC <- event{
		csv: newReadBlockEvent(
			time.Now().UnixNano(),
			op.Txn().ID,
			tableID,
			buf.writeHexWithBytes(block),
		),
	}
	s.entryC <- event{
		buffer: buf,
	}
}

func (s *service) TxnWrite(
	op client.TxnOperator,
	tableID uint64,
	typ string,
	bat *batch.Batch,
) {
	if !s.Enabled(FeatureTraceTxnWorkspace) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	entryData := newWriteEntryData(tableID, bat, time.Now().UnixNano())
	defer func() {
		entryData.close()
	}()

	tableFilters := s.atomic.tableFilters.Load()
	if skipped := tableFilters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	entryData.createWrite(
		op.Txn().ID,
		buf,
		typ,
		func(e dataEvent) {
			s.entryC <- event{
				csv: e,
			}
		},
		&s.atomic.complexPKTables)
	s.entryC <- event{
		buffer: buf,
	}
}

func (s *service) TxnAdjustWorkspace(
	op client.TxnOperator,
	adjustCount int,
	writes func() (tableID uint64, typ string, bat *batch.Batch, more bool),
) {
	if !s.Enabled(FeatureTraceTxnWorkspace) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	at := time.Now().UnixNano()

	offsetCount := 0
	for {
		tableID, typ, bat, more := writes()
		if !more {
			return
		}

		func() {
			entryData := newWorkspaceEntryData(tableID, bat, at)
			defer func() {
				entryData.close()
			}()

			tableFilters := s.atomic.tableFilters.Load()
			if skipped := tableFilters.filter(entryData); skipped {
				return
			}

			buf := reuse.Alloc[buffer](nil)
			entryData.createWorkspace(
				op.Txn().ID,
				buf,
				typ,
				adjustCount,
				offsetCount,
				func(e dataEvent) {
					s.entryC <- event{
						csv: e,
					}
				},
				&s.atomic.complexPKTables)
			s.entryC <- event{
				buffer: buf,
			}
		}()
		offsetCount++
	}
}

func (s *service) TxnEventEnabled() bool {
	return s.atomic.txnEventEnabled.Load()
}

func (s *service) handleTxnActive(e client.TxnEvent) {
	if s.atomic.closed.Load() {
		return
	}

	if !e.CostEvent && s.atomic.txnEventEnabled.Load() {
		s.txnC <- event{
			csv: newTxnActive(e.Txn),
		}
	}

	if s.atomic.txnActionEventEnabled.Load() {
		s.doTxnEventAction(e)
	}
}

func (s *service) handleTxnUpdateSnapshot(e client.TxnEvent) {
	if s.atomic.closed.Load() {
		return
	}

	if e.CostEvent && s.atomic.txnEventEnabled.Load() {
		s.txnC <- event{
			csv: newTxnSnapshotUpdated(e.Txn),
		}
	}

	if s.atomic.txnActionEventEnabled.Load() {
		s.doTxnEventAction(e)
	}
}

func (s *service) handleTxnCommit(e client.TxnEvent) {
	if s.atomic.closed.Load() {
		return
	}

	if e.CostEvent && s.atomic.txnEventEnabled.Load() {
		s.txnC <- event{
			csv: newTxnClosed(e.Txn),
		}
		if e.Err != nil {
			s.addTxnError(
				e.Txn.ID,
				e.Err,
			)
		}
	}

	if s.atomic.txnActionEventEnabled.Load() {
		s.doTxnEventAction(e)
	}
}

func (s *service) handleTxnRollback(e client.TxnEvent) {
	if s.atomic.closed.Load() {
		return
	}

	if e.CostEvent && s.atomic.txnEventEnabled.Load() {
		s.txnC <- event{
			csv: newTxnClosed(e.Txn),
		}
		if e.Err != nil {
			s.addTxnError(
				e.Txn.ID,
				e.Err,
			)
		}
	}

	if s.atomic.txnActionEventEnabled.Load() {
		s.doTxnEventAction(e)
	}
}

func (s *service) handleTxnActionEvent(event client.TxnEvent) {
	if s.atomic.closed.Load() {
		return
	}

	if s.atomic.txnActionEventEnabled.Load() {
		s.doTxnEventAction(event)
	}
}

func (s *service) TxnError(
	op client.TxnOperator,
	value error,
) {
	if s.atomic.closed.Load() {
		return
	}

	if op.TxnOptions().TraceDisabled() {
		return
	}

	if !s.atomic.txnEventEnabled.Load() {
		return
	}

	s.addTxnError(
		op.Txn().ID,
		value,
	)
}

func (s *service) addTxnError(
	txnID []byte,
	value error,
) {
	ts := time.Now().UnixNano()
	msg := value.Error()
	if me, ok := value.(*moerr.Error); ok {
		msg += ": " + me.Detail()
	}

	sql := fmt.Sprintf("insert into %s (ts, txn_id, error_info) values (%d, '%x', '%s')",
		EventErrorTable,
		ts,
		txnID,
		escape(msg))

	select {
	case s.txnErrorC <- sql:
	default:
		s.logger.Info("failed to added txn error trace",
			zap.String("sql", sql))
	}
}

func (s *service) TxnStatementStart(
	op client.TxnOperator,
	sql string,
	seq uint64,
) {
	if !s.Enabled(FeatureTraceTxnAction) &&
		!s.Enabled(FeatureTraceTxn) {
		return
	}

	s.TxnExecSQL(op, sql)
	s.AddTxnDurationAction(
		op,
		client.ExecuteSQLEvent,
		seq,
		0,
		0,
		nil)
}

func (s *service) TxnStatementCompleted(
	op client.TxnOperator,
	sql string,
	cost time.Duration,
	seq uint64,
	affectRows int,
	err error,
) {
	if !s.Enabled(FeatureTraceTxnAction) &&
		!s.Enabled(FeatureTraceTxn) &&
		!s.Enabled(FeatureTraceStatement) {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	if err == nil {
		err = infoErr{
			info: buf.writeInt(int64(affectRows)),
		}
	}

	s.AddTxnDurationAction(
		op,
		client.ExecuteSQLEvent,
		seq,
		0,
		cost,
		err)

	s.AddStatement(
		op,
		sql,
		cost)

	s.txnActionC <- event{
		buffer: buf,
	}
}

func (s *service) AddTxnDurationAction(
	op client.TxnOperator,
	eventType client.EventType,
	seq uint64,
	tableID uint64,
	value time.Duration,
	err error,
) {
	s.AddTxnAction(
		op,
		eventType,
		seq,
		tableID,
		value.Microseconds(),
		"us",
		err)
}

func (s *service) AddTxnAction(
	op client.TxnOperator,
	eventType client.EventType,
	seq uint64,
	tableID uint64,
	value int64,
	unit string,
	err error,
) {
	if !s.Enabled(FeatureTraceTxnAction) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	s.doAddTxnAction(
		op.Txn().ID,
		eventType.Name,
		seq,
		tableID,
		value,
		unit,
		err)
}

func (s *service) AddTxnActionInfo(
	op client.TxnOperator,
	eventType client.EventType,
	seq uint64,
	tableID uint64,
	value func(Writer),
) {
	if !s.Enabled(FeatureTraceTxnAction) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	filters := s.atomic.txnFilters.Load()
	if skipped := filters.filter(op); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	w := writer{
		buf: buf.buf,
		dst: buf.alloc(1024),
		idx: buf.buf.GetWriteIndex(),
	}
	value(w)

	s.txnActionC <- event{
		csv: actionEvent{
			ts:        time.Now().UnixNano(),
			action:    eventType.Name,
			actionSeq: seq,
			tableID:   tableID,
			value:     0,
			unit:      w.data(),
			txnID:     op.Txn().ID,
			err:       "",
		},
	}
	s.txnActionC <- event{
		buffer: buf,
	}
}

func (s *service) doTxnEventAction(event client.TxnEvent) {
	unit := ""
	if event.CostEvent {
		unit = "us"
	}
	s.doAddTxnAction(
		event.Txn.ID,
		event.Event.Name,
		event.Sequence,
		0,
		event.Cost.Microseconds(),
		unit,
		event.Err)
}

func (s *service) doAddTxnAction(
	txnID []byte,
	action string,
	actionSequence uint64,
	tableID uint64,
	value int64,
	unit string,
	err error,
) {
	e := ""
	if err != nil {
		e = err.Error()
	}
	s.txnActionC <- event{
		csv: actionEvent{
			ts:        time.Now().UnixNano(),
			action:    action,
			actionSeq: actionSequence,
			tableID:   tableID,
			value:     int64(value),
			unit:      unit,
			txnID:     txnID,
			err:       e,
		},
	}
}

func (s *service) AddTxnFilter(method, value string) error {
	switch method {
	case sessionMethod, connectionMethod, tenantMethod, userMethod:
	default:
		return moerr.NewNotSupportedNoCtx("method %s not support", method)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	return s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			r, err := txn.Exec(addTxnFilterSQL(method, value), executor.StatementOption{})
			if err != nil {
				return err
			}
			r.Close()
			return nil
		},
		executor.Options{}.
			WithDatabase(DebugDB).
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied().
			WithDisableTrace())
}

func (s *service) ClearTxnFilters() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("truncate table %s",
					TraceTxnFilterTable),
				executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}

	return s.RefreshTxnFilters()
}

func (s *service) RefreshTxnFilters() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var filters []TxnFilter
	var methods []string
	var values []string
	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("select method, value from %s",
					TraceTxnFilterTable),
				executor.StatementOption{})
			if err != nil {
				return err
			}
			defer res.Close()

			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				for i := 0; i < rows; i++ {
					methods = append(methods, cols[0].UnsafeGetStringAt(i))
					values = append(values, cols[1].UnsafeGetStringAt(i))
				}
				return true
			})
			return nil
		},
		executor.Options{}.
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}

	filters = append(filters, &disableFilter{})
	for i, method := range methods {
		switch method {
		case sessionMethod:
			filters = append(filters, &sessionIDFilter{sessionID: values[i]})
		case connectionMethod:
			params := strings.Split(values[i], ",")
			filters = append(filters,
				&connectionIDFilter{
					sessionID:    params[0],
					connectionID: format.MustParseStringUint32(params[1]),
				})
		case tenantMethod:
			params := strings.Split(values[i], ",")
			filters = append(filters,
				&tenantFilter{
					accountID: format.MustParseStringUint32(params[0]),
					userName:  params[1],
				})
		case userMethod:
			filters = append(filters,
				&userFilter{
					userName: values[i],
				})
		}
	}

	s.atomic.txnFilters.Store(&txnFilters{filters: filters})
	return nil
}

func (s *service) handleTxnEvents(ctx context.Context) {
	s.handleEvent(
		ctx,
		8,
		EventTxnTable,
		s.txnC,
	)
}

func (s *service) handleTxnActionEvents(ctx context.Context) {
	s.handleEvent(
		ctx,
		9,
		EventTxnActionTable,
		s.txnActionC,
	)
}

func (s *service) handleTxnError(
	ctx context.Context,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case sql := <-s.txnErrorC:
			s.doAddTxnError(sql)
		}
	}
}

func (s *service) doAddTxnError(
	sql string,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(sql,
				executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.
			WithDatabase(DebugDB).
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied().
			WithDisableTrace())
	if err != nil {
		s.logger.Error("exec txn error trace failed",
			zap.String("sql", sql),
			zap.Error(err))
	}
}

func addTxnFilterSQL(
	method string,
	value string,
) string {
	return fmt.Sprintf("insert into %s (method, value) values ('%s', '%s')",
		TraceTxnFilterTable,
		method,
		value)
}

type infoErr struct {
	info string
}

func (e infoErr) Error() string {
	return e.info
}
