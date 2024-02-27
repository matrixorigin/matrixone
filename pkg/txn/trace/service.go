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
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func WithBufferSize(value int) Option {
	return func(s *service) {
		s.options.bufferSize = value
	}
}

func WithFlushBytes(value int) Option {
	return func(s *service) {
		s.options.flushBytes = value
	}
}

func WithFlushDuration(value time.Duration) Option {
	return func(s *service) {
		s.options.flushDuration = value
	}
}

type service struct {
	cn        string
	client    client.TxnClient
	clock     clock.Clock
	executor  executor.SQLExecutor
	stopper   *stopper.Stopper
	txnC      chan csvEvent
	entryC    chan csvEvent
	txnBufC   chan *buffer
	entryBufC chan *buffer
	loadC     chan loadAction
	seq       atomic.Uint64
	dir       string
	logger    *log.MOLogger

	mu struct {
		sync.RWMutex
		entryFilters []EntryFilter
		closed       bool
	}

	atomic struct {
		enabled         atomic.Bool
		loadCSV         atomic.Bool
		complexPKTables sync.Map // uint64 -> bool
	}

	options struct {
		flushDuration time.Duration
		flushBytes    int
		bufferSize    int
	}
}

func NewService(
	dataDir string,
	cn string,
	client client.TxnClient,
	clock clock.Clock,
	executor executor.SQLExecutor,
	opts ...Option) (Service, error) {
	if err := os.RemoveAll(dataDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	s := &service{
		stopper:  stopper.NewStopper("txn-trace"),
		client:   client,
		clock:    clock,
		executor: executor,
		dir:      dataDir,
		logger:   runtime.ProcessLevelRuntime().Logger().Named("txn-trace"),
		loadC:    make(chan loadAction, 100000),
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.options.flushDuration == 0 {
		s.options.flushDuration = 30 * time.Second
	}
	if s.options.flushBytes == 0 {
		s.options.flushBytes = 8 * 1024 * 1024
	}
	if s.options.bufferSize == 0 {
		s.options.bufferSize = 1000
	}

	s.txnBufC = make(chan *buffer, s.options.bufferSize)
	s.entryBufC = make(chan *buffer, s.options.bufferSize)
	s.entryC = make(chan csvEvent, s.options.bufferSize)
	s.txnC = make(chan csvEvent, s.options.bufferSize)

	if err := s.stopper.RunTask(s.handleTxnEvents); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleEntryEvents); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleLoad); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.watch); err != nil {
		panic(err)
	}
	return s, nil
}

func (s *service) TxnCreated(op client.TxnOperator) {
	if op.Txn().DisableTrace {
		return
	}

	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}
	s.txnC <- newTxnCreated(op.Txn())
	op.AppendEventCallback(client.ActiveEvent, s.handleTxnActive)
	op.AppendEventCallback(client.ClosedEvent, s.handleTxnClosed)
	op.AppendEventCallback(client.SnapshotUpdatedEvent, s.handleTxnSnapshotUpdated)
	op.AppendEventCallback(client.CommitFailedEvent, s.handleTxnCommitFailed)

}

func (s *service) handleTxnActive(
	meta txn.TxnMeta,
	_ error) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.txnC <- newTxnActive(meta)
}

func (s *service) handleTxnClosed(
	meta txn.TxnMeta,
	_ error) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.txnC <- newTxnClosed(meta)
}

func (s *service) handleTxnSnapshotUpdated(
	meta txn.TxnMeta,
	_ error) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.txnC <- newTxnSnapshotUpdated(meta)
}

func (s *service) handleTxnCommitFailed(
	meta txn.TxnMeta,
	err error) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.AddTxnError(meta.ID, err)
}

func (s *service) CommitEntries(
	txnID []byte,
	entries []*api.Entry) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	if len(s.mu.entryFilters) == 0 {
		return
	}

	ts := time.Now().UnixNano()
	buf := reuse.Alloc[buffer](nil)
	var entryData *EntryData
	defer func() {
		entryData.close()
	}()

	n := 0
	for _, entry := range entries {
		if entryData == nil {
			entryData = newEntryData(entry, -1, ts)
		} else {
			entryData.reset()
			entryData.init(entry, -1, ts)
		}

		skipped := true
		for _, f := range s.mu.entryFilters {
			if !f.Filter(entryData) {
				skipped = false
				break
			}
		}
		if skipped {
			continue
		}

		entryData.createCommit(
			txnID,
			buf,
			func(e entryEvent) {
				s.entryC <- e
			},
			&s.atomic.complexPKTables)
		n++
	}

	if n == 0 {
		buf.close()
		return
	}
	s.entryBufC <- buf
}

func (s *service) TxnRead(
	txnID []byte,
	snapshotTS timestamp.Timestamp,
	tableID uint64,
	columns []string,
	bat *batch.Batch) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	entryData := newReadEntryData(tableID, snapshotTS, bat, columns, time.Now().UnixNano())
	defer func() {
		entryData.close()
	}()

	skipped := true
	for _, f := range s.mu.entryFilters {
		if !f.Filter(entryData) {
			skipped = false
			break
		}
	}
	if skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	entryData.createRead(
		txnID,
		buf,
		func(e entryEvent) {
			s.entryC <- e
		},
		&s.atomic.complexPKTables)
	s.entryBufC <- buf
}

func (s *service) ChangedCheck(
	txnID []byte,
	tableID uint64,
	from, to timestamp.Timestamp,
	changed bool) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	skipped := true
	l := reuse.Alloc[EntryData](nil)
	defer l.close()
	l.id = tableID
	l.empty = true
	for _, f := range s.mu.entryFilters {
		if !f.Filter(l) {
			skipped = false
			break
		}
	}
	if skipped {
		return
	}
	s.txnC <- newTxnCheckChanged(txnID, from, to, changed)
}

func (s *service) AddTxnError(
	txnID []byte,
	value error) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ts := time.Now().UnixNano()
	msg := value.Error()
	if me, ok := value.(*moerr.Error); ok {
		msg += ": " + me.Detail()
	}

	sql := fmt.Sprintf("insert into %s (ts, txn_id, error_info) values (%d, '%x', '%s')",
		eventErrorTable,
		ts,
		txnID,
		escape(msg))

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
			WithDisableLock().
			WithDisableTrace())
	if err != nil {
		s.logger.Error("exec txn error trace failed",
			zap.String("sql", sql),
			zap.Error(err))
	}
}

func (s *service) ApplyLogtail(
	entry *api.Entry,
	commitTSIndex int) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	if len(s.mu.entryFilters) == 0 {
		return
	}

	entryData := newEntryData(entry, commitTSIndex, time.Now().UnixNano())
	defer func() {
		entryData.close()
	}()

	skipped := true
	for _, f := range s.mu.entryFilters {
		if !f.Filter(entryData) {
			skipped = false
			break
		}
	}
	if skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	entryData.createApply(
		buf,
		func(e entryEvent) {
			s.entryC <- e
		},
		&s.atomic.complexPKTables)
	s.entryBufC <- buf
}

func (s *service) Enable() error {
	return s.updateState(stateEnable)
}

func (s *service) Disable() error {
	return s.updateState(stateDisable)
}

func (s *service) AddEntryFilter(name string, columns []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use("mo_catalog")
			res, err := txn.Exec(fmt.Sprintf("select rel_id from mo_tables where relname = '%s'", name), executor.StatementOption{})
			if err != nil {
				return err
			}
			defer res.Close()

			var tables []uint64
			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				tables = append(tables, vector.MustFixedCol[uint64](cols[0])...)
				return true
			})
			if len(tables) == 0 {
				return moerr.NewNoSuchTableNoCtx("", name)
			}

			txn.Use(DebugDB)
			for _, id := range tables {
				r, err := txn.Exec(getEntryFilterSQL(id, name, columns), executor.StatementOption{})
				if err != nil {
					return err
				}
				r.Close()
			}
			return nil
		},
		executor.Options{}.
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied().
			WithDisableLock().
			WithDisableTrace())
	if err != nil {
		return err
	}

	return s.RefreshFilters()
}

func (s *service) ClearFilters() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("truncate table %s",
					traceTable),
				executor.StatementOption{})
			if err != nil {
				return err
			}
			res.Close()
			return nil
		},
		executor.Options{}.
			WithDisableLock().
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}

	return s.RefreshFilters()
}

func (s *service) RefreshFilters() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var filters []EntryFilter
	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("select table_id, columns from %s",
					traceTable),
				executor.StatementOption{})
			if err != nil {
				return err
			}
			defer res.Close()

			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				for i := 0; i < rows; i++ {
					id := vector.MustFixedCol[uint64](cols[0])[i]
					columns := cols[1].GetStringAt(i)
					filters = append(filters, NewKeepTableFilter(id, strings.Split(columns, ",")))
				}
				return true
			})
			return nil
		},
		executor.Options{}.
			WithDisableLock().
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.entryFilters = filters
	return nil
}

func (s *service) DecodeHexComplexPK(hexPK string) (string, error) {
	v, err := hex.DecodeString(hexPK)
	if err != nil {
		return "", err
	}

	buf := reuse.Alloc[buffer](nil)
	defer buf.close()

	dst := buf.alloc(20)
	idx := buf.buf.GetWriteIndex()
	writeCompletedValue(v, buf, dst)
	return string(buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())), nil
}

func (s *service) Close() {
	s.stopper.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.closed = true
	close(s.entryBufC)
	close(s.entryC)
	close(s.txnC)
	close(s.txnBufC)
	close(s.loadC)
}

func (s *service) txnCSVFile() string {
	return filepath.Join(s.dir, fmt.Sprintf("txn-%d.csv", s.seq.Add(1)))
}

func (s *service) entryCSVFile() string {
	return filepath.Join(s.dir, fmt.Sprintf("entry-%d.csv", s.seq.Add(1)))
}

func (s *service) handleEntryEvents(ctx context.Context) {
	s.handleEvent(
		ctx,
		s.entryCSVFile,
		9,
		eventDataTable,
		s.entryC,
		s.entryBufC)
}

func (s *service) handleTxnEvents(ctx context.Context) {
	s.handleEvent(
		ctx,
		s.txnCSVFile,
		8,
		eventTxnTable,
		s.txnC,
		s.txnBufC)
}

func (s *service) handleEvent(
	ctx context.Context,
	fileCreator func() string,
	columns int,
	tableName string,
	csvC chan csvEvent,
	bufferC chan *buffer) {
	ticker := time.NewTicker(s.options.flushDuration)
	defer ticker.Stop()

	var w *csv.Writer
	var f *os.File
	records := make([]string, columns)
	current := ""
	sum := 0

	buf := reuse.Alloc[buffer](nil)
	defer buf.close()

	open := func() {
		current = fileCreator()

		v, err := os.OpenFile(current, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			s.logger.Fatal("failed to open csv file",
				zap.String("file", current),
				zap.Error(err))
		}
		f = v
		w = csv.NewWriter(f)
	}

	bytes := func() int {
		n := 0
		for _, s := range records {
			n += len(s)
		}
		return n
	}

	flush := func() {
		defer buf.reset()

		if sum == 0 {
			return
		}

		w.Flush()
		if err := w.Error(); err != nil {
			s.logger.Fatal("failed to flush csv file",
				zap.String("table", tableName),
				zap.Error(err))
		}

		if err := f.Close(); err != nil {
			s.logger.Fatal("failed to close csv file",
				zap.String("table", tableName),
				zap.Error(err))
		}

		s.loadC <- loadAction{
			sql: fmt.Sprintf("load data infile '%s' into table %s fields terminated by ','",
				current,
				tableName),
			file: current,
		}
		sum = 0
		open()
	}

	open()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush()
		case e := <-csvC:
			e.toCSVRecord(s.cn, buf, records)
			if err := w.Write(records); err != nil {
				s.logger.Fatal("failed to write csv record",
					zap.Error(err))
			}

			sum += bytes()
			if sum > s.options.flushBytes {
				flush()
			}

			select {
			case v := <-bufferC:
				v.close()
			default:
			}
		}
	}
}

func (s *service) handleLoad(ctx context.Context) {
	load := func(sql string) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		return s.executor.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				res, err := txn.Exec(sql, executor.StatementOption{})
				if err != nil {
					return err
				}
				res.Close()
				return nil
			},
			executor.Options{}.
				WithDatabase(DebugDB).
				WithDisableTrace().
				WithDisableLock())
	}

	for {
		select {
		case <-ctx.Done():
			return
		case e := <-s.loadC:
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if err := load(e.sql); err != nil {
					s.logger.Error("load trace data to table failed, retry later",
						zap.String("sql", e.sql),
						zap.Error(err))
					time.Sleep(time.Second * 5)
					continue
				}

				if err := os.Remove(e.file); err != nil {
					s.logger.Fatal("failed to remove csv file",
						zap.String("file", e.file),
						zap.Error(err))
				}
				break
			}
		}
	}
}

func (s *service) watch(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	fetch := func() (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		state := "disable"
		err := s.executor.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				sql := fmt.Sprintf("select state from %s where name = '%s'",
					featuresTables,
					featureTraceTxn)
				res, err := txn.Exec(sql, executor.StatementOption{})
				if err != nil {
					return err
				}
				defer res.Close()
				res.ReadRows(func(rows int, cols []*vector.Vector) bool {
					state = cols[0].GetStringAt(0)
					return true
				})
				return nil
			},
			executor.Options{}.
				WithDatabase(DebugDB).
				WithDisableTrace().
				WithDisableLock())
		return state, err
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state, err := fetch()
			if err != nil {
				s.logger.Error("failed to fetch trace state",
					zap.Error(err))
				continue
			}

			if state == stateEnable {
				s.active()
			} else {
				s.inactive()
			}
		}
	}
}

func (s *service) updateState(state string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	return s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(
				fmt.Sprintf("update %s set state = '%s'",
					featuresTables,
					state),
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
			WithDisableLock().
			WithDisableTrace())
}

func (s *service) active() {
	if s.atomic.enabled.Load() {
		return
	}

	if err := s.RefreshFilters(); err != nil {
		return
	}
	s.atomic.enabled.Store(true)
}

func (s *service) inactive() error {
	s.atomic.enabled.Store(false)
	return nil
}

// EntryData entry data
type EntryData struct {
	id         uint64
	empty      bool
	at         int64
	snapshotTS timestamp.Timestamp
	entryType  api.Entry_EntryType
	columns    []string
	vecs       []*vector.Vector
	commitVec  *vector.Vector
}

func newEntryData(
	entry *api.Entry,
	commitTSIndex int,
	at int64) *EntryData {
	l := reuse.Alloc[EntryData](nil)
	l.init(entry, commitTSIndex, at)
	return l
}

func newReadEntryData(
	tableID uint64,
	snapshotTS timestamp.Timestamp,
	bat *batch.Batch,
	columns []string,
	at int64) *EntryData {
	l := reuse.Alloc[EntryData](nil)
	l.at = at
	l.snapshotTS = snapshotTS
	l.id = tableID
	l.columns = append(l.columns, columns...)
	l.vecs = append(l.vecs, bat.Vecs...)
	return l
}

func (l *EntryData) needFilterColumns() bool {
	return !l.snapshotTS.IsEmpty() || l.entryType == api.Entry_Insert
}

func (l *EntryData) init(
	entry *api.Entry,
	commitTSIndex int,
	at int64) {
	l.at = at
	l.id = entry.TableId
	l.entryType = entry.EntryType
	l.columns = append(l.columns, entry.Bat.Attrs...)
	for i, vec := range entry.Bat.Vecs {
		v, err := vector.ProtoVectorToVector(vec)
		if err != nil {
			panic(err)
		}
		l.vecs = append(l.vecs, v)
		if commitTSIndex == i {
			l.commitVec = v
		}
	}
}

func (l *EntryData) reset() {
	for i := range l.vecs {
		l.vecs[i] = nil
	}

	l.id = 0
	l.empty = false
	l.commitVec = nil
	l.columns = l.columns[:0]
	l.vecs = l.vecs[:0]
	l.entryType = api.Entry_EntryType(-1)
	l.snapshotTS = timestamp.Timestamp{}
}

func (l *EntryData) close() {
	reuse.Free(l, nil)
}

func (l *EntryData) createApply(
	buf *buffer,
	fn func(e entryEvent),
	completedPKTables *sync.Map) {
	commitTS := vector.MustFixedCol[types.TS](l.commitVec)

	l.writeToBuf(
		buf,
		func(data []byte, row int) entryEvent {
			return newApplyLogtailEvent(
				l.at,
				l.id,
				l.entryType,
				data,
				commitTS[row].ToTimestamp())
		},
		fn,
		completedPKTables)
}

func (l *EntryData) createCommit(
	txnID []byte,
	buf *buffer,
	fn func(e entryEvent),
	completedPKTables *sync.Map) {
	l.writeToBuf(
		buf,
		func(data []byte, _ int) entryEvent {
			return newCommitEntryEvent(
				l.at,
				txnID,
				l.id,
				l.entryType,
				data)
		},
		fn,
		completedPKTables)
}

func (l *EntryData) createRead(
	txnID []byte,
	buf *buffer,
	fn func(e entryEvent),
	completedPKTables *sync.Map) {
	l.writeToBuf(
		buf,
		func(data []byte, row int) entryEvent {
			return newReadEntryEvent(
				l.at,
				txnID,
				l.id,
				l.entryType,
				data,
				l.snapshotTS)
		},
		fn,
		completedPKTables)
}

func (l *EntryData) writeToBuf(
	buf *buffer,
	factory func([]byte, int) entryEvent,
	fn func(e entryEvent),
	completedPKTables *sync.Map) {
	if len(l.vecs) == 0 {
		return
	}

	_, ok := completedPKTables.Load(l.id)
	isCompletedPKTable := false
	dst := buf.alloc(20)
	rows := l.vecs[0].Length()
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		for col, name := range l.columns {
			if _, ok := disableColumns[name]; ok {
				continue
			}
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			if isComplexColumn(name) ||
				(ok &&
					l.entryType == api.Entry_Delete &&
					isDeletePKColumn(name)) {
				writeCompletedValue(columnsData.GetBytesAt(row), buf, dst)
				isCompletedPKTable = true
			} else {
				writeValue(columnsData, row, buf, dst)
			}
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
		fn(factory(data, row))
	}
	if !ok && isCompletedPKTable {
		completedPKTables.Store(l.id, true)
	}
}

func (l EntryData) TypeName() string {
	return "txn.trace.entryData"
}

type buffer struct {
	buf *buf.ByteBuf
}

func (b buffer) TypeName() string {
	return "trace.buffer"
}

func (b *buffer) reset() {
	b.buf.Reset()
}

func (b *buffer) close() {
	reuse.Free(b, nil)
}

func (b buffer) alloc(n int) []byte {
	b.buf.Grow(n)
	idx := b.buf.GetWriteIndex()
	b.buf.SetWriteIndex(idx + n)
	return b.buf.RawSlice(idx, b.buf.GetWriteIndex())
}

func (b buffer) writeInt(v int64) string {
	dst := b.alloc(20)
	return util.UnsafeBytesToString(intToString(dst, v))
}

func (b buffer) writeUint(v uint64) string {
	dst := b.alloc(20)
	return util.UnsafeBytesToString(uintToString(dst, v))
}

func (b buffer) writeHex(src []byte) string {
	if len(src) == 0 {
		return ""
	}
	dst := b.alloc(hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return util.UnsafeBytesToString(dst)
}

func (b buffer) writeTimestamp(value timestamp.Timestamp) string {
	dst := b.alloc(20)
	dst2 := b.alloc(20)
	idx := b.buf.GetWriteIndex()
	b.buf.MustWrite(intToString(dst, value.PhysicalTime))
	b.buf.MustWriteByte('-')
	b.buf.MustWrite(uintToString(dst2, uint64(value.LogicalTime)))
	return util.UnsafeBytesToString(b.buf.RawSlice(idx, b.buf.GetWriteIndex()))
}

func (b buffer) writeBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func getEntryFilterSQL(
	id uint64,
	name string,
	columns []string) string {
	return fmt.Sprintf("insert into %s (table_id, table_name, columns) values (%d, '%s', '%s')",
		traceTable,
		id,
		name,
		strings.Join(columns, ","))
}

func escape(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

type loadAction struct {
	sql  string
	file string
}
