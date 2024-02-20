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
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type service struct {
	cn           string
	client       client.TxnClient
	clock        clock.Clock
	executor     executor.SQLExecutor
	txnEventC    chan txnEvent
	entryC       chan entryEvent
	closeBufferC chan *buffer
	stopper      *stopper.Stopper

	mu struct {
		sync.RWMutex
		entryFilters []EntryFilter
		closed       bool
	}

	atomic struct {
		enabled           atomic.Bool
		completedPKTables sync.Map // uint64 -> bool
	}

	options struct {
		forceFlush    time.Duration
		maxTxnBatch   int
		maxEntryBatch int
	}
}

func NewService(cn string,
	client client.TxnClient,
	clock clock.Clock,
	executor executor.SQLExecutor,
	opts ...Option) (Service, error) {
	s := &service{
		stopper:      stopper.NewStopper("txn-trace"),
		client:       client,
		clock:        clock,
		executor:     executor,
		txnEventC:    make(chan txnEvent, 1024),
		entryC:       make(chan entryEvent, 10240),
		closeBufferC: make(chan *buffer, 10240),
	}
	s.options.forceFlush = 10 * time.Second
	s.options.maxTxnBatch = 256
	s.options.maxEntryBatch = 256
	for _, opt := range opts {
		opt(s)
	}

	if err := s.stopper.RunTask(s.handleEvents); err != nil {
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

	s.txnEventC <- newTxnCreated(op.Txn())
	op.AppendEventCallback(client.ActiveEvent, s.handleTxnActive)
	op.AppendEventCallback(client.ClosedEvent, s.handleTxnClosed)
	op.AppendEventCallback(client.SnapshotUpdatedEvent, s.handleTxnSnapshotUpdated)
}

func (s *service) handleTxnActive(meta txn.TxnMeta) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.txnEventC <- newTxnActive(meta)
}

func (s *service) handleTxnClosed(meta txn.TxnMeta) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.txnEventC <- newTxnClosed(meta)
}

func (s *service) handleTxnSnapshotUpdated(meta txn.TxnMeta) {
	if !s.atomic.enabled.Load() {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mu.closed {
		return
	}

	s.txnEventC <- newTxnSnapshotUpdated(meta)
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
			})
		n++
	}

	if n == 0 {
		buf.close()
		return
	}
	s.closeBufferC <- buf
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
		})
	s.closeBufferC <- buf
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
	s.txnEventC <- newTxnChangedCheck(txnID, from, to, changed)
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
		})
	s.closeBufferC <- buf
}

func (s *service) Enable() error {
	if err := s.RefreshFilters(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.atomic.enabled.Store(true)
	return nil
}

func (s *service) Disable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.atomic.enabled.Store(false)
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
					TraceEntryFilterTable),
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
					TraceEntryFilterTable),
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

func (s *service) Close() {
	s.stopper.Stop()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.closed = true
	close(s.txnEventC)
	close(s.entryC)
	close(s.closeBufferC)
}

func (s *service) handleEvents(ctx context.Context) {
	ticker := time.NewTicker(s.options.forceFlush)
	defer ticker.Stop()

	txnEvents := make([]txnEvent, 0, s.options.maxTxnBatch)
	entryEvents := make([]entryEvent, 0, s.options.maxEntryBatch)

	flush := func() {
		if len(txnEvents) == 0 {
			return
		}
		defer func() {
			txnEvents = txnEvents[:0]
			entryEvents = entryEvents[:0]
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := s.executor.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				for _, e := range txnEvents {
					res, err := txn.Exec(e.toSQL(s.cn), executor.StatementOption{})
					if err != nil {
						return err
					}
					res.Close()
				}

				for _, e := range entryEvents {
					res, err := txn.Exec(e.toSQL(s.cn), executor.StatementOption{})
					if err != nil {
						return err
					}
					res.Close()
				}
				return nil
			},
			executor.Options{}.
				WithDatabase(DebugDB).
				WithDisableTrace().
				WithDisableLock())
		if err != nil {
			panic(err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flush()
		case e := <-s.txnEventC:
			txnEvents = append(txnEvents, e)
			if len(txnEvents) >= s.options.maxTxnBatch {
				flush()
			}
		case e := <-s.entryC:
			entryEvents = append(entryEvents, e)
			if len(entryEvents) >= s.options.maxEntryBatch {
				flush()
			}
		case buf := <-s.closeBufferC:
			buf.close()
		}
	}
}

// EntryData entry data
type EntryData struct {
	id         uint64
	empty      bool
	at         int64
	snapshotTS timestamp.Timestamp
	entryType  string
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
	l.entryType = "read"
	l.columns = append(l.columns, columns...)
	l.vecs = append(l.vecs, bat.Vecs...)
	return l
}

func (l *EntryData) init(
	entry *api.Entry,
	commitTSIndex int,
	at int64) {
	l.at = at
	l.id = entry.TableId
	l.entryType = entry.EntryType.String()
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
	l.entryType = ""
	l.snapshotTS = timestamp.Timestamp{}
}

func (l *EntryData) close() {
	reuse.Free(l, nil)
}

func (l *EntryData) createApply(
	buf *buffer,
	fn func(e entryEvent)) {
	if len(l.vecs) == 0 {
		return
	}

	dst := buf.alloc(20)
	rows := l.vecs[0].Length()
	commitTS := vector.MustFixedCol[types.TS](l.commitVec)
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		for col, name := range l.columns {
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			if name == "__mo_cpkey_col" {
				writeCompletedValue(columnsData, row, buf, dst)
			} else {
				writeValue(columnsData, row, buf, dst)
			}
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
		fn(newApplyLogtailEvent(l.at, l.id, l.entryType, data, commitTS[row].ToTimestamp()))
	}
}

func (l *EntryData) createCommit(
	txnID []byte,
	buf *buffer,
	fn func(e entryEvent)) {
	if len(l.vecs) == 0 {
		return
	}

	dst := buf.alloc(20)
	rows := l.vecs[0].Length()
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		for col, name := range l.columns {
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			if name == "__mo_cpkey_col" {
				writeCompletedValue(columnsData, row, buf, dst)
			} else {
				writeValue(columnsData, row, buf, dst)
			}
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
		fn(newCommitEntryEvent(l.at, txnID, l.id, l.entryType, data))
	}
}

func (l *EntryData) createRead(
	txnID []byte,
	buf *buffer,
	fn func(e entryEvent)) {
	if len(l.vecs) == 0 {
		return
	}

	dst := buf.alloc(20)
	rows := l.vecs[0].Length()
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		for col, name := range l.columns {
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			if name == "__mo_cpkey_col" {
				writeCompletedValue(columnsData, row, buf, dst)
			} else {
				writeValue(columnsData, row, buf, dst)
			}
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
		fn(newReadEntryEvent(l.at, txnID, l.id, l.entryType, data, l.snapshotTS))
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
	b.buf.Close()
	reuse.Free(b, nil)
}

func (b buffer) alloc(n int) []byte {
	b.buf.Grow(n)
	idx := b.buf.GetWriteIndex()
	b.buf.SetWriteIndex(idx + n)
	return b.buf.RawSlice(idx, b.buf.GetWriteIndex())
}

func getEntryFilterSQL(
	id uint64,
	name string,
	columns []string) string {
	return fmt.Sprintf("insert into %s (table_id, table_name, columns) values (%d, '%s', '%s')",
		TraceEntryFilterTable,
		id,
		name,
		strings.Join(columns, ","))
}
