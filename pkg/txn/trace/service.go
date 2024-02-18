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
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
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
		databaseCreated bool
		enable          bool
		entryFilters    []EntryFilter
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

	s.txnEventC <- newTxnCreated(op.Txn())
	op.AppendEventCallback(client.ActiveEvent, s.handleTxnActive)
	op.AppendEventCallback(client.ClosedEvent, s.handleTxnClosed)
	op.AppendEventCallback(client.SnapshotUpdatedEvent, s.handleTxnSnapshotUpdated)
}

func (s *service) handleTxnActive(meta txn.TxnMeta) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mu.enable {
		return
	}

	s.txnEventC <- newTxnActive(meta)
}

func (s *service) handleTxnClosed(meta txn.TxnMeta) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mu.enable {
		return
	}

	s.txnEventC <- newTxnClosed(meta)
}

func (s *service) handleTxnSnapshotUpdated(meta txn.TxnMeta) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mu.enable {
		return
	}

	s.txnEventC <- newTxnSnapshotUpdated(meta)
}

func (s *service) CommitEntries(
	txnID []byte,
	entries []*api.Entry) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mu.enable {
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
		}

		skipped := true
		for _, f := range s.mu.entryFilters {
			if !f.Filter(entryData) {
				skipped = false
				break
			}
		}
		if skipped {
			entryData.close()
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

func (s *service) ApplyLogtail(
	entry *api.Entry,
	commitTSIndex int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.mu.enable {
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
		entryData.close()
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

func (s *service) Enable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.mu.databaseCreated {

	}
	s.mu.enable = true
}

func (s *service) Disable() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.enable = false
}

func (s *service) AddEntryFilters(filters []EntryFilter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.entryFilters = append(s.mu.entryFilters, filters...)
}

func (s *service) ClearEntryFilters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.entryFilters = s.mu.entryFilters[:0]
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
				WithDatabase(db).
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

func (s *service) mustCreateDatabaseLocked() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	now, _ := s.clock.Now()
	s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			_, err := txn.Exec("show databases", executor.StatementOption{})
			if err != nil {
				return err
			}

			return nil
		},
		executor.Options{}.
			WithDatabase(db).
			WithMinCommittedTS(now))
}

// EntryData entry data
type EntryData struct {
	id            uint64
	at            int64
	entryType     api.Entry_EntryType
	columns       []string
	vecs          []*vector.Vector
	commitTSIndex int
}

func newEntryData(
	entry *api.Entry,
	commitTSIndex int,
	at int64) *EntryData {
	l := reuse.Alloc[EntryData](nil)
	l.at = at
	l.id = entry.TableId
	l.commitTSIndex = commitTSIndex
	l.columns = append(l.columns, entry.Bat.Attrs...)
	for _, vec := range entry.Bat.Vecs {
		v, err := vector.ProtoVectorToVector(vec)
		if err != nil {
			panic(err)
		}
		l.vecs = append(l.vecs, v)
	}
	l.entryType = entry.EntryType
	return l
}

func (l *EntryData) reset() {
	for i := range l.vecs {
		l.vecs[i] = nil
	}

	l.id = 0
	l.commitTSIndex = -1
	l.columns = l.columns[:0]
	l.vecs = l.vecs[:0]
	l.entryType = api.Entry_EntryType(-1)
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

	rows := l.vecs[0].Length()
	commitTS := vector.MustFixedCol[types.TS](l.vecs[l.commitTSIndex])
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		for col, name := range l.columns {
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			writeValue(columnsData, row, buf)
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
		fn(newApplyLogtailEvent(l.at, l.id, data, commitTS[row].ToTimestamp()))
	}
}

func (l *EntryData) createCommit(
	txnID []byte,
	buf *buffer,
	fn func(e entryEvent)) {
	if len(l.vecs) == 0 {
		return
	}

	rows := l.vecs[0].Length()
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		for col, name := range l.columns {
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			writeValue(columnsData, row, buf)
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
		fn(newCommitEntryEvent(l.at, txnID, l.id, data))
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
