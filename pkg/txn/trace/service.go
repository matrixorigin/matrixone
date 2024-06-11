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
	stRuntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"go.uber.org/zap"
)

func WithEnable(
	value bool,
	tables []uint64) Option {
	return func(s *service) {
		s.atomic.dataEventEnabled.Store(value)
		s.atomic.txnEventEnabled.Store(value)
		s.atomic.txnWorkspaceEnabled.Store(value)
		s.atomic.txnActionEventEnabled.Store(value)

		if value {
			s.atomic.txnFilters.Store(&txnFilters{
				filters: []TxnFilter{&allTxnFilter{}},
			})
			f := &tableFilters{}
			for _, id := range tables {
				f.filters = append(f.filters,
					NewKeepTableFilter(id, nil))
			}
			s.atomic.tableFilters.Store(f)
		}
	}
}

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

func WithLoadToS3(
	writeToS3 bool,
	fs fileservice.FileService,
) Option {
	return func(s *service) {
		if writeToS3 {
			s.options.writeFunc = s.writeToS3
			s.options.fs = fs
		}
	}
}

type service struct {
	cn         string
	client     client.TxnClient
	clock      clock.Clock
	executor   executor.SQLExecutor
	stopper    *stopper.Stopper
	txnC       chan event
	entryC     chan event
	txnActionC chan event
	statementC chan event
	txnErrorC  chan string

	loadC  chan loadAction
	dir    string
	logger *log.MOLogger

	atomic struct {
		flushEnabled          atomic.Bool
		txnEventEnabled       atomic.Bool
		txnWorkspaceEnabled   atomic.Bool
		txnActionEventEnabled atomic.Bool
		dataEventEnabled      atomic.Bool
		statementEnabled      atomic.Bool
		tableFilters          atomic.Pointer[tableFilters]
		txnFilters            atomic.Pointer[txnFilters]
		statementFilters      atomic.Pointer[statementFilters]
		closed                atomic.Bool
		complexPKTables       sync.Map // uint64 -> bool
	}

	options struct {
		fs            fileservice.FileService
		writeFunc     func(loadAction) error
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
		stopper:   stopper.NewStopper("txn-trace"),
		cn:        cn,
		client:    client,
		clock:     clock,
		executor:  executor,
		dir:       dataDir,
		logger:    runtime.ProcessLevelRuntime().Logger().Named("txn-trace"),
		loadC:     make(chan loadAction, 4),
		txnErrorC: make(chan string, stRuntime.NumCPU()*10),
	}
	for _, opt := range opts {
		opt(s)
	}

	if s.options.flushDuration == 0 {
		s.options.flushDuration = 30 * time.Second
	}
	if s.options.flushBytes == 0 {
		s.options.flushBytes = 16 * 1024 * 1024
	}
	if s.options.bufferSize == 0 {
		s.options.bufferSize = 1000000
	}
	if s.options.writeFunc == nil {
		s.options.writeFunc = s.writeToMO
	}

	s.entryC = make(chan event, s.options.bufferSize)
	s.txnC = make(chan event, s.options.bufferSize)
	s.txnActionC = make(chan event, s.options.bufferSize)
	s.statementC = make(chan event, s.options.bufferSize)

	if err := s.stopper.RunTask(s.handleTxnEvents); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleTxnActionEvents); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleDataEvents); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleLoad); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleStatements); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.watch); err != nil {
		panic(err)
	}
	if err := s.stopper.RunTask(s.handleTxnError); err != nil {
		panic(err)
	}
	return s, nil
}

func (s *service) EnableFlush() {
	s.atomic.flushEnabled.Store(true)
}

func (s *service) Enable(feature string) error {
	return s.updateState(feature, StateEnable)
}

func (s *service) Disable(feature string) error {
	return s.updateState(feature, StateDisable)
}

func (s *service) Enabled(feature string) bool {
	switch feature {
	case FeatureTraceData:
		return s.atomic.dataEventEnabled.Load()
	case FeatureTraceTxn:
		return s.atomic.txnEventEnabled.Load()
	case FeatureTraceTxnAction:
		return s.atomic.txnActionEventEnabled.Load()
	case FeatureTraceStatement:
		return s.atomic.statementEnabled.Load()
	case FeatureTraceTxnWorkspace:
		return s.atomic.txnWorkspaceEnabled.Load()
	}
	return false
}

func (s *service) Sync() {
	if !s.Enabled(FeatureTraceTxn) &&
		!s.Enabled(FeatureTraceData) &&
		!s.Enabled(FeatureTraceTxnAction) &&
		!s.Enabled(FeatureTraceStatement) &&
		!s.Enabled(FeatureTraceTxnWorkspace) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	time.Sleep(s.options.flushDuration * 2)
}

func (s *service) DecodeHexComplexPK(hexPK string) (string, error) {
	v, err := hex.DecodeString(hexPK)
	if err != nil {
		return "", err
	}

	buf := reuse.Alloc[buffer](nil)
	defer buf.close()

	dst := buf.alloc(100)
	idx := buf.buf.GetWriteIndex()
	writeCompletedValue(v, buf, dst)
	return string(buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())), nil
}

func (s *service) Close() {
	s.stopper.Stop()
	s.atomic.closed.Store(true)
	close(s.entryC)
	close(s.txnC)
	close(s.loadC)
}

func (s *service) handleEvent(
	ctx context.Context,
	columns int,
	tableName string,
	eventC chan event) {
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
		current = s.newFileName()

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
			file:  current,
			table: tableName,
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
			if s.atomic.flushEnabled.Load() {
				flush()
			}
		case e := <-eventC:
			if e.buffer != nil {
				e.buffer.close()
			} else {
				e.csv.toCSVRecord(s.cn, buf, records)
				if err := w.Write(records); err != nil {
					s.logger.Fatal("failed to write csv record",
						zap.Error(err))
				}

				sum += bytes()
				if sum > s.options.flushBytes &&
					s.atomic.flushEnabled.Load() {
					flush()
				}
			}
		}
	}
}

func (s *service) handleLoad(ctx context.Context) {
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

				if err := s.options.writeFunc(e); err != nil {
					s.logger.Error("load trace data to table failed, retry later",
						zap.String("file", e.file),
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

	fetch := func() ([]string, []string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		var features []string
		var states []string
		err := s.executor.ExecTxn(
			ctx,
			func(txn executor.TxnExecutor) error {
				sql := fmt.Sprintf("select name, state from %s", FeaturesTables)
				res, err := txn.Exec(sql, executor.StatementOption{})
				if err != nil {
					return err
				}
				defer res.Close()
				res.ReadRows(func(rows int, cols []*vector.Vector) bool {
					for i := 0; i < rows; i++ {
						features = append(features, cols[0].UnsafeGetStringAt(i))
						states = append(states, cols[1].UnsafeGetStringAt(i))
					}
					return true
				})
				return nil
			},
			executor.Options{}.
				WithDatabase(DebugDB).
				WithDisableTrace())
		return features, states, err
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.atomic.flushEnabled.Load() {
				continue
			}

			features, states, err := fetch()
			if err != nil {
				s.logger.Error("failed to fetch trace state",
					zap.Error(err))
				continue
			}

			needRefresh := false
			for i, feature := range features {
				enable := states[i] == StateEnable
				if enable {
					needRefresh = true
				}
				switch feature {
				case FeatureTraceData:
					s.atomic.dataEventEnabled.Store(enable)
				case FeatureTraceTxnWorkspace:
					s.atomic.txnWorkspaceEnabled.Store(enable)
				case FeatureTraceTxn:
					s.atomic.txnEventEnabled.Store(enable)
				case FeatureTraceTxnAction:
					s.atomic.txnActionEventEnabled.Store(enable)
				case FeatureTraceStatement:
					s.atomic.statementEnabled.Store(enable)
				}
			}

			if needRefresh {
				if err := s.RefreshTableFilters(); err != nil {
					s.logger.Error("failed to refresh table filters",
						zap.Error(err))
				}

				if err := s.RefreshTxnFilters(); err != nil {
					s.logger.Error("failed to refresh txn filters",
						zap.Error(err))
				}

				if err := s.RefreshStatementFilters(); err != nil {
					s.logger.Error("failed to refresh statement filters",
						zap.Error(err))
				}
			}
		}
	}
}

func (s *service) updateState(feature, state string) error {
	switch feature {
	case FeatureTraceData, FeatureTraceTxnAction, FeatureTraceTxn, FeatureTraceStatement, FeatureTraceTxnWorkspace:
	default:
		return moerr.NewNotSupportedNoCtx("feature %s", feature)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	return s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			res, err := txn.Exec(
				fmt.Sprintf("update %s set state = '%s' where name = '%s'",
					FeaturesTables,
					state,
					feature),
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
}

func (s *service) newFileName() string {
	return filepath.Join(s.dir, uuid.Must(uuid.NewV7()).String())
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

func newWriteEntryData(
	tableID uint64,
	bat *batch.Batch,
	at int64) *EntryData {
	l := reuse.Alloc[EntryData](nil)
	l.at = at
	l.id = tableID
	if bat != nil && bat.RowCount() > 0 {
		l.columns = append(l.columns, bat.Attrs...)
		l.vecs = append(l.vecs, bat.Vecs...)
	}
	return l
}

func newWorkspaceEntryData(
	tableID uint64,
	bat *batch.Batch,
	at int64) *EntryData {
	l := reuse.Alloc[EntryData](nil)
	l.at = at
	l.id = tableID
	if bat != nil && bat.RowCount() > 0 {
		l.columns = append(l.columns, bat.Attrs...)
		l.vecs = append(l.vecs, bat.Vecs...)
	}
	return l
}

func newTableOnlyEntryData(tableID uint64) *EntryData {
	l := reuse.Alloc[EntryData](nil)
	l.id = tableID
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
	fn func(e dataEvent),
	completedPKTables *sync.Map) {
	commitTS := vector.MustFixedCol[types.TS](l.commitVec)

	l.writeToBuf(
		buf,
		func(data []byte, row int) dataEvent {
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
	fn func(e dataEvent),
	completedPKTables *sync.Map) {
	l.writeToBuf(
		buf,
		func(data []byte, _ int) dataEvent {
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
	fn func(e dataEvent),
	completedPKTables *sync.Map) {
	l.writeToBuf(
		buf,
		func(data []byte, row int) dataEvent {
			return newReadEntryEvent(
				l.at,
				txnID,
				l.id,
				data,
				l.snapshotTS)
		},
		fn,
		completedPKTables)
}

func (l *EntryData) createWrite(
	txnID []byte,
	buf *buffer,
	typ string,
	fn func(e dataEvent),
	completedPKTables *sync.Map) {

	if typ == "delete" {
		l.entryType = api.Entry_Delete
	}

	l.writeToBuf(
		buf,
		func(data []byte, row int) dataEvent {
			return newWriteEntryEvent(
				l.at,
				txnID,
				typ,
				l.id,
				data)
		},
		fn,
		completedPKTables)
}

func (l *EntryData) createWorkspace(
	txnID []byte,
	buf *buffer,
	typ string,
	adjustCount int,
	offsetCount int,
	fn func(e dataEvent),
	completedPKTables *sync.Map) {
	l.writeToBuf(
		buf,
		func(data []byte, row int) dataEvent {
			adjust := buf.writeInt(int64(adjustCount))
			offset := buf.writeInt(int64(offsetCount))
			idx := buf.buf.GetWriteIndex()
			buf.buf.WriteString(adjust)
			buf.buf.WriteString("-")
			buf.buf.WriteString(offset)
			buf.buf.WriteString(": ")
			buf.buf.Write(data)
			data = buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
			return newWorkspaceEntryEvent(
				l.at,
				txnID,
				typ,
				l.id,
				data)
		},
		fn,
		completedPKTables)
}

func (l *EntryData) writeToBuf(
	buf *buffer,
	factory func([]byte, int) dataEvent,
	fn func(e dataEvent),
	completedPKTables *sync.Map) {
	if len(l.vecs) == 0 {
		fn(factory([]byte(""), 0))
		return
	}

	_, ok := completedPKTables.Load(l.id)
	isCompletedPKTable := false
	dst := buf.alloc(100)
	rows := l.vecs[0].Length()
	for row := 0; row < rows; row++ {
		idx := buf.buf.GetWriteIndex()
		buf.buf.WriteString("row-")
		buf.buf.MustWrite(intToString(dst, int64(row)))
		buf.buf.WriteString("{")
		for col, name := range l.columns {
			if _, ok := disableColumns[name]; ok {
				continue
			}
			columnsData := l.vecs[col]
			buf.buf.WriteString(name)
			buf.buf.WriteString(":")
			if isComplexColumn(name) ||
				(isDeletePKColumn(name) &&
					ok &&
					l.entryType == api.Entry_Delete) {
				writeCompletedValue(columnsData.GetBytesAt(row), buf, dst)
				isCompletedPKTable = true
			} else {
				writeValue(columnsData, row, buf, dst)
			}
			if col != len(l.columns)-1 {
				buf.buf.WriteString(", ")
			}
		}
		buf.buf.WriteString("}")
		if buf.buf.GetWriteIndex() > idx {
			data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())
			fn(factory(data, row))
		}
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

func (b *buffer) alloc(n int) []byte {
	b.buf.Grow(n)
	idx := b.buf.GetWriteIndex()
	b.buf.SetWriteIndex(idx + n)
	return b.buf.RawSlice(idx, b.buf.GetWriteIndex())
}

func (b buffer) writeInt(v int64) string {
	dst := b.alloc(20)
	return util.UnsafeBytesToString(intToString(dst, v))
}

func (b buffer) writeIntWithBytes(v int64) []byte {
	dst := b.alloc(20)
	return intToString(dst, v)
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

func (b buffer) writeHexWithBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := b.alloc(hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
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

func escape(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

type loadAction struct {
	sql   string
	file  string
	table string
}

type writer struct {
	buf *buf.ByteBuf
	dst []byte
	idx int
}

func (w writer) WriteUint(v uint64) {
	w.buf.MustWrite(uintToString(w.dst, v))
}

func (w writer) WriteInt(v int64) {
	w.buf.MustWrite(intToString(w.dst, v))
}

func (w writer) WriteString(v string) {
	w.buf.WriteString(v)
}

func (w writer) WriteHex(v []byte) {
	if len(v) == 0 {
		return
	}
	dst := w.dst[:hex.EncodedLen(len(v))]
	hex.Encode(dst, v)
	w.buf.MustWrite(dst)
}

func (w writer) data() string {
	return util.UnsafeBytesToString(w.buf.RawSlice(w.idx, w.buf.GetWriteIndex()))
}
