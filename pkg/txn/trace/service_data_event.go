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
	"path/filepath"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func (s *service) ApplyLogtail(
	entry *api.Entry,
	commitTSIndex int) {
	if !s.Enabled(FeatureTraceData) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	entryData := newEntryData(entry, commitTSIndex, time.Now().UnixNano())
	defer func() {
		entryData.close()
	}()

	filters := s.atomic.tableFilters.Load()
	if skipped := filters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	entryData.createApply(
		buf,
		func(e dataEvent) {
			s.entryC <- e
		},
		&s.atomic.complexPKTables)
	s.entryBufC <- buf
}

func (s *service) ApplyFlush(
	txnID []byte,
	tableID uint64,
	from, to timestamp.Timestamp,
	count int) {
	if !s.Enabled(FeatureTraceData) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	entryData := newTableOnlyEntryData(tableID)
	defer func() {
		entryData.close()
	}()

	filters := s.atomic.tableFilters.Load()
	if skipped := filters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	fromTS := buf.writeTimestamp(from)
	toTS := buf.writeTimestamp(to)
	result := buf.writeIntWithBytes(int64(count))

	idx := buf.buf.GetWriteIndex()
	buf.buf.WriteString(fromTS)
	buf.buf.WriteString(" ")
	buf.buf.WriteString(toTS)
	buf.buf.WriteString(" ")
	buf.buf.MustWrite(result)

	s.entryC <- newFlushEvent(
		time.Now().UnixNano(),
		txnID,
		tableID,
		buf.buf.RawSlice(idx, buf.buf.GetWriteIndex()))
	s.entryBufC <- buf
}

func (s *service) ApplyTransferRowID(
	txnID []byte,
	tableID uint64,
	from, to []byte,
	fromBlockID, toBlockID []byte,
	vec *vector.Vector,
	row int) {
	if !s.Enabled(FeatureTraceData) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	entryData := newTableOnlyEntryData(tableID)
	defer func() {
		entryData.close()
	}()

	filters := s.atomic.tableFilters.Load()
	if skipped := filters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)

	fromRowID := types.Rowid(from)
	toRowID := types.Rowid(to)
	fromBlockIDHex := buf.writeHex(fromBlockID)
	toBlockIDHex := buf.writeHex(toBlockID)

	tmp := buf.alloc(100)
	idx := buf.buf.GetWriteIndex()

	_, ok := s.atomic.complexPKTables.Load(tableID)
	if ok {
		writeCompletedValue(vec.GetBytesAt(row), buf, tmp)
	} else {
		writeValue(vec, row, buf, tmp)
	}

	buf.buf.WriteString(" row_id: ")
	buf.buf.WriteString(fromRowID.String())
	buf.buf.WriteString("->")
	buf.buf.WriteString(toRowID.String())
	buf.buf.WriteString(" block_id:")
	buf.buf.WriteString(fromBlockIDHex)
	buf.buf.WriteString("->")
	buf.buf.WriteString(toBlockIDHex)
	data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())

	s.entryC <- newTransferEvent(
		time.Now().UnixNano(),
		txnID,
		tableID,
		data)
	s.entryBufC <- buf
}

func (s *service) ApplyDeleteObject(
	tableID uint64,
	ts timestamp.Timestamp,
	objName string,
	tag string) {
	if !s.Enabled(FeatureTraceData) {
		return
	}

	if s.atomic.closed.Load() {
		return
	}

	entryData := newTableOnlyEntryData(tableID)
	defer func() {
		entryData.close()
	}()

	filters := s.atomic.tableFilters.Load()
	if skipped := filters.filter(entryData); skipped {
		return
	}

	buf := reuse.Alloc[buffer](nil)
	version := buf.writeTimestamp(ts)

	idx := buf.buf.GetWriteIndex()
	buf.buf.WriteString(objName)
	buf.buf.MustWriteByte(' ')
	buf.buf.WriteString(version)
	buf.buf.MustWriteByte(' ')
	buf.buf.WriteString(tag)
	data := buf.buf.RawSlice(idx, buf.buf.GetWriteIndex())

	s.entryC <- newDeleteObjectEvent(
		time.Now().UnixNano(),
		tableID,
		data)
	s.entryBufC <- buf
}

func (s *service) AddTableFilter(name string, columns []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	return s.executor.ExecTxn(
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
				r, err := txn.Exec(addTableFilterSQL(id, name, columns), executor.StatementOption{})
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
			WithDisableTrace())
}

func (s *service) ClearTableFilters() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now, _ := s.clock.Now()
	err := s.executor.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(DebugDB)
			res, err := txn.Exec(
				fmt.Sprintf("truncate table %s",
					TraceTableFilterTable),
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

	return s.RefreshTableFilters()
}

func (s *service) RefreshTableFilters() error {
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
					TraceTableFilterTable),
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
			WithDisableTrace().
			WithMinCommittedTS(now).
			WithWaitCommittedLogApplied())
	if err != nil {
		return err
	}

	s.atomic.tableFilters.Store(&tableFilters{filters: filters})
	return nil
}

func (s *service) handleDataEvents(ctx context.Context) {
	s.handleEvent(
		ctx,
		s.dataCSVFile,
		9,
		EventDataTable,
		s.entryC,
		s.entryBufC)
}

func (s *service) dataCSVFile() string {
	return filepath.Join(s.dir, fmt.Sprintf("data-%d.csv", s.seq.Add(1)))
}

func addTableFilterSQL(
	id uint64,
	name string,
	columns []string) string {
	return fmt.Sprintf("insert into %s (table_id, table_name, columns) values (%d, '%s', '%s')",
		TraceTableFilterTable,
		id,
		name,
		strings.Join(columns, ","))
}
