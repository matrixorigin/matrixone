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

package logservicedriver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

type replayer struct {
	readMaxSize int

	truncatedPSN uint64

	minDSN uint64
	maxDSN uint64

	// DSN->PSN mapping
	// it is built by reading records from log service
	// and it is deleted when the record is replayed
	dsnToPSNMap map[uint64]uint64

	replayHandle  driver.ApplyHandle
	dsnWatermark  uint64
	inited        bool
	safeLsn       uint64
	nextToReadPSN uint64
	d             *LogServiceDriver
	writeTokens   []uint64

	recordChan        chan *entry.Entry
	lastEntry         *entry.Entry
	firstEntryIsFound atomic.Bool
	readEnd           bool

	readDuration  time.Duration
	applyDuration time.Duration
	readCount     int
	internalCount int
	applyCount    int

	wg sync.WaitGroup
}

func newReplayer(
	h driver.ApplyHandle,
	d *LogServiceDriver,
	readmaxsize int,
) *replayer {
	truncatedPSN := d.getTruncatedPSNFromRemote()
	r := &replayer{
		minDSN:        math.MaxUint64,
		dsnToPSNMap:   make(map[uint64]uint64),
		replayHandle:  h,
		readMaxSize:   readmaxsize,
		nextToReadPSN: truncatedPSN + 1,
		dsnWatermark:  math.MaxUint64,
		d:             d,
		writeTokens:   make([]uint64, 0),
		recordChan:    make(chan *entry.Entry, 100),
		truncatedPSN:  truncatedPSN,
	}
	return r
}

func (r *replayer) replay() {
	var err error

	r.wg.Add(1)
	// replay records in another goroutine
	go r.replayRecords()

	for !r.readNextBatch() {
		for r.dsnWatermark < r.safeLsn {
			err := r.replayLogserviceEntry(r.dsnWatermark + 1)
			if err != nil {
				panic(err)
			}
		}
	}
	r.readEnd = true
	err = r.replayLogserviceEntry(r.dsnWatermark + 1)
	for err != ErrAllRecordsRead {
		err = r.replayLogserviceEntry(r.dsnWatermark + 1)

	}
	r.d.psns = r.d.psns[:0]
	r.recordChan <- entry.NewEndEntry()
	r.wg.Wait()
	close(r.recordChan)
}

func (r *replayer) readNextBatch() (readEnd bool) {
	start := time.Now()
	defer func() {
		r.readDuration += time.Since(start)
	}()
	nextPSN, safeLsn := r.d.readFromLogServiceInReplay(
		r.nextToReadPSN,
		r.readMaxSize,
		func(psn uint64, record *recordEntry) {
			r.readCount++
			if record.Meta.metaType == TReplay {
				r.internalCount++
				logutil.Info(
					"Wal-Replay-Skip-Entry-By-CMD",
					zap.Any("dsn-psn", record.cmd.skipMap),
				)
				r.removeEntries(record.cmd.skipMap)
				return
			}
			dsn := record.GetMinLsn()
			r.dsnToPSNMap[dsn] = psn
			if dsn-1 < r.dsnWatermark {
				if r.inited {
					panic("logic err")
				}
				r.dsnWatermark = dsn - 1
			}
		},
	)
	if safeLsn > r.safeLsn {
		r.safeLsn = safeLsn
	}
	if nextPSN == r.nextToReadPSN {
		return true
	}
	r.nextToReadPSN = nextPSN
	return false
}
func (r *replayer) removeEntries(skipMap map[uint64]uint64) {
	for lsn := range skipMap {
		if _, ok := r.dsnToPSNMap[lsn]; !ok {
			panic(fmt.Sprintf("lsn %d not existed, map is %v", lsn, r.dsnToPSNMap))
		}
		delete(r.dsnToPSNMap, lsn)
	}
}

func (r *replayer) replayRecords() {
	defer r.wg.Done()
	for {
		e := <-r.recordChan
		if e.IsEnd() {
			if r.lastEntry == nil {
				logutil.Info("Wal-Replay-Trace-Replay-End-Empty")
			} else {
				logutil.Info("Wal-Replay-Trace-Replay-End",
					zap.Uint64("last-dsn", r.lastEntry.Lsn))
			}
			break
		}
		t0 := time.Now()
		state := r.replayHandle(e)
		if state == driver.RE_Nomal {
			if !r.firstEntryIsFound.Load() {
				logutil.Info("Wal-Replay-Trace-Find-First-Entry", zap.Uint64("dsn", e.Lsn))
			}
			r.firstEntryIsFound.Store(true)
		}
		e.DoneWithErr(nil)
		e.Entry.Free()
		r.applyDuration += time.Since(t0)
	}
}

func (r *replayer) replayLogserviceEntry(dsn uint64) error {
	psn, ok := r.dsnToPSNMap[dsn]
	if !ok {
		skipFn := func() {
			if len(r.dsnToPSNMap) != 0 {
				r.AppendSkipCmd(r.dsnToPSNMap)
			}
		}
		firstEntryIsFound := r.firstEntryIsFound.Load()
		if !firstEntryIsFound && r.lastEntry != nil {
			r.lastEntry.WaitDone()
			firstEntryIsFound = r.firstEntryIsFound.Load()
		}
		safe := dsn <= r.safeLsn
		if safe {
			if !firstEntryIsFound {
				logutil.Info(
					"Wal-Replay-Trace-Skip-Entry",
					zap.Uint64("dsn", dsn),
				)
				r.minDSN = dsn + 1
				r.dsnWatermark++
				return nil
			} else {
				panic(fmt.Sprintf(
					"logic error, dsn %d is missing, safe dsn %d, map %v",
					dsn, r.safeLsn, r.dsnToPSNMap,
				))
			}
		} else {
			if !r.readEnd {
				panic(fmt.Sprintf("logic error, safe dsn %d, dsn %d", r.safeLsn, dsn))
			}
			skipFn()
			return ErrAllRecordsRead
		}
	}
	if !r.inited {
		logutil.Info("Wal-Replay-Trace-Replay-First-Entry", zap.Uint64("dsn", dsn))
	}
	logRecord, err := r.d.readFromCache(psn)
	if err == ErrAllRecordsRead {
		return err
	}
	if err != nil {
		panic(err)
	}

	dsnRange := logRecord.scheduleReplay(r)
	r.applyCount++

	// update replayer state after scheduling replay for the record
	r.updateDSN(dsnRange.GetMax())
	r.updateDSN(dsnRange.GetMin())
	r.dsnWatermark = dsnRange.GetMax()
	delete(r.dsnToPSNMap, dsn)

	// update the driver state
	r.d.recordPSNInfo(psn, dsnRange)
	r.d.dropRecordFromCache(psn)

	r.inited = true
	return nil
}

func (r *replayer) AppendSkipCmd(skipMap map[uint64]uint64) {
	if len(skipMap) > r.d.config.ClientMaxCount {
		panic(fmt.Sprintf(
			"logic error, skip %d entries, client max count is %v, skip map is %v",
			len(skipMap),
			r.d.config.ClientMaxCount,
			skipMap),
		)
	}
	logutil.Info(
		"Wal-Replay-Append-Skip-Entries",
		zap.Any("gsn-psn-map", skipMap),
	)
	cmd := NewReplayCmd(skipMap)
	recordEntry := newRecordEntry()
	recordEntry.Meta.metaType = TReplay
	recordEntry.cmd = cmd
	size := recordEntry.prepareRecord()
	c, writeToken := r.d.getClientForWrite()
	r.writeTokens = append(r.writeTokens, writeToken)
	c.TryResize(size)
	record := c.record
	copy(record.Payload(), recordEntry.payload)
	record.ResizePayload(size)
	ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*10, moerr.CauseAppendSkipCmd)
	_, err := c.c.Append(ctx, c.record)
	err = moerr.AttachCause(ctx, err)
	cancel()
	if err != nil {
		err = RetryWithTimeout(r.d.config.RetryTimeout, func() (shouldReturn bool) {
			ctx, cancel := context.WithTimeoutCause(context.Background(), time.Second*10, moerr.CauseAppendSkipCmd2)
			_, err := c.c.Append(ctx, c.record)
			err = moerr.AttachCause(ctx, err)
			cancel()
			return err == nil
		})
		if err != nil {
			panic(err)
		}
	}
}

// updateDSN updates the minDSN and maxDSN
func (r *replayer) updateDSN(dsn uint64) {
	if dsn == 0 {
		return
	}
	if dsn < r.minDSN {
		r.minDSN = dsn
	}
	if dsn > r.maxDSN {
		r.maxDSN = dsn
	}
}

type ReplayCmd struct {
	// DSN->PSN mapping
	skipMap map[uint64]uint64
}

func NewReplayCmd(skipMap map[uint64]uint64) *ReplayCmd {
	return &ReplayCmd{
		skipMap: skipMap,
	}
}
func NewEmptyReplayCmd() *ReplayCmd {
	return &ReplayCmd{
		skipMap: make(map[uint64]uint64),
	}
}

func (c *ReplayCmd) WriteTo(w io.Writer) (n int64, err error) {
	length := uint16(len(c.skipMap))
	if _, err = w.Write(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	for dsn, psn := range c.skipMap {
		if _, err = w.Write(types.EncodeUint64(&dsn)); err != nil {
			return
		}
		n += 8
		if _, err = w.Write(types.EncodeUint64(&psn)); err != nil {
			return
		}
		n += 8
	}
	return
}

func (c *ReplayCmd) ReadFrom(r io.Reader) (n int64, err error) {
	length := uint16(0)
	if _, err = r.Read(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	for i := 0; i < int(length); i++ {
		dsn := uint64(0)
		lsn := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&dsn)); err != nil {
			return
		}
		n += 8
		if _, err = r.Read(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		c.skipMap[dsn] = lsn
	}
	return
}

func (c *ReplayCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := c.ReadFrom(bbuf)
	return err
}

func (c *ReplayCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
