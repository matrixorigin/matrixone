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

	truncatedLogserviceLsn uint64

	minDriverLsn uint64
	maxDriverLsn uint64

	driverLsnLogserviceLsnMap map[uint64]uint64 //start-lsn

	replayHandle  driver.ApplyHandle
	replayedLsn   uint64
	inited        bool
	safeLsn       uint64
	nextToReadLsn uint64
	d             *LogServiceDriver
	appended      []uint64

	recordChan        chan *entry.Entry
	lastEntry         *entry.Entry
	firstEntryIsFound atomic.Bool
	readEnd           bool

	applyDuration time.Duration
	readCount     int
	internalCount int
	applyCount    int

	wg sync.WaitGroup

	readFn func() bool
}

func newReplayer(h driver.ApplyHandle, readmaxsize int, d *LogServiceDriver) *replayer {
	truncated := d.getLogserviceTruncate()
	logutil.Info("Wal-Replay-Trace-Get-Truncated", zap.Uint64("psn", truncated))
	r := &replayer{
		minDriverLsn:              math.MaxUint64,
		driverLsnLogserviceLsnMap: make(map[uint64]uint64),
		replayHandle:              h,
		readMaxSize:               readmaxsize,
		nextToReadLsn:             truncated + 1,
		replayedLsn:               math.MaxUint64,
		d:                         d,
		appended:                  make([]uint64, 0),
		recordChan:                make(chan *entry.Entry, 100),
		wg:                        sync.WaitGroup{},
		truncatedLogserviceLsn:    truncated,
	}
	r.readFn = r.readRecords
	return r
}

func (r *replayer) replay() {
	var err error
	r.wg.Add(1)
	go r.replayRecords()
	for !r.readRecords() {
		for r.replayedLsn < r.safeLsn {
			err := r.replayLogserviceEntry(r.replayedLsn + 1)
			if err != nil {
				panic(err)
			}
		}
	}
	r.readEnd = true
	err = r.replayLogserviceEntry(r.replayedLsn + 1)
	for err != ErrAllRecordsRead {
		err = r.replayLogserviceEntry(r.replayedLsn + 1)

	}
	r.d.lsns = make([]uint64, 0)
	r.recordChan <- entry.NewEndEntry()
	r.wg.Wait()
	close(r.recordChan)
}

func (r *replayer) readRecords() (readEnd bool) {
	nextLsn, safeLsn := r.d.readFromLogServiceInReplay(r.nextToReadLsn, r.readMaxSize, func(lsn uint64, record *recordEntry) {
		r.readCount++
		if record.Meta.metaType == TReplay {
			r.internalCount++
			logutil.Info("Wal-Replay-Trace-Replay-Skip-Entry-By-CMD",
				zap.Any("drlsn-psn", record.cmd.skipLsns))
			r.removeEntries(record.cmd.skipLsns)
			return
		}
		drlsn := record.GetMinLsn()
		r.driverLsnLogserviceLsnMap[drlsn] = lsn
		if drlsn-1 < r.replayedLsn {
			if r.inited {
				panic("logic err")
			}
			r.replayedLsn = drlsn - 1
		}
	})
	if safeLsn > r.safeLsn {
		r.safeLsn = safeLsn
	}
	if nextLsn == r.nextToReadLsn {
		return true
	}
	r.nextToReadLsn = nextLsn
	return false
}
func (r *replayer) removeEntries(skipMap map[uint64]uint64) {
	for lsn := range skipMap {
		if _, ok := r.driverLsnLogserviceLsnMap[lsn]; !ok {
			panic(fmt.Sprintf("lsn %d not existed, map is %v", lsn, r.driverLsnLogserviceLsnMap))
		}
		delete(r.driverLsnLogserviceLsnMap, lsn)
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
					zap.Uint64("last-drlsn", r.lastEntry.Lsn))
			}
			break
		}
		t0 := time.Now()
		state := r.replayHandle(e)
		if state == driver.RE_Nomal {
			if !r.firstEntryIsFound.Load() {
				logutil.Info("Wal-Replay-Trace-Find-First-Entry", zap.Uint64("drlsn", e.Lsn))
			}
			r.firstEntryIsFound.Store(true)
		}
		e.DoneWithErr(nil)
		e.Entry.Free()
		r.applyDuration += time.Since(t0)
	}
}

func (r *replayer) replayLogserviceEntry(lsn uint64) error {
	logserviceLsn, ok := r.driverLsnLogserviceLsnMap[lsn]
	if !ok {
		skipFn := func() {
			if len(r.driverLsnLogserviceLsnMap) != 0 {
				r.AppendSkipCmd(r.driverLsnLogserviceLsnMap)
			}
		}
		firstEntryIsFound := r.firstEntryIsFound.Load()
		if !firstEntryIsFound && r.lastEntry != nil {
			r.lastEntry.WaitDone()
			firstEntryIsFound = r.firstEntryIsFound.Load()
		}
		safe := lsn <= r.safeLsn
		if safe {
			if !firstEntryIsFound {
				logutil.Info("Wal-Replay-Trace-Skip-Entry", zap.Uint64("drlsn", lsn))
				r.minDriverLsn = lsn + 1
				r.replayedLsn++
				return nil
			} else {
				panic(fmt.Sprintf("logic error, lsn %d is missing, safe lsn %d, map %v",
					lsn, r.safeLsn, r.driverLsnLogserviceLsnMap))
			}
		} else {
			if !r.readEnd {
				panic(fmt.Sprintf("logic error, safe lsn %d, lsn %d", r.safeLsn, lsn))
			}
			skipFn()
			return ErrAllRecordsRead
		}
	}
	if !r.inited {
		logutil.Info("Wal-Replay-Trace-Replay-First-Entry", zap.Uint64("drlsn", lsn))
	}
	record, err := r.d.readFromCache(logserviceLsn)
	if err == ErrAllRecordsRead {
		return err
	}
	if err != nil {
		panic(err)
	}
	r.applyCount++
	intervals := record.replay(r)
	r.d.onReplayRecordEntry(logserviceLsn, intervals)
	r.onReplayDriverLsn(intervals.GetMax())
	r.onReplayDriverLsn(intervals.GetMin())
	r.d.dropRecordByLsn(logserviceLsn)
	r.replayedLsn = record.GetMaxLsn()
	r.inited = true
	delete(r.driverLsnLogserviceLsnMap, lsn)
	return nil
}

func (r *replayer) AppendSkipCmd(skipMap map[uint64]uint64) {
	if len(skipMap) > r.d.config.ClientMaxCount {
		panic(fmt.Sprintf("logic error, skip %d entries, client max count is %v, skip map is %v",
			len(skipMap), r.d.config.ClientMaxCount, skipMap))
	}
	logutil.Info("Wal-Replay-Trace-Skip-Entries", zap.Any("drlsn-psn", skipMap))
	cmd := NewReplayCmd(skipMap)
	recordEntry := newRecordEntry()
	recordEntry.Meta.metaType = TReplay
	recordEntry.cmd = cmd
	size := recordEntry.prepareRecord()
	c, lsn := r.d.getClient()
	r.appended = append(r.appended, lsn)
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
func (r *replayer) onReplayDriverLsn(lsn uint64) {
	if lsn == 0 {
		return
	}
	if lsn < r.minDriverLsn {
		r.minDriverLsn = lsn
	}
	if lsn > r.maxDriverLsn {
		r.maxDriverLsn = lsn
	}
}

type ReplayCmd struct {
	skipLsns map[uint64]uint64
}

func NewReplayCmd(skipLsns map[uint64]uint64) *ReplayCmd {
	return &ReplayCmd{
		skipLsns: skipLsns,
	}
}
func NewEmptyReplayCmd() *ReplayCmd {
	return &ReplayCmd{
		skipLsns: make(map[uint64]uint64),
	}
}

func (c *ReplayCmd) WriteTo(w io.Writer) (n int64, err error) {
	length := uint16(len(c.skipLsns))
	if _, err = w.Write(types.EncodeUint16(&length)); err != nil {
		return
	}
	n += 2
	for drlsn, logserviceLsn := range c.skipLsns {
		if _, err = w.Write(types.EncodeUint64(&drlsn)); err != nil {
			return
		}
		n += 8
		if _, err = w.Write(types.EncodeUint64(&logserviceLsn)); err != nil {
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
		drlsn := uint64(0)
		lsn := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&drlsn)); err != nil {
			return
		}
		n += 8
		if _, err = r.Read(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		c.skipLsns[drlsn] = lsn
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
