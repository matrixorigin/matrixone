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

package checkpoint

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"go.uber.org/zap"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
)

const (
	PrefetchData uint16 = iota
	PrefetchMetaIdx
	ReadMetaIdx
	ReadData
)

const (
	DefaultObjectReplayWorkerCount = 10
)

type CkpReplayer struct {
	dir        string
	r          *runner
	ckpEntries []*CheckpointEntry
	ckpReader  *CheckpointReader

	readDuration, applyDuration       time.Duration
	readCount, applyCount, totalCount int

	objectReplayWorker []sm.Queue
	wg                 sync.WaitGroup
	objectCountMap     map[uint64]int
}

func (c *CkpReplayer) Close() {
	for _, worker := range c.objectReplayWorker {
		worker.Stop()
	}
}

func (c *CkpReplayer) readCheckpointEntries() (
	allEntries []*CheckpointEntry, maxGlobalTS types.TS, err error,
) {
	var (
		now   = time.Now()
		files []ioutil.TSRangeFile
	)

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Read-CKP-From-Meta",
			zap.String("cost", time.Since(now).String()),
			zap.Int("count", len(allEntries)),
			zap.Error(err),
		)
	}()

	if files, err = ioutil.ListTSRangeFiles(
		c.r.ctx,
		c.dir,
		c.r.rt.Fs,
	); err != nil {
		return
	}

	if len(files) == 0 {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].GetEnd().LT(files[j].GetEnd())
	})

	var (
		metaFiles        = make([]ioutil.TSRangeFile, 0)
		compactedEntries = make([]ioutil.TSRangeFile, 0)
	)

	// classify the files into metaFiles and compactedEntries
	for _, file := range files {
		c.r.store.AddMetaFile(file.GetName())
		if file.IsCompactExt() {
			compactedEntries = append(compactedEntries, file)
		} else if file.IsMetadataFile() {
			metaFiles = append(metaFiles, file)
		}
	}

	// replay the compactedEntries
	if len(compactedEntries) > 0 {
		maxEntry := compactedEntries[len(compactedEntries)-1]
		var entries []*CheckpointEntry
		if entries, err = ReadEntriesFromMeta(
			c.r.ctx,
			c.r.rt.SID(),
			c.dir,
			maxEntry.GetName(),
			0,
			nil,
			common.CheckpointAllocator,
			c.r.rt.Fs,
		); err != nil {
			return
		}
		for _, entry := range entries {
			logutil.Info(
				"Read-CKP-COMPACT",
				zap.String("entry", entry.String()),
			)
		}
		if len(entries) != 1 {
			panic(fmt.Sprintf("invalid compacted checkpoint file %s", maxEntry.GetName()))
		}
		if err = c.r.ReplayCKPEntry(entries[0]); err != nil {
			return
		}
	}

	// always replay from the max metaEntry
	if len(metaFiles) > 0 {
		var maxGlobalFile ioutil.TSRangeFile
		for i := len(metaFiles) - 1; i >= 0; i-- {
			start := metaFiles[i].GetStart()
			if start.IsEmpty() {
				maxGlobalFile = metaFiles[i]
				break
			}
		}

		maxFile := metaFiles[len(metaFiles)-1]

		toReadFiles := []ioutil.TSRangeFile{maxFile}
		if maxGlobalFile.GetName() != maxFile.GetName() {
			toReadFiles = append(toReadFiles, maxGlobalFile)
		}

		updateGlobal := func(entry *CheckpointEntry) {
			if entry.GetType() == ET_Global {
				thisEnd := entry.GetEnd()
				if thisEnd.GT(&maxGlobalTS) {
					maxGlobalTS = thisEnd
				}
			}
		}
		var loopEntries []*CheckpointEntry
		for _, oneFile := range toReadFiles {
			if loopEntries, err = ReadEntriesFromMeta(
				c.r.ctx,
				c.r.rt.SID(),
				c.dir,
				oneFile.GetName(),
				0,
				updateGlobal,
				common.CheckpointAllocator,
				c.r.rt.Fs,
			); err != nil {
				return
			}
			if len(allEntries) == 0 {
				allEntries = loopEntries
			} else {
				allEntries = append(allEntries, loopEntries...)
			}
		}
		allEntries = compute.SortAndDedup(
			allEntries,
			func(lh, rh **CheckpointEntry) bool {
				lhEnd, rhEnd := (*lh).GetEnd(), (*rh).GetEnd()
				return lhEnd.LT(&rhEnd)
			},
			func(lh, rh **CheckpointEntry) bool {
				lhStart, rhStart := (*lh).GetStart(), (*rh).GetStart()
				if !lhStart.EQ(&rhStart) {
					return false
				}
				lhEnd, rhEnd := (*lh).GetEnd(), (*rh).GetEnd()
				return lhEnd.EQ(&rhEnd)
			},
		)
		for _, entry := range allEntries {
			entry.sid = c.r.rt.SID()
			logutil.Info(
				"Read-CKP-META",
				zap.String("entry", entry.String()),
			)
		}
	}
	return
}

func (c *CkpReplayer) ReadCkpFiles() (err error) {
	var (
		t0           = time.Now()
		r            = c.r
		ctx          = r.ctx
		maxGlobalEnd types.TS
	)

	if c.ckpEntries, maxGlobalEnd, err = c.readCheckpointEntries(); err != nil {
		return
	}
	c.totalCount = len(c.ckpEntries)

	// step2. read checkpoint data, output is the ckpdatas

	c.ckpReader = NewCheckpointReader(ctx, c.ckpEntries, maxGlobalEnd, common.CheckpointAllocator, r.rt.Fs)

	if err = c.ckpReader.Prepare(ctx); err != nil {
		return
	}
	c.readDuration += time.Since(t0)

	// step3. Add entries to the runner
	for i, entry := range c.ckpEntries {
		if entry == nil {
			continue
		}
		if entry.IsGlobal() {
			c.ckpReader.globalIdx = i
		}
		if err = r.ReplayCKPEntry(entry); err != nil {
			return
		}
	}

	if len(c.ckpEntries) > 0 {
		select {
		case <-c.r.ctx.Done():
			err = context.Cause(c.r.ctx)
			return
		case <-c.ckpEntries[len(c.ckpEntries)-1].Wait():
		}
	}

	return
}

func (c *CkpReplayer) replayObjectList(
	ctx context.Context,
	forSys bool,
) (maxTS types.TS, maxLsn uint64, err error) {
	tmpBatch := ckputil.MakeDataScanTableIDBatch()
	defer tmpBatch.Clean(common.CheckpointAllocator)
	defer c.ckpReader.Reset(ctx)
	for {
		tmpBatch.CleanOnlyData()
		var end bool
		if end, err = c.ckpReader.Read(ctx, tmpBatch); err != nil {
			return
		}
		if end {
			maxTS = c.ckpReader.maxTS
			maxLsn = c.ckpReader.maxLSN
			return
		}
		dbids := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[1])
		tableIds := vector.MustFixedColNoTypeCheck[uint64](tmpBatch.Vecs[2])
		objectTypes := vector.MustFixedColNoTypeCheck[int8](tmpBatch.Vecs[3])
		objectStatsVec := tmpBatch.Vecs[4]
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[5])
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](tmpBatch.Vecs[6])
		for i, rows := 0, tmpBatch.RowCount(); i < rows; i++ {
			if forSys == pkgcatalog.IsSystemTable(tableIds[i]) {
				c.r.catalog.OnReplayObjectBatch_V2(
					dbids[i],
					tableIds[i],
					objectTypes[i],
					objectio.ObjectStats(objectStatsVec.GetBytesAt(i)),
					createTSs[i],
					deleteTSs[i],
				)
			}
		}
	}
}

// ReplayThreeTablesObjectlist replays the object list the three tables, and check the LSN and TS.
func (c *CkpReplayer) ReplayThreeTablesObjectlist(phase string) (
	maxTs types.TS,
	maxLSN uint64,
	err error,
) {
	t0 := time.Now()
	defer func() {
		c.applyDuration += time.Since(t0)
	}()

	if len(c.ckpEntries) == 0 {
		return
	}

	ctx := c.r.ctx

	maxTs, maxLSN, err = c.replayObjectList(ctx, true)
	logger := logutil.Info
	if err != nil {
		logger = logutil.Error
	}
	logger(
		"Replay-3-Table-From-Global",
		zap.String("phase", phase),
		zap.Duration("cost", time.Since(t0)),
		zap.Error(err),
	)
	if err != nil {
		return
	}

	c.wg.Wait()
	return
}

func (c *CkpReplayer) ReplayCatalog(
	readTxn txnif.AsyncTxn,
	phase string,
) (err error) {
	start := time.Now()

	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Replay-Catalog",
			zap.String("phase", phase),
			zap.Duration("cost", time.Since(start)),
			zap.Error(err),
		)
	}()

	if len(c.ckpEntries) == 0 {
		return
	}

	// logutil.Info(c.r.catalog.SimplePPString(common.PPL3))
	sortFunc := func(cols []containers.Vector, pkidx int) (err2 error) {
		_, err2 = mergesort.SortBlockColumns(cols, pkidx, c.r.rt.VectorPool.Transient)
		return
	}
	closeFn := c.r.catalog.RelayFromSysTableObjects(
		c.r.ctx,
		readTxn,
		tables.ReadSysTableBatch,
		sortFunc,
		c,
	)
	c.wg.Wait()
	for _, fn := range closeFn {
		fn()
	}
	c.resetObjectCountMap()
	// logutil.Info(c.r.catalog.SimplePPString(common.PPL0))
	return
}

// ReplayObjectlist replays the data part of the checkpoint.
func (c *CkpReplayer) ReplayObjectlist(ctx context.Context, phase string) (err error) {
	if len(c.ckpEntries) == 0 {
		return
	}
	t0 := time.Now()
	r := c.r

	var maxTS types.TS
	if maxTS, _, err = c.replayObjectList(ctx, false); err != nil {
		logutil.Error(
			"Replay-Checkpoints",
			zap.String("phase", phase),
			zap.Duration("cost", time.Since(t0)),
			zap.Error(err),
		)
		return
	}

	c.wg.Wait()
	c.applyDuration += time.Since(t0)
	r.source.Init(maxTS)
	maxTableID, maxObjectCount := uint64(0), 0
	for tid, count := range c.objectCountMap {
		if count > maxObjectCount {
			maxTableID = tid
			maxObjectCount = count
		}
	}
	logutil.Info(
		"Replay-Checkpoints",
		zap.String("phase", phase),
		zap.Uint64("max-table-id", maxTableID),
		zap.Int("object-count", maxObjectCount),
		zap.Duration("apply-cost", c.applyDuration),
		zap.Duration("read-cost", c.readDuration),
		zap.Int("apply-count", c.applyCount),
		zap.Int("total-count", c.totalCount),
		zap.Int("read-count", c.readCount),
	)
	return
}

func (c *CkpReplayer) Submit(tid uint64, replayFn func()) {
	c.wg.Add(1)
	workerOffset := tid % uint64(len(c.objectReplayWorker))
	c.objectCountMap[tid] = c.objectCountMap[tid] + 1
	c.objectReplayWorker[workerOffset].Enqueue(replayFn)
}

func (c *CkpReplayer) resetObjectCountMap() {
	c.objectCountMap = map[uint64]int{}
}

func (r *runner) BuildReplayer(
	dir string,
) *CkpReplayer {
	replayer := &CkpReplayer{
		r:              r,
		dir:            dir,
		objectCountMap: make(map[uint64]int),
	}
	objectWorker := make([]sm.Queue, DefaultObjectReplayWorkerCount)
	for i := 0; i < DefaultObjectReplayWorkerCount; i++ {
		objectWorker[i] = sm.NewSafeQueue(10000, 100, func(items ...any) {
			for _, item := range items {
				fn := item.(func())
				fn()
				replayer.wg.Done()
			}
		})
		objectWorker[i].Start()
	}
	replayer.objectReplayWorker = objectWorker
	return replayer
}

func MergeCkpMeta(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	cnLocation, tnLocation objectio.Location,
	startTs, ts types.TS,
) (string, error) {
	var (
		metaFiles []ioutil.TSRangeFile
		err       error
	)
	if metaFiles, err = ckputil.ListCKPMetaFiles(
		ctx, fs,
	); err != nil {
		return "", err
	}

	if len(metaFiles) == 0 {
		return "", nil
	}

	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].GetEnd().LT(metaFiles[j].GetEnd())
	})

	maxFile := metaFiles[len(metaFiles)-1]

	reader, err := ioutil.NewFileReader(fs, maxFile.GetCKPFullName())
	if err != nil {
		return "", err
	}
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.CheckpointAllocator)
	if err != nil {
		return "", err
	}
	defer func() {
		for i := range bats {
			for j := range bats[i].Vecs {
				bats[i].Vecs[j].Free(common.CheckpointAllocator)
			}
		}
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	for i := range bats[0].Vecs {
		if len(bats) == 0 {
			continue
		}
		var vec containers.Vector
		if bats[0].Vecs[i].Length() == 0 {
			vec = containers.MakeVector(colTypes[i], common.CheckpointAllocator)
		} else {
			vec = containers.ToTNVector(bats[0].Vecs[i], common.CheckpointAllocator)
		}
		bat.AddVector(colNames[i], vec)
	}
	last := bat.Vecs[0].Length() - 1
	bat.GetVectorByName(CheckpointAttr_StartTS).Append(startTs, false)
	bat.GetVectorByName(CheckpointAttr_EndTS).Append(ts, false)
	bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(cnLocation), false)
	bat.GetVectorByName(CheckpointAttr_EntryType).Append(true, false)
	bat.GetVectorByName(CheckpointAttr_Version).Append(bat.GetVectorByName(CheckpointAttr_Version).Get(last), false)
	bat.GetVectorByName(CheckpointAttr_AllLocations).Append([]byte(tnLocation), false)
	bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Append(bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Get(last), false)
	bat.GetVectorByName(CheckpointAttr_TruncateLSN).Append(bat.GetVectorByName(CheckpointAttr_TruncateLSN).Get(last), false)
	bat.GetVectorByName(CheckpointAttr_Type).Append(int8(ET_Backup), false)
	name := ioutil.EncodeCKPMetadataFullName(startTs, ts)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCheckpoint, name, fs)
	if err != nil {
		return "", err
	}
	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return "", err
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	return name, err
}

func ReplayCheckpointEntries(bat *containers.Batch, checkpointVersion int) (entries []*CheckpointEntry, maxGlobalEnd types.TS) {
	entries = make([]*CheckpointEntry, bat.Length())
	for i := 0; i < bat.Length(); i++ {
		start := bat.GetVectorByName(CheckpointAttr_StartTS).Get(i).(types.TS)
		end := bat.GetVectorByName(CheckpointAttr_EndTS).Get(i).(types.TS)
		cnLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))
		typ := ET_Global
		if checkpointVersion > 2 {
			typ = EntryType(bat.GetVectorByName(CheckpointAttr_Type).Get(i).(int8))
		} else {
			isIncremental := bat.GetVectorByName(CheckpointAttr_EntryType).Get(i).(bool)
			if isIncremental {
				typ = ET_Incremental
			}
		}
		version := bat.GetVectorByName(CheckpointAttr_Version).Get(i).(uint32)
		tnLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_AllLocations).Get(i).([]byte))
		var ckpLSN, truncateLSN uint64
		ckpLSN = bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Get(i).(uint64)
		truncateLSN = bat.GetVectorByName(CheckpointAttr_TruncateLSN).Get(i).(uint64)
		checkpointEntry := &CheckpointEntry{
			start:       start,
			end:         end,
			cnLocation:  cnLoc,
			tnLocation:  tnLoc,
			state:       ST_Finished,
			entryType:   typ,
			version:     version,
			ckpLSN:      ckpLSN,
			truncateLSN: truncateLSN,
			doneC:       make(chan struct{}),
		}
		entries[i] = checkpointEntry
		if typ == ET_Global {
			if end.GT(&maxGlobalEnd) {
				maxGlobalEnd = end
			}
		}
	}
	return
}

type CheckpointReader struct {
	checkpointEntries []*CheckpointEntry
	maxGlobalEnd      types.TS

	readers    []*logtail.CKPReader
	currentIdx int
	globalIdx  int
	globalDone bool

	mp *mpool.MPool
	fs fileservice.FileService

	maxTS  types.TS
	maxLSN uint64
}

func NewCheckpointReader(
	ctx context.Context,
	checkpointEntries []*CheckpointEntry,
	maxGlobalEnd types.TS,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *CheckpointReader {
	return &CheckpointReader{
		checkpointEntries: checkpointEntries,
		maxGlobalEnd:      maxGlobalEnd,
		readers:           make([]*logtail.CKPReader, len(checkpointEntries)),
		mp:                mp,
		fs:                fs,
		globalIdx:         -1,
	}
}

func (reader *CheckpointReader) Prepare(ctx context.Context) (err error) {
	for i, entry := range reader.checkpointEntries {
		if entry.end.LT(&reader.maxGlobalEnd) {
			continue
		}
		ioutil.Prefetch(entry.sid, reader.fs, entry.GetLocation())
		reader.readers[i] = logtail.NewCKPReader(
			entry.version,
			entry.GetLocation(),
			reader.mp,
			reader.fs,
		)
	}

	for i, entry := range reader.checkpointEntries {
		if entry.end.LT(&reader.maxGlobalEnd) {
			continue
		}
		if err = reader.readers[i].ReadMeta(ctx); err != nil {
			return
		}
		reader.readers[i].PrefetchData(entry.sid)
	}
	return
}

func (reader *CheckpointReader) Read(
	ctx context.Context,
	bat *batch.Batch,
) (end bool, err error) {
	if !reader.globalDone && reader.globalIdx != -1 {
		ckpEntry := reader.checkpointEntries[reader.globalIdx]
		if end, err = reader.readers[reader.globalIdx].Read(
			ctx,
			bat,
			reader.mp,
		); err != nil {
			logutil.Error(
				"Read Checkpoint Failed",
				zap.String("checkpoint", ckpEntry.String()),
				zap.Error(err),
			)
			return
		}
		if !end {
			return
		} else {
			logutil.Info(
				"Read Checkpoint",
				zap.String("checkpoint", ckpEntry.String()),
			)
			reader.globalDone = true
			if reader.maxTS.LT(&ckpEntry.end) {
				reader.maxTS = ckpEntry.end
			}
			// for force checkpoint, ckp LSN is 0.
			if ckpEntry.ckpLSN > 0 {
				if ckpEntry.ckpLSN < reader.maxLSN {
					reader.maxLSN = ckpEntry.ckpLSN
				}
			}
			if ckpEntry.ckpLSN < reader.maxLSN {
				panic(fmt.Sprintf("logic error, current lsn %d, incoming lsn %d", reader.maxLSN, ckpEntry.ckpLSN))
			}
			reader.maxLSN = ckpEntry.ckpLSN
		}
	}
	for {
		if reader.currentIdx >= len(reader.readers) {
			return true, nil
		}
		if reader.checkpointEntries[reader.currentIdx] == nil {
			reader.currentIdx++
			continue
		}
		if reader.checkpointEntries[reader.currentIdx].end.LT(&reader.maxGlobalEnd) {
			reader.currentIdx++
			continue
		}
		ckpEntry := reader.checkpointEntries[reader.currentIdx]
		if end, err = reader.readers[reader.currentIdx].Read(
			ctx,
			bat,
			reader.mp,
		); err != nil {
			logutil.Error(
				"Read Checkpoint Failed",
				zap.String("checkpoint", ckpEntry.String()),
				zap.Error(err),
			)
			return
		}
		if !end {
			return
		}
		logutil.Info(
			"Read Checkpoint",
			zap.String("checkpoint", ckpEntry.String()),
		)
		if reader.maxTS.LT(&ckpEntry.end) {
			reader.maxTS = ckpEntry.end
		}
		// for backup checkpoint, ckp LSN is 0.
		if ckpEntry.ckpLSN != 0 {
			if ckpEntry.ckpLSN < reader.maxLSN {
				panic(fmt.Sprintf("logic error, current lsn %d, incoming lsn %d", reader.maxLSN, ckpEntry.ckpLSN))
			}
			reader.maxLSN = ckpEntry.ckpLSN
		}
		reader.currentIdx++
	}
}

func (reader *CheckpointReader) Reset(ctx context.Context) {
	for i, entry := range reader.checkpointEntries {
		if entry.end.LT(&reader.maxGlobalEnd) {
			continue
		}
		reader.readers[i].Reset(ctx)
	}
	reader.currentIdx = 0
	reader.globalDone = false
	reader.maxLSN = 0
	reader.maxTS = types.TS{}
}
