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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"go.uber.org/zap"
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
	dataF      catalog.DataFactory
	ckpEntries []*CheckpointEntry
	ckpdatas   []*logtail.CheckpointData
	closes     []func()
	emptyFile  []*CheckpointEntry

	globalCkpIdx int

	readDuration, applyDuration       time.Duration
	readCount, applyCount, totalCount int

	objectReplayWorker []sm.Queue
	wg                 sync.WaitGroup
	objectCountMap     map[uint64]int
}

func (c *CkpReplayer) Close() {
	for _, close := range c.closes {
		close()
	}
	for _, worker := range c.objectReplayWorker {
		worker.Stop()
	}
	for _, data := range c.ckpdatas {
		if data != nil {
			data.Close()
		}
	}
}

func (c *CkpReplayer) readCheckpointEntries() (
	allEntries []*CheckpointEntry, maxGlobalTS types.TS, err error,
) {
	var (
		now   = time.Now()
		files []fileservice.DirEntry
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

	if files, err = fileservice.SortedList(c.r.rt.Fs.ListDir(c.dir)); err != nil {
		return
	}
	if len(files) == 0 {
		return
	}

	var (
		metaEntries      = make([]MetaFile, 0)
		compactedEntries = make([]MetaFile, 0)
	)
	// classify the files into metaEntries and compactedEntries
	for _, file := range files {
		c.r.store.AddMetaFile(file.Name)
		start, end, ext := blockio.DecodeCheckpointMetadataFileName(file.Name)
		entry := MetaFile{
			start: start,
			end:   end,
			name:  file.Name,
		}
		if ext == blockio.CompactedExt {
			compactedEntries = append(compactedEntries, entry)
		} else if IsMetadataFile(file.Name) {
			metaEntries = append(metaEntries, entry)
		}
	}

	sort.Slice(metaEntries, func(i, j int) bool {
		return metaEntries[i].end.LT(&metaEntries[j].end)
	})
	sort.Slice(compactedEntries, func(i, j int) bool {
		return compactedEntries[i].end.LT(&compactedEntries[j].end)
	})

	// replay the compactedEntries
	if len(compactedEntries) > 0 {
		maxEntry := compactedEntries[len(compactedEntries)-1]
		var entries []*CheckpointEntry
		if entries, err = ReadEntriesFromMeta(
			c.r.ctx,
			c.r.rt.SID(),
			c.dir,
			maxEntry.name,
			0,
			nil,
			common.CheckpointAllocator,
			c.r.rt.Fs.Service,
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
			panic(fmt.Sprintf("invalid compacted checkpoint file %s", maxEntry.name))
		}
		c.r.store.TryAddNewCompactedCheckpointEntry(entries[0])
	}

	// always replay from the max metaEntry
	if len(metaEntries) > 0 {
		maxEntry := metaEntries[len(metaEntries)-1]
		updateGlobal := func(entry *CheckpointEntry) {
			if entry.GetType() == ET_Global {
				thisEnd := entry.GetEnd()
				if thisEnd.GT(&maxGlobalTS) {
					maxGlobalTS = thisEnd
				}
			}
		}
		if allEntries, err = ReadEntriesFromMeta(
			c.r.ctx,
			c.r.rt.SID(),
			c.dir,
			maxEntry.name,
			0,
			updateGlobal,
			common.CheckpointAllocator,
			c.r.rt.Fs.Service,
		); err != nil {
			return
		}
		for _, entry := range allEntries {
			logutil.Debug(
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

	c.ckpdatas = make([]*logtail.CheckpointData, len(c.ckpEntries))

	readfn := func(i int, readType uint16) (err error) {
		checkpointEntry := c.ckpEntries[i]
		checkpointEntry.sid = r.rt.SID()
		if checkpointEntry.end.LT(&maxGlobalEnd) {
			return
		}
		var err2 error
		if readType == PrefetchData {
			if err2 = checkpointEntry.Prefetch(ctx, r.rt.Fs, c.ckpdatas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			}
		} else if readType == PrefetchMetaIdx {
			c.readCount++
			c.ckpdatas[i], err = checkpointEntry.PrefetchMetaIdx(ctx, r.rt.Fs)
			if err != nil {
				return
			}
		} else if readType == ReadMetaIdx {
			err = checkpointEntry.ReadMetaIdx(ctx, r.rt.Fs, c.ckpdatas[i])
			if err != nil {
				return
			}
		} else {
			if err2 = checkpointEntry.Read(ctx, r.rt.Fs, c.ckpdatas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
				c.emptyFile = append(c.emptyFile, checkpointEntry)
			}
		}
		return nil
	}

	for _, entry := range c.ckpEntries {
		if err = blockio.PrefetchMeta(
			r.rt.SID(), r.rt.Fs.Service, entry.GetLocation(),
		); err != nil {
			return
		}
	}

	for i := 0; i < len(c.ckpEntries); i++ {
		if err = readfn(i, PrefetchMetaIdx); err != nil {
			return
		}
	}

	for i := 0; i < len(c.ckpEntries); i++ {
		if err = readfn(i, ReadMetaIdx); err != nil {
			return
		}
	}

	for i := 0; i < len(c.ckpEntries); i++ {
		if err = readfn(i, ReadMetaIdx); err != nil {
			return
		}
	}

	for i := 0; i < len(c.ckpEntries); i++ {
		if err = readfn(i, PrefetchData); err != nil {
			return
		}
	}

	for i := 0; i < len(c.ckpEntries); i++ {
		if err = readfn(i, ReadData); err != nil {
			return
		}
	}

	c.readDuration += time.Since(t0)

	// step3. Add entries to the runner
	for i, entry := range c.ckpEntries {
		if entry == nil {
			continue
		}
		if entry.GetType() == ET_Global {
			c.globalCkpIdx = i
			r.store.AddGCKPReplayEntry(entry)
		} else if entry.GetType() == ET_Incremental {
			r.store.TryAddNewIncrementalCheckpointEntry(entry)
		} else if entry.GetType() == ET_Backup {
			r.store.TryAddNewBackupCheckpointEntry(entry)
		}
	}
	return nil
}

// ReplayThreeTablesObjectlist replays the object list the three tables, and check the LSN and TS.
func (c *CkpReplayer) ReplayThreeTablesObjectlist(phase string) (
	maxTs types.TS,
	maxLSN uint64,
	isLSNValid bool,
	err error) {
	t0 := time.Now()
	defer func() {
		c.applyDuration += time.Since(t0)
		if maxTs.IsEmpty() {
			isLSNValid = true
		}
	}()

	if len(c.ckpEntries) == 0 {
		return
	}

	r := c.r
	ctx := c.r.ctx
	entries := c.ckpEntries
	datas := c.ckpdatas
	dataFactory := c.dataF
	maxGlobal := r.MaxGlobalCheckpoint()
	if maxGlobal != nil {
		err = datas[c.globalCkpIdx].ApplyReplayTo(c, r.catalog, dataFactory, true)
		c.applyCount++
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Replay-3-Table-From-Global",
			zap.String("phase", phase),
			zap.String("checkpoint", maxGlobal.String()),
			zap.Duration("cost", time.Since(t0)),
			zap.Error(err),
		)
		if err != nil {
			return
		}
		if maxTs.LT(&maxGlobal.end) {
			maxTs = maxGlobal.end
		}
		// for force checkpoint, ckpLSN is 0.
		if maxGlobal.ckpLSN > 0 {
			if maxGlobal.ckpLSN < maxLSN {
				panic(fmt.Sprintf("logic error, current lsn %d, incoming lsn %d", maxLSN, maxGlobal.ckpLSN))
			}
			isLSNValid = true
			maxLSN = maxGlobal.ckpLSN
		}
	}
	for _, e := range c.emptyFile {
		if e.end.GE(&maxTs) {
			return types.TS{}, 0, false,
				moerr.NewInternalErrorf(ctx,
					"read checkpoint %v failed",
					e.String())
		}
	}
	logger := logutil.Info
	for i := 0; i < len(entries); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
		if checkpointEntry.end.LE(&maxTs) {
			continue
		}
		start := time.Now()
		if err = datas[i].ApplyReplayTo(c, r.catalog, dataFactory, true); err != nil {
			logger = logutil.Error
		}
		logger(
			"Replay-3-Table-From-Incremental",
			zap.String("phase", phase),
			zap.String("checkpoint", checkpointEntry.String()),
			zap.Duration("cost", time.Since(start)),
			zap.Error(err),
		)
		c.applyCount++
		if err != nil {
			return
		}
		if maxTs.LT(&checkpointEntry.end) {
			maxTs = checkpointEntry.end
		}
		if checkpointEntry.ckpLSN != 0 {
			if checkpointEntry.ckpLSN < maxLSN {
				panic(fmt.Sprintf("logic error, current lsn %d, incoming lsn %d", maxLSN, checkpointEntry.ckpLSN))
			}
			isLSNValid = true
			maxLSN = checkpointEntry.ckpLSN
		}
		// For version 7, all ckp LSN of force ickp is 0.
		// In db.ForceIncrementalCheckpoint，it truncates.
		// If the last ckp is force ickp，LSN check should be disable.
		if checkpointEntry.ckpLSN == 0 {
			isLSNValid = false
		}
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
	c.r.catalog.RelayFromSysTableObjects(
		c.r.ctx,
		readTxn,
		c.dataF,
		tables.ReadSysTableBatch,
		sortFunc,
	)
	// logutil.Info(c.r.catalog.SimplePPString(common.PPL0))
	return
}

// ReplayObjectlist replays the data part of the checkpoint.
func (c *CkpReplayer) ReplayObjectlist(phase string) (err error) {
	if len(c.ckpEntries) == 0 {
		return
	}
	t0 := time.Now()
	r := c.r
	entries := c.ckpEntries
	datas := c.ckpdatas
	dataFactory := c.dataF
	maxTs := types.TS{}
	var ckpVers []uint32
	var ckpDatas []*logtail.CheckpointData
	if maxGlobal := r.MaxGlobalCheckpoint(); maxGlobal != nil {
		err = datas[c.globalCkpIdx].ApplyReplayTo(c, r.catalog, dataFactory, false)
		if err != nil {
			return
		}
		maxTs = maxGlobal.end
		ckpVers = append(ckpVers, maxGlobal.version)
		ckpDatas = append(ckpDatas, datas[c.globalCkpIdx])
	}
	for i := 0; i < len(entries); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
		if checkpointEntry.end.LE(&maxTs) {
			continue
		}
		err = datas[i].ApplyReplayTo(
			c,
			r.catalog,
			dataFactory,
			false)
		if err != nil {
			return
		}
		if maxTs.LT(&checkpointEntry.end) {
			maxTs = checkpointEntry.end
		}
		ckpVers = append(ckpVers, checkpointEntry.version)
		ckpDatas = append(ckpDatas, datas[i])
	}
	c.wg.Wait()
	c.applyDuration += time.Since(t0)
	r.catalog.GetUsageMemo().(*logtail.TNUsageMemo).PrepareReplay(ckpDatas, ckpVers)
	r.source.Init(maxTs)
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
		zap.Uint64("max table tid", maxTableID),
		zap.Int("object count (create count + delete count)", maxObjectCount),
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

func (r *runner) BuildReplayer(
	dir string,
	dataFactory catalog.DataFactory,
) *CkpReplayer {
	replayer := &CkpReplayer{
		r:              r,
		dir:            dir,
		dataF:          dataFactory,
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
	dirs, err := fileservice.SortedList(fs.List(ctx, CheckpointDir))
	if err != nil {
		return "", err
	}
	if len(dirs) == 0 {
		return "", nil
	}
	metaFiles := make([]*MetaFile, 0)
	for i, dir := range dirs {
		start, end, _ := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &MetaFile{
			start: start,
			end:   end,
			index: i,
		})
	}
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.LT(&metaFiles[j].end)
	})
	targetIdx := metaFiles[len(metaFiles)-1].index
	dir := dirs[targetIdx]
	reader, err := blockio.NewFileReader(sid, fs, CheckpointDir+dir.Name)
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
	name := blockio.EncodeCheckpointMetadataFileName(CheckpointDir, PrefixMetadata, startTs, ts)
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
