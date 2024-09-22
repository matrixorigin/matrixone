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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
)

const (
	PrefetchData uint16 = iota
	PrefetchMetaIdx
	ReadMetaIdx
	ReadData
)

type CkpReplayer struct {
	r          *runner
	dataF      catalog.DataFactory
	ckpEntries []*CheckpointEntry
	ckpdatas   []*logtail.CheckpointData
	closes     []func()
	emptyFile  []*CheckpointEntry

	globalCkpIdx int

	readDuration, applyDuration       time.Duration
	readCount, applyCount, totalCount int
}

func (c *CkpReplayer) Close() {
	for _, close := range c.closes {
		close()
	}
}

func (c *CkpReplayer) ReadCkpFiles() (err error) {
	r := c.r
	ctx := r.ctx

	// step1. read checkpoint meta data, output is the ckpEntries
	t0 := time.Now()
	dirs, err := r.rt.Fs.ListDir(CheckpointDir)
	if err != nil {
		return
	}
	if len(dirs) == 0 {
		return
	}
	metaFiles := make([]*MetaFile, 0)
	r.checkpointMetaFiles.Lock()
	for i, dir := range dirs {
		r.checkpointMetaFiles.files[dir.Name] = struct{}{}
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		metaFiles = append(metaFiles, &MetaFile{
			start: start,
			end:   end,
			index: i,
		})
	}
	r.checkpointMetaFiles.Unlock()
	sort.Slice(metaFiles, func(i, j int) bool {
		return metaFiles[i].end.LT(&metaFiles[j].end)
	})
	targetIdx := metaFiles[len(metaFiles)-1].index
	dir := dirs[targetIdx]
	reader, err := blockio.NewFileReader(r.rt.SID(), r.rt.Fs.Service, CheckpointDir+dir.Name)
	if err != nil {
		return
	}
	bats, closeCB, err := reader.LoadAllColumns(ctx, nil, common.CheckpointAllocator)
	if err != nil {
		return
	}
	defer func() {
		if closeCB != nil {
			closeCB()
		}
	}()
	bat := containers.NewBatch()
	defer bat.Close()
	colNames := CheckpointSchema.Attrs()
	colTypes := CheckpointSchema.Types()
	var checkpointVersion int
	// in version 1, checkpoint metadata doesn't contain 'version'.
	vecLen := len(bats[0].Vecs)
	logutil.Infof("checkpoint version: %d, list and load duration: %v", vecLen, time.Since(t0))
	if vecLen < CheckpointSchemaColumnCountV1 {
		checkpointVersion = 1
	} else if vecLen < CheckpointSchemaColumnCountV2 {
		checkpointVersion = 2
	} else {
		checkpointVersion = 3
	}
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
	c.readDuration += time.Since(t0)

	entries, maxGlobalEnd := ReplayCheckpointEntries(bat, checkpointVersion)
	c.ckpEntries = entries

	// step2. read checkpoint data, output is the ckpdatas
	datas := make([]*logtail.CheckpointData, bat.Length())

	closecbs := make([]func(), 0)
	c.totalCount = len(entries)
	readfn := func(i int, readType uint16) {
		checkpointEntry := entries[i]
		checkpointEntry.sid = r.rt.SID()
		if checkpointEntry.end.LT(&maxGlobalEnd) {
			return
		}
		var err2 error
		if readType == PrefetchData {
			if err2 = checkpointEntry.Prefetch(ctx, r.rt.Fs, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
			}
		} else if readType == PrefetchMetaIdx {
			c.readCount++
			datas[i], err = checkpointEntry.PrefetchMetaIdx(ctx, r.rt.Fs)
			if err != nil {
				return
			}
		} else if readType == ReadMetaIdx {
			err = checkpointEntry.ReadMetaIdx(ctx, r.rt.Fs, datas[i])
			if err != nil {
				return
			}
		} else {
			if err2 = checkpointEntry.Read(ctx, r.rt.Fs, datas[i]); err2 != nil {
				logutil.Warnf("read %v failed: %v", checkpointEntry.String(), err2)
				c.emptyFile = append(c.emptyFile, checkpointEntry)
			} else {
				entries[i] = checkpointEntry
				closecbs = append(closecbs, func() { datas[i].CloseWhenLoadFromCache(checkpointEntry.version) })
			}
		}
	}
	c.closes = append(c.closes, closecbs...)
	t0 = time.Now()
	for i := 0; i < bat.Length(); i++ {
		metaLoc := objectio.Location(bat.GetVectorByName(CheckpointAttr_MetaLocation).Get(i).([]byte))
		err = blockio.PrefetchMeta(r.rt.SID(), r.rt.Fs.Service, metaLoc)
		if err != nil {
			return
		}
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadMetaIdx)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, PrefetchData)
	}
	for i := 0; i < bat.Length(); i++ {
		readfn(i, ReadData)
	}
	c.ckpdatas = datas
	c.readDuration += time.Since(t0)

	// step3. Add entries to the runner
	for i := 0; i < bat.Length(); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
		if checkpointEntry.GetType() == ET_Global {
			c.globalCkpIdx = i
			r.tryAddNewGlobalCheckpointEntry(checkpointEntry)
		} else if checkpointEntry.GetType() == ET_Incremental {
			r.tryAddNewIncrementalCheckpointEntry(checkpointEntry)
		} else if checkpointEntry.GetType() == ET_Backup {
			r.tryAddNewBackupCheckpointEntry(checkpointEntry)
		}
	}
	return nil
}

// ReplayThreeTablesObjectlist replays the object list the three tables, and check the LSN and TS.
func (c *CkpReplayer) ReplayThreeTablesObjectlist() (
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
		logutil.Infof("replay checkpoint %v", maxGlobal)
		err = datas[c.globalCkpIdx].ApplyReplayTo(r.catalog, dataFactory, true)
		c.applyCount++
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
	for i := 0; i < len(entries); i++ {
		checkpointEntry := entries[i]
		if checkpointEntry == nil {
			continue
		}
		if checkpointEntry.end.LE(&maxTs) {
			continue
		}
		logutil.Infof("replay checkpoint %v", checkpointEntry)
		err = datas[i].ApplyReplayTo(r.catalog, dataFactory, true)
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
	return
}

func (c *CkpReplayer) ReplayCatalog(readTxn txnif.AsyncTxn) {
	if len(c.ckpEntries) == 0 {
		return
	}

	logutil.Info(c.r.catalog.SimplePPString(common.PPL3))
	sortFunc := func(cols []containers.Vector, pkidx int) error {
		_, err := mergesort.SortBlockColumns(cols, pkidx, c.r.rt.VectorPool.Transient)
		return err
	}
	// logutil.Infof(c.r.catalog.SimplePPString(common.PPL3))
	c.r.catalog.RelayFromSysTableObjects(c.r.ctx, readTxn, c.dataF, tables.ReadSysTableBatch, sortFunc)
}

// ReplayObjectlist replays the data part of the checkpoint.
func (c *CkpReplayer) ReplayObjectlist() (err error) {
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
		err = datas[c.globalCkpIdx].ApplyReplayTo(r.catalog, dataFactory, false)
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
		err = datas[i].ApplyReplayTo(r.catalog, dataFactory, false)
		if err != nil {
			return
		}
		if maxTs.LT(&checkpointEntry.end) {
			maxTs = checkpointEntry.end
		}
		ckpVers = append(ckpVers, checkpointEntry.version)
		ckpDatas = append(ckpDatas, datas[i])
	}
	c.applyDuration += time.Since(t0)
	r.catalog.GetUsageMemo().(*logtail.TNUsageMemo).PrepareReplay(ckpDatas, ckpVers)
	r.source.Init(maxTs)
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("checkpoint"),
		common.AnyField("apply cost", c.applyDuration),
		common.AnyField("read cost", c.readDuration),
		common.AnyField("total entry count", c.totalCount),
		common.AnyField("read entry count", c.readCount),
		common.AnyField("apply entry count", c.applyCount))
	return
}

func (r *runner) Replay(dataFactory catalog.DataFactory) *CkpReplayer {
	return &CkpReplayer{
		r:     r,
		dataF: dataFactory,
	}
}

func MergeCkpMeta(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	cnLocation, tnLocation objectio.Location,
	startTs, ts types.TS,
) (string, error) {
	dirs, err := fs.List(ctx, CheckpointDir)
	if err != nil {
		return "", err
	}
	if len(dirs) == 0 {
		return "", nil
	}
	metaFiles := make([]*MetaFile, 0)
	for i, dir := range dirs {
		start, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
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
