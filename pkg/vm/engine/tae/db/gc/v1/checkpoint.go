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

package v1

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"go.uber.org/zap"
)

type checkpointCleaner struct {
	fs  *objectio.ObjectFS
	ctx context.Context

	// ckpClient is used to get the instance of the specified checkpoint
	ckpClient checkpoint.RunnerReader

	// maxConsumed is to mark which checkpoint the current DiskCleaner has processed,
	// through which you can get the next checkpoint to be processed
	maxConsumed atomic.Pointer[checkpoint.CheckpointEntry]

	// minMerged is to mark at which checkpoint the full
	// GCTable in the current DiskCleaner is generated，
	// UT case needs to use
	minMerged atomic.Pointer[checkpoint.CheckpointEntry]

	maxCompared atomic.Pointer[checkpoint.CheckpointEntry]

	ckpStage atomic.Pointer[types.TS]
	ckpGC    atomic.Pointer[types.TS]

	// minMergeCount is the configuration of the merge GC metadata file.
	// When the GC file is greater than or equal to minMergeCount,
	// the merge GC metadata file will be triggered and the expired file will be deleted.
	minMergeCount struct {
		sync.RWMutex
		count int
	}

	// inputs is to record the currently valid GCTable
	inputs struct {
		sync.RWMutex
		tables []*GCTable
	}

	// outputs is a list of files that have been deleted
	outputs struct {
		sync.RWMutex
		files []string
	}

	// checker is to check whether the checkpoint can be consumed
	checker struct {
		sync.RWMutex
		extras map[string]func(item any) bool
	}

	// delWorker is a worker that deletes s3‘s objects or local
	// files, and only one worker will run
	delWorker *GCWorker

	disableGC bool

	// checkGC is to check the correctness of GC
	checkGC bool

	option struct {
		sync.RWMutex
		enableGC bool
	}

	snapshotMeta *logtail.SnapshotMeta

	mPool *mpool.MPool

	sid string
}

func NewCheckpointCleaner(
	ctx context.Context,
	sid string,
	fs *objectio.ObjectFS,
	ckpClient checkpoint.RunnerReader,
	disableGC bool,
) Cleaner {
	cleaner := &checkpointCleaner{
		ctx:       ctx,
		sid:       sid,
		fs:        fs,
		ckpClient: ckpClient,
		disableGC: disableGC,
	}
	cleaner.delWorker = NewGCWorker(fs, cleaner)
	cleaner.minMergeCount.count = MinMergeCount
	cleaner.snapshotMeta = logtail.NewSnapshotMeta()
	cleaner.option.enableGC = true
	cleaner.mPool = common.DebugAllocator
	cleaner.checker.extras = make(map[string]func(item any) bool)
	return cleaner
}

func (c *checkpointCleaner) Stop() {
}

func (c *checkpointCleaner) GetMPool() *mpool.MPool {
	return c.mPool
}

func (c *checkpointCleaner) SetTid(tid uint64) {
	c.snapshotMeta.Lock()
	defer c.snapshotMeta.Unlock()
	c.snapshotMeta.SetTid(tid)
}

func (c *checkpointCleaner) EnableGCForTest() {
	c.option.Lock()
	defer c.option.Unlock()
	c.option.enableGC = true
}

func (c *checkpointCleaner) DisableGCForTest() {
	c.option.Lock()
	defer c.option.Unlock()
	c.option.enableGC = false
}

func (c *checkpointCleaner) isEnableGC() bool {
	c.option.Lock()
	defer c.option.Unlock()
	return c.option.enableGC
}

func (c *checkpointCleaner) SetCheckGC(enable bool) {
	c.checkGC = enable
}

func (c *checkpointCleaner) isEnableCheckGC() bool {
	return c.checkGC
}

func (c *checkpointCleaner) Replay() error {
	dirs, err := c.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}
	minMergedStart := types.TS{}
	minMergedEnd := types.TS{}
	maxConsumedStart := types.TS{}
	maxConsumedEnd := types.TS{}
	maxSnapEnd := types.TS{}
	maxAcctEnd := types.TS{}
	var fullGCFile fileservice.DirEntry
	// Get effective minMerged
	var snapFile, acctFile string
	for _, dir := range dirs {
		start, end, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.GCFullExt {
			if minMergedStart.IsEmpty() || minMergedStart.Less(&start) {
				minMergedStart = start
				minMergedEnd = end
				maxConsumedStart = start
				maxConsumedEnd = end
				fullGCFile = dir
			}
		}
		if ext == blockio.SnapshotExt && maxSnapEnd.Less(&end) {
			maxSnapEnd = end
			snapFile = dir.Name
		}
		if ext == blockio.AcctExt && maxAcctEnd.Less(&end) {
			maxAcctEnd = end
			acctFile = dir.Name
		}
	}
	readDirs := make([]fileservice.DirEntry, 0)
	if !minMergedStart.IsEmpty() {
		readDirs = append(readDirs, fullGCFile)
	}
	for _, dir := range dirs {
		start, end, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.GCFullExt || ext == blockio.SnapshotExt || ext == blockio.AcctExt {
			continue
		}
		if (maxConsumedStart.IsEmpty() || maxConsumedStart.Less(&end)) &&
			minMergedEnd.Less(&end) {
			maxConsumedStart = start
			maxConsumedEnd = end
			readDirs = append(readDirs, dir)
		}
	}
	if len(readDirs) == 0 {
		return nil
	}
	for _, dir := range readDirs {
		table := NewGCTable()
		_, end, _ := blockio.DecodeGCMetadataFileName(dir.Name)
		err = table.ReadTable(c.ctx, GCMetaDir+dir.Name, dir.Size, c.fs, end)
		if err != nil {
			return err
		}
		c.updateInputs(table)
	}
	if snapFile != "" {
		err = c.snapshotMeta.ReadMeta(c.ctx, GCMetaDir+snapFile, c.fs.Service)
		if err != nil {
			return err
		}
	}
	ckp := checkpoint.NewCheckpointEntry(c.sid, maxConsumedStart, maxConsumedEnd, checkpoint.ET_Incremental)
	c.updateMaxConsumed(ckp)
	defer func() {
		// Ensure that updateMinMerged is executed last, because minMergedEnd is not empty means that the replay is completed
		// For UT
		ckp = checkpoint.NewCheckpointEntry(c.sid, minMergedStart, minMergedEnd, checkpoint.ET_Incremental)
		c.updateMinMerged(ckp)
	}()
	if acctFile != "" {
		err = c.snapshotMeta.ReadTableInfo(c.ctx, GCMetaDir+acctFile, c.fs.Service)
		if err != nil {
			return err
		}
	} else {
		//No account table information, it may be a new cluster or an upgraded cluster,
		//and the table information needs to be initialized from the checkpoint
		maxConsumed := c.maxConsumed.Load()
		isConsumedGCkp := false
		checkpointEntries, err := checkpoint.ListSnapshotCheckpoint(c.ctx, c.sid, c.fs.Service, maxConsumed.GetEnd(), 0, checkpoint.SpecifiedCheckpoint)
		if err != nil {
			logutil.Warnf("list checkpoint failed, err[%v]", err)
		}
		if len(checkpointEntries) == 0 {
			return nil
		}
		for _, entry := range checkpointEntries {
			logutil.Infof("load checkpoint: %s, consumedEnd: %s", entry.String(), maxConsumed.String())
			ckpData, err := c.collectCkpData(entry)
			if err != nil {
				logutil.Warnf("load checkpoint data failed, err[%v]", err)
				continue
			}
			if entry.GetType() == checkpoint.ET_Global {
				isConsumedGCkp = true
			}
			c.snapshotMeta.InitTableInfo(ckpData)
		}
		if !isConsumedGCkp {
			// The global checkpoint that Specified checkpoint depends on may have been GC,
			// so we need to load a latest global checkpoint
			entry := c.ckpClient.MaxGlobalCheckpoint()
			if entry == nil {
				logutil.Warnf("not found max global checkpoint!")
				return nil
			}
			logutil.Infof("load max global checkpoint: %s, consumedEnd: %s", entry.String(), maxConsumed.String())
			ckpData, err := c.collectCkpData(entry)
			if err != nil {
				logutil.Warnf("load max global checkpoint data failed, err[%v]", err)
				return nil
			}
			c.snapshotMeta.InitTableInfo(ckpData)
		}
		logutil.Infof("table info initialized: %s", c.snapshotMeta.TableInfoString())
	}
	return nil

}

func (c *checkpointCleaner) GetCheckpoints() map[string]struct{} {
	return c.ckpClient.GetCheckpointMetaFiles()
}

func (c *checkpointCleaner) updateMaxConsumed(e *checkpoint.CheckpointEntry) {
	c.maxConsumed.Store(e)
}

func (c *checkpointCleaner) updateMinMerged(e *checkpoint.CheckpointEntry) {
	c.minMerged.Store(e)
}

func (c *checkpointCleaner) updateMaxCompared(e *checkpoint.CheckpointEntry) {
	c.maxCompared.Store(e)
}

func (c *checkpointCleaner) updateCkpStage(ts *types.TS) {
	c.ckpStage.Store(ts)
}

func (c *checkpointCleaner) updateCkpGC(ts *types.TS) {
	c.ckpGC.Store(ts)
}

func (c *checkpointCleaner) updateInputs(input *GCTable) {
	c.inputs.Lock()
	defer c.inputs.Unlock()
	c.inputs.tables = append(c.inputs.tables, input)
}

func (c *checkpointCleaner) updateOutputs(files []string) {
	c.outputs.Lock()
	defer c.outputs.Unlock()
	c.outputs.files = append(c.outputs.files, files...)
}

func (c *checkpointCleaner) GetMaxConsumed() *checkpoint.CheckpointEntry {
	return c.maxConsumed.Load()
}

func (c *checkpointCleaner) GetMinMerged() *checkpoint.CheckpointEntry {
	return c.minMerged.Load()
}

func (c *checkpointCleaner) GetMaxCompared() *checkpoint.CheckpointEntry {
	return c.maxCompared.Load()
}

func (c *checkpointCleaner) GeteCkpStage() *types.TS {
	return c.ckpStage.Load()
}

func (c *checkpointCleaner) GeteCkpGC() *types.TS {
	return c.ckpGC.Load()
}

func (c *checkpointCleaner) GetInputs() *GCTable {
	c.inputs.RLock()
	defer c.inputs.RUnlock()
	return c.inputs.tables[0]
}

func (c *checkpointCleaner) GetGCTables() []*GCTable {
	c.inputs.RLock()
	defer c.inputs.RUnlock()
	return c.inputs.tables
}

func (c *checkpointCleaner) SetMinMergeCountForTest(count int) {
	c.minMergeCount.Lock()
	defer c.minMergeCount.Unlock()
	c.minMergeCount.count = count
}

func (c *checkpointCleaner) getMinMergeCount() int {
	c.minMergeCount.RLock()
	defer c.minMergeCount.RUnlock()
	return c.minMergeCount.count
}

func (c *checkpointCleaner) GetAndClearOutputs() []string {
	c.outputs.RLock()
	defer c.outputs.RUnlock()
	files := c.outputs.files
	//Empty the array, in order to store the next file list
	c.outputs.files = make([]string, 0)
	return files
}

func (c *checkpointCleaner) mergeGCFile() error {
	maxConsumed := c.GetMaxConsumed()
	if maxConsumed == nil {
		return nil
	}
	now := time.Now()
	logutil.Info("[DiskCleaner]",
		zap.String("MergeGCFile-Start", maxConsumed.String()))
	defer func() {
		logutil.Info("[DiskCleaner]",
			zap.String("MergeGCFile-End", maxConsumed.String()),
			zap.String("cost :", time.Since(now).String()))
	}()
	maxSnapEnd := types.TS{}
	maxAcctEnd := types.TS{}
	var snapFile, acctFile string
	dirs, err := c.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	deleteFiles := make([]string, 0)
	mergeSnapAcctFile := func(name string, ts, max *types.TS, file *string) error {
		if *file != "" {
			if max.Less(ts) {
				max = ts
				err = c.fs.Delete(*file)
				if err != nil {
					logutil.Errorf("DelFiles failed: %v, max: %v", err.Error(), max.ToString())
					return err
				}
				*file = GCMetaDir + name
			} else {
				err = c.fs.Delete(GCMetaDir + name)
				if err != nil {
					logutil.Errorf("DelFiles failed: %v, max: %v", err.Error(), max.ToString())
					return err
				}
			}
		} else {
			*file = GCMetaDir + name
			max = ts
			logutil.Infof("mergeSnapAcctFile: %v, max: %v", name, max.ToString())
		}
		return nil
	}
	for _, dir := range dirs {
		_, ts, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.SnapshotExt {
			err = mergeSnapAcctFile(dir.Name, &ts, &maxSnapEnd, &snapFile)
			if err != nil {
				return err
			}
			continue
		}
		if ext == blockio.AcctExt {
			err = mergeSnapAcctFile(dir.Name, &ts, &maxAcctEnd, &acctFile)
			if err != nil {
				return err
			}
			continue
		}
		_, end := blockio.DecodeCheckpointMetadataFileName(dir.Name)
		maxEnd := maxConsumed.GetEnd()
		if end.LessEq(&maxEnd) {
			deleteFiles = append(deleteFiles, GCMetaDir+dir.Name)
		}
	}
	if len(deleteFiles) < c.getMinMergeCount() {
		return nil
	}
	var mergeTable *GCTable
	c.inputs.RLock()
	if len(c.inputs.tables) == 0 {
		c.inputs.RUnlock()
		return nil
	}
	// tables[0] has always been a full GCTable
	if len(c.inputs.tables) > 1 {
		mergeTable = NewGCTable()
		for _, table := range c.inputs.tables {
			mergeTable.Merge(table)
		}
	} else {
		mergeTable = c.inputs.tables[0]
	}
	c.inputs.RUnlock()
	_, err = mergeTable.SaveFullTable(maxConsumed.GetStart(), maxConsumed.GetEnd(), c.fs, nil)
	if err != nil {
		logutil.Errorf("SaveTable failed: %v", err.Error())
		return err
	}
	err = c.fs.DelFiles(c.ctx, deleteFiles)
	if err != nil {
		logutil.Errorf("DelFiles failed: %v", err.Error())
		return err
	}
	c.updateMinMerged(maxConsumed)
	return nil
}

// getAllowedMergeFiles returns the files that can be merged.
// files: all checkpoint meta files before snapshot.
// idxes: idxes is the index of the global checkpoint in files,
// and the merge file will only process the files in one global checkpoint interval each time.
func getAllowedMergeFiles(
	metas map[string]struct{},
	snapshot types.TS,
	listFunc checkpoint.GetCheckpointRange) (ok bool, files []*checkpoint.MetaFile, idxes []int, err error) {
	var idx int
	files, idx, err, _ = checkpoint.ListSnapshotMetaWithDiskCleaner(snapshot, listFunc, metas)
	if err != nil {
		return
	}
	if len(files) == 0 {
		return
	}
	idxes = make([]int, 0)
	for i := 0; i <= idx; i++ {
		start := files[i].GetStart()
		if start.IsEmpty() {
			if i != 0 {
				idxes = append(idxes, i-1)
			}
		}
	}
	if len(idxes) == 0 {
		return
	}
	ok = true
	return
}

func (c *checkpointCleaner) getDeleteFile(
	ctx context.Context,
	fs fileservice.FileService,
	files []*checkpoint.MetaFile,
	idx int,
	ts, stage types.TS,
	ckpSnapList []types.TS,
) ([]string, error) {
	ckps, err := checkpoint.ListSnapshotCheckpointWithMeta(ctx, c.sid, fs, files, idx, ts, true)
	if err != nil {
		return nil, err
	}
	if len(ckps) == 0 {
		return nil, nil
	}
	deleteFiles := make([]string, 0)

	for i := len(ckps) - 1; i >= 0; i-- {
		// TODO: remove this log
		logutil.Info("[MergeCheckpoint]",
			common.OperationField("List Checkpoint"),
			common.OperandField(ckps[i].String()))
	}

	for i := len(ckps) - 1; i >= 0; i-- {
		ckp := ckps[i]
		end := ckp.GetEnd()
		if end.Less(&stage) {
			if isSnapshotCKPRefers(ckp.GetStart(), ckp.GetEnd(), ckpSnapList) &&
				ckp.GetType() != checkpoint.ET_Global {
				// TODO: remove this log
				logutil.Info("[MergeCheckpoint]",
					common.OperationField("isSnapshotCKPRefers"),
					common.OperandField(ckp.String()))
				break
			}
			logutil.Info("[MergeCheckpoint]",
				common.OperationField("GC checkpoint"),
				common.OperandField(ckp.String()))
			nameMeta := blockio.EncodeCheckpointMetadataFileName(
				checkpoint.CheckpointDir, checkpoint.PrefixMetadata,
				ckp.GetStart(), ckp.GetEnd())
			locations, err := logtail.LoadCheckpointLocations(
				c.ctx, c.sid, ckp.GetTNLocation(), ckp.GetVersion(), c.fs.Service)
			if err != nil {
				if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
					deleteFiles = append(deleteFiles, nameMeta)
					continue
				}
				return nil, err
			}
			deleteFiles = append(deleteFiles, nameMeta)
			if i == len(ckps)-1 {
				c.updateCkpGC(&end)
			}
			for name := range locations {
				deleteFiles = append(deleteFiles, name)
			}
			deleteFiles = append(deleteFiles, ckp.GetTNLocation().Name().String())

			if ckp.GetType() == checkpoint.ET_Global {
				// After the global checkpoint is processed,
				// subsequent checkpoints need to be processed in the next getDeleteFile
				logutil.Info("[MergeCheckpoint]",
					common.OperationField("GC Global checkpoint"),
					common.OperandField(ckp.String()))
				break
			}
		}
	}
	return deleteFiles, nil
}

func (c *checkpointCleaner) mergeCheckpointFiles(stage types.TS, snapshotList map[uint32][]types.TS) error {
	if stage.IsEmpty() ||
		(c.GeteCkpStage() != nil && c.GeteCkpStage().GreaterEq(&stage)) {
		return nil
	}
	metas := c.GetCheckpoints()
	logutil.Infof("[MergeCheckpoint] metas len %d", len(metas))
	ok, files, idxes, err := getAllowedMergeFiles(metas, stage, nil)
	if err != nil {
		return err
	}
	if !ok {
		c.updateCkpStage(&stage)
		return nil
	}
	ckpGC := c.GeteCkpGC()
	if ckpGC == nil {
		ckpGC = new(types.TS)
	}
	deleteFiles := make([]string, 0)
	ckpSnapList := make([]types.TS, 0)
	for _, ts := range snapshotList {
		ckpSnapList = append(ckpSnapList, ts...)
	}
	sort.Slice(ckpSnapList, func(i, j int) bool {
		return ckpSnapList[i].Less(&ckpSnapList[j])
	})
	for _, idx := range idxes {
		logutil.Info("[MergeCheckpoint]",
			common.OperationField("MergeCheckpointFiles"),
			common.OperandField(stage.ToString()),
			common.OperandField(idx))
		delFiles, err := c.getDeleteFile(c.ctx, c.fs.Service, files, idx, *ckpGC, stage, ckpSnapList)
		if err != nil {
			return err
		}
		ckpGC = new(types.TS)
		deleteFiles = append(deleteFiles, delFiles...)
	}

	logutil.Info("[MergeCheckpoint]",
		common.OperationField("CKP GC"),
		common.OperandField(deleteFiles))
	if !c.disableGC {
		err = c.fs.DelFiles(c.ctx, deleteFiles)
		if err != nil {
			logutil.Errorf("DelFiles failed: %v", err.Error())
			return err
		}
		for _, file := range deleteFiles {
			if strings.Contains(file, checkpoint.PrefixMetadata) {
				info := strings.Split(file, checkpoint.CheckpointDir+"/")
				name := info[1]
				c.ckpClient.RemoveCheckpointMetaFile(name)
			}
		}
	}
	c.updateCkpStage(&stage)
	return nil
}

func (c *checkpointCleaner) collectGlobalCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	_, data, err = logtail.LoadCheckpointEntriesFromKey(c.ctx, c.sid, c.fs.Service,
		ckp.GetLocation(), ckp.GetVersion(), nil, &types.TS{})
	return
}

func (c *checkpointCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	_, data, err = logtail.LoadCheckpointEntriesFromKey(c.ctx, c.sid, c.fs.Service,
		ckp.GetLocation(), ckp.GetVersion(), nil, &types.TS{})
	return
}

func (c *checkpointCleaner) TryGC() error {
	maxGlobalCKP := c.ckpClient.MaxGlobalCheckpoint()
	if maxGlobalCKP != nil {
		data, err := c.collectGlobalCkpData(maxGlobalCKP)
		if err != nil {
			return err
		}
		defer data.Close()
		err = c.tryGC(data, maxGlobalCKP)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *checkpointCleaner) tryGC(data *logtail.CheckpointData, gckp *checkpoint.CheckpointEntry) error {
	if !c.delWorker.Start() {
		return nil
	}
	var err error
	var snapshots map[uint32]containers.Vector
	defer func() {
		if err != nil {
			logutil.Errorf("[DiskCleaner] tryGC failed: %v", err.Error())
			c.delWorker.Idle()
		}
		logtail.CloseSnapshotList(snapshots)
	}()
	gcTable := NewGCTable()
	gcTable.UpdateTable(data)
	snapshots, err = c.GetSnapshots()
	if err != nil {
		logutil.Errorf("[DiskCleaner] GetSnapshots failed: %v", err.Error())
		return nil
	}
	gc, snapshotList := c.softGC(gcTable, gckp, snapshots)
	// Delete files after softGC
	// TODO:Requires Physical Removal Policy
	err = c.delWorker.ExecDelete(c.ctx, gc, c.disableGC)
	if err != nil {
		return err
	}
	err = c.mergeCheckpointFiles(c.ckpClient.GetStage(), snapshotList)

	if err != nil {
		// TODO: Error handle
		logutil.Errorf("[DiskCleaner] mergeCheckpointFiles failed: %v", err.Error())
		return err
	}
	return nil
}

func (c *checkpointCleaner) softGC(
	t *GCTable,
	gckp *checkpoint.CheckpointEntry,
	snapshots map[uint32]containers.Vector,
) ([]string, map[uint32][]types.TS) {
	c.inputs.Lock()
	defer c.inputs.Unlock()
	now := time.Now()
	var softCost, mergeCost time.Duration
	defer func() {
		logutil.Info("[DiskCleaner] softGC cost",
			zap.String("soft-gc cost", softCost.String()),
			zap.String("merge-table cost", mergeCost.String()))
	}()
	if len(c.inputs.tables) == 0 {
		return nil, nil
	}
	mergeTable := NewGCTable()
	for _, table := range c.inputs.tables {
		mergeTable.Merge(table)
	}
	gc, snapList := mergeTable.SoftGC(t, gckp.GetEnd(), snapshots, c.snapshotMeta)
	softCost = time.Since(now)
	now = time.Now()
	c.inputs.tables = make([]*GCTable, 0)
	c.inputs.tables = append(c.inputs.tables, mergeTable)
	c.updateMaxCompared(gckp)
	c.snapshotMeta.MergeTableInfo(snapList)
	mergeCost = time.Since(now)
	//logutil.Infof("SoftGC is %v, merge table: %v", gc, mergeTable.String())
	return gc, snapList
}

func (c *checkpointCleaner) createDebugInput(
	ckps []*checkpoint.CheckpointEntry) (input *GCTable, err error) {
	input = NewGCTable()
	var data *logtail.CheckpointData
	for _, candidate := range ckps {
		data, err = c.collectCkpData(candidate)
		if err != nil {
			logutil.Errorf("processing clean %s: %v", candidate.String(), err)
			// TODO
			return
		}
		defer data.Close()
		input.UpdateTable(data)
	}

	return
}

func (c *checkpointCleaner) CheckGC() error {
	debugCandidates := c.ckpClient.GetAllIncrementalCheckpoints()
	c.inputs.RLock()
	defer c.inputs.RUnlock()
	maxConsumed := c.GetMaxConsumed()
	if maxConsumed == nil {
		return moerr.NewInternalErrorNoCtx("GC has not yet run")
	}
	gCkp := c.GetMaxCompared()
	if gCkp == nil {
		gCkp = c.ckpClient.MaxGlobalCheckpoint()
		if gCkp == nil {
			return nil
		}
		logutil.Warnf("MaxCompared is nil, use maxGlobalCkp %v", gCkp.String())
	}
	data, err := c.collectGlobalCkpData(gCkp)
	if err != nil {
		return err
	}
	defer data.Close()
	gcTable := NewGCTable()
	gcTable.UpdateTable(data)
	for i, ckp := range debugCandidates {
		maxEnd := maxConsumed.GetEnd()
		ckpEnd := ckp.GetEnd()
		if ckpEnd.Equal(&maxEnd) {
			debugCandidates = debugCandidates[:i+1]
			break
		}
	}
	start1 := debugCandidates[len(debugCandidates)-1].GetEnd()
	start2 := maxConsumed.GetEnd()
	if !start1.Equal(&start2) {
		logutil.Info("[DiskCleaner]", common.OperationField("Compare not equal"),
			common.OperandField(start1.ToString()), common.OperandField(start2.ToString()))
		return moerr.NewInternalErrorNoCtx("TS Compare not equal")
	}
	debugTable, err := c.createDebugInput(debugCandidates)
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		// TODO
		return moerr.NewInternalErrorNoCtxf("processing clean %s: %v", debugCandidates[0].String(), err)
	}
	snapshots, err := c.GetSnapshots()
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		return moerr.NewInternalErrorNoCtxf("processing clean GetSnapshots %s: %v", debugCandidates[0].String(), err)
	}
	defer logtail.CloseSnapshotList(snapshots)
	debugTable.SoftGC(gcTable, gCkp.GetEnd(), snapshots, c.snapshotMeta)
	var mergeTable *GCTable
	if len(c.inputs.tables) > 1 {
		mergeTable = NewGCTable()
		for _, table := range c.inputs.tables {
			mergeTable.Merge(table)
		}
	} else {
		mergeTable = c.inputs.tables[0]
	}
	mergeTable.SoftGC(gcTable, gCkp.GetEnd(), snapshots, c.snapshotMeta)
	if !mergeTable.Compare(debugTable) {
		logutil.Errorf("inputs :%v", c.inputs.tables[0].String())
		logutil.Errorf("debugTable :%v", debugTable.String())
		return moerr.NewInternalErrorNoCtx("Compare is failed")
	} else {
		logutil.Info("[DiskCleaner]", common.OperationField("Compare is End"),
			common.AnyField("table :", debugTable.String()),
			common.OperandField(start1.ToString()))
	}
	return nil
}

func (c *checkpointCleaner) Process() {
	var ts types.TS
	if !c.isEnableGC() {
		return
	}

	now := time.Now()
	defer func() {
		logutil.Info("[DiskCleaner]",
			zap.String("Process costs", time.Since(now).String()))

	}()
	maxConsumed := c.maxConsumed.Load()
	if maxConsumed != nil {
		ts = maxConsumed.GetEnd()
	}

	checkpoints := c.ckpClient.ICKPSeekLT(ts, 10)

	if len(checkpoints) == 0 {
		return
	}
	candidates := make([]*checkpoint.CheckpointEntry, 0)
	for _, ckp := range checkpoints {
		if !c.checkExtras(ckp) {
			break
		}
		candidates = append(candidates, ckp)
	}

	if len(candidates) == 0 {
		return
	}
	var input *GCTable
	var err error
	if input, err = c.createNewInput(candidates); err != nil {
		logutil.Errorf("[DiskCleaner] processing clean %s: %v", candidates[0].String(), err)
		// TODO
		return
	}
	c.updateInputs(input)
	c.updateMaxConsumed(candidates[len(candidates)-1])

	var compareTS types.TS
	maxCompared := c.maxCompared.Load()
	if maxCompared != nil {
		compareTS = maxCompared.GetEnd()
	}
	maxGlobalCKP := c.ckpClient.MaxGlobalCheckpoint()
	if maxGlobalCKP == nil {
		return
	}
	maxEnd := maxGlobalCKP.GetEnd()
	if compareTS.Less(&maxEnd) {
		logutil.Info("[DiskCleaner]", common.OperationField("Try GC"),
			common.AnyField("maxGlobalCKP :", maxGlobalCKP.String()),
			common.AnyField("compareTS :", compareTS.ToString()))
		data, err := c.collectGlobalCkpData(maxGlobalCKP)
		if err != nil {
			c.inputs.RUnlock()
			logutil.Errorf("[DiskCleaner] processing clean %s: %v", candidates[0].String(), err)
			return
		}
		defer data.Close()
		err = c.tryGC(data, maxGlobalCKP)
		if err != nil {
			logutil.Errorf("[DiskCleaner] processing clean %s: %v", candidates[0].String(), err)
			return
		}
	}
	err = c.mergeGCFile()
	if err != nil {
		// TODO: Error handle
		return
	}

	if !c.isEnableCheckGC() {
		return
	}
	ck := checker{
		cleaner: c,
	}
	ck.Check()
}

func (c *checkpointCleaner) checkExtras(item any) bool {
	c.checker.RLock()
	defer c.checker.RUnlock()
	for _, checker := range c.checker.extras {
		if !checker(item) {
			return false
		}
	}
	return true
}

// AddChecker add&update a checker to the cleaner，return the number of checkers
// key is the unique identifier of the checker
func (c *checkpointCleaner) AddChecker(checker func(item any) bool, key string) int {
	c.checker.Lock()
	defer c.checker.Unlock()
	c.checker.extras[key] = checker
	return len(c.checker.extras)
}

// RemoveChecker remove a checker from the cleaner，return true if the checker is removed successfully
func (c *checkpointCleaner) RemoveChecker(key string) error {
	c.checker.Lock()
	defer c.checker.Unlock()
	if len(c.checker.extras) == 1 {
		return moerr.NewCantDelGCCheckerNoCtx()
	}
	delete(c.checker.extras, key)
	return nil
}

func (c *checkpointCleaner) createNewInput(
	ckps []*checkpoint.CheckpointEntry) (input *GCTable, err error) {
	now := time.Now()
	var snapSize, tableSize uint32
	input = NewGCTable()
	logutil.Info("[DiskCleaner]", zap.String("op", "Consume-Start"),
		zap.Int("entry count :", len(ckps)))
	defer func() {
		logutil.Info("[DiskCleaner]", zap.String("op", "Consume-End"),
			zap.String("cost :", time.Since(now).String()),
			zap.Uint32("snap meta size :", snapSize),
			zap.Uint32("table meta size :", tableSize),
			zap.Int("objects :", len(input.objects)),
			zap.Int("tombstones :", len(input.tombstones)),
			zap.String("snapshots", c.snapshotMeta.String()))
	}()
	var data *logtail.CheckpointData
	for _, candidate := range ckps {
		data, err = c.collectCkpData(candidate)
		if err != nil {
			logutil.Errorf("processing clean %s: %v", candidate.String(), err)
			// TODO
			return
		}
		defer data.Close()
		input.UpdateTable(data)
		c.updateSnapshot(data)
	}
	name := blockio.EncodeSnapshotMetadataFileName(GCMetaDir,
		PrefixSnapMeta, ckps[0].GetStart(), ckps[len(ckps)-1].GetEnd())
	snapSize, err = c.snapshotMeta.SaveMeta(name, c.fs.Service)
	if err != nil {
		logutil.Errorf("SaveMeta is failed")
		return
	}
	name = blockio.EncodeTableMetadataFileName(GCMetaDir,
		PrefixAcctMeta, ckps[0].GetStart(), ckps[len(ckps)-1].GetEnd())
	tableSize, err = c.snapshotMeta.SaveTableInfo(name, c.fs.Service)
	if err != nil {
		logutil.Errorf("SaveTableInfo is failed")
		return
	}
	files := c.GetAndClearOutputs()
	_, err = input.SaveTable(
		ckps[0].GetStart(),
		ckps[len(ckps)-1].GetEnd(),
		c.fs,
		files,
	)
	if err != nil {
		return
	}

	return
}

func (c *checkpointCleaner) updateSnapshot(data *logtail.CheckpointData) error {
	c.snapshotMeta.Update(data)
	return nil
}

func (c *checkpointCleaner) GetSnapshots() (map[uint32]containers.Vector, error) {
	return c.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mPool)
}

func isSnapshotCKPRefers(start, end types.TS, snapVec []types.TS) bool {
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GreaterEq(&start) && snapTS.Less(&end) {
			logutil.Debugf("isSnapshotRefers: %s, create %v, drop %v",
				snapTS.ToString(), start.ToString(), end.ToString())
			return true
		} else if snapTS.Less(&start) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}
