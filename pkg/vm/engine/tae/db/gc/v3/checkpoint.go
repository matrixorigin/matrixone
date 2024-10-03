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

package gc

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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"go.uber.org/zap"
)

type checkpointCleaner struct {
	fs  *objectio.ObjectFS
	ctx context.Context

	// checkpointCli is used to get the instance of the specified checkpoint
	checkpointCli checkpoint.RunnerReader

	watermarks struct {
		// scanWatermark is the watermark of the incremental checkpoint which has been
		// scanned by the cleaner. After the cleaner scans the checkpoint, it
		// records all the object-list found in the checkpoint into some GC-specific
		// files. The scanWatermark is used to record the end of the checkpoint.
		// For example:
		// Incremental checkpoint: [t100, t200), [t200, t300), [t300, t400)
		// scanWatermark: [t100, t200)
		// remainingObjects: windows: [t100, t200), [f1, f2, f3]
		// The cleaner will scan the checkpoint [t200, t300) next time. Then:
		// scanWatermark: [t100, t200), [t200, t300)
		// remainingObjects: windows:
		// {[t100, t200), [f1, f2, f3]}, {[t200, t300), [f4, f5, f6]}
		scanWatermark atomic.Pointer[checkpoint.CheckpointEntry]

		gcWatermark atomic.Pointer[checkpoint.CheckpointEntry]
	}

	// minMerged is to mark at which checkpoint the full
	// GCWindow in the current DiskCleaner is generated，
	// UT case needs to use
	minMerged atomic.Pointer[checkpoint.CheckpointEntry]

	ckpStage atomic.Pointer[types.TS]
	ckpGC    atomic.Pointer[types.TS]

	config struct {
		// minMergeCount is the configuration of the merge GC metadata file.
		// When the GC file is greater than or equal to minMergeCount,
		// the merge GC metadata file will be triggered and the expired file will be deleted.
		minMergeCount atomic.Int64
	}

	// remainingObjects is to record the currently valid GCWindow
	remainingObjects struct {
		sync.RWMutex
		windows []*GCWindow
	}

	// gcState is used to record state of the executed GCs
	gcState struct {
		sync.RWMutex
		filesGCed []string
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
		gcEnabled atomic.Bool
	}

	snapshotMeta *logtail.SnapshotMeta

	mp *mpool.MPool

	sid string
}

func NewCheckpointCleaner(
	ctx context.Context,
	sid string,
	fs *objectio.ObjectFS,
	checkpointCli checkpoint.RunnerReader,
	disableGC bool,
) Cleaner {
	cleaner := &checkpointCleaner{
		ctx:           ctx,
		sid:           sid,
		fs:            fs,
		checkpointCli: checkpointCli,
		disableGC:     disableGC,
	}
	cleaner.delWorker = NewGCWorker(fs, cleaner)
	cleaner.config.minMergeCount.Store(MinMergeCount)
	cleaner.snapshotMeta = logtail.NewSnapshotMeta()
	cleaner.option.gcEnabled.Store(true)
	cleaner.mp = common.CheckpointAllocator
	cleaner.checker.extras = make(map[string]func(item any) bool)
	return cleaner
}

func (c *checkpointCleaner) Stop() {
}

func (c *checkpointCleaner) GetMPool() *mpool.MPool {
	return c.mp
}

func (c *checkpointCleaner) SetTid(tid uint64) {
	c.snapshotMeta.Lock()
	defer c.snapshotMeta.Unlock()
	c.snapshotMeta.SetTid(tid)
}

func (c *checkpointCleaner) EnableGCForTest() {
	c.option.gcEnabled.Store(true)
}

func (c *checkpointCleaner) DisableGCForTest() {
	c.option.gcEnabled.Store(false)
}

func (c *checkpointCleaner) GCEnabled() bool {
	return c.option.gcEnabled.Load()
}

func (c *checkpointCleaner) IsEnableGC() bool {
	return c.GCEnabled()
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
			if minMergedStart.IsEmpty() || minMergedStart.LT(&start) {
				minMergedStart = start
				minMergedEnd = end
				maxConsumedStart = start
				maxConsumedEnd = end
				fullGCFile = dir
			}
		}
		if ext == blockio.SnapshotExt && maxSnapEnd.LT(&end) {
			maxSnapEnd = end
			snapFile = dir.Name
		}
		if ext == blockio.AcctExt && maxAcctEnd.LT(&end) {
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
		if (maxConsumedStart.IsEmpty() || maxConsumedStart.LT(&end)) &&
			minMergedEnd.LT(&end) {
			maxConsumedStart = start
			maxConsumedEnd = end
			readDirs = append(readDirs, dir)
		}
	}
	if len(readDirs) == 0 {
		return nil
	}
	logger := logutil.Info
	for _, dir := range readDirs {
		start := time.Now()
		window := NewGCWindow(c.mp, c.fs.Service)
		_, end, _ := blockio.DecodeGCMetadataFileName(dir.Name)
		err = window.ReadTable(c.ctx, GCMetaDir+dir.Name, dir.Size, c.fs, end)
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Replay-GC-Metadata-File",
			zap.String("name", dir.Name),
			zap.Duration("cost", time.Since(start)),
			zap.Error(err),
		)
		if err != nil {
			return err
		}
		c.addGCWindow(window)
	}
	if acctFile != "" {
		err = c.snapshotMeta.ReadTableInfo(c.ctx, GCMetaDir+acctFile, c.fs.Service)
		if err != nil {
			return err
		}
	}
	if snapFile != "" {
		err = c.snapshotMeta.ReadMeta(c.ctx, GCMetaDir+snapFile, c.fs.Service)
		if err != nil {
			return err
		}
	}
	ckp := checkpoint.NewCheckpointEntry(c.sid, maxConsumedStart, maxConsumedEnd, checkpoint.ET_Incremental)
	c.updateScanWatermark(ckp)
	defer func() {
		// Ensure that updateMinMerged is executed last, because minMergedEnd is not empty means that the replay is completed
		// For UT
		ckp = checkpoint.NewCheckpointEntry(c.sid, minMergedStart, minMergedEnd, checkpoint.ET_Incremental)
		c.updateMinMerged(ckp)
	}()
	if acctFile == "" {
		//No account table information, it may be a new cluster or an upgraded cluster,
		//and the table information needs to be initialized from the checkpoint
		scanWatermark := c.GetScanWatermark()
		isConsumedGCkp := false
		checkpointEntries, err := checkpoint.ListSnapshotCheckpoint(c.ctx, c.sid, c.fs.Service, scanWatermark.GetEnd(), 0, checkpoint.SpecifiedCheckpoint)
		if err != nil {
			// TODO: why only warn???
			logutil.Warn(
				"Replay-GC-List-Error",
				zap.Error(err),
			)
		}
		if len(checkpointEntries) == 0 {
			return nil
		}
		for _, entry := range checkpointEntries {
			logutil.Infof("load checkpoint: %s, consumedEnd: %s", entry.String(), scanWatermark.String())
			ckpData, err := c.collectCkpData(entry)
			if err != nil {
				// TODO: why only warn???
				logutil.Warn(
					"Replay-GC-Collect-Error",
					zap.Error(err),
				)
				continue
			}
			if entry.GetType() == checkpoint.ET_Global {
				isConsumedGCkp = true
			}
			c.snapshotMeta.InitTableInfo(c.ctx, c.fs.Service, ckpData, entry.GetStart(), entry.GetEnd())
		}
		if !isConsumedGCkp {
			// The global checkpoint that Specified checkpoint depends on may have been GC,
			// so we need to load a latest global checkpoint
			entry := c.checkpointCli.MaxGlobalCheckpoint()
			if entry == nil {
				logutil.Warn("not found max global checkpoint!")
				return nil
			}
			logutil.Info(
				"Replay-GC-Load-Global-Checkpoint",
				zap.String("max-gloabl", entry.String()),
				zap.String("max-consumed", scanWatermark.String()),
			)
			ckpData, err := c.collectCkpData(entry)
			if err != nil {
				// TODO: why only warn???
				logutil.Warn(
					"Replay-GC-Collect-Global-Error",
					zap.Error(err),
				)
				return nil
			}
			c.snapshotMeta.InitTableInfo(c.ctx, c.fs.Service, ckpData, entry.GetStart(), entry.GetEnd())
		}
		logutil.Info(
			"Replay-GC-Init-Table-Info",
			zap.String("info", c.snapshotMeta.TableInfoString()),
		)
	}
	return nil

}

func (c *checkpointCleaner) GetCheckpoints() map[string]struct{} {
	return c.checkpointCli.GetCheckpointMetaFiles()
}

func (c *checkpointCleaner) updateScanWatermark(e *checkpoint.CheckpointEntry) {
	c.watermarks.scanWatermark.Store(e)
}

func (c *checkpointCleaner) updateMinMerged(e *checkpoint.CheckpointEntry) {
	c.minMerged.Store(e)
}

func (c *checkpointCleaner) updateGCWatermark(e *checkpoint.CheckpointEntry) {
	c.watermarks.gcWatermark.Store(e)
}

func (c *checkpointCleaner) updateCkpStage(ts *types.TS) {
	c.ckpStage.Store(ts)
}

func (c *checkpointCleaner) updateCkpGC(ts *types.TS) {
	c.ckpGC.Store(ts)
}

func (c *checkpointCleaner) addGCWindow(window *GCWindow) {
	c.remainingObjects.Lock()
	defer c.remainingObjects.Unlock()
	c.remainingObjects.windows = append(c.remainingObjects.windows, window)
}

func (c *checkpointCleaner) recordFilesGCed(files []string) {
	c.gcState.Lock()
	defer c.gcState.Unlock()
	c.gcState.filesGCed = append(c.gcState.filesGCed, files...)
}

func (c *checkpointCleaner) GetScanWatermark() *checkpoint.CheckpointEntry {
	return c.watermarks.scanWatermark.Load()
}

func (c *checkpointCleaner) GetMinMerged() *checkpoint.CheckpointEntry {
	return c.minMerged.Load()
}

func (c *checkpointCleaner) GetGCWatermark() *checkpoint.CheckpointEntry {
	return c.watermarks.gcWatermark.Load()
}

func (c *checkpointCleaner) GeteCkpStage() *types.TS {
	return c.ckpStage.Load()
}

func (c *checkpointCleaner) GeteCkpGC() *types.TS {
	return c.ckpGC.Load()
}

func (c *checkpointCleaner) GetFirstWindow() *GCWindow {
	c.remainingObjects.RLock()
	defer c.remainingObjects.RUnlock()
	return c.remainingObjects.windows[0]
}

func (c *checkpointCleaner) SetMinMergeCountForTest(count int) {
	c.config.minMergeCount.Store(int64(count))
}

func (c *checkpointCleaner) getMinMergeCount() int {
	return int(c.config.minMergeCount.Load())
}

func (c *checkpointCleaner) OrhpanFilesGCed() []string {
	c.gcState.RLock()
	defer c.gcState.RUnlock()
	filesGCed := c.gcState.filesGCed
	//Empty the array, in order to store the next file list
	c.gcState.filesGCed = make([]string, 0)
	return filesGCed
}

func (c *checkpointCleaner) mergeGCFile() error {
	// get the scan watermark
	scanWatermark := c.GetScanWatermark()
	if scanWatermark == nil {
		return nil
	}
	now := time.Now()
	logutil.Info(
		"[DiskCleaner]",
		zap.String("MergeGCFile-Start", scanWatermark.String()),
	)
	defer func() {
		logutil.Info(
			"[DiskCleaner]",
			zap.String("MergeGCFile-End", scanWatermark.String()),
			zap.Duration("cost", time.Since(now)),
		)
	}()

	var (
		maxSnapEnd types.TS
		maxAcctEnd types.TS
		snapFile   string
		acctFile   string
	)

	dirs, err := c.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}

	deleteFiles := make([]string, 0)
	mergeSnapAcctFile := func(name string, ts, max *types.TS, file *string) error {
		if *file != "" {
			if max.LT(ts) {
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
			logutil.Info(
				"Merging-GC-File-SnapAcct-File",
				zap.String("name", name),
				zap.String("max", max.ToString()),
			)
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
		maxEnd := scanWatermark.GetEnd()
		if end.LT(&maxEnd) {
			deleteFiles = append(deleteFiles, GCMetaDir+dir.Name)
		}
	}
	if len(deleteFiles) < c.getMinMergeCount() {
		return nil
	}
	c.remainingObjects.RLock()
	if len(c.remainingObjects.windows) == 0 {
		c.remainingObjects.RUnlock()
		return nil
	}
	// windows[0] has always been a full GCWindow
	c.remainingObjects.RUnlock()
	err = c.fs.DelFiles(c.ctx, deleteFiles)
	if err != nil {
		logutil.Error(
			"Merging-GC-File-Error",
			zap.Error(err),
		)
		return err
	}
	c.updateMinMerged(scanWatermark)
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
	files, _, idx, err = checkpoint.ListSnapshotMetaWithDiskCleaner(snapshot, listFunc, metas)
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
) ([]string, []*checkpoint.CheckpointEntry, error) {
	ckps, err := checkpoint.ListSnapshotCheckpointWithMeta(ctx, c.sid, fs, files, idx, ts, true)
	if err != nil {
		return nil, nil, err
	}
	if len(ckps) == 0 {
		return nil, nil, nil
	}
	deleteFiles := make([]string, 0)
	var mergeFiles []*checkpoint.CheckpointEntry
	for i := len(ckps) - 1; i >= 0; i-- {
		// TODO: remove this log
		logutil.Info("[MergeCheckpoint]",
			common.OperationField("List Checkpoint"),
			common.OperandField(ckps[i].String()))
	}
	for i := len(ckps) - 1; i >= 0; i-- {
		ckp := ckps[i]
		end := ckp.GetEnd()
		if end.LT(&stage) {
			if isSnapshotCKPRefers(ckp.GetStart(), ckp.GetEnd(), ckpSnapList) &&
				ckp.GetType() != checkpoint.ET_Global {
				// TODO: remove this log
				logutil.Info("[MergeCheckpoint]",
					common.OperationField("isSnapshotCKPRefers"),
					common.OperandField(ckp.String()))
				mergeFiles = ckps[:i+1]
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
				return nil, nil, err
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
	return deleteFiles, mergeFiles, nil
}

func (c *checkpointCleaner) mergeCheckpointFiles(
	stage types.TS,
	accoutSnapshots map[uint32][]types.TS,
) error {
	if stage.IsEmpty() ||
		(c.GeteCkpStage() != nil && c.GeteCkpStage().GE(&stage)) {
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
	for _, ts := range accoutSnapshots {
		ckpSnapList = append(ckpSnapList, ts...)
	}
	sort.Slice(ckpSnapList, func(i, j int) bool {
		return ckpSnapList[i].LT(&ckpSnapList[j])
	})
	for _, idx := range idxes {
		logutil.Info("[MergeCheckpoint]",
			common.OperationField("MergeCheckpointFiles"),
			common.OperandField(stage.ToString()),
			common.OperandField(idx))
		delFiles, _, err := c.getDeleteFile(c.ctx, c.fs.Service, files, idx, *ckpGC, stage, ckpSnapList)
		if err != nil {
			return err
		}
		ckpGC = new(types.TS)
		deleteFiles = append(deleteFiles, delFiles...)

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
				c.checkpointCli.RemoveCheckpointMetaFile(name)
			}
		}
	}
	c.updateCkpStage(&stage)
	return nil
}

func (c *checkpointCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	return logtail.GetCheckpointData(
		c.ctx, c.sid, c.fs.Service, ckp.GetLocation(), ckp.GetVersion())
}

func (c *checkpointCleaner) GetPITRs() (*logtail.PitrInfo, error) {
	ts := time.Now()
	return c.snapshotMeta.GetPITR(c.ctx, c.sid, ts, c.fs.Service, c.mp)
}

func (c *checkpointCleaner) TryGC() error {
	if maxGlobalCKP := c.checkpointCli.MaxGlobalCheckpoint(); maxGlobalCKP != nil {
		return c.tryGCAgainstGlobalCheckpoint(maxGlobalCKP)
	}
	return nil
}

func (c *checkpointCleaner) tryGCAgainstGlobalCheckpoint(gckp *checkpoint.CheckpointEntry) error {
	if !c.delWorker.Start() {
		return nil
	}
	var err error
	var snapshots map[uint32]containers.Vector
	defer func() {
		if err != nil {
			logutil.Errorf("[DiskCleaner] tryGCAgainstGlobalCheckpoint failed: %v", err.Error())
			c.delWorker.Idle()
		}
		logtail.CloseSnapshotList(snapshots)
	}()
	pitrs, err := c.GetPITRs()
	if err != nil {
		logutil.Errorf("[DiskCleaner] GetPitrs failed: %v", err.Error())
		return nil
	}
	snapshots, err = c.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
	if err != nil {
		logutil.Errorf("[DiskCleaner] GetSnapshot failed: %v", err.Error())
		return nil
	}
	accountSnapshots := TransformToTSList(snapshots)
	gc, err := c.doGCAgainstGlobalCheckpoint(gckp, accountSnapshots, pitrs)
	if err != nil {
		logutil.Errorf("[DiskCleaner] doGCAgainstGlobalCheckpoint failed: %v", err.Error())
		return err
	}
	// Delete files after doGCAgainstGlobalCheckpoint
	// TODO:Requires Physical Removal Policy
	err = c.delWorker.ExecDelete(c.ctx, gc, c.disableGC)
	if err != nil {
		logutil.Infof("[DiskCleaner] ExecDelete failed: %v", err.Error())
		return err
	}
	err = c.mergeCheckpointFiles(c.checkpointCli.GetStage(), accountSnapshots)

	if err != nil {
		// TODO: Error handle
		logutil.Errorf("[DiskCleaner] mergeCheckpointFiles failed: %v", err.Error())
		return err
	}
	return nil
}

func (c *checkpointCleaner) doGCAgainstGlobalCheckpoint(
	gckp *checkpoint.CheckpointEntry,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
) ([]string, error) {
	c.remainingObjects.Lock()
	defer c.remainingObjects.Unlock()

	now := time.Now()

	var softCost, mergeCost time.Duration
	defer func() {
		logutil.Info("[DiskCleaner] doGCAgainstGlobalCheckpoint cost",
			zap.String("soft-gc cost", softCost.String()),
			zap.String("merge-table cost", mergeCost.String()))
	}()
	if len(c.remainingObjects.windows) < 2 {
		return nil, nil
	}

	// merge all windows into one for the following GC
	// [t100, t200] [t200, t300] [t300, t400] => [t100, t400]
	// [f1, f2, f3] [f4, f5, f6] [f7, f8, f9] => [f1, f2, f3, f4, f5, f6, f7, f8, f9
	oneWindow := c.remainingObjects.windows[0]
	for i := 1; i < len(c.remainingObjects.windows); i++ {
		oneWindow.Merge(c.remainingObjects.windows[i])
		c.remainingObjects.windows[i].Close()
		c.remainingObjects.windows[i] = nil
	}
	c.remainingObjects.windows = c.remainingObjects.windows[:1]

	// create a buffer as the GC memory pool
	memoryBuffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer memoryBuffer.Close(c.mp)

	var (
		filesToGC []string
		err       error
	)
	// do GC against the global checkpoint
	// the result is the files that need to be deleted
	// it will update the file list in the oneWindow
	// Before:
	// [t100, t400] [f1, f2, f3, f4, f5, f6, f7, f8, f9]
	// After:
	// [t100, t400] [f10, f11]
	// Also, it will update the GC metadata
	if filesToGC, err = oneWindow.ExecuteGlobalCheckpointBasedGC(
		c.ctx,
		gckp,
		accountSnapshots,
		pitrs,
		c.snapshotMeta,
		memoryBuffer,
		c.mp,
		c.fs.Service,
	); err != nil {
		logutil.Errorf("doGCAgainstGlobalCheckpoint failed: %v", err.Error())
		return nil, err
	}

	softCost = time.Since(now)

	// update gc watermark and refresh snapshot meta with the latest gc result
	// gcWatermark will be updated to the end of the global checkpoint after each GC
	// Before:
	// gcWatermark: GCKP[t100, t200)
	// After:
	// gcWatermark: GCKP[t200, t400)
	now = time.Now()
	// TODO:
	c.updateGCWatermark(gckp)
	c.snapshotMeta.MergeTableInfo(accountSnapshots, pitrs)
	mergeCost = time.Since(now)
	return filesToGC, nil
}

func (c *checkpointCleaner) scanCheckpointsAsDebugWindow(
	ckps []*checkpoint.CheckpointEntry,
	buffer *containers.OneSchemaBatchBuffer,
) (window *GCWindow, err error) {
	window = NewGCWindow(c.mp, c.fs.Service, WithMetaPrefix("debug/"))
	if err = window.ScanCheckpoints(
		c.ctx, ckps, c.collectCkpData, nil, buffer,
	); err != nil {
		window.Close()
		window = nil
	}
	return
}

func (c *checkpointCleaner) CheckGC() error {
	debugCandidates := c.checkpointCli.GetAllIncrementalCheckpoints()

	c.remainingObjects.RLock()
	defer c.remainingObjects.RUnlock()

	// no scan watermark, GC has not yet run
	var scanWatermark *checkpoint.CheckpointEntry
	if scanWatermark = c.GetScanWatermark(); scanWatermark == nil {
		return moerr.NewInternalErrorNoCtx("GC has not yet run")
	}

	gCkp := c.GetGCWatermark()
	testutils.WaitExpect(10000, func() bool {
		gCkp = c.GetGCWatermark()
		return gCkp != nil
	})
	if gCkp == nil {
		gCkp = c.checkpointCli.MaxGlobalCheckpoint()
		if gCkp == nil {
			return nil
		}
		logutil.Warnf("MaxCompared is nil, use maxGlobalCkp %v", gCkp.String())
	}
	for i, ckp := range debugCandidates {
		maxEnd := scanWatermark.GetEnd()
		ckpEnd := ckp.GetEnd()
		if ckpEnd.Equal(&maxEnd) {
			debugCandidates = debugCandidates[:i+1]
			break
		}
	}
	start1 := debugCandidates[len(debugCandidates)-1].GetEnd()
	start2 := scanWatermark.GetEnd()
	if !start1.Equal(&start2) {
		logutil.Info("[DiskCleaner]", common.OperationField("Compare not equal"),
			common.OperandField(start1.ToString()), common.OperandField(start2.ToString()))
		return moerr.NewInternalErrorNoCtx("TS Compare not equal")
	}

	buffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer buffer.Close(c.mp)

	debugWindow, err := c.scanCheckpointsAsDebugWindow(
		debugCandidates, buffer,
	)
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

	pitr, err := c.GetPITRs()
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		return moerr.NewInternalErrorNoCtxf("processing clean GetPITRs %s: %v", debugCandidates[0].String(), err)
	}

	mergeWindow := NewGCWindow(c.mp, c.fs.Service)
	for _, window := range c.remainingObjects.windows {
		if len(window.files) == 0 {
			logutil.Infof("mergeWindow is empty")
		} else {
			logutil.Infof("mergeWindow is %d, stats is %v", len(window.files), window.files[0].ObjectName().String())
		}
		mergeWindow.Merge(window)
	}
	defer mergeWindow.Close()

	accoutSnapshots := TransformToTSList(snapshots)
	logutil.Infof(
		"merge table is %d, stats is %v",
		len(mergeWindow.files),
		mergeWindow.files[0].ObjectName().String(),
	)
	if _, err = mergeWindow.ExecuteGlobalCheckpointBasedGC(
		c.ctx,
		gCkp,
		accoutSnapshots,
		pitr,
		c.snapshotMeta,
		buffer,
		c.mp,
		c.fs.Service,
	); err != nil {
		logutil.Infof("err is %v", err)
		return err
	}

	logutil.Infof(
		"merge table2 is %d, stats is %v",
		len(mergeWindow.files),
		mergeWindow.files[0].ObjectName().String(),
	)

	//logutil.Infof("debug table is %d, stats is %v", len(debugWindow.files.stats), debugWindow.files.stats[0].ObjectName().String())
	if _, err = debugWindow.ExecuteGlobalCheckpointBasedGC(
		c.ctx,
		gCkp,
		accoutSnapshots,
		pitr,
		c.snapshotMeta,
		buffer,
		c.mp,
		c.fs.Service,
	); err != nil {
		logutil.Infof("err is %v", err)
		return err
	}

	//logutil.Infof("debug table2 is %d, stats is %v", len(debugWindow.files.stats), debugWindow.files.stats[0].ObjectName().String())
	objects1, objects2, equal := mergeWindow.Compare(debugWindow, buffer)
	if !equal {
		logutil.Errorf("remainingObjects :%v", c.remainingObjects.windows[0].String(objects1))
		logutil.Errorf("debugWindow :%v", debugWindow.String(objects2))
		return moerr.NewInternalErrorNoCtx("Compare is failed")
	} else {
		logutil.Info("[DiskCleaner]", common.OperationField("Compare is End"),
			common.AnyField("table :", debugWindow.String(objects2)),
			common.OperandField(start1.ToString()))
	}
	return nil
}

func (c *checkpointCleaner) Process() {
	if !c.GCEnabled() {
		return
	}

	now := time.Now()
	defer func() {
		logutil.Info(
			"DiskCleaner-Process-End",
			zap.Duration("duration", time.Since(now)),
		)
	}()

	// 1. get the max consumed timestamp water level
	var tsWaterLevel types.TS
	if scanWatermark := c.GetScanWatermark(); scanWatermark != nil {
		tsWaterLevel = scanWatermark.GetEnd()
	}

	// 2. get up to 10 incremental checkpoints starting from the tsWaterLevel
	checkpoints := c.checkpointCli.ICKPSeekLT(tsWaterLevel, 10)
	// if there is no incremental checkpoint, quickly return
	if len(checkpoints) == 0 {
		logutil.Info(
			"DiskCleaner-Process-NoCheckpoint",
			zap.String("water-level", tsWaterLevel.ToString()),
		)
		return
	}

	// 3. filter out the incremental checkpoints that do not meet the requirements
	candidates := make([]*checkpoint.CheckpointEntry, 0, len(checkpoints))
	for _, ckp := range checkpoints {
		// TODO:
		// 1. break or continue?
		// 2. use cases reveiw!
		if !c.checkExtras(ckp) {
			break
		}
		candidates = append(candidates, ckp)
	}
	if len(candidates) == 0 {
		return
	}

	var (
		gcWindow *GCWindow
		err      error
	)
	if gcWindow, err = c.scanCheckpointsAsOneWindow(candidates); err != nil {
		logutil.Error(
			"DiskCleaner-Process-CreateNewInput-Error",
			zap.Error(err),
			zap.String("checkpoint", candidates[0].String()),
		)
		// TODO
		return
	}
	c.addGCWindow(gcWindow)
	c.updateScanWatermark(candidates[len(candidates)-1])

	// get the max global checkpoint
	// if there is no global checkpoint, quickly return
	var maxGlobalCKP *checkpoint.CheckpointEntry
	if maxGlobalCKP = c.checkpointCli.MaxGlobalCheckpoint(); maxGlobalCKP == nil {
		return
	}

	// get the gc watermark, which is the end of the global checkpoint after last GC
	var gcWatermarkTS types.TS
	if gcWatermark := c.GetGCWatermark(); gcWatermark != nil {
		gcWatermarkTS = gcWatermark.GetEnd()
	}

	// if the gc watermark is less than the end of the global checkpoint, it means
	// that the maxGlobalCKP has not been GCed, and we need to GC it now
	nextGCTS := maxGlobalCKP.GetEnd()
	if gcWatermarkTS.LT(&nextGCTS) {
		logutil.Info(
			"DiskCleaner-Process-TryGC",
			zap.String("max-global", maxGlobalCKP.String()),
			zap.String("gc-watermark", gcWatermarkTS.ToString()),
		)
		if err = c.tryGCAgainstGlobalCheckpoint(
			maxGlobalCKP,
		); err != nil {
			logutil.Error(
				"DiskCleaner-Process-TryGC-Error",
				zap.Error(err),
				zap.String("checkpoint", maxGlobalCKP.String()),
			)
			return
		}
	}

	if err = c.mergeGCFile(); err != nil {
		// TODO: Error handle
		return
	}

	if !c.isEnableCheckGC() {
		return
	}
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

func (c *checkpointCleaner) scanCheckpointsAsOneWindow(
	ckps []*checkpoint.CheckpointEntry,
) (gcWindow *GCWindow, err error) {
	now := time.Now()
	var snapSize, tableSize uint32
	gcWindow = NewGCWindow(c.mp, c.fs.Service)
	logutil.Info(
		"DiskCleaner-Consume-Start",
		zap.Int("entry-count :", len(ckps)),
	)
	defer func() {
		logutil.Info("DiskCleaner-Consume-End",
			zap.Duration("duration", time.Since(now)),
			zap.Uint32("snap-meta-size :", snapSize),
			zap.Uint32("table-meta-size :", tableSize),
			zap.String("snapshot-detail", c.snapshotMeta.String()))
	}()
	buffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer buffer.Close(c.mp)
	if err = gcWindow.ScanCheckpoints(
		c.ctx,
		ckps,
		c.collectCkpData,
		c.updateSnapshot,
		buffer,
	); err != nil {
		return
	}
	name := blockio.EncodeSnapshotMetadataFileName(GCMetaDir,
		PrefixSnapMeta, ckps[0].GetStart(), ckps[len(ckps)-1].GetEnd())
	snapSize, err = c.snapshotMeta.SaveMeta(name, c.fs.Service)
	if err != nil {
		logutil.Error(
			"DiskCleaner-Error-SaveMeta",
			zap.Error(err),
		)
		return
	}
	name = blockio.EncodeTableMetadataFileName(GCMetaDir,
		PrefixAcctMeta, ckps[0].GetStart(), ckps[len(ckps)-1].GetEnd())
	tableSize, err = c.snapshotMeta.SaveTableInfo(name, c.fs.Service)
	if err != nil {
		logutil.Error(
			"DiskCleaner-Error-SaveTableInfo",
			zap.Error(err),
		)
		return
	}
	return
}

func (c *checkpointCleaner) updateSnapshot(
	ckp *checkpoint.CheckpointEntry,
	data *logtail.CheckpointData,
) error {
	c.snapshotMeta.Update(c.ctx, c.fs.Service, data, ckp.GetStart(), ckp.GetEnd())
	return nil
}

func (c *checkpointCleaner) GetSnapshots() (map[uint32]containers.Vector, error) {
	return c.snapshotMeta.GetSnapshot(c.ctx, c.sid, c.fs.Service, c.mp)
}

func isSnapshotCKPRefers(start, end types.TS, snapVec []types.TS) bool {
	if len(snapVec) == 0 {
		return false
	}
	left, right := 0, len(snapVec)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapVec[mid]
		if snapTS.GE(&start) && snapTS.LT(&end) {
			logutil.Debugf("isSnapshotRefers: %s, create %v, drop %v",
				snapTS.ToString(), start.ToString(), end.ToString())
			return true
		} else if snapTS.LT(&start) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}
