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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
	"sync/atomic"
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
		extras []func(item any) bool
	}

	// delWorker is a worker that deletes s3‘s objects or local
	// files, and only one worker will run
	delWorker *GCWorker

	disableGC bool

	option struct {
		sync.RWMutex
		enableGC bool
	}

	snapshot struct {
		sync.RWMutex
		snapshotMeta *logtail.SnapshotMeta
	}
}

func NewCheckpointCleaner(
	ctx context.Context,
	fs *objectio.ObjectFS,
	ckpClient checkpoint.RunnerReader,
	disableGC bool,
) Cleaner {
	cleaner := &checkpointCleaner{
		ctx:       ctx,
		fs:        fs,
		ckpClient: ckpClient,
		disableGC: disableGC,
	}
	cleaner.delWorker = NewGCWorker(fs, cleaner)
	cleaner.minMergeCount.count = MinMergeCount
	cleaner.snapshot.snapshotMeta = logtail.NewSnapshotMeta()
	cleaner.option.enableGC = true
	return cleaner
}

func (c *checkpointCleaner) SetTid(tid uint64) {
	c.snapshot.Lock()
	defer c.snapshot.Unlock()
	c.snapshot.snapshotMeta.SetTid(tid)
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
	var fullGCFile fileservice.DirEntry
	// Get effective minMerged
	var snapFile string
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
		if ext == blockio.SnapshotExt && maxConsumedEnd.Equal(&end) {
			snapFile = dir.Name
		}
	}
	readDirs := make([]fileservice.DirEntry, 0)
	if !minMergedStart.IsEmpty() {
		readDirs = append(readDirs, fullGCFile)
	}
	for _, dir := range dirs {
		start, end, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.GCFullExt || ext == blockio.SnapshotExt {
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
		err = c.snapshot.snapshotMeta.ReadMeta(c.ctx, GCMetaDir+snapFile, c.fs.Service)
		if err != nil {
			return err
		}
		c.GetSnapshots()
		logutil.Infof("Replay snapshot file: %s", snapFile)
	}
	ckp := checkpoint.NewCheckpointEntry(maxConsumedStart, maxConsumedEnd, checkpoint.ET_Incremental)
	c.updateMaxConsumed(ckp)
	ckp = checkpoint.NewCheckpointEntry(minMergedStart, minMergedEnd, checkpoint.ET_Incremental)
	c.updateMinMerged(ckp)
	return nil

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

func (c *checkpointCleaner) GetInputs() *GCTable {
	c.inputs.RLock()
	defer c.inputs.RUnlock()
	return c.inputs.tables[0]
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
	dirs, err := c.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	deleteFiles := make([]string, 0)
	for _, dir := range dirs {
		_, _, ext := blockio.DecodeGCMetadataFileName(dir.Name)
		if ext == blockio.SnapshotExt {
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
	mergeTable = c.inputs.tables[0]
	c.inputs.RUnlock()
	logutil.Info("[DiskCleaner]",
		common.OperationField("MergeGCFile start"),
		common.OperandField(maxConsumed.String()))
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
	logutil.Info("[DiskCleaner]",
		common.OperationField("MergeGCFile end"),
		common.OperandField(maxConsumed.String()))
	return nil
}

func (c *checkpointCleaner) collectGlobalCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	_, data, err = logtail.LoadCheckpointEntriesFromKey(c.ctx, c.fs.Service,
		ckp.GetLocation(), ckp.GetVersion(), nil)
	return
}

func (c *checkpointCleaner) collectCkpData(
	ckp *checkpoint.CheckpointEntry,
) (data *logtail.CheckpointData, err error) {
	_, data, err = logtail.LoadCheckpointEntriesFromKey(c.ctx, c.fs.Service,
		ckp.GetLocation(), ckp.GetVersion(), nil)
	return
}

func (c *checkpointCleaner) TryGC() error {
	maxGlobalCKP := c.ckpClient.MaxGlobalCheckpoint()
	if maxGlobalCKP != nil {
		logutil.Infof("maxGlobalCKP is %v", maxGlobalCKP.String())
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
	gcTable := NewGCTable()
	gcTable.UpdateTable(data)
	snapshots, err := c.GetSnapshots()
	if err != nil {
		logutil.Errorf("GetSnapshots failed: %v", err.Error())
		return nil
	}
	gc := c.softGC(gcTable, gckp, snapshots)
	// Delete files after softGC
	// TODO:Requires Physical Removal Policy
	logutil.Infof("gc files: %v", gc)
	for snapshotID, snapshot := range snapshots {
		for _, ts := range snapshot {
			logutil.Infof("snapshotID: %v, ts: %v", snapshotID, ts.ToString())
		}
	}
	err = c.delWorker.ExecDelete(c.ctx, gc, c.disableGC)
	if err != nil {
		return err
	}
	return nil
}

func (c *checkpointCleaner) softGC(t *GCTable, gckp *checkpoint.CheckpointEntry, snapshots map[uint64][]types.TS) []string {
	c.inputs.Lock()
	defer c.inputs.Unlock()
	if len(c.inputs.tables) == 0 {
		return nil
	}
	mergeTable := NewGCTable()
	for _, table := range c.inputs.tables {
		mergeTable.Merge(table)
	}
	gc := mergeTable.SoftGC(t, gckp.GetEnd(), snapshots)
	c.inputs.tables = make([]*GCTable, 0)
	c.inputs.tables = append(c.inputs.tables, mergeTable)
	c.updateMaxCompared(gckp)
	//logutil.Infof("SoftGC is %v, merge table: %v", gc, mergeTable.String())
	return gc
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
		return moerr.NewInternalErrorNoCtx("processing clean %s: %v", debugCandidates[0].String(), err)
	}
	snapshots, err := c.GetSnapshots()
	if err != nil {
		logutil.Errorf("processing clean %s: %v", debugCandidates[0].String(), err)
		return moerr.NewInternalErrorNoCtx("processing clean GetSnapshots %s: %v", debugCandidates[0].String(), err)
	}
	logutil.Infof("snapshots1 is %v", snapshots)
	debugTable.SoftGC(gcTable, gCkp.GetEnd(), snapshots)
	var mergeTable *GCTable
	if len(c.inputs.tables) > 1 {
		mergeTable = NewGCTable()
		for _, table := range c.inputs.tables {
			mergeTable.Merge(table)
		}
	} else {
		mergeTable = c.inputs.tables[0]
	}
	mergeTable.SoftGC(gcTable, gCkp.GetEnd(), snapshots)
	for _, snapshot := range snapshots {
		for _, s := range snapshot {
			logutil.Infof("snapshot is %v", s.ToString())
		}
	}
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
		logutil.Errorf("processing clean %s: %v", candidates[0].String(), err)
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
	if maxGlobalCKP != nil && compareTS.Less(&maxEnd) {
		logutil.Infof("maxGlobalCKP is %v, compareTS is %v", maxGlobalCKP.String(), compareTS.ToString())
		data, err := c.collectGlobalCkpData(maxGlobalCKP)
		if err != nil {
			c.inputs.RUnlock()
			return
		}
		defer data.Close()
		err = c.tryGC(data, maxGlobalCKP)
		if err != nil {
			return
		}
	}
	err = c.mergeGCFile()
	if err != nil {
		// TODO: Error handle
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

func (c *checkpointCleaner) AddChecker(checker func(item any) bool) {
	c.checker.Lock()
	defer c.checker.Unlock()
	c.checker.extras = append(c.checker.extras, checker)
}

func (c *checkpointCleaner) createNewInput(
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
		c.updateSnapshot(data)
	}
	name := blockio.EncodeSnapshotMetadataFileName(GCMetaDir,
		PrefixSnapMeta, ckps[0].GetStart(), ckps[len(ckps)-1].GetEnd())
	err = c.snapshot.snapshotMeta.SaveMeta(name, c.fs.Service)
	if err != nil {
		logutil.Infof("SaveMeta is failed")
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
	c.snapshot.Lock()
	defer c.snapshot.Unlock()
	c.snapshot.snapshotMeta.Update(data)
	return nil
}

func (c *checkpointCleaner) GetSnapshots() (map[uint64][]types.TS, error) {
	c.snapshot.RLock()
	defer c.snapshot.RUnlock()
	snapshotList, err := c.snapshot.snapshotMeta.GetSnapshot(c.fs.Service)
	for tid, snapshot := range snapshotList {
		for _, s := range snapshot {
			logutil.Infof("GetSnapshots is %v, num is %d", s.ToString(), tid)
		}
	}
	return snapshotList, err
}
