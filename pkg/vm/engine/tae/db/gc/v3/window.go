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
	"bytes"
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type ObjectEntry struct {
	stats    *objectio.ObjectStats
	createTS types.TS
	dropTS   types.TS
	db       uint64
	table    uint64
}

type WindowOption func(*GCWindow)

func WithMetaPrefix(prefix string) WindowOption {
	return func(table *GCWindow) {
		table.metaDir = prefix
	}
}

func NewGCWindow(
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...WindowOption,
) *GCWindow {
	window := GCWindow{
		mp: mp,
		fs: fs,
	}
	for _, opt := range opts {
		opt(&window)
	}
	if window.metaDir == "" {
		window.metaDir = GCMetaDir
	}
	return &window
}

type GCWindow struct {
	metaDir string
	mp      *mpool.MPool
	fs      fileservice.FileService

	files []objectio.ObjectStats

	tsRange struct {
		start types.TS
		end   types.TS
	}
}

func (w *GCWindow) Clone() GCWindow {
	w2 := *w
	w2.files = make([]objectio.ObjectStats, len(w.files))
	copy(w2.files, w.files)
	return w2
}

func (w *GCWindow) MakeFilesReader(
	ctx context.Context,
	fs fileservice.FileService,
) engine.Reader {
	return engine_util.SimpleMultiObjectsReader(
		ctx,
		fs,
		w.files,
		timestamp.Timestamp{},
		engine_util.WithColumns(
			ObjectTableSeqnums,
			ObjectTableTypes,
		),
	)
}

// ExecuteGlobalCheckpointBasedGC is to remove objectentry that can be deleted from GCWindow
// it will refresh the files in GCWindow with the files that can not be GC'ed
// it will return the files that could be GC'ed
func (w *GCWindow) ExecuteGlobalCheckpointBasedGC(
	ctx context.Context,
	gCkp *checkpoint.CheckpointEntry,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	buffer *containers.OneSchemaBatchBuffer,
	cacheSize int,
	mp *mpool.MPool,
	fs fileservice.FileService,
) ([]string, string, error) {

	sourcer := w.MakeFilesReader(ctx, fs)

	gcTS := gCkp.GetEnd()
	job := NewCheckpointBasedGCJob(
		&gcTS,
		gCkp.GetLocation(),
		sourcer,
		pitrs,
		accountSnapshots,
		snapshotMeta,
		buffer,
		false,
		mp,
		fs,
		WithGCJobCoarseConfig(0, 0, cacheSize),
	)
	defer job.Close()

	if err := job.Execute(ctx); err != nil {
		return nil, "", err
	}

	filesToGC, filesNotGC := job.Result()
	var metaFile string
	var err error
	if metaFile, err = w.writeMetaForRemainings(
		ctx, filesNotGC,
	); err != nil {
		return nil, "", err
	}

	w.files = filesNotGC
	return filesToGC, metaFile, nil
}

// ScanCheckpoints will load data from the `checkpointEntries` one by one and
// update `w.tsRange` and `w.files`
// At the end, it will save the metadata into a specified file as the finish of scan
func (w *GCWindow) ScanCheckpoints(
	ctx context.Context,
	checkpointEntries []*checkpoint.CheckpointEntry,
	collectCkpData func(*checkpoint.CheckpointEntry) (*logtail.CheckpointData, error),
	processCkpData func(*checkpoint.CheckpointEntry, *logtail.CheckpointData) error,
	onScanDone func() error,
	buffer *containers.OneSchemaBatchBuffer,
) (metaFile string, err error) {
	if len(checkpointEntries) == 0 {
		return
	}
	start := checkpointEntries[0].GetStart()
	end := checkpointEntries[len(checkpointEntries)-1].GetEnd()
	getOneBatch := func(cxt context.Context, bat *batch.Batch, mp *mpool.MPool) (bool, error) {
		if len(checkpointEntries) == 0 {
			return true, nil
		}
		data, err := collectCkpData(checkpointEntries[0])
		if err != nil {
			return false, err
		}
		if processCkpData != nil {
			if err = processCkpData(checkpointEntries[0], data); err != nil {
				return false, err
			}
		}
		objects := make(map[string]*ObjectEntry)
		collectObjectsFromCheckpointData(data, objects)
		if err = collectMapData(objects, bat, mp); err != nil {
			return false, err
		}
		checkpointEntries = checkpointEntries[1:]
		return false, nil
	}
	sinker := w.getSinker(0, buffer)
	defer sinker.Close()
	if err = engine_util.StreamBatchProcess(
		ctx,
		getOneBatch,
		w.sortOneBatch,
		sinker.Write,
		buffer,
		w.mp,
	); err != nil {
		logutil.Error(
			"GCWindow-createInput-SINK-ERROR",
			zap.Error(err),
		)
		return
	}

	if onScanDone != nil {
		if err = onScanDone(); err != nil {
			return
		}
	}

	if err = sinker.Sync(ctx); err != nil {
		return
	}

	w.tsRange.start = start
	w.tsRange.end = end
	newFiles, _ := sinker.GetResult()
	if metaFile, err = w.writeMetaForRemainings(
		ctx, newFiles,
	); err != nil {
		return
	}
	w.files = append(w.files, newFiles...)
	return metaFile, nil
}

func isSnapshotRefers(
	obj *objectio.ObjectStats,
	pitr, createTS, dropTS *types.TS,
	snapshots []types.TS,
) bool {
	// no snapshot and no pitr
	if len(snapshots) == 0 && (pitr == nil || pitr.IsEmpty()) {
		return false
	}

	// if dropTS is empty, it means the object is not dropped
	if dropTS.IsEmpty() {
		common.DoIfDebugEnabled(func() {
			logutil.Debug(
				"GCJOB-DEBUG-1",
				zap.String("obj", obj.ObjectName().String()),
				zap.String("create-ts", createTS.ToString()),
				zap.String("drop-ts", createTS.ToString()),
			)
		})
		return true
	}

	// if pitr is not empty, and pitr is greater than dropTS, it means the object is not dropped
	if pitr != nil && !pitr.IsEmpty() {
		if dropTS.GT(pitr) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"GCJOB-PITR-PIN",
					zap.String("name", obj.ObjectName().String()),
					zap.String("pitr", pitr.ToString()),
					zap.String("create-ts", createTS.ToString()),
					zap.String("drop-ts", dropTS.ToString()),
				)
			})
			return true
		}
	}

	left, right := 0, len(snapshots)-1
	for left <= right {
		mid := left + (right-left)/2
		snapTS := snapshots[mid]
		if snapTS.GE(createTS) && snapTS.LT(dropTS) {
			common.DoIfDebugEnabled(func() {
				logutil.Debug(
					"GCJOB-DEBUG-2",
					zap.String("name", obj.ObjectName().String()),
					zap.String("pitr", snapTS.ToString()),
					zap.String("create-ts", createTS.ToString()),
					zap.String("drop-ts", dropTS.ToString()),
				)
			})
			return true
		} else if snapTS.LT(createTS) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return false
}

func (w *GCWindow) getSinker(
	tailSize int,
	buffer *containers.OneSchemaBatchBuffer,
) *engine_util.Sinker {
	return engine_util.NewSinker(
		ObjectTablePrimaryKeyIdx,
		ObjectTableAttrs,
		ObjectTableTypes,
		FSinkerFactory,
		w.mp,
		w.fs,
		engine_util.WithTailSizeCap(tailSize),
		engine_util.WithBuffer(buffer, false),
	)
}

func (w *GCWindow) writeMetaForRemainings(
	ctx context.Context,
	stats []objectio.ObjectStats,
) (string, error) {
	name := blockio.EncodeGCMetadataFileName(PrefixGCMeta, w.tsRange.start, w.tsRange.end)
	ret := batch.NewWithSchema(
		false, false, ObjectTableMetaAttrs, ObjectTableMetaTypes,
	)
	defer ret.Clean(w.mp)
	for _, s := range stats {
		if err := vector.AppendBytes(
			ret.GetVector(0), s[:], false, w.mp,
		); err != nil {
			return "", err
		}
	}
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterGC, w.metaDir+name, w.fs)
	if err != nil {
		return "", err
	}
	if _, err := writer.WriteWithoutSeqnum(ret); err != nil {
		return "", err
	}
	_, err = writer.WriteEnd(ctx)
	return name, err
}

func (w *GCWindow) Merge(o *GCWindow) {
	if o == nil || (o.tsRange.start.IsEmpty() && o.tsRange.end.IsEmpty()) {
		return
	}
	if w.tsRange.start.IsEmpty() && w.tsRange.end.IsEmpty() {
		w.tsRange.start = o.tsRange.start
		w.tsRange.end = o.tsRange.end
		w.files = append(w.files, o.files...)
		return
	}

	for _, file := range o.files {
		w.files = append(w.files, file)
	}

	if w.tsRange.start.GT(&o.tsRange.start) {
		w.tsRange.start = o.tsRange.start
	}
	if w.tsRange.end.LT(&o.tsRange.end) {
		w.tsRange.end = o.tsRange.end
	}
}

func collectObjectsFromCheckpointData(data *logtail.CheckpointData, objects map[string]*ObjectEntry) {
	ins := data.GetObjectBatchs()
	insDeleteTSVec := ins.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	insCreateTSVec := ins.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	dbid := ins.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	tableID := ins.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()

	for i := 0; i < ins.Length(); i++ {
		buf := ins.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		stats := (objectio.ObjectStats)(buf)
		name := stats.ObjectName().String()
		deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](insDeleteTSVec, i)
		createTS := vector.GetFixedAtNoTypeCheck[types.TS](insCreateTSVec, i)
		object := &ObjectEntry{
			stats:    &stats,
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAtNoTypeCheck[uint64](dbid, i),
			table:    vector.GetFixedAtNoTypeCheck[uint64](tableID, i),
		}
		objects[name] = object
	}
	del := data.GetTombstoneObjectBatchs()
	delDeleteTSVec := del.GetVectorByName(catalog.EntryNode_DeleteAt).GetDownstreamVector()
	delCreateTSVec := del.GetVectorByName(catalog.EntryNode_CreateAt).GetDownstreamVector()
	delDbid := del.GetVectorByName(catalog.SnapshotAttr_DBID).GetDownstreamVector()
	delTableID := del.GetVectorByName(catalog.SnapshotAttr_TID).GetDownstreamVector()
	for i := 0; i < del.Length(); i++ {
		buf := del.GetVectorByName(catalog.ObjectAttr_ObjectStats).Get(i).([]byte)
		stats := (objectio.ObjectStats)(buf)
		name := stats.ObjectName().String()
		deleteTS := vector.GetFixedAtNoTypeCheck[types.TS](delDeleteTSVec, i)
		createTS := vector.GetFixedAtNoTypeCheck[types.TS](delCreateTSVec, i)
		object := &ObjectEntry{
			stats:    &stats,
			createTS: createTS,
			dropTS:   deleteTS,
			db:       vector.GetFixedAtNoTypeCheck[uint64](delDbid, i),
			table:    vector.GetFixedAtNoTypeCheck[uint64](delTableID, i),
		}
		objects[name] = object
	}
}

func (w *GCWindow) Close() {
	w.files = nil
}

// collectData collects data from memory that can be written to s3
func collectMapData(
	objects map[string]*ObjectEntry,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	if len(objects) == 0 {
		return nil
	}
	for _, entry := range objects {
		err := addObjectToBatch(bat, entry.stats, entry, mp)
		if err != nil {
			return err
		}
	}
	batch.SetLength(bat, len(objects))
	return nil
}

func (w *GCWindow) sortOneBatch(
	ctx context.Context,
	data *batch.Batch,
	mp *mpool.MPool,
) error {
	if err := mergesort.SortColumnsByIndex(
		data.Vecs,
		ObjectTablePrimaryKeyIdx,
		mp,
	); err != nil {
		return err
	}
	return nil
}

// collectData collects data from memory that can be written to s3
func (w *GCWindow) LoadBatchData(
	ctx context.Context,
	_ []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	if len(w.files) == 0 {
		return true, nil
	}
	bat.CleanOnlyData()
	pint := "LoadBatchData is "
	for _, s := range w.files {
		pint += s.ObjectName().String() + ";"
	}
	logutil.Infof(pint)
	err := loader(ctx, w.fs, &w.files[0], bat, mp)
	if err != nil {
		return false, err
	}
	w.files = w.files[1:]
	return false, nil
}

func loader(
	cxt context.Context,
	fs fileservice.FileService,
	stats *objectio.ObjectStats,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	for id := uint32(0); id < stats.BlkCnt(); id++ {
		stats.ObjectLocation().SetID(uint16(id))
		data, _, err := blockio.LoadOneBlock(cxt, fs, stats.ObjectLocation(), objectio.SchemaData)
		if err != nil {
			return err
		}
		bat.Append(cxt, mp, data)
	}
	return nil

}

func (w *GCWindow) rebuildTable(bat *batch.Batch) {
	for i := 0; i < bat.Vecs[0].Length(); i++ {
		var stats objectio.ObjectStats
		stats.UnMarshal(bat.Vecs[0].GetRawBytesAt(i))
		w.files = append(w.files, stats)
	}
}

func (w *GCWindow) replayData(
	ctx context.Context,
	bs []objectio.BlockObject,
	reader *blockio.BlockReader) (*batch.Batch, func(), error) {
	idxes := []uint16{0}
	bat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), common.DefaultAllocator)
	if err != nil {
		return nil, nil, err
	}
	return bat, release, nil
}

// ReadTable reads an s3 file and replays a GCWindow in memory
func (w *GCWindow) ReadTable(ctx context.Context, name string, fs *objectio.ObjectFS) error {
	var release1 func()
	var buffer *batch.Batch
	defer func() {
		if release1 != nil {
			release1()
		}
	}()
	start, end, _ := blockio.DecodeGCMetadataFileName(name)
	w.tsRange.start = start
	w.tsRange.end = end
	reader, err := blockio.NewFileReaderNoCache(fs.Service, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, common.DefaultAllocator)
	if err != nil {
		return err
	}
	buffer, release1, err = w.replayData(ctx, bs, reader)
	if err != nil {
		return err
	}
	w.rebuildTable(buffer)
	return nil
}

// For test

func (w *GCWindow) Compare(
	table *GCWindow,
	buffer *containers.OneSchemaBatchBuffer,
) (
	map[string]*ObjectEntry,
	map[string]*ObjectEntry,
	bool,
) {
	if buffer == nil {
		buffer = MakeGCWindowBuffer(mpool.MB)
		defer buffer.Close(w.mp)
	}
	bat := buffer.Fetch()
	defer buffer.Putback(bat, w.mp)

	objects := make(map[string]*ObjectEntry)
	objects2 := make(map[string]*ObjectEntry)

	buildObjects := func(table *GCWindow,
		objects map[string]*ObjectEntry,
		loadfn func(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error),
	) error {
		for {
			bat.CleanOnlyData()
			done, err := loadfn(context.Background(), nil, nil, w.mp, bat)
			if err != nil {
				logutil.Infof("load data error")
				return err
			}

			if done {
				break
			}

			createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
			deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
			dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
			tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
			for i := 0; i < bat.Vecs[0].Length(); i++ {
				buf := bat.Vecs[0].GetRawBytesAt(i)
				stats := (objectio.ObjectStats)(buf)
				name := stats.ObjectName().String()
				tableID := tableIDs[i]
				createTS := createTSs[i]
				dropTS := deleteTSs[i]
				object := &ObjectEntry{
					createTS: createTS,
					dropTS:   dropTS,
					db:       dbs[i],
					table:    tableID,
				}
				objects[name] = object
			}
		}
		return nil
	}
	buildObjects(w, objects, w.LoadBatchData)
	buildObjects(table, objects2, table.LoadBatchData)
	if !w.compareObjects(objects, objects2) {
		logutil.Infof("objects are not equal")
		return objects, objects2, false
	}
	return objects, objects2, true
}

func (w *GCWindow) compareObjects(objects, compareObjects map[string]*ObjectEntry) bool {
	for name, entry := range compareObjects {
		object := objects[name]
		if object == nil {
			logutil.Infof("object %s is nil, create %v, drop %v",
				name, entry.createTS.ToString(), entry.dropTS.ToString())
			return false
		}
		if !entry.createTS.Equal(&object.createTS) {
			logutil.Infof("object %s createTS is not equal", name)
			return false
		}
	}

	return len(compareObjects) == len(objects)
}

func (w *GCWindow) String(objects map[string]*ObjectEntry) string {
	if len(objects) == 0 {
		return ""
	}
	var buf bytes.Buffer
	_, _ = buf.WriteString("objects:[\n")
	for name, entry := range objects {
		_, _ = buf.WriteString(fmt.Sprintf("name: %s, createTS: %v ", name, entry.createTS.ToString()))
	}
	_, _ = buf.WriteString("]\n")
	return buf.String()
}
