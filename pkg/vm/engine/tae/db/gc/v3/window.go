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
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

func WithWindowDir(dir string) WindowOption {
	return func(table *GCWindow) {
		table.dir = dir
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
	if window.dir == "" {
		window.dir = ioutil.GetGCDir()
	}
	return &window
}

type GCWindow struct {
	dir string
	mp  *mpool.MPool
	fs  fileservice.FileService

	files []objectio.ObjectStats

	tsRange struct {
		start types.TS
		end   types.TS
	}
}

func (w *GCWindow) GetObjectStats() []objectio.ObjectStats {
	return w.files
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
	return readutil.SimpleMultiObjectsReader(
		ctx,
		fs,
		w.files,
		timestamp.Timestamp{},
		readutil.WithColumns(
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
	snapshots *logtail.SnapshotInfo,
	pitrs *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	iscpTables map[uint64]types.TS,
	checkpointCli checkpoint.Runner,
	buffer *containers.OneSchemaBatchBuffer,
	cacheSize int,
	estimateRows int,
	probility float64,
	mp *mpool.MPool,
	fs fileservice.FileService,
) ([]string, string, error) {
	// Record memory usage at start
	v2.GCMemoryObjectsGauge.Set(float64(len(w.files)))

	sourcer := w.MakeFilesReader(ctx, fs)

	gcTS := gCkp.GetEnd()
	job := NewCheckpointBasedGCJob(
		&gcTS,
		gCkp.GetLocation(),
		gCkp.GetVersion(),
		sourcer,
		pitrs,
		snapshots,
		iscpTables,
		snapshotMeta,
		checkpointCli,
		buffer,
		false,
		mp,
		fs,
		WithGCJobCoarseConfig(estimateRows, probility, cacheSize),
	)
	defer job.Close()

	if err := job.Execute(ctx); err != nil {
		return nil, "", err
	}

	vecToGC, filesNotGC := job.Result()
	defer vecToGC.Free(w.mp)
	var metaFile string
	var err error
	var bf *bloomfilter.BloomFilter
	if metaFile, err = w.writeMetaForRemainings(
		ctx, filesNotGC,
	); err != nil {
		return nil, "", err
	}
	w.files = filesNotGC
	sourcer = w.MakeFilesReader(ctx, fs)
	bf, err = BuildBloomfilter(
		ctx,
		Default_Coarse_EstimateRows,
		Default_Coarse_Probility,
		0,
		sourcer.Read,
		buffer,
		w.mp,
		dataProcess,
	)
	if err != nil {
		return nil, "", err
	}
	// Use a map to deduplicate file names, as the same object may appear
	// multiple times in vecToGC (e.g., when referenced by multiple tables).
	// This avoids sending duplicate delete requests to S3.
	// Note: We use map instead of bloom filter because bloom filter's false positive
	// could cause file leaks (files that should be deleted but are skipped).
	filesToGCSet := make(map[string]struct{})
	bf.Test(vecToGC,
		func(exists bool, i int) {
			if !exists {
				filesToGCSet[string(vecToGC.GetBytesAt(i))] = struct{}{}
				return
			}
		})
	filesToGC := make([]string, 0, len(filesToGCSet))
	for file := range filesToGCSet {
		filesToGC = append(filesToGC, file)
	}
	return filesToGC, metaFile, nil
}

// ScanCheckpoints will load data from the `checkpointEntries` one by one and
// update `w.tsRange` and `w.files`
// At the end, it will save the metadata into a specified file as the finish of scan
func (w *GCWindow) ScanCheckpoints(
	ctx context.Context,
	checkpointEntries []*checkpoint.CheckpointEntry,
	getCkpReader func(context.Context, *checkpoint.CheckpointEntry) (*logtail.CKPReader, error),
	processCkpData func(*checkpoint.CheckpointEntry, *logtail.CKPReader) error,
	onScanDone func() error,
	buffer *containers.OneSchemaBatchBuffer,
) (metaFile string, err error) {
	if len(checkpointEntries) == 0 {
		return
	}
	start := checkpointEntries[0].GetStart()
	end := checkpointEntries[len(checkpointEntries)-1].GetEnd()
	getOneBatch := func(cxt context.Context, bat *batch.Batch, mp *mpool.MPool) (bool, error) {
		select {
		case <-cxt.Done():
			return false, context.Cause(cxt)
		default:
		}
		if len(checkpointEntries) == 0 {
			return true, nil
		}
		ckpReader, err := getCkpReader(ctx, checkpointEntries[0])
		if err != nil {
			return false, err
		}
		if processCkpData != nil {
			if err = processCkpData(checkpointEntries[0], ckpReader); err != nil {
				return false, err
			}
		}
		objects := make(map[string]map[uint64]*ObjectEntry)
		collectObjectsFromCheckpointData(ctx, ckpReader, objects)
		if err = collectMapData(objects, bat, mp); err != nil {
			return false, err
		}
		checkpointEntries = checkpointEntries[1:]
		return false, nil
	}
	sinker := w.getSinker(0, buffer)
	defer sinker.Close()
	if err = readutil.StreamBatchProcess(
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

func (w *GCWindow) getSinker(
	tailSize int,
	buffer *containers.OneSchemaBatchBuffer,
) *ioutil.Sinker {
	return ioutil.NewSinker(
		ObjectTablePrimaryKeyIdx,
		ObjectTableAttrs,
		ObjectTableTypes,
		FSinkerFactory,
		w.mp,
		w.fs,
		ioutil.WithTailSizeCap(tailSize),
		ioutil.WithBuffer(buffer, false),
	)
}

func (w *GCWindow) writeMetaForRemainings(
	ctx context.Context,
	stats []objectio.ObjectStats,
) (string, error) {
	select {
	case <-ctx.Done():
		return "", context.Cause(ctx)
	default:
	}
	name := ioutil.EncodeGCMetadataName(w.tsRange.start, w.tsRange.end)
	ret := batch.NewWithSchema(
		false, ObjectTableMetaAttrs, ObjectTableMetaTypes,
	)
	defer ret.Clean(w.mp)
	for _, s := range stats {
		if err := vector.AppendBytes(
			ret.GetVector(0), s[:], false, w.mp,
		); err != nil {
			return "", err
		}
	}
	writer, err := objectio.NewObjectWriterSpecial(
		objectio.WriterGC,
		ioutil.MakeFullName(w.dir, name),
		w.fs,
	)
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
	w.files = append(w.files, o.files...)
	if w.tsRange.start.IsEmpty() && w.tsRange.end.IsEmpty() {
		w.tsRange.start = o.tsRange.start
		w.tsRange.end = o.tsRange.end
		return
	}

	if w.tsRange.start.GT(&o.tsRange.start) {
		w.tsRange.start = o.tsRange.start
	}
	if w.tsRange.end.LT(&o.tsRange.end) {
		w.tsRange.end = o.tsRange.end
	}
}

func collectObjectsFromCheckpointData(ctx context.Context, ckpReader *logtail.CKPReader, objects map[string]map[uint64]*ObjectEntry) {
	ckpReader.ForEachRow(
		ctx,
		func(
			account uint32,
			dbid, tid uint64,
			objectType int8,
			stats objectio.ObjectStats,
			createTS, deleteTS types.TS,
			rowID types.Rowid,
		) error {
			name := stats.ObjectName().String()
			if objects[name] == nil {
				objects[name] = make(map[uint64]*ObjectEntry)
			}
			object := &ObjectEntry{
				stats:    &stats,
				createTS: createTS,
				dropTS:   deleteTS,
				db:       dbid,
				table:    tid,
			}
			objects[name][tid] = object
			return nil
		},
	)
}

func (w *GCWindow) Close() {
	w.files = nil
}

// collectData collects data from memory that can be written to s3
func collectMapData(
	objects map[string]map[uint64]*ObjectEntry,
	bat *batch.Batch,
	mp *mpool.MPool,
) error {
	if len(objects) == 0 {
		return nil
	}
	rows := 0
	for _, tables := range objects {
		for _, entry := range tables {
			err := addObjectToBatch(bat, entry.stats, entry, mp)
			if err != nil {
				return err
			}
			rows++
		}
	}
	batch.SetLength(bat, rows)
	return nil
}

func (w *GCWindow) sortOneBatch(
	ctx context.Context,
	data *batch.Batch,
	mp *mpool.MPool,
) error {
	if err := mergeutil.SortColumnsByIndex(
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
	err := loader(ctx, w.fs, &w.files[0], bat, mp)
	logger := logutil.Info
	if err != nil {
		logger = logutil.Error
	}
	logger(
		"GCWindow-LoadBatchData",
		zap.Int("cnt", len(w.files)),
		zap.String("file", w.files[0].ObjectName().String()),
		zap.Error(err),
	)
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
		location := stats.ObjectLocation()
		location.SetID(uint16(id))
		data, _, err := ioutil.LoadOneBlock(cxt, fs, location, objectio.SchemaData)
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
	reader *ioutil.BlockReader) (*batch.Batch, func(), error) {
	idxes := []uint16{0}
	bat, release, err := reader.LoadColumns(ctx, idxes, nil, bs[0].GetID(), w.mp)
	if err != nil {
		return nil, nil, err
	}
	return bat, release, nil
}

// ReadTable reads an s3 file and replays a GCWindow in memory
func (w *GCWindow) ReadTable(ctx context.Context, name string, fs fileservice.FileService) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}

	var release1 func()
	var buffer *batch.Batch
	defer func() {
		if release1 != nil {
			release1()
		}
	}()
	meta := ioutil.DecodeGCMetadataName(name)
	w.tsRange.start = *meta.GetStart()
	w.tsRange.end = *meta.GetEnd()
	reader, err := ioutil.NewFileReaderNoCache(fs, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(ctx, w.mp)
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
				logutil.Error(
					"GCWindow-Compre-Err",
					zap.Error(err),
				)
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
		_, _ = buf.WriteString(fmt.Sprintf("name: %s, createTS: %v dropTS: %v",
			name, entry.createTS.ToString(), entry.dropTS.ToString()))
	}
	_, _ = buf.WriteString("]\n")
	return buf.String()
}

type TableStats struct {
	SharedCnt  uint64
	SharedSize uint64
	TotalCnt   uint64
	TotalSize  uint64
}

func (w *GCWindow) Details(ctx context.Context, snapshotMeta *logtail.SnapshotMeta, mp *mpool.MPool) (map[uint32]*TableStats, error) {
	buffer := MakeGCWindowBuffer(16 * mpool.MB)
	defer buffer.Close(mp)
	bat := buffer.Fetch()
	defer buffer.Putback(bat, mp)
	sourcer := w.MakeFilesReader(ctx, w.fs)

	details := make(map[uint32]*TableStats)
	objects := make(map[string]map[uint64]*ObjectEntry)
	for {
		bat.CleanOnlyData()
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}
		done, err := sourcer.Read(ctx, bat.Attrs, nil, mp, bat)
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		dropTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		nameVec := vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(i))
			name := stats.ObjectName().String()
			if objects[name] == nil {
				objects[name] = make(map[uint64]*ObjectEntry)
			}
			object := &ObjectEntry{
				stats:    &stats,
				createTS: createTSs[i],
				dropTS:   dropTSs[i],
				db:       dbs[i],
				table:    tableIDs[i],
			}
			objects[name][tableIDs[i]] = object
		}
		defer nameVec.Free(mp)
	}

	for _, tables := range objects {
		if len(tables) > 1 {
			shard := false
			for tid, entry := range tables {
				accountID, ok := snapshotMeta.GetAccountId(tid)
				if !ok {
					continue
				}
				if details[accountID] == nil {
					details[accountID] = &TableStats{
						SharedCnt:  1,
						SharedSize: uint64(entry.stats.Size()),
						TotalCnt:   1,
						TotalSize:  uint64(entry.stats.Size()),
					}
					shard = true
					continue
				}
				if !shard {
					details[accountID].SharedSize += uint64(entry.stats.Size())
					shard = true
				}
				details[accountID].SharedCnt++
				details[accountID].TotalCnt++
				details[accountID].TotalSize += uint64(entry.stats.Size())
			}

			continue
		}
		if snapshotMeta == nil {
			continue
		}
		for tid, entry := range tables {
			accountID, ok := snapshotMeta.GetAccountId(tid)
			if !ok {
				continue
			}
			if details[accountID] == nil {
				details[accountID] = &TableStats{
					TotalCnt:  1,
					TotalSize: uint64(entry.stats.Size()),
				}
				continue
			}
			details[accountID].TotalCnt++
			details[accountID].TotalSize += uint64(entry.stats.Size())
		}
	}
	return details, nil
}
