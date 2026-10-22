// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

type scanFaultFS struct {
	fileservice.FileService
	readFault, writeFault func(string) error
}

func (fs *scanFaultFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	if fs.readFault != nil {
		if err := fs.readFault(v.FilePath); err != nil {
			return err
		}
	}
	return fs.FileService.Read(ctx, v)
}

func (fs *scanFaultFS) Write(ctx context.Context, v fileservice.IOVector) error {
	if fs.writeFault != nil {
		if err := fs.writeFault(v.FilePath); err != nil {
			return err
		}
	}
	return fs.FileService.Write(ctx, v)
}

type scanCheckpointRunner struct {
	checkpoint.Runner
	entries []*checkpoint.CheckpointEntry
}

func (r *scanCheckpointRunner) ICKPSeekLT(ts types.TS, limit int) []*checkpoint.CheckpointEntry {
	var entries []*checkpoint.CheckpointEntry
	for _, entry := range r.entries {
		start := entry.GetStart()
		if start.GE(&ts) && len(entries) < limit {
			entries = append(entries, entry)
		}
	}
	return entries
}

// One real appendable catalog row, including the storage commit/abort columns.
func writeScanCatalogRow(t *testing.T, fs fileservice.FileService, mp *mpool.MPool, tid uint64, ts int64) objectio.ObjectStats {
	t.Helper()
	bat := batch.NewWithSize(len(catalog.MoTablesTypes) + 2)
	defer bat.Clean(mp)
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(1)
	packer.EncodeStringType([]byte("gc_scan"))
	packer.EncodeStringType([]byte(fmt.Sprintf("t%d", tid)))
	seqnums := make([]uint16, len(bat.Vecs))
	for i, typ := range catalog.MoTablesTypes {
		seqnums[i] = uint16(i)
		bat.Vecs[i] = vector.NewVec(typ)
		var value any
		switch i {
		case 0:
			value = tid
		case 1:
			value = []byte(fmt.Sprintf("t%d", tid))
		case 2:
			value = []byte("gc_scan")
		case 3:
			value = uint64(2)
		case 11:
			value = uint32(1)
		case catalog.MO_TABLES_CPKEY_IDX:
			value = packer.Bytes()
		}
		require.NoError(t, vector.AppendAny(bat.Vecs[i], value, value == nil, mp))
	}
	n := len(catalog.MoTablesTypes)
	seqnums[n], seqnums[n+1] = objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT
	bat.Vecs[n], bat.Vecs[n+1] = vector.NewVec(objectio.TSType), vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[n], types.BuildTS(ts, 0), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[n+1], false, false, mp))
	bat.SetRowCount(1)
	writer := ioutil.ConstructWriter(0, seqnums, -1, false, false, fs)
	writer.SetAppendable()
	_, err := writer.WriteBatch(bat)
	require.NoError(t, err)
	_, _, err = writer.Sync(t.Context())
	require.NoError(t, err)
	return writer.GetObjectStats(objectio.WithAppendable())
}

func writeScanCheckpoint(t *testing.T, fs fileservice.FileService, mp *mpool.MPool, start, end int64, objects ...objectio.ObjectStats) *checkpoint.CheckpointEntry {
	t.Helper()
	bat := ckputil.NewObjectListBatch()
	defer bat.Clean(mp)
	packer := types.NewPacker()
	defer packer.Close()
	for i, object := range objects {
		packer.Reset()
		ckputil.EncodeCluser(packer, catalog.MO_TABLES_ID, ckputil.ObjectType_Data, object.ObjectName().ObjectId(), false)
		values := []any{uint32(0), uint64(catalog.MO_CATALOG_ID), uint64(catalog.MO_TABLES_ID), ckputil.ObjectType_Data,
			object[:], types.BuildTS(start+int64(i)+1, 0), types.TS{}, packer.Bytes()}
		for col, value := range values {
			require.NoError(t, vector.AppendAny(bat.Vecs[col], value, false, mp))
		}
	}
	bat.SetRowCount(len(objects))
	sinker := ckputil.NewDataSinker(mp, fs, ioutil.WithMemorySizeThreshold(mpool.MB))
	defer sinker.Close()
	require.NoError(t, sinker.Write(t.Context(), bat))
	data := logtail.NewCheckpointDataWithSinker(sinker, mp)
	location, _, err := data.Sync(t.Context(), fs)
	require.NoError(t, err)
	entry := createMockCheckpointEntry(start, end)
	entry.SetLocation(location, location)
	return entry
}

// Inspect only the indexes whose duplicate history/alias contract public getters
// cannot express. This avoids adding production inspection APIs for this test.
func scanTableHistory(meta *logtail.SnapshotMeta, tid uint64) (int, bool) {
	state := reflect.ValueOf(meta).Elem()
	info := state.FieldByName("tableIDIndex").MapIndex(reflect.ValueOf(tid))
	if !info.IsValid() {
		return 0, false
	}
	pk := info.Elem().FieldByName("pk").String()
	history := state.FieldByName("tablePKIndex").MapIndex(reflect.ValueOf(pk))
	account := state.FieldByName("tables").MapIndex(reflect.ValueOf(uint32(1)))
	return history.Len(), info.Pointer() == account.MapIndex(reflect.ValueOf(tid)).Pointer() &&
		info.Pointer() == history.Index(history.Len()-1).Pointer()
}

func TestCheckpointScanFailureReplay(t *testing.T) {
	for _, stage := range []string{"after_update", "later_checkpoint", "inside_update"} {
		t.Run(stage, func(t *testing.T) {
			ctx := t.Context()
			baseFS, err := fileservice.NewFileService(ctx, fileservice.Config{
				Name: "gc-scan", Backend: "DISK", DataDir: t.TempDir(), Cache: fileservice.DisabledCacheConfig,
			}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { baseFS.Close(context.Background()) })
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			a := writeScanCatalogRow(t, baseFS, mp, 1001, 50)
			b := writeScanCatalogRow(t, baseFS, mp, 1002, 150)
			entries := []*checkpoint.CheckpointEntry{
				writeScanCheckpoint(t, baseFS, mp, 1, 100, a),
				writeScanCheckpoint(t, baseFS, mp, 100, 200, b),
			}
			if stage == "inside_update" {
				entries = []*checkpoint.CheckpointEntry{writeScanCheckpoint(t, baseFS, mp, 1, 200, a, b)}
			}
			fs := &scanFaultFS{FileService: baseFS}
			c := NewCheckpointCleaner(ctx, "gc-scan", fs, nil, &scanCheckpointRunner{entries: entries}, WithMaxScanCheckpointCount(2)).(*checkpointCleaner)
			c.mp = mp
			t.Cleanup(func() {
				if c.mutation.scanned != nil {
					c.mutation.scanned.Close()
				}
				c.Stop()
			})
			seed := createMockCheckpointEntry(0, 1)
			c.watermarks.scanWaterMark.Store(seed)
			before := c.mutation.snapshotMeta.Clone()
			injected := errors.New("checkpoint scan storage failure")
			hits := 0
			published := false
			observePublished := func(tids ...uint64) {
				published = true
				for _, tid := range tids {
					n, coherent := scanTableHistory(c.mutation.snapshotMeta, tid)
					published = published && n == 1 && coherent
				}
			}
			switch stage {
			case "after_update":
				name := ioutil.MakeGCFullName(ioutil.EncodeAcctMetadataName(entries[0].GetStart(), entries[len(entries)-1].GetEnd()))
				fs.writeFault = func(path string) error {
					if path != name {
						return nil
					}
					observePublished(1001, 1002)
					hits++
					return injected
				}
			case "later_checkpoint":
				name := entries[1].GetLocation().Name().String()
				fs.readFault = func(path string) error {
					if path != name {
						return nil
					}
					observePublished(1001)
					hits++
					return injected
				}
			case "inside_update":
				fs.readFault = func(path string) error {
					if path != b.ObjectName().String() {
						return nil
					}
					// Update owns its write lock here; do not call locking getters.
					observePublished(1001)
					hits++
					return injected
				}
			}
			buffer := containers.NewOneSchemaBatchBuffer(mpool.MB, ObjectTableAttrs, ObjectTableTypes, false)
			defer buffer.Close(mp)
			c.mutation.Lock()
			defer c.mutation.Unlock()
			for attempt := 0; attempt < 2; attempt++ {
				if stage == "inside_update" {
					entry := entries[0]
					reader, readErr := logtail.GetCheckpointReader(ctx, c.sid, fs, entry.GetLocation(), entry.GetVersion())
					require.NoError(t, readErr)
					err = c.mutation.snapshotMeta.Update(ctx, fs, reader, entry.GetStart(), entry.GetEnd(), t.Name())
				} else {
					err, _ = c.tryScanLocked(ctx, buffer, func(*checkpoint.CheckpointEntry) bool { return true })
				}
				require.ErrorIs(t, err, injected)
				require.Equal(t, attempt+1, hits)
				require.True(t, published, "fault must occur after the earlier catalog update")
				require.Same(t, seed, c.GetScanWaterMark())
				require.Nil(t, c.mutation.scanned)
				require.Empty(t, c.mutation.metaFiles)
				require.Equal(t, before, c.mutation.snapshotMeta.Clone())
			}
			fs.readFault, fs.writeFault = nil, nil
			err, _ = c.tryScanLocked(ctx, buffer, func(*checkpoint.CheckpointEntry) bool { return true })
			require.NoError(t, err)
			require.Same(t, entries[len(entries)-1], c.GetScanWaterMark())
			require.NotNil(t, c.mutation.scanned)
			require.Len(t, c.mutation.metaFiles, 3)
			for _, tid := range []uint64{1001, 1002} {
				n, coherent := scanTableHistory(c.mutation.snapshotMeta, tid)
				require.Equal(t, 1, n)
				require.True(t, coherent)
			}
			after := c.mutation.snapshotMeta.Clone()
			err, _ = c.tryScanLocked(ctx, buffer, nil)
			require.NoError(t, err)
			require.Equal(t, after, c.mutation.snapshotMeta.Clone())
		})
	}
}
