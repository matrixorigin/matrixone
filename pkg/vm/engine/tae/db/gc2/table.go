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

package gc2

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"sync"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type ObjectEntry struct {
	commitTS types.TS
}

// GCTable is a data structure in memory after consuming checkpoint
type GCTable struct {
	sync.Mutex
	objects map[string]*ObjectEntry
}

func NewGCTable() *GCTable {
	table := GCTable{
		objects: make(map[string]*ObjectEntry),
	}
	return &table
}

func (t *GCTable) addObject(name string, commitTS types.TS) {
	t.Lock()
	defer t.Unlock()
	object := t.objects[name]
	if object == nil {
		object = &ObjectEntry{
			commitTS: commitTS,
		}
		t.objects[name] = object
		return
	}
	if object.commitTS.Less(commitTS) {
		t.objects[name].commitTS = commitTS
	}
}

func (t *GCTable) deleteObject(name string) {
	t.Lock()
	defer t.Unlock()
	delete(t.objects, name)
}

// Merge can merge two GCTables
func (t *GCTable) Merge(GCTable *GCTable) {
	for name, entry := range GCTable.objects {
		t.addObject(name, entry.commitTS)
	}
}

func (t *GCTable) getObjects() map[string]*ObjectEntry {
	t.Lock()
	defer t.Unlock()
	return t.objects
}

// SoftGC is to remove objectentry that can be deleted from GCTable
func (t *GCTable) SoftGC(table *GCTable, ts types.TS) []string {
	gc := make([]string, 0)
	objects := t.getObjects()
	for name, entry := range objects {
		objectEntry := table.objects[name]
		if objectEntry == nil && entry.commitTS.Less(ts) {
			gc = append(gc, name)
			t.deleteObject(name)
		}
	}
	return gc
}

func (t *GCTable) UpdateTable(data *logtail.CheckpointData) {
	ins, _, del, _ := data.GetBlkBatchs()
	insMetaObjectVec := ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	insDeltaObjectVec := ins.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	insCommitTSVec := ins.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()

	delMetaObjectVec := del.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).GetDownstreamVector()
	delDeltaObjectVec := del.GetVectorByName(pkgcatalog.BlockMeta_DeltaLoc).GetDownstreamVector()
	delCommitTSVec := del.GetVectorByName(pkgcatalog.BlockMeta_CommitTs).GetDownstreamVector()
	for i := 0; i < insMetaObjectVec.Length(); i++ {
		metaLoc := objectio.Location(vector.GetFixedAt[[]byte](insMetaObjectVec, i))
		deltaLoc := objectio.Location(vector.GetFixedAt[[]byte](insDeltaObjectVec, i))
		commitTS := vector.GetFixedAt[types.TS](insCommitTSVec, i)
		if !metaLoc.IsEmpty() {
			t.addObject(metaLoc.Name().String(), commitTS)
		}
		if !deltaLoc.IsEmpty() {
			t.addObject(deltaLoc.Name().String(), commitTS)
		}
	}
	for i := 0; i < del.Length(); i++ {
		metaLoc := objectio.Location(vector.GetFixedAt[[]byte](delMetaObjectVec, i))
		deltaLoc := objectio.Location(vector.GetFixedAt[[]byte](delDeltaObjectVec, i))
		commitTS := vector.GetFixedAt[types.TS](delCommitTSVec, i)
		if !metaLoc.IsEmpty() {
			t.addObject(metaLoc.Name().String(), commitTS)
		}
		if !deltaLoc.IsEmpty() {
			t.addObject(deltaLoc.Name().String(), commitTS)
		}
	}
}
