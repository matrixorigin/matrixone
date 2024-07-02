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

package catalog

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/tidwall/btree"
)

const (
	ObjectState_Create_Active uint8 = iota
	ObjectState_Create_PrepareCommit
	ObjectState_Create_ApplyCommit
	ObjectState_Delete_Active
	ObjectState_Delete_PrepareCommit
	ObjectState_Delete_ApplyCommit
)

type ObjectList struct {
	*sync.RWMutex
	*btree.BTreeG[ObjectEntry]
}

func NewObjectList() *ObjectList {
	panic("todo")
	// opts := btree.Options{
	// 	Degree: 4,
	// }
}

func (l *ObjectList) Delete(id objectio.ObjectId) {
	panic("todo")
}

func (l *ObjectList) Copy() *ObjectList {
	panic("todo")
}

func (l *ObjectList) TryGetObject() *ObjectEntry {
	panic("todo")
}

func (l *ObjectList) GetObjectByID(id *types.Objectid) (obj *ObjectEntry, err error) {
	panic("todo")
}

func (l *ObjectList) Insert(obj ObjectEntry) {
	panic("todo")
}

func (l *ObjectList) deleteEntryLocked(obj *objectio.ObjectId) error {
	panic("todo")
}
