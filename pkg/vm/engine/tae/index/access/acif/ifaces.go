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

package acif

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type IAppendableBlockIndexHolder interface {
	IBlockIndexHolder
	BatchInsert(keys *vector.Vector, start uint32, count int, offset uint32, verify bool) error
	Delete(key interface{}) error
	Search(key interface{}) (uint32, error)
	//Upgrade() (INonAppendableBlockIndexHolder, error)
	BatchDedup(keys *vector.Vector) error
}

type INonAppendableBlockIndexHolder interface {
	IBlockIndexHolder
	MayContainsKey(key interface{}) bool
	MayContainsAnyKeys(keys *vector.Vector) (error, *roaring.Bitmap)
	InitFromHost(host data.Block, schema *catalog.Schema, bufManager base.INodeManager) error
}

type IBlockIndexHolder interface {
	GetHostBlockId() uint64
	Destroy() error
}
