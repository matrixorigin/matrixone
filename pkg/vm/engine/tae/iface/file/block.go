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

package file

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type Block interface {
	Base
	Sync() error
	ReadRows(meta string) uint32
	WriteDeletes(buf []byte) error
	ReadDeletes(buf []byte) error
	LoadDeletes() (*roaring.Bitmap, error)
	OpenColumn(colIdx int) (ColumnBlock, error)
	WriteBatch(bat *containers.Batch, ts types.TS) (objectio.BlockObject, error)
	GetWriter() objectio.Writer
	LoadBatch([]types.Type, []string, []bool, *containers.Options) (bat *containers.Batch, err error)
	GetMeta() objectio.BlockObject
	GetMetaFormKey(location string) objectio.BlockObject
	Destroy() error
}

type ColumnBlock interface {
	io.Closer
	GetDataObject(location string) objectio.ColumnObject
	GetData(metaLoc string, NullAbility bool, typ types.Type, _ *bytes.Buffer) (vec containers.Vector, err error)
}
