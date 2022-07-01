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
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

type Block interface {
	Base
	Sync() error
	// IsAppendable() bool
	WriteTS(ts uint64) error
	ReadTS() (uint64, error)
	WriteRows(rows uint32) error
	ReadRows() uint32

	// OpenDeletesFile() common.IRWFile
	WriteDeletes(buf []byte) error
	ReadDeletes(buf []byte) error
	GetDeletesFileStat() common.FileInfo
	LoadDeletes() (*roaring.Bitmap, error)

	LoadUpdates() (map[uint16]*roaring.Bitmap, map[uint16]map[uint32]any)

	LoadIndexMeta() (any, error)
	WriteIndexMeta(buf []byte) (err error)

	OpenColumn(colIdx int) (ColumnBlock, error)
	// WriteColumn(colIdx int, ts uint64, data []byte, updates []byte) (common.IVFile, error)

	// TODO: Remove later
	WriteSnapshot(bat *containers.Batch, ts uint64, masks map[uint16]*roaring.Bitmap,
		vals map[uint16]map[uint32]any, deletes *roaring.Bitmap) error
	WriteBatch(bat *containers.Batch, ts uint64) error
	LoadBatch([]types.Type, []string, []bool, *containers.Options) (bat *containers.Batch, err error)
	WriteColumnVec(ts uint64, colIdx int, vec containers.Vector) error

	Destroy() error
}

type ColumnBlock interface {
	io.Closer
	WriteTS(ts uint64) error
	WriteData(buf []byte) error
	WriteIndex(idx int, buf []byte) error
	WriteUpdates(buf []byte) error

	ReadTS() uint64
	ReadData(buf []byte) error
	ReadIndex(idx int, buf []byte) error
	ReadUpdates(buf []byte) error

	GetDataFileStat() common.FileInfo

	OpenUpdateFile() (common.IRWFile, error)
	OpenIndexFile(idx int) (common.IRWFile, error)
	OpenDataFile() (common.IRWFile, error)
}
