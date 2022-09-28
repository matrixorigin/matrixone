// Copyright 2022 Matrix Origin
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

package indexwrapper

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func TranslateError(err error) error {
	if err == nil {
		return err
	}
	if err == index.ErrDuplicate {
		return moerr.NewDuplicate()
	}
	if err == index.ErrNotFound {
		return moerr.NewNotFound()
	}
	if err == index.ErrWrongType {
		return moerr.NewInternalError("wrong type")
	}
	return err
}

type Index interface {
	io.Closer
	Destroy() error

	// Dedup returns wether the specified key is existed
	// If key is existed, return ErrDuplicate
	// If any other unknown error happens, return error
	// If key is not found, return nil
	Dedup(key any) error

	BatchDedup(keys containers.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error)

	// BatchUpsert batch insert the specific keys
	// If any deduplication, it will fetch the old value first, fill the active map with new value, insert the old value into delete map
	// If any other unknown error hanppens, return error
	BatchUpsert(keysCtx *index.KeysCtx, offset int, ts types.TS) (*index.BatchResp, error)

	// Delete delete the specific key
	// If the specified key not found in active map, return ErrNotFound
	// If any other error happens, return error
	// Delete the specific key from active map and then insert it into delete map
	Delete(key any, ts types.TS) error
	GetActiveRow(key any) (row uint32, err error)
	// Check deletes map for specified key @ts
	// If deleted is true, the specified key was deleted @ts
	// If existed is false, the specified key was not found in deletes map
	IsKeyDeleted(key any, ts types.TS) (deleted bool, existed bool)
	HasDeleteFrom(key any, ts types.TS) bool
	GetMaxDeleteTS() types.TS

	// RevertUpsert(keys containers.Vector, ts types.TS) error

	String() string

	ReadFrom(data.Block, *catalog.ColDef, file.ColumnBlock) error
	WriteTo(data.Block) error
}

// what is defaultImpl? PANIC!
type defaultIndexImpl struct{}

func (idx *defaultIndexImpl) Close() error {
	panic("not supported")
}

func (idx *defaultIndexImpl) Destroy() error {
	panic("not supported")
}

func (idx *defaultIndexImpl) Dedup(key any) error {
	panic("not supported")
}

func (idx *defaultIndexImpl) BatchDedup(keys containers.Vector, rowmask *roaring.Bitmap) (keyselects *roaring.Bitmap, err error) {
	panic("not supported")
}

func (idx *defaultIndexImpl) BatchUpsert(keysCtx *index.KeysCtx, offset int, ts types.TS) (*index.BatchResp, error) {
	panic("not supported")
}

func (idx *defaultIndexImpl) Delete(key any, ts types.TS) error {
	panic("not supported")
}

func (idx *defaultIndexImpl) GetActiveRow(key any) (row uint32, err error) {
	panic("not supported")
}

func (idx *defaultIndexImpl) IsKeyDeleted(key any, ts types.TS) (deleted bool, existed bool) {
	panic("not supported")
}

func (idx *defaultIndexImpl) HasDeleteFrom(key any, fromts types.TS) bool {
	panic("not supported")
}

func (idx *defaultIndexImpl) GetMaxDeleteTS() types.TS {
	panic("not supported")
}

func (idx *defaultIndexImpl) String() string {
	panic("not supported")
}

func (idx *defaultIndexImpl) ReadFrom(_ data.Block, _ *catalog.ColDef, _ file.ColumnBlock) error {
	panic("not supported")
}

func (idx *defaultIndexImpl) WriteTo(_ data.Block) error {
	panic("not supported")
}
