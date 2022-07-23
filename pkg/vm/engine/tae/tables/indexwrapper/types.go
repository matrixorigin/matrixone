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
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func TranslateError(err error) error {
	if err == nil {
		return err
	}
	if err == index.ErrDuplicate {
		return data.ErrDuplicate
	}
	if err == index.ErrNotFound {
		return data.ErrNotFound
	}
	if err == index.ErrWrongType {
		return data.ErrWrongType
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
	BatchUpsert(keysCtx *index.KeysCtx, offset int, ts uint64) (*index.BatchResp, error)

	// Delete delete the specific key
	// If the specified key not found in active map, return ErrNotFound
	// If any other error happens, return error
	// Delete the specific key from active map and then insert it into delete map
	Delete(key any, ts uint64) error
	GetActiveRow(key any) (row uint32, err error)
	// Check deletes map for specified key @ts
	// If deleted is true, the specified key was deleted @ts
	// If existed is false, the specified key was not found in deletes map
	IsKeyDeleted(key any, ts uint64) (deleted bool, existed bool)
	HasDeleteFrom(key any, fromTs uint64) bool
	GetMaxDeleteTS() uint64

	// RevertUpsert(keys containers.Vector, ts uint64) error

	String() string

	ReadFrom(data.Block) error
	WriteTo(data.Block) error
}
