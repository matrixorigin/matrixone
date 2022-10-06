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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

	BatchDedup(keys containers.Vector, skipfn func(row uint32) (err error)) (keyselects *roaring.Bitmap, err error)

	// BatchUpsert batch insert the specific keys
	// If any deduplication, it will fetch the old value first, fill the active map with new value, insert the old value into delete map
	// If any other unknown error hanppens, return error
	BatchUpsert(keysCtx *index.KeysCtx, offset int) (err error)

	GetActiveRow(key any) (row []uint32, err error)

	String() string
}
