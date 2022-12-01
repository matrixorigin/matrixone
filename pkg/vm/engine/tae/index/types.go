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

package index

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	// "github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var (
	ErrNotFound  = moerr.NewInternalErrorNoCtx("tae index: key not found")
	ErrDuplicate = moerr.NewInternalErrorNoCtx("tae index: key duplicate")
	ErrWrongType = moerr.NewInternalErrorNoCtx("tae index: wrong type")
)

type KeysCtx struct {
	Keys containers.Vector

	// Select the key where this bitmap indicates.
	// Nil to select all
	Selects *roaring.Bitmap
	// Select a continuous interval [Start, Start+Count) from keys
	Start, Count int

	// Whether need to verify Keys
	NeedVerify bool
}

func (ctx *KeysCtx) SelectAll() {
	ctx.Count = ctx.Keys.Length()
}

type BatchResp struct {
	UpdatedKeys *roaring.Bitmap
	UpdatedRows *roaring.Bitmap
}

type SecondaryIndex interface {
	Insert(key any, offset uint32) (err error)
	BatchInsert(keys *KeysCtx, startRow uint32) (err error)
	Delete(key any) (old uint32, err error)
	Search(key any) ([]uint32, error)
	String() string
	Size() int
}
