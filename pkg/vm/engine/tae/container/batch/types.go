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

package batch

import (
	"errors"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dbi"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

var (
	BatNotFoundErr      = errors.New("not found error")
	BatAlreadyClosedErr = errors.New("already closed error")
)

type IBatch interface {
	dbi.IBatchReader
	GetVectorByAttr(attrId int) (vector.IVector, error)
    Marshal() ([]byte, error)
    Unmarshal([]byte) error
}

type Batch struct {
	sync.RWMutex
	AttrsMap   map[int]int
	Attrs      []int
	Vecs       []vector.IVector
	ClosedMask *roaring.Bitmap
}
