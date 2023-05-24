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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var (
	ErrNotFound  = moerr.NewInternalErrorNoCtx("tae index: key not found")
	ErrDuplicate = moerr.NewInternalErrorNoCtx("tae index: key duplicate")
)

type SecondaryIndex interface {
	Insert(key []byte, offset uint32) (err error)
	BatchInsert(keys containers.Vector, offset, length int, startRow uint32) (err error)
	Delete(key any) (old uint32, err error)
	Search(key []byte) ([]uint32, error)
	String() string
	Size() int
}
