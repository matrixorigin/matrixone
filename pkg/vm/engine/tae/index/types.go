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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

var (
	ErrNotFound  = moerr.NewInternalErrorNoCtx("tae index: key not found")
	ErrDuplicate = moerr.NewInternalErrorNoCtx("tae index: key duplicate")
	ErrPrefix    = moerr.NewInternalErrorNoCtx("tae index: prefix filter error")
)

const (
	BF = iota
	PBF
	HBF
)

type PrefixFn struct {
	Id uint8
	Fn func([]byte) []byte
}

type SecondaryIndex interface {
	Insert(key []byte, offset uint32) (err error)
	BatchInsert(keys *vector.Vector, offset, length int, startRow uint32) (err error)
	Delete(key any) (old uint32, err error)
	Search(key []byte) ([]uint32, error)
	String() string
	Size() int
}

type StaticFilter interface {
	MayContainsKey(key []byte) (bool, error)
	MayContainsAnyKeys(keys containers.Vector) (bool, *nulls.Bitmap, error)
	MayContainsAny(keys *vector.Vector, lowerBound int, upperBound int) bool

	PrefixMayContainsKey(key []byte, prefixFnId uint8, level uint8) (bool, error)
	PrefixMayContainsAny(
		keys *vector.Vector, lowerBound int, upperBound int, prefixFnId uint8, level uint8,
	) bool

	Marshal() ([]byte, error)
	Unmarshal(buf []byte) error
	String() string
	PrefixFnId(level uint8) uint8
	GetType() uint8
	MaxLevel() uint8
}
