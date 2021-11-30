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

package dedup

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
)

const (
	UnitLimit = 256
)

const (
	H8 = iota
	H16
	H24
	H32
	HStr
)

type Container struct {
	typ      int
	rows     uint64
	inserts  []uint8
	zinserts []uint8
	hashs    []uint64
	values   []*uint64
	h8       struct {
		keys  []uint64
		zkeys []uint64
		ht    *hashtable.Int64HashMap
	}
	/*
		h16 struct {
			keys  [][2]uint64
			zkeys [][2]uint64
			ht    *hashtable.String16HashTable
		}
		h24 struct {
			keys  [][3]uint64
			zkeys [][3]uint64
			ht    *hashtable.String24HashTable
		}
		h32 struct {
			keys  [][4]uint64
			zkeys [][4]uint64
			ht    *hashtable.String32HashTable
		}
	*/
	hstr struct {
		keys [][]byte
		ht   *hashtable.StringHashMap
	}
	bat *batch.Batch
}

type Argument struct {
	ctr *Container
}
