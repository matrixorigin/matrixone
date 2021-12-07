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

package plus

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
)

const (
	Fill = iota
	Eval
)

const (
	UnitLimit = 256
)

const (
	H8 = iota
	H24
	H32
	H40
	HStr
)

const (
	Bare = iota
	BoundVars
	FreeVarsAndBoundVars
)

type Container struct {
	state    int
	typ      int
	rows     uint64
	vars     []string
	keyOffs  []uint32
	zKeyOffs []uint32
	inserts  []uint8
	zinserts []uint8
	hashs    []uint64
	values   []*uint64
	h8       struct {
		keys  []uint64
		zKeys []uint64
		ht    *hashtable.Int64HashMap
	}
	h24 struct {
		keys  [][3]uint64
		zKeys [][3]uint64
		ht    *hashtable.String24HashMap
	}
	h32 struct {
		keys  [][4]uint64
		zKeys [][4]uint64
		ht    *hashtable.String32HashMap
	}
	h40 struct {
		keys  [][5]uint64
		zKeys [][5]uint64
		ht    *hashtable.String40HashMap
	}
	hstr struct {
		ht *hashtable.StringHashMap
	}
	bat *batch.Batch
}

type Argument struct {
	Typ int
	ctr *Container
}
