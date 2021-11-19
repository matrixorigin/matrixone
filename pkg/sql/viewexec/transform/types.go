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

package transform

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/hashtable"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/viewexec/transformer"
)

const (
	UnitLimit = 256
)

const (
	Bare = iota
	BoundVars
	FreeVarsAndBoundVars
)

const (
	H8 = iota
	H16
	H24
	HStr
)

type Container struct {
	n       int
	typ     int
	rows    uint64
	is      []int
	vars    []string
	key     []byte
	inserts []bool
	hashs   []uint64
	values  []*uint64
	h8      struct {
		keys  []uint64
		zkeys []uint64
		sizes []int
	}
	h16 struct {
		keys [][2]uint64
	}
	h24 struct {
		keys [][3]uint64
	}
	bat *batch.Batch
	mp  *hashtable.MockStringHashTable
}

type Argument struct {
	Typ        int
	IsMerge    bool
	FreeVars   []string // free variables
	ctr        *Container
	Restrict   *restrict.Argument
	Projection *projection.Argument
	BoundVars  []transformer.Transformer // bound variables
}
