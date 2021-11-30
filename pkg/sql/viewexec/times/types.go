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

package times

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
)

const (
	End = iota
	Fill
	Probe
)

const (
	UnitLimit = 256
)

type view struct {
	isB  bool
	vis  []int
	ris  []int
	rows uint64
	rn   string
	key  string
	sels [][]int64
	typ  types.Type
	h8   struct {
		ht *hashtable.Int64HashMap
	}
	h24 struct {
		ht *hashtable.String24HashMap
	}
	h32 struct {
		ht *hashtable.String32HashMap
	}
	h40 struct {
		ht *hashtable.String40HashMap
	}
	hstr struct {
		ht *hashtable.StringHashMap
	}
	bat *batch.Batch
}

type Container struct {
	state    int
	is       []int
	rows     int64
	inserts  []uint8
	zinserts []uint8
	hashs    []uint64
	views    []*view
	vars     []string
	values   []*uint64
	zvalues  []*uint64
	h8       struct {
		keys []uint64
	}
	h16 struct {
		keys [][2]uint64
	}
	h24 struct {
		keys [][3]uint64
	}
	h32 struct {
		keys [][4]uint64
	}
	h40 struct {
		keys  [][5]uint64
		zkeys [][5]uint64
	}
	bat      *batch.Batch
	varsMap  map[string]uint8
	fvarsMap map[string]uint8
}

type Argument struct {
	IsBare   bool
	SisBares []bool
	R        string
	Rvars    []string
	Ss       []string
	Svars    []string
	ctr      *Container
	VarsMap  map[string]int
	Bats     []*batch.Batch
	Arg      *transform.Argument
}
