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

package engine

import (
	"bytes"
	catalog3 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
	"sync"
)

// aoe engine
type aoeEngine struct {
	catalog *catalog3.Catalog
}

type SegmentInfo struct {
	Version  uint64
	Id       string
	GroupId  string
	TabletId string
	Node     engine.Node
}

type aoeReader struct {
	zs     []int64
	cds    []*bytes.Buffer
	dds    []*bytes.Buffer
	blocks []aoe.Block
	reader *store
	id 		int
}

type store struct {
	rel	   		*relation
	readers 	[]engine.Reader
	rhs    		chan *batch.Batch
	blocks 		[]aoe.Block
	workers		int
	start    	bool
	mu      sync.RWMutex
	enqueue int64
	dequeue int64
}

type worker struct {
	id 			int32
	zs     		[]int64
	cds    		[]*bytes.Buffer
	dds    		[]*bytes.Buffer
	blocks 		[]aoe.Block
	storeReader *store
}

type AoeSparseFilter struct {
	storeReader 		*store
	reader 				*aoeReader
}

type database struct {
	id      uint64            //id of the database
	typ     int               //type of the database
	catalog *catalog3.Catalog //the catalog of the aoeEngine
}

type relation struct {
	pid      uint64            //database id
	tbl      *aoe.TableInfo    //table of the tablets
	catalog  *catalog3.Catalog //the catalog
	nodes    engine.Nodes
	segments []SegmentInfo           //segments of the table
	tablets  []aoe.TabletInfo        //tablets of the table
	mp       map[string]*aoedb.Relation //a map of each tablet and its relation
	reader	 *store
}
