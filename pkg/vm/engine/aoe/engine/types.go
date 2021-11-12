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
	catalog3 "matrixone/pkg/catalog"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/mheap"
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

type Segment interface {
	engine.Statistics

	ID() string
	Blocks() []string
	Block(string) Block
}

type Block interface {
	engine.Statistics

	ID() string
	Prefetch([]string)
	Read([]uint64, []string, []*bytes.Buffer, []*bytes.Buffer) (*batch.Batch, error) // read only arguments
}

type aoeReader struct {
	mp *mheap.Mheap
	blocks []Block
}

type database struct {
	id      uint64            //id of the database
	typ     int               //type of the database
	catalog *catalog3.Catalog //the catalog of the aoeEngine
}

type relation struct {
	pid      uint64                  //database id
	tbl      *aoe.TableInfo          //table of the tablets
	catalog  *catalog3.Catalog       //the catalog
	nodes	 engine.Nodes
	segments []SegmentInfo    //segments of the table
	tablets  []aoe.TabletInfo        //tablets of the table
	mp       map[string]*db.Relation //a map of each tablet and its relation
}

