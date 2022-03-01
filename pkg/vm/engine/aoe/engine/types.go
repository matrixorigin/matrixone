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
	"sync"

	catalog3 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"
)

// aoe engine
type aoeEngine struct {
	catalog *catalog3.Catalog
	config  *EngineConfig
}

type EngineConfig struct {
	ReaderBufferCount   uint64 `toml:"reader_buffer_count"`    // The number of buffers allocated by each reader
	QueueMaxReaderCount uint64 `toml:"queue_max_reader_count"` // The number of readers allocated per queue
}

type SegmentInfo struct {
	Version  uint64
	Id       string
	GroupId  string
	TabletId string
	Node     engine.Node
}

type filterContext struct {
	filterType int32
	attr       string
	param1     interface{}
	param2     interface{}
}

type aoeReader struct {
	reader   *store
	id       int32
	prv      *batData
	dequeue  int64
	enqueue  int64
	workerid int32
	filter   []filterContext
}

type store struct {
	rel     *relation
	readers []engine.Reader
	rhs     []chan *batData
	chs     []chan *batData
	blocks  []aoe.Block
	start   bool
	mutex   sync.RWMutex
	iodepth int
}

type batData struct {
	bat *batch.Batch
	cds []*bytes.Buffer
	dds []*bytes.Buffer
	use bool
	id  int8
}

type worker struct {
	id           int32
	bufferCount  int
	zs           []int64
	batDatas     []*batData
	blocks       []aoe.Block
	storeReader  *store
	enqueue      int64
	allocLatency int64
	readLatency  int64
}

type AoeSparseFilter struct {
	storeReader *store
	reader      *aoeReader
}

type database struct {
	id      uint64            //id of the database
	typ     int               //type of the database
	catalog *catalog3.Catalog //the catalog of the aoeEngine
	cfg     *EngineConfig
}

type relation struct {
	mu       sync.Mutex
	pid      uint64            //database id
	tbl      *aoe.TableInfo    //table of the tablets
	catalog  *catalog3.Catalog //the catalog
	nodes    engine.Nodes
	segments []SegmentInfo              //segments of the table
	tablets  []aoe.TabletInfo           //tablets of the table
	mp       map[string]*aoedb.Relation //a map of each tablet and its relation
	reader   *store
	cfg      *EngineConfig
}
