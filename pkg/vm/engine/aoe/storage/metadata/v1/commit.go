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

package metadata

import "sync"

type commitPipeline struct {
	mu      *sync.RWMutex
	catalog *Catalog
}

func newCommitPipeline(catalog *Catalog) *commitPipeline {
	return &commitPipeline{
		mu:      catalog.RWMutex,
		catalog: catalog,
	}
}

func (p *commitPipeline) prepare(ctx interface{}) (LogEntry, error) {
	switch v := ctx.(type) {
	case *createTableCtx:
		return p.catalog.prepareCreateTable(v)
	case *dropTableCtx:
		return v.table.prepareSoftDelete(v)
	case *deleteTableCtx:
		return v.table.prepareHardDelete(v)
	case *createSegmentCtx:
		return v.table.prepareCreateSegment(v)
	case *upgradeSegmentCtx:
		return v.segment.prepareUpgrade(v)
	case *createBlockCtx:
		return v.segment.prepareCreateBlock(v)
	case *upgradeBlockCtx:
		return v.block.prepareUpgrade(v)
	case *replaceShardCtx:
		return p.catalog.prepareReplaceShard(v)
	}
	panic("not supported")
}

func (p *commitPipeline) commit(entry LogEntry) error {
	if err := entry.WaitDone(); err != nil {
		return err
	}
	return nil
}
