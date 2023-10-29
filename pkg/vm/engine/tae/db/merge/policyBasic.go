// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"fmt"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	_                  Policy = (*Basic)(nil)
	defaultBasicConfig        = &BasicPolicyConfig{MergeMaxOneRun: common.DefaultMaxMergeObjN}
)

type BasicPolicyConfig struct {
	ObjectMinRows  int
	MergeMaxOneRun int
}

func (c *BasicPolicyConfig) String() string {
	return fmt.Sprintf("minRowsObj:%d, maxOneRun:%d", c.ObjectMinRows, c.MergeMaxOneRun)
}

type customConfigProvider struct {
	sync.Mutex
	configs map[uint64]*BasicPolicyConfig
}

func NewCustomConfigProvider() *customConfigProvider {
	return &customConfigProvider{
		configs: make(map[uint64]*BasicPolicyConfig),
	}
}

func (o *customConfigProvider) GetConfig(id uint64) *BasicPolicyConfig {
	o.Lock()
	defer o.Unlock()
	return o.configs[id]
}

func (o *customConfigProvider) SetConfig(id uint64, c *BasicPolicyConfig) {
	o.Lock()
	defer o.Unlock()
	o.configs[id] = c
	logutil.Infof("mergeblocks config map: %v", o.configs)
}

func (o *customConfigProvider) ResetConfig() {
	o.Lock()
	defer o.Unlock()
	o.configs = make(map[uint64]*BasicPolicyConfig)
}

type Basic struct {
	id      uint64
	schema  *catalog.Schema
	hist    *common.MergeHistory
	objHeap *heapBuilder[*catalog.SegmentEntry]
	accBuf  []int

	config         *BasicPolicyConfig
	configProvider *customConfigProvider
}

func NewBasicPolicy() *Basic {
	return &Basic{
		objHeap: &heapBuilder[*catalog.SegmentEntry]{
			items: make(itemSet[*catalog.SegmentEntry], 0, 32),
		},
		accBuf:         make([]int, 1, 32),
		configProvider: NewCustomConfigProvider(),
	}
}

// impl Policy for Basic
func (o *Basic) OnObject(obj *catalog.SegmentEntry) {
	rowsLeftOnSeg := obj.Stat.RemainingRows
	// it has too few rows, merge it
	iscandidate := func() bool {
		if rowsLeftOnSeg < o.config.ObjectMinRows {
			return true
		}
		if rowsLeftOnSeg < obj.Stat.Rows/2 {
			return true
		}
		return false
	}

	if iscandidate() {
		o.objHeap.pushWithCap(&mItem[*catalog.SegmentEntry]{
			row:   rowsLeftOnSeg,
			entry: obj,
		}, o.config.MergeMaxOneRun)
	}
}

func (o *Basic) Config(id uint64, c any) {
	if id == 0 {
		o.configProvider.ResetConfig()
		return
	}
	cfg := c.(*BasicPolicyConfig)
	o.configProvider.SetConfig(id, cfg)
}

func (o *Basic) GetConfig(id uint64) any {
	r := o.configProvider.GetConfig(id)
	if r == nil {
		r = &BasicPolicyConfig{
			ObjectMinRows:  int(common.RuntimeMinRowsQualified.Load()),
			MergeMaxOneRun: int(common.RuntimeMaxMergeObjN.Load()),
		}
	}
	return r
}

func (o *Basic) Revise(cpu, mem int64) []*catalog.SegmentEntry {
	segs := o.objHeap.finish()
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].Stat.RemainingRows < segs[j].Stat.RemainingRows
	})
	segs = o.controlMem(segs, mem)
	segs = o.optimize(segs)
	return segs
}

func (o *Basic) optimize(segs []*catalog.SegmentEntry) []*catalog.SegmentEntry {
	// segs are sorted by remaining rows
	o.accBuf = o.accBuf[:1]
	for i, seg := range segs {
		o.accBuf = append(o.accBuf, o.accBuf[i]+seg.Stat.RemainingRows)
	}
	acc := o.accBuf

	isBigGap := func(small, big int) bool {
		if big < int(o.schema.BlockMaxRows) {
			return false
		}
		return big-small > 3*small
	}

	var i int
	// skip merging objects with big row count gaps, 3x and more
	for i = len(acc) - 1; i > 1 && isBigGap(acc[i-1], acc[i]); i-- {
	}
	readyToMergeRows := acc[i]

	// if o.schema.Name == "rawlog" || o.schema.Name == "statement_info" {
	// 	logutil.Infof("mergeblocks %d-%s acc: %v, tryMerge: %v", o.id, o.schema.Name, acc, readyToMergeRows)
	// }

	// avoid frequent small object merge
	if readyToMergeRows < int(o.schema.BlockMaxRows) &&
		!o.hist.IsLastBefore(constSmallMergeGap) &&
		i < o.config.MergeMaxOneRun {
		return nil
	}

	segs = segs[:i]

	return segs
}

func (o *Basic) controlMem(segs []*catalog.SegmentEntry, mem int64) []*catalog.SegmentEntry {
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	needPopout := func(ss []*catalog.SegmentEntry) bool {
		_, esize := estimateMergeConsume(ss)
		return esize > int(2*mem/3)
	}
	popCnt := 0
	for needPopout(segs) {
		segs = segs[:len(segs)-1]
		popCnt++
	}
	if popCnt > 0 {
		logutil.Infof(
			"mergeblocks skip %d-%s pop %d out of %d objects due to %s mem cap",
			o.id, o.schema.Name, popCnt, len(segs)+popCnt, common.HumanReadableBytes(int(mem)),
		)
	}
	return segs
}

func (o *Basic) ResetForTable(id uint64, entry *catalog.TableEntry) {
	o.id = id
	o.schema = entry.GetLastestSchema()
	o.hist = entry.Stats.GetLastMerge()
	o.objHeap.reset()
	o.config = o.configProvider.GetConfig(id)
	if o.config == nil && o.schema.Name == "rawlog" {
		o.config = &BasicPolicyConfig{
			ObjectMinRows:  500000,
			MergeMaxOneRun: 512,
		}
		o.configProvider.SetConfig(id, o.config)
	}
	if o.config == nil {
		o.config = defaultBasicConfig
		o.config.ObjectMinRows = determineObjectMinRows(o.schema)
		o.config.MergeMaxOneRun = int(common.RuntimeMaxMergeObjN.Load())
	}
}

func determineObjectMinRows(schema *catalog.Schema) int {
	runtimeMinRows := int(common.RuntimeMinRowsQualified.Load())
	if runtimeMinRows > common.DefaultMinRowsQualified {
		return runtimeMinRows
	}
	// the max rows of a full object
	objectFullRows := int(schema.SegmentMaxBlocks) * int(schema.BlockMaxRows)
	// we want every object has at least 5 blks rows
	objectMinRows := constMergeMinBlks * int(schema.BlockMaxRows)
	if objectFullRows < objectMinRows { // for small config in unit test
		return objectFullRows
	}
	return objectMinRows
}
