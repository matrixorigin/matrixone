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
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var (
	_                  Policy = (*Basic)(nil)
	defaultBasicConfig        = &BasicPolicyConfig{MergeMaxOneRun: common.DefaultMaxMergeObjN}
)

type BasicPolicyConfig struct {
	ObjectMinRows    int
	MergeMaxOneRun   int
	MaxRowsMergedObj int
	MergeHints       []api.MergeHint
}

func (c *BasicPolicyConfig) String() string {
	return fmt.Sprintf("minRowsObj:%d, maxOneRun:%d, hints: %v", c.ObjectMinRows, c.MergeMaxOneRun, c.MergeHints)
}

type customConfigProvider struct {
	sync.Mutex
	configs map[uint64]*BasicPolicyConfig // works like a cache
}

func NewCustomConfigProvider() *customConfigProvider {
	return &customConfigProvider{
		configs: make(map[uint64]*BasicPolicyConfig),
	}
}

func (o *customConfigProvider) GetConfig(tbl *catalog.TableEntry) *BasicPolicyConfig {
	o.Lock()
	defer o.Unlock()
	p, ok := o.configs[tbl.ID]
	if !ok {
		extra := tbl.GetLastestSchema().Extra
		if extra.MaxObjOnerun != 0 || extra.MinRowsQuailifed != 0 {
			p = &BasicPolicyConfig{
				ObjectMinRows:    int(extra.MinRowsQuailifed),
				MergeMaxOneRun:   int(extra.MaxObjOnerun),
				MaxRowsMergedObj: int(extra.MaxRowsMergedObj),
				MergeHints:       extra.Hints,
			}
			o.configs[tbl.ID] = p
		}
	}
	return p
}

func (o *customConfigProvider) InvalidCache(tbl *catalog.TableEntry) {
	o.Lock()
	defer o.Unlock()
	delete(o.configs, tbl.ID)
}

func (o *customConfigProvider) SetCache(tbl *catalog.TableEntry, cfg *BasicPolicyConfig) {
	o.Lock()
	defer o.Unlock()
	o.configs[tbl.ID] = cfg
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
	objHeap *heapBuilder[*catalog.ObjectEntry]
	accBuf  []int

	config         *BasicPolicyConfig
	configProvider *customConfigProvider
}

func NewBasicPolicy() *Basic {
	return &Basic{
		objHeap: &heapBuilder[*catalog.ObjectEntry]{
			items: make(itemSet[*catalog.ObjectEntry], 0, 32),
		},
		accBuf:         make([]int, 1, 32),
		configProvider: NewCustomConfigProvider(),
	}
}

// impl Policy for Basic
func (o *Basic) OnObject(obj *catalog.ObjectEntry) {
	rowsLeftOnObj := obj.Stat.GetRemainingRows()
	// it has too few rows, merge it
	iscandidate := func() bool {
		if rowsLeftOnObj < o.config.ObjectMinRows {
			return true
		}
		if rowsLeftOnObj < obj.Stat.GetRows()/2 {
			return true
		}
		return false
	}

	if iscandidate() {
		o.objHeap.pushWithCap(&mItem[*catalog.ObjectEntry]{
			row:   rowsLeftOnObj,
			entry: obj,
		}, o.config.MergeMaxOneRun)
	}
}

func (o *Basic) SetConfig(tbl *catalog.TableEntry, f func() txnif.AsyncTxn, c any) {
	txn := f()
	if tbl == nil || txn == nil {
		return
	}
	db, err := txn.GetDatabaseByID(tbl.GetDB().ID)
	if err != nil {
		return
	}
	tblHandle, err := db.GetRelationByID(tbl.ID)
	if err != nil {
		return
	}
	cfg := c.(*BasicPolicyConfig)
	ctx := context.Background()
	tblHandle.AlterTable(
		ctx,
		api.NewUpdatePolicyReq(cfg.ObjectMinRows, cfg.MergeMaxOneRun, cfg.MaxRowsMergedObj, cfg.MergeHints...),
	)
	logutil.Infof("mergeblocks set %v-%v config: %v", tbl.ID, tbl.GetLastestSchema().Name, cfg)
	txn.Commit(ctx)
	o.configProvider.InvalidCache(tbl)
}

func (o *Basic) GetConfig(tbl *catalog.TableEntry) any {
	r := o.configProvider.GetConfig(tbl)
	if r == nil {
		r = &BasicPolicyConfig{
			ObjectMinRows:  int(common.RuntimeMinRowsQualified.Load()),
			MergeMaxOneRun: int(common.RuntimeMaxMergeObjN.Load()),
		}
	}
	return r
}

func (o *Basic) Revise(cpu, mem int64) []*catalog.ObjectEntry {
	objs := o.objHeap.finish()
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].Stat.GetRemainingRows() < objs[j].Stat.GetRemainingRows()
	})
	objs = o.controlMem(objs, mem)
	objs = o.optimize(objs)
	return objs
}

func (o *Basic) optimize(objs []*catalog.ObjectEntry) []*catalog.ObjectEntry {
	// objs are sorted by remaining rows
	o.accBuf = o.accBuf[:1]
	for i, obj := range objs {
		o.accBuf = append(o.accBuf, o.accBuf[i]+obj.Stat.GetRemainingRows())
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

	objs = objs[:i]

	return objs
}

func (o *Basic) controlMem(objs []*catalog.ObjectEntry, mem int64) []*catalog.ObjectEntry {
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	needPopout := func(ss []*catalog.ObjectEntry) bool {
		_, esize := estimateMergeConsume(ss)
		return esize > int(2*mem/3)
	}
	popCnt := 0
	for needPopout(objs) {
		objs = objs[:len(objs)-1]
		popCnt++
	}
	if popCnt > 0 {
		logutil.Infof(
			"mergeblocks skip %d-%s pop %d out of %d objects due to %s mem cap",
			o.id, o.schema.Name, popCnt, len(objs)+popCnt, common.HumanReadableBytes(int(mem)),
		)
	}
	return objs
}

func (o *Basic) ResetForTable(entry *catalog.TableEntry) {
	o.id = entry.ID
	o.schema = entry.GetLastestSchema()
	o.hist = entry.Stats.GetLastMerge()
	o.objHeap.reset()
	o.config = o.configProvider.GetConfig(entry)
	if o.config == nil && o.schema.Name == "rawlog" {
		o.config = &BasicPolicyConfig{
			ObjectMinRows:  500000,
			MergeMaxOneRun: 512,
		}
		o.configProvider.SetCache(entry, o.config)
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
	objectFullRows := int(schema.ObjectMaxBlocks) * int(schema.BlockMaxRows)
	// we want every object has at least 5 blks rows
	objectMinRows := constMergeMinBlks * int(schema.BlockMaxRows)
	if objectFullRows < objectMinRows { // for small config in unit test
		return objectFullRows
	}
	return objectMinRows
}
