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
	"bytes"
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
	_                  Policy = (*basic)(nil)
	defaultBasicConfig        = &BasicPolicyConfig{
		MergeMaxOneRun:    common.DefaultMaxMergeObjN,
		MaxOsizeMergedObj: common.DefaultMaxOsizeObjMB * common.Const1MBytes,
		ObjectMinOsize:    common.DefaultMinOsizeQualifiedMB * common.Const1MBytes,
		MinCNMergeSize:    common.DefaultMinCNMergeSize * common.Const1MBytes,
	}
)

/// TODO(aptend): codes related storing and fetching configs are too annoying!

type BasicPolicyConfig struct {
	name              string
	MergeMaxOneRun    int
	ObjectMinOsize    uint32
	MaxOsizeMergedObj uint32
	MinCNMergeSize    uint64
	FromUser          bool
	MergeHints        []api.MergeHint
}

func (c *BasicPolicyConfig) String() string {
	return fmt.Sprintf(
		"minOsizeObj:%v, maxOneRun:%v, maxOsizeMergedObj: %v, offloadToCNSize:%v, hints: %v",
		common.HumanReadableBytes(int(c.ObjectMinOsize)),
		c.MergeMaxOneRun,
		common.HumanReadableBytes(int(c.MaxOsizeMergedObj)),
		common.HumanReadableBytes(int(c.MinCNMergeSize)),
		c.MergeHints,
	)
}

type customConfigProvider struct {
	sync.Mutex
	configs map[uint64]*BasicPolicyConfig // works like a cache
}

func newCustomConfigProvider() *customConfigProvider {
	return &customConfigProvider{
		configs: make(map[uint64]*BasicPolicyConfig),
	}
}

func (o *customConfigProvider) GetConfig(tbl *catalog.TableEntry) *BasicPolicyConfig {
	o.Lock()
	defer o.Unlock()
	p, ok := o.configs[tbl.ID]
	if !ok {
		// load from an atomic value
		extra := tbl.GetLastestSchemaLocked().Extra
		if extra.MaxObjOnerun != 0 || extra.MinOsizeQuailifed != 0 {
			// compatible with old version
			cnSize := extra.MinCnMergeSize
			if cnSize == 0 {
				cnSize = common.DefaultMinCNMergeSize * common.Const1MBytes
			}
			// if the values are smaller than default, it map old rows -> bytes size
			minOsize := extra.MinOsizeQuailifed
			if v := uint32(80 * 8192); minOsize < v {
				minOsize = v
			}
			maxOsize := extra.MaxOsizeMergedObj
			if v := uint32(500 * 8192); maxOsize < v {
				maxOsize = v
			}

			p = &BasicPolicyConfig{
				ObjectMinOsize:    minOsize,
				MergeMaxOneRun:    int(extra.MaxObjOnerun),
				MaxOsizeMergedObj: maxOsize,
				MinCNMergeSize:    cnSize,
				FromUser:          true,
				MergeHints:        extra.Hints,
			}
			o.configs[tbl.ID] = p
		} else {
			p = defaultBasicConfig
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

func (o *customConfigProvider) String() string {
	o.Lock()
	defer o.Unlock()
	keys := make([]uint64, 0, len(o.configs))
	for k := range o.configs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	buf := bytes.Buffer{}
	buf.WriteString("customConfigProvider: ")
	for _, k := range keys {
		c := o.configs[k]
		buf.WriteString(fmt.Sprintf("%d-%v:%v,%v | ", k, c.name, c.ObjectMinOsize, c.MergeMaxOneRun))
	}
	return buf.String()
}

func (o *customConfigProvider) ResetConfig() {
	o.Lock()
	defer o.Unlock()
	o.configs = make(map[uint64]*BasicPolicyConfig)
}

type basic struct {
	id        uint64
	schema    *catalog.Schema
	hist      *common.MergeHistory
	objHeap   *heapBuilder[*catalog.ObjectEntry]
	guessType common.WorkloadKind
	accBuf    []int

	config         *BasicPolicyConfig
	configProvider *customConfigProvider
}

func NewBasicPolicy() Policy {
	return &basic{
		objHeap: &heapBuilder[*catalog.ObjectEntry]{
			items: make(itemSet[*catalog.ObjectEntry], 0, 32),
		},
		accBuf:         make([]int, 1, 32),
		configProvider: newCustomConfigProvider(),
	}
}

// impl Policy for Basic
func (o *basic) OnObject(obj *catalog.ObjectEntry) {
	rowsLeftOnObj := obj.GetRemainingRows()
	osize := obj.GetOriginSize()

	iscandidate := func() bool {
		// objext with a lot of holes
		if rowsLeftOnObj < obj.GetRows()/2 {
			return true
		}
		if osize < int(o.config.ObjectMinOsize) {
			return true
		}
		// skip big object as an insurance
		if osize > 110*common.Const1MBytes {
			return false
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

func (o *basic) SetConfig(tbl *catalog.TableEntry, f func() txnif.AsyncTxn, c any) {
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
		NewUpdatePolicyReq(cfg),
	)
	logutil.Infof("mergeblocks set %v-%v config: %v", tbl.ID, tbl.GetLastestSchemaLocked().Name, cfg)
	txn.Commit(ctx)
	o.configProvider.InvalidCache(tbl)
}

func (o *basic) GetConfig(tbl *catalog.TableEntry) any {
	r := o.configProvider.GetConfig(tbl)
	if r == nil {
		r = &BasicPolicyConfig{
			ObjectMinOsize:    common.RuntimeOsizeRowsQualified.Load(),
			MaxOsizeMergedObj: common.RuntimeMaxObjOsize.Load(),
			MergeMaxOneRun:    int(common.RuntimeMaxMergeObjN.Load()),
			MinCNMergeSize:    common.RuntimeMinCNMergeSize.Load(),
		}
	}
	return r
}

func (o *basic) Revise(cpu, mem int64) ([]*catalog.ObjectEntry, TaskHostKind) {
	objs := o.objHeap.finish()
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].GetRemainingRows() < objs[j].GetRemainingRows()
	})

	isStandalone := common.IsStandaloneBoost.Load()
	mergeOnDNIfStandalone := !common.ShouldStandaloneCNTakeOver.Load()

	dnobjs := o.controlMem(objs, mem)
	dnobjs = o.optimize(dnobjs)

	dnosize, _, _ := estimateMergeConsume(dnobjs)

	schedDN := func() ([]*catalog.ObjectEntry, TaskHostKind) {
		if cpu > 85 {
			if dnosize > 25*common.Const1MBytes {
				logutil.Infof("mergeblocks skip big merge for high level cpu usage, %d", cpu)
				return nil, TaskHostDN
			}
		}
		return dnobjs, TaskHostDN
	}

	schedCN := func() ([]*catalog.ObjectEntry, TaskHostKind) {
		cnobjs := o.controlMem(objs, int64(common.RuntimeCNMergeMemControl.Load()))
		cnobjs = o.optimize(cnobjs)
		return cnobjs, TaskHostCN
	}

	if isStandalone && mergeOnDNIfStandalone {
		return schedDN()
	}

	// CNs come into the picture in two cases:
	// 1.cluster deployed
	// 2.standalone deployed but it's asked to merge on cn
	if common.RuntimeCNTakeOverAll.Load() || dnosize > int(common.RuntimeMinCNMergeSize.Load()) {
		return schedCN()
	}

	// CNs don't take over the task, leave it on dn.
	return schedDN()
}

func (o *basic) ConfigString() string {
	r := o.configProvider.String()
	return r
}

func (o *basic) optimize(objs []*catalog.ObjectEntry) []*catalog.ObjectEntry {
	// objs are sorted by remaining rows
	o.accBuf = o.accBuf[:1]
	for i, obj := range objs {
		o.accBuf = append(o.accBuf, o.accBuf[i]+obj.GetRemainingRows())
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

	// avoid frequent small object merge
	if readyToMergeRows < int(o.schema.BlockMaxRows) &&
		!o.hist.IsLastBefore(constSmallMergeGap) &&
		i < o.config.MergeMaxOneRun {
		return nil
	}

	objs = objs[:i]

	return objs
}

func (o *basic) controlMem(objs []*catalog.ObjectEntry, mem int64) []*catalog.ObjectEntry {
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}

	needPopout := func(ss []*catalog.ObjectEntry) bool {
		osize, esize, _ := estimateMergeConsume(ss)
		if esize > int(2*mem/3) {
			return true
		}

		if len(ss) <= 2 {
			return false
		}
		// make object averaged size
		return osize > int(o.config.MaxOsizeMergedObj)
	}
	for needPopout(objs) {
		objs = objs[:len(objs)-1]
	}

	return objs
}

func (o *basic) ResetForTable(entry *catalog.TableEntry) {
	o.id = entry.ID
	o.schema = entry.GetLastestSchemaLocked()
	o.hist = entry.Stats.GetLastMerge()
	o.guessType = entry.Stats.GetWorkloadGuess()
	o.objHeap.reset()

	o.config = o.configProvider.GetConfig(entry)
}
