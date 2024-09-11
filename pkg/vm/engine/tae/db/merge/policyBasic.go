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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type reviseResult struct {
	objs []*catalog.ObjectEntry
	kind TaskHostKind
}

type policyGroup struct {
	policies []policy

	config         *BasicPolicyConfig
	configProvider *customConfigProvider
}

func newPolicyGroup(policies ...policy) *policyGroup {
	return &policyGroup{
		policies:       policies,
		configProvider: newCustomConfigProvider(),
	}
}

func (g *policyGroup) onObject(obj *catalog.ObjectEntry) {
	for _, p := range g.policies {
		if p.onObject(obj, g.config) {
			return
		}
	}
}

func (g *policyGroup) revise(cpu, mem int64) []reviseResult {
	results := make([]reviseResult, 0, len(g.policies))
	for _, p := range g.policies {
		pResult := p.revise(cpu, mem, g.config)
		for _, r := range pResult {
			if len(r.objs) > 0 {
				results = append(results, r)
			}
		}
	}
	return results
}

func (g *policyGroup) resetForTable(entry *catalog.TableEntry) {
	for _, p := range g.policies {
		p.resetForTable(entry)
	}
	g.config = g.configProvider.getConfig(entry)
}

func (g *policyGroup) setConfig(tbl *catalog.TableEntry, txn txnif.AsyncTxn, cfg *BasicPolicyConfig) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tblHandle.AlterTable(
		ctx,
		NewUpdatePolicyReq(cfg),
	)
	logutil.Infof("mergeblocks set %v-%v config: %v", tbl.ID, tbl.GetLastestSchemaLocked(false).Name, cfg)
	txn.Commit(ctx)
	g.configProvider.invalidCache(tbl)
}

func (g *policyGroup) getConfig(tbl *catalog.TableEntry) *BasicPolicyConfig {
	r := g.configProvider.getConfig(tbl)
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

type basic struct {
	schema  *catalog.Schema
	hist    *common.MergeHistory
	objHeap *heapBuilder[*catalog.ObjectEntry]
	accBuf  []int
}

func newBasicPolicy() policy {
	return &basic{
		objHeap: &heapBuilder[*catalog.ObjectEntry]{
			items: make(itemSet[*catalog.ObjectEntry], 0, 32),
		},
		accBuf: make([]int, 1, 32),
	}
}

// impl policy for Basic
func (o *basic) onObject(obj *catalog.ObjectEntry, config *BasicPolicyConfig) bool {
	if obj.IsTombstone {
		return false
	}

	osize := obj.GetOriginSize()

	isCandidate := func() bool {
		if osize < int(config.ObjectMinOsize) {
			return true
		}
		// skip big object as an insurance
		if osize > 110*common.Const1MBytes {
			return false
		}

		return false
	}

	if isCandidate() {
		o.objHeap.pushWithCap(&mItem[*catalog.ObjectEntry]{
			row:   obj.GetRows(),
			entry: obj,
		}, config.MergeMaxOneRun)
		return true
	}
	return false
}

func (o *basic) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	objs := o.objHeap.finish()

	isStandalone := common.IsStandaloneBoost.Load()
	mergeOnDNIfStandalone := !common.ShouldStandaloneCNTakeOver.Load()

	dnobjs := controlMem(objs, mem)
	dnobjs = o.optimize(dnobjs, config)

	dnosize, _ := estimateMergeConsume(dnobjs)

	schedDN := func() []reviseResult {
		if cpu > 85 {
			if dnosize > 25*common.Const1MBytes {
				logutil.Infof("mergeblocks skip big merge for high level cpu usage, %d", cpu)
				return nil
			}
		}
		if len(dnobjs) > 1 {
			return []reviseResult{{dnobjs, TaskHostDN}}
		}
		return nil
	}

	schedCN := func() []reviseResult {
		cnobjs := controlMem(objs, int64(common.RuntimeCNMergeMemControl.Load()))
		cnobjs = o.optimize(cnobjs, config)
		return []reviseResult{{cnobjs, TaskHostCN}}
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

func (o *basic) optimize(objs []*catalog.ObjectEntry, config *BasicPolicyConfig) []*catalog.ObjectEntry {
	// objs are sorted by remaining rows
	o.accBuf = o.accBuf[:1]
	for i, obj := range objs {
		o.accBuf = append(o.accBuf, o.accBuf[i]+obj.GetRows())
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
		i < config.MergeMaxOneRun {
		return nil
	}

	objs = objs[:i]

	return objs
}

func controlMem(objs []*catalog.ObjectEntry, mem int64) []*catalog.ObjectEntry {
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}

	needPopout := func(ss []*catalog.ObjectEntry) bool {
		_, esize := estimateMergeConsume(ss)
		return esize > int(2*mem/3)
	}
	for needPopout(objs) {
		objs = objs[:len(objs)-1]
	}

	return objs
}

func (o *basic) resetForTable(entry *catalog.TableEntry) {
	o.schema = entry.GetLastestSchemaLocked(false)
	o.hist = entry.Stats.GetLastMerge()
	o.objHeap.reset()
}
