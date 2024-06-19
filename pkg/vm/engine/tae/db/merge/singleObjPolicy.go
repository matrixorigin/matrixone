// Copyright 2024 Matrix Origin
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

package merge

import (
	"cmp"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type singleObjConfig struct {
	maxObjs            int
	minDeletes         int
	maxOSizeMergedObjs int
	oldDeleteThreshold time.Duration
}

func (s *singleObjConfig) adjust() {
	if s.maxObjs == 0 {
		s.maxObjs = common.DefaultMaxMergeObjN
	}
	if s.maxOSizeMergedObjs == 0 {
		s.maxOSizeMergedObjs = common.DefaultMaxOsizeObjMB * common.Const1MBytes
	}
	if s.minDeletes == 0 {
		s.minDeletes = 2000
	}
	if s.oldDeleteThreshold == 0 {
		s.oldDeleteThreshold = 10 * time.Minute
	}
}

type singleObjPolicy struct {
	config  *singleObjConfig
	objects []*catalog.ObjectEntry

	threshold types.TS
}

func newSingleObjPolicy(config *singleObjConfig) *singleObjPolicy {
	if config == nil {
		config = &singleObjConfig{}
	}
	config.adjust()

	s := &singleObjPolicy{
		config:  config,
		objects: make([]*catalog.ObjectEntry, 0, config.maxObjs),
	}
	s.refreshTimeThreshold()
	return s
}

func (s *singleObjPolicy) refreshTimeThreshold() {
	s.threshold = types.BuildTS(time.Now().Add(-s.config.oldDeleteThreshold).UnixNano(), 0)
}

func (s *singleObjPolicy) OnObject(obj *catalog.ObjectEntry) {
	dels := obj.GetObjectData().GetTotalChanges()

	obj.RLock()
	createAt := obj.GetCreatedAtLocked()
	obj.RUnlock()
	iscandidate := func() bool {
		// objext with a lot of holes
		if dels > s.config.minDeletes && dels > obj.GetRows()/2 {
			return true
		}
		// if deletes is older than now-oldDeleteThreshold, then merge it.
		if dels > s.config.minDeletes && createAt.Less(&s.threshold) {
			return true
		}
		return false
	}

	if iscandidate() && len(s.objects) < s.config.maxObjs {
		s.objects = append(s.objects, obj)
	}
}

func (s *singleObjPolicy) Revise(cpu, mem int64) ([]*catalog.ObjectEntry, TaskHostKind) {
	slices.SortFunc(s.objects, func(a, b *catalog.ObjectEntry) int {
		return cmp.Compare(a.GetRemainingRows(), b.GetRemainingRows())
	})

	isStandalone := common.IsStandaloneBoost.Load()
	mergeOnDNIfStandalone := !common.ShouldStandaloneCNTakeOver.Load()

	dnobjs := s.controlMem(s.objects, mem)
	dnosize, _, _ := estimateMergeConsume(dnobjs)

	schedDN := func() ([]*catalog.ObjectEntry, TaskHostKind) {
		if cpu > 85 {
			if dnosize > 25*common.Const1MBytes {
				logutil.Infof("mergeblocks skip big merge for high level cpu usage, %d", cpu)
				return nil, TaskHostDN
			}
		}
		revisedObjs := make([]*catalog.ObjectEntry, len(dnobjs))
		copy(revisedObjs, dnobjs)
		return revisedObjs, TaskHostDN
	}

	schedCN := func() ([]*catalog.ObjectEntry, TaskHostKind) {
		cnobjs := s.controlMem(s.objects, int64(common.RuntimeCNMergeMemControl.Load()))
		revisedObjs := make([]*catalog.ObjectEntry, len(cnobjs))
		copy(revisedObjs, cnobjs)
		return revisedObjs, TaskHostDN
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

func (s *singleObjPolicy) Clear() {
	s.objects = s.objects[:0]
	s.refreshTimeThreshold()
}

func (s *singleObjPolicy) ObjCnt() int {
	return len(s.objects)
}

func (s *singleObjPolicy) controlMem(objs []*catalog.ObjectEntry, mem int64) []*catalog.ObjectEntry {
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}

	needPopout := func(ss []*catalog.ObjectEntry) bool {
		osize, esize, _ := estimateMergeConsume(ss)
		if esize > int(2*mem/3) {
			return true
		}

		if len(ss) == 0 {
			return false
		}
		// make object averaged size
		return osize > s.config.maxOSizeMergedObjs
	}
	for needPopout(objs) {
		objs = objs[:len(objs)-1]
	}

	return objs
}

func (s *singleObjPolicy) OnPostTable() {}
