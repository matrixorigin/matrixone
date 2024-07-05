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

	"github.com/matrixorigin/matrixone/pkg/common/util"
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
	config *singleObjConfig

	tableEntry       *catalog.TableEntry
	distinctDeltaLoc map[string]struct{}

	objects []*catalog.ObjectEntry
}

func newSingleObjPolicy(config *singleObjConfig) *singleObjPolicy {
	if config == nil {
		config = &singleObjConfig{}
	}
	config.adjust()

	s := &singleObjPolicy{
		config:           config,
		distinctDeltaLoc: make(map[string]struct{}),
		objects:          make([]*catalog.ObjectEntry, 0, config.maxObjs),
	}
	return s
}

func (s *singleObjPolicy) onObject(obj *catalog.ObjectEntry) {
	tombstone := s.tableEntry.TryGetTombstone(obj.ID)
	if tombstone == nil {
		return
	}

	deltaLocRows := uint32(0)
	for j := range obj.BlockCnt() {
		deltaLoc := tombstone.GetLatestDeltaloc(uint16(j))
		if deltaLoc == nil || deltaLoc.IsEmpty() {
			continue
		}
		if _, ok := s.distinctDeltaLoc[util.UnsafeBytesToString(deltaLoc)]; !ok {
			s.distinctDeltaLoc[util.UnsafeBytesToString(deltaLoc)] = struct{}{}
			deltaLocRows += deltaLoc.Rows()
		}
	}

	isCandidate := func() bool {
		// object with a lot of holes
		dels := obj.GetObjectData().GetTotalChanges()
		if dels > s.config.minDeletes {
			if dels > obj.GetRows()/2 {
				return true
			}
			if deltaLocRows > uint32(obj.GetRemainingRows()) {
				return true
			}
		}
		return false
	}

	if isCandidate() && len(s.objects) < s.config.maxObjs {
		s.objects = append(s.objects, obj)
	}
}

func (s *singleObjPolicy) revise(cpu, mem int64) ([]*catalog.ObjectEntry, TaskHostKind) {
	slices.SortFunc(s.objects, func(a, b *catalog.ObjectEntry) int {
		return cmp.Compare(a.GetRemainingRows(), b.GetRemainingRows())
	})

	dnobjs := controlMem(s.objects, mem)
	dnosize, _, _ := estimateMergeConsume(dnobjs)

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

func (s *singleObjPolicy) resetForTable(tableEntry *catalog.TableEntry) {
	s.tableEntry = tableEntry
	s.objects = s.objects[:0]
	clear(s.distinctDeltaLoc)
}
