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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type tombstonePolicy struct {
	tombstones []*catalog.ObjectEntry
}

func (t *tombstonePolicy) onObject(entry *catalog.ObjectEntry, config *BasicPolicyConfig) bool {
	if len(t.tombstones) == config.MergeMaxOneRun {
		return false
	}
	if entry.IsTombstone {
		t.tombstones = append(t.tombstones, entry)
		return true
	}
	return false
}

func (t *tombstonePolicy) revise(cpu, mem int64, config *BasicPolicyConfig) []reviseResult {
	tombstones := t.tombstones
	slices.SortFunc(tombstones, func(a, b *catalog.ObjectEntry) int {
		return cmp.Compare(a.GetRows(), b.GetRows())
	})

	isStandalone := common.IsStandaloneBoost.Load()
	mergeOnDNIfStandalone := !common.ShouldStandaloneCNTakeOver.Load()

	dnobjs := controlMem(tombstones, mem)
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
		cnobjs := controlMem(tombstones, int64(common.RuntimeCNMergeMemControl.Load()))
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

func (t *tombstonePolicy) resetForTable(*catalog.TableEntry) {
	t.tombstones = t.tombstones[:0]
}

func newTombstonePolicy() policy {
	return &tombstonePolicy{
		tombstones: make([]*catalog.ObjectEntry, 0),
	}
}
