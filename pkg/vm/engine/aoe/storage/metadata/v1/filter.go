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

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type commitChecker func(info *CommitInfo) bool

var replacedStopper = func(info *CommitInfo) bool {
	return info.Op == OpReplaced
}

var deleteStopper = func(info *CommitInfo) bool {
	return info.Op >= OpSoftDelete
}

func createDeleteAndIndexStopper(index uint64) commitChecker {
	return func(info *CommitInfo) bool {
		if info.LogIndex != nil {
			if info.LogIndex.Id.Id <= index && info.Op >= OpSoftDelete {
				return true
			}
			return false
		}
		return info.Op >= OpSoftDelete
	}
}

func createShardChecker(shardId uint64) commitChecker {
	return func(info *CommitInfo) bool {
		if info.LogIndex == nil {
			return true
		}
		return info.LogIndex.ShardId == shardId
	}
}

func createCommitIdChecker(id uint64) commitChecker {
	interval := &common.Range{
		Right: id,
	}
	return func(info *CommitInfo) bool {
		if IsTransientCommitId(info.CommitId) || !interval.ClosedIn(info.CommitId) {
			return false
		}
		return true
	}
}

func createIndexRangeChecker(id uint64) commitChecker {
	return func(info *CommitInfo) bool {
		var ret bool
		if info.LogRange == nil {
			// logutil.Infof("%s-%d %v", info.LogRange.String(), id, ret)
			return true
		}
		ret = info.LogRange.Range.GT(id)
		return !ret
	}
}

func createIndexChecker(id uint64) commitChecker {
	return func(info *CommitInfo) bool {
		if info.LogIndex == nil {
			return true
		}
		return info.LogIndex.Id.Id <= id
	}
}

type commitFilter struct {
	checkers []commitChecker
	stoppers []commitChecker
}

func newCommitFilter() *commitFilter {
	return &commitFilter{
		checkers: make([]commitChecker, 0),
		stoppers: make([]commitChecker, 0),
	}
}

func (f *commitFilter) AddChecker(checker commitChecker) {
	f.checkers = append(f.checkers, checker)
}

func (f *commitFilter) AddStopper(stopper commitChecker) {
	f.stoppers = append(f.stoppers, stopper)
}

func (f *commitFilter) Eval(info *CommitInfo) bool {
	if f == nil {
		return true
	}
	for _, checker := range f.checkers {
		if !checker(info) {
			return false
		}
	}
	return true
}

func (f *commitFilter) EvalStop(info *CommitInfo) bool {
	if f == nil {
		return false
	}
	for _, stopper := range f.stoppers {
		if stopper(info) {
			return true
		}
	}
	return false
}

type Filter struct {
	dbFilter      *commitFilter
	tableFilter   *commitFilter
	segmentFilter *commitFilter
	blockFilter   *commitFilter
}
