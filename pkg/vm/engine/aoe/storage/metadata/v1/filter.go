package metadata

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type commitChecker func(info *CommitInfo) bool

func createShardChecker(shardId uint64) commitChecker {
	return func(info *CommitInfo) bool {
		if info.LogIndex == nil {
			return shardId == uint64(0)
		}
		return info.LogIndex.ShardId == shardId
	}
}

func createIndexRangeChecker(id uint64) commitChecker {
	return func(info *CommitInfo) bool {
		if info.LogRange == nil {
			return true
		}
		ret := info.LogRange.Range.GT(id)
		// logutil.Infof("%d %s [%v]", id, info.LogRange.Range.String(), !ret)
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
	interval *common.Range
	checkers []commitChecker
}

func newCommitFilter(commitId uint64) *commitFilter {
	return &commitFilter{
		interval: &common.Range{
			Right: commitId,
		},
		checkers: make([]commitChecker, 0),
	}
}

func (f *commitFilter) AddChecker(checker commitChecker) {
	f.checkers = append(f.checkers, checker)
}

func (f *commitFilter) LatestId() uint64 {
	if f.interval == nil {
		return 0
	}
	return f.interval.Right
}

func (f *commitFilter) Eval(info *CommitInfo) bool {
	if f == nil || f.interval == nil {
		return true
	}
	if IsTransientCommitId(info.CommitId) || !f.interval.ClosedIn(info.CommitId) {
		return false
	}
	for _, checker := range f.checkers {
		if !checker(info) {
			return false
		}
	}
	return true
}

type Filter struct {
	tableFilter   *commitFilter
	segmentFilter *commitFilter
	blockFilter   *commitFilter
}
