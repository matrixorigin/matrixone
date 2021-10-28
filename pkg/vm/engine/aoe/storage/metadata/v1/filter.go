package metadata

import "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"

type commitFilter struct {
	interval *common.Range
}

func newCommitFilter(commitId uint64) *commitFilter {
	return &commitFilter{
		interval: &common.Range{
			Right: commitId,
		},
	}
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
	if !IsTransientCommitId(info.CommitId) && f.interval.ClosedIn(info.CommitId) {
		return true
	}
	return false
}

type Filter struct {
	commitFilter *commitFilter
}

func (f *Filter) SetCommitFilter(filter *commitFilter) {
	f.commitFilter = filter
}

func (f *Filter) FilteBaseEntry(entry *BaseEntry) bool {
	ret := true
	if f == nil {
		return ret
	}
	if f.commitFilter != nil {
		ret = f.commitFilter.Eval(entry.CommitInfo)
		if !ret {
			return ret
		}
	}
	return ret
}
