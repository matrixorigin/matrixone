package logstore

import (
	"errors"
	"sync"
)

var (
	RangeNotContinousErr = errors.New("aoe: range not continous")
	RangeInvalidErr      = errors.New("aoe: invalid range")
)

type PostVersionDeleteCB = func(uint64)

type Range struct {
	left  uint64
	right uint64
}

func (r *Range) Valid() bool {
	return r.left <= r.right
}

func (r *Range) CanCover(o *Range) bool {
	if r == nil {
		return false
	}
	if o == nil {
		return true
	}
	return r.left <= o.left && r.right >= o.right
}

func (r *Range) Union(o *Range) error {
	if o.left > r.right+1 || r.left > o.right+1 {
		return RangeNotContinousErr
	}
	if r.left > o.left {
		r.left = o.left
	}
	if r.right < o.right {
		r.right = o.right
	}
	return nil
}

func (r *Range) Append(right uint64) error {
	if right < r.left || right > r.right+1 {
		return RangeInvalidErr
	}
	r.right = right
	return nil
}

type archivedVersion struct {
	id         uint64
	commit     Range
	checkpoint *Range
	offset     int
}

func (meta *archivedVersion) AppendCommit(id uint64) error {
	return meta.commit.Append(id)
}

func (meta *archivedVersion) AppendCheckpoint(id uint64) error {
	return meta.checkpoint.Append(id)
}

func (meta *archivedVersion) UnionCheckpointRange(r Range) error {
	if meta.checkpoint == nil {
		meta.checkpoint = &r
		return nil
	}
	return meta.checkpoint.Union(&r)
}

type archivedInfo struct {
	sync.RWMutex
	versions []*archivedVersion
	remote   IHistory
	store    BufferedStore
}

func newArchivedInfo(remote IHistory) *archivedInfo {
	return &archivedInfo{
		remote:   remote,
		versions: make([]*archivedVersion, 0),
	}
}

func (vs *archivedInfo) Append(version *archivedVersion) {
	vs.Lock()
	defer vs.Unlock()
	vs.versions = append(vs.versions, version)
}

// Only one truncate worker
func (vs *archivedInfo) TryTruncate(cb PostVersionDeleteCB) error {
	vs.RLock()
	versions := make([]*archivedVersion, len(vs.versions))
	for i, version := range vs.versions {
		versions[i] = version
	}
	vs.RUnlock()
	var globCkpRange *Range
	toDelete := make([]*archivedVersion, 0, 4)
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		if globCkpRange.CanCover(&version.commit) && globCkpRange.CanCover(version.checkpoint) {
			version.offset = i
			toDelete = append(toDelete, version)
			continue
		}

		if version.checkpoint != nil {
			if globCkpRange == nil {
				globCkpRange = &Range{
					left:  version.checkpoint.left,
					right: version.checkpoint.right,
				}
			} else {
				if err := globCkpRange.Union(version.checkpoint); err != nil {
					panic(err)
				}
			}
		}
	}
	vs.Lock()
	for _, version := range toDelete {
		vs.versions = append(vs.versions[:version.offset], vs.versions[version.offset+1:]...)
		if cb != nil {
			cb(version.id)
		}
	}
	vs.Unlock()
	if vs.remote != nil {
		for _, version := range toDelete {
			if err := vs.remote.Truncate(version.id); err != nil {
				return err
			}
		}
	}
	return nil
}
