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

type versionInfo struct {
	id         uint64
	commit     Range
	checkpoint *Range
	offset     int
	archived   *archivedHub
}

func (meta *versionInfo) AppendCommit(id uint64) error {
	return meta.commit.Append(id)
}

func (meta *versionInfo) AppendCheckpoint(id uint64) error {
	return meta.checkpoint.Append(id)
}

func (meta *versionInfo) UnionCheckpointRange(r Range) error {
	if meta.checkpoint == nil {
		meta.checkpoint = &r
		return nil
	}
	return meta.checkpoint.Union(&r)
}

type archivedHub struct {
	sync.RWMutex
	versions []*versionInfo
	remote   IHistory
	store    BufferedStore
}

func newArchivedHub(remote IHistory) *archivedHub {
	return &archivedHub{
		remote:   remote,
		versions: make([]*versionInfo, 0),
	}
}

func (vs *archivedHub) Append(version *versionInfo) {
	vs.Lock()
	defer vs.Unlock()
	vs.versions = append(vs.versions, version)
}

// Only one truncate worker
func (vs *archivedHub) TryTruncate(cb PostVersionDeleteCB) error {
	vs.RLock()
	versions := make([]*versionInfo, len(vs.versions))
	for i, version := range vs.versions {
		versions[i] = version
	}
	vs.RUnlock()
	var globCkpRange *Range
	toDelete := make([]*versionInfo, 0, 4)
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
