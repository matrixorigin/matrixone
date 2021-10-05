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

type versionMeta struct {
	id     uint64
	commit Range
	ckp    *Range
	offset int
}

func (meta *versionMeta) AppendCommit(id uint64) error {
	return meta.commit.Append(id)
}

func (meta *versionMeta) AppendCheckpoint(id uint64) error {
	return meta.ckp.Append(id)
}

func (meta *versionMeta) UnionCheckpointRange(r Range) error {
	if meta.ckp == nil {
		meta.ckp = &r
		return nil
	}
	return meta.ckp.Union(&r)
}

type versionsMeta struct {
	sync.RWMutex
	versions []*versionMeta
	history  IHistory
	store    BufferedStore
}

func newVersionsMeta(history IHistory) *versionsMeta {
	return &versionsMeta{
		history:  history,
		versions: make([]*versionMeta, 0),
	}
}

func (vs *versionsMeta) Append(version *versionMeta) {
	vs.Lock()
	defer vs.Unlock()
	vs.versions = append(vs.versions, version)
}

// Only one truncate worker
func (vs *versionsMeta) TryTruncate(cb PostVersionDeleteCB) error {
	vs.RLock()
	versions := make([]*versionMeta, len(vs.versions))
	for i, version := range vs.versions {
		versions[i] = version
	}
	vs.RUnlock()
	var globCkpRange *Range
	toDelete := make([]*versionMeta, 0, 4)
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		if globCkpRange.CanCover(&version.commit) && globCkpRange.CanCover(version.ckp) {
			version.offset = i
			toDelete = append(toDelete, version)
			continue
		}

		if version.ckp != nil {
			if globCkpRange == nil {
				globCkpRange = &Range{
					left:  version.ckp.left,
					right: version.ckp.right,
				}
			} else {
				if err := globCkpRange.Union(version.ckp); err != nil {
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
	if vs.history != nil {
		for _, version := range toDelete {
			if err := vs.history.Truncate(version.id); err != nil {
				return err
			}
		}
	}
	return nil
}
