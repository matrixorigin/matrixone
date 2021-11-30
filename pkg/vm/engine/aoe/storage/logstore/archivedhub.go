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

package logstore

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
)

type PostVersionDeleteCB = func(uint64)

type versionInfo struct {
	id         uint64
	commit     common.Range
	checkpoint *common.Range
	offset     int
	hub        *archivedHub
}

func newVersionInfo(hub *archivedHub, id uint64) *versionInfo {
	return &versionInfo{
		id:  id,
		hub: hub,
	}
}

func (info *versionInfo) AppendCommit(id uint64) error {
	return info.commit.Append(id)
}

func (info *versionInfo) AppendCheckpoint(id uint64) error {
	return info.checkpoint.Append(id)
}

func (info *versionInfo) UnionCheckpointRange(r common.Range) error {
	if info.checkpoint == nil {
		info.checkpoint = &r
		return nil
	}
	return info.checkpoint.Union(&r)
}

func (info *versionInfo) Archive() {
	info.hub.Append(info)
}

type archivedHub struct {
	sync.RWMutex
	versions []*versionInfo
	remote   IHistory
	store    AwareStore
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
	var globCkpRange *common.Range
	toDelete := make([]*versionInfo, 0, 4)
	for i := len(versions) - 1; i >= 0; i-- {
		version := versions[i]
		// logutil.Infof("%d, %s, %s", version.id, version.commit.String(), version.checkpoint.String())
		if globCkpRange.CanCover(&version.commit) && globCkpRange.CanCover(version.checkpoint) {
			version.offset = i
			toDelete = append(toDelete, version)
			continue
		}

		if version.checkpoint != nil {
			if globCkpRange == nil {
				globCkpRange = &common.Range{
					Left:  version.checkpoint.Left,
					Right: version.checkpoint.Right,
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
