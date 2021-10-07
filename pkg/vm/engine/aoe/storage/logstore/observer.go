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

import "matrixone/pkg/vm/engine/aoe/storage/common"

type Observer interface {
	OnSynced()
	OnRotated(*VersionFile)
}

type ReplayObserver interface {
	OnNewVersion(uint64)
	OnReplayCommit(uint64)
	OnReplayCheckpoint(common.Range)
}

type emptyObserver struct{}

func (o *emptyObserver) OnSynced()              {}
func (o *emptyObserver) OnRotated(*VersionFile) {}

var (
	defaultObserver = &emptyObserver{}
)

type observers struct {
	ers []Observer
}

func NewObservers(o1, o2 Observer) *observers {
	obs := &observers{
		ers: make([]Observer, 2),
	}
	obs.ers[0] = o1
	obs.ers[1] = o2
	return obs
}

func (o *observers) OnSynced() {
	for _, ob := range o.ers {
		ob.OnSynced()
	}
}

func (o *observers) OnRotated(vf *VersionFile) {
	for _, ob := range o.ers {
		ob.OnRotated(vf)
	}
}
