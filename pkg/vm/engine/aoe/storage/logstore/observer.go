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
