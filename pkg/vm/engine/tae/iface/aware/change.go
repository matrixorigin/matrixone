package aware

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"

type ActionAware interface {
	OnForced(data.CheckpointUnit)
	OnRecommended(data.CheckpointUnit)
}

type DataMutationAware interface {
	OnUpdateColumn(unit data.CheckpointUnit)
	OnDeleteRow(unit data.CheckpointUnit)
	OnNonAppendable(unit data.CheckpointUnit)
}
