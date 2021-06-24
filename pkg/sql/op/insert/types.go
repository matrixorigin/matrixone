package insert

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
)

type Insert struct {
	ID  string
	DB  string
	Bat *batch.Batch
	R   engine.Relation
}
