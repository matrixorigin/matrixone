package export

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type BatchProcessor interface {
	Collect(context.Context, batchpipe.HasName) error
	Start() bool
	Stop(graceful bool) (<-chan struct{}, bool)
}
