package export

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"sync/atomic"
)

func init() {
	var p BatchProcessor = &noopBatchProcessor{}
	SetGlobalBatchProcessor(p)
}

type BatchProcessor interface {
	Collect(context.Context, batchpipe.HasName) error
	Start() bool
	Stop(graceful bool) error
}

func Register(name batchpipe.HasName, impl batchpipe.PipeImpl[batchpipe.HasName, any]) {
	if ok := gPipeImplHolder.Put(name.GetName(), impl); !ok {
		// record double Register
	}
}

var gBatchProcessor atomic.Value

type processorHolder struct {
	p BatchProcessor
}

func SetGlobalBatchProcessor(p BatchProcessor) {
	gBatchProcessor.Store(&processorHolder{p: p})
}

func GetGlobalBatchProcessor() BatchProcessor {
	return gBatchProcessor.Load().(*processorHolder).p
}
