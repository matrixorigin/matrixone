package gcreqs

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/ops"
)

type catalogCompactionRequest struct {
	gc.BaseRequest
	catalog    *metadata.Catalog
	interval   time.Duration
	lastExecTS int64
}

func NewCatalogCompactionRequest(catalog *metadata.Catalog, interval time.Duration) *catalogCompactionRequest {
	req := new(catalogCompactionRequest)
	req.catalog = catalog
	req.interval = interval
	req.Op = ops.Op{
		Impl:   req,
		ErrorC: make(chan error),
	}
	return req
}

func (req *catalogCompactionRequest) IncIteration() {}

func (req *catalogCompactionRequest) updateExecTS() { req.lastExecTS = time.Now().UnixMilli() }
func (req *catalogCompactionRequest) checkInterval() bool {
	now := time.Now().UnixMilli()
	return now-req.lastExecTS >= req.interval.Milliseconds()
}

func (req *catalogCompactionRequest) Execute() error {
	req.Next = req
	if !req.checkInterval() {
		return nil
	}
	req.catalog.Compact(nil, nil)
	req.updateExecTS()
	return nil
}
