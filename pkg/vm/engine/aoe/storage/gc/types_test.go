package gc

import (
	// "matrixone/pkg/container/types"
	// bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	// "matrixone/pkg/vm/engine/aoe/storage/common"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/gc/gci"
	"matrixone/pkg/vm/engine/aoe/storage/ops"

	"github.com/stretchr/testify/assert"

	// iworker "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync"
	"testing"
	"time"
)

type testRequest struct {
	BaseRequest
	Id  int
	Ret []int
	Wg  *sync.WaitGroup
}

func newTestRequst(id int, ret []int) *testRequest {
	req := new(testRequest)
	req.Id = id
	req.Ret = ret
	req.Op = ops.Op{
		Impl:   req,
		ErrorC: make(chan error),
	}
	return req
}

func (req *testRequest) Execute() error {
	req.Ret = append(req.Ret, req.Id)
	time.Sleep(time.Duration(50) * time.Microsecond)
	return nil
}

func TestWorker(t *testing.T) {
	cfg := new(gci.WorkerCfg)
	cfg.Interval = 10
	worker := NewWorker(cfg)
	worker.Start()
	ret := make([]int, 0)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		req := newTestRequst(i, ret)
		worker.Accept(req)
	}
	assert.True(t, time.Since(now).Microseconds() < int64(50*1000)/2)
	worker.Stop()
}
