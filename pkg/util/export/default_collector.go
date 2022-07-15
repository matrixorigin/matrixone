package export

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"sync"
)

var _ collector = &defaultCollector{}

type defaultCollector struct {
	bufferPipes map[string]batchpipe.ItemBuffer[batchpipe.HasName, any]
	pipeImpl    map[string]batchpipe.PipeImpl[batchpipe.HasName, any]
	mux         sync.RWMutex
}

func (c *defaultCollector) Register(name batchpipe.HasName, impl batchpipe.PipeImpl[batchpipe.HasName, any]) {

	c.mux.Lock()
	defer c.mux.Unlock()
	if _, has := c.pipeImpl[name.GetName()]; !has {
		c.pipeImpl[name.GetName()] = impl
	}

}

func (c *defaultCollector) Collect(ctx context.Context, i batchpipe.HasName) error {

	c.mux.RLock()
	defer c.mux.RUnlock()
	if _, has := c.bufferPipes[i.GetName()]; !has {
		c.mux.Lock()
		defer c.mux.Unlock()
		if impl, has := c.pipeImpl[i.GetName()]; !has {
			// TODO: PanicError
			panic("unknown item type")
		} else {
			c.bufferPipes[i.GetName()] = impl.NewItemBuffer(i.GetName())
		}
	}
	c.bufferPipes[i.GetName()].Add(i)
	return nil
}

func (c *defaultCollector) Start() bool {
	go c.loop()
	return true
}

func (c *defaultCollector) Stop(graceful bool) (<-chan struct{}, bool) {
	//TODO implement me
	panic("implement me")
}

func (c *defaultCollector) loop() {

}
