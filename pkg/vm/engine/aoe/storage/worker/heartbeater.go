package ops

import (
	"context"
	"matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"time"
	// log "github.com/sirupsen/logrus"
)

type heartbeater struct {
	handle   base.IHBHandle
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewHeartBeater(interval time.Duration, handle base.IHBHandle) base.IHeartbeater {
	c := &heartbeater{
		interval: interval,
		handle:   handle,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	return c
}

func (c *heartbeater) Start() {
	go func() {
		ticker := time.NewTicker(c.interval)
		for {
			select {
			case <-c.ctx.Done():
				ticker.Stop()
				c.handle.OnStopped()
				return
			case <-ticker.C:
				c.handle.OnExec()
			}
		}
	}()
}

func (c *heartbeater) Stop() {
	c.cancel()
}
