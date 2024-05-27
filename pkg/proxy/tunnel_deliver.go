// Copyright 2021 - 2024 Matrix Origin
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

package proxy

import (
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"go.uber.org/zap"
)

type TunnelDeliver interface {
	// Deliver enqueues the tunnel to the transfer queue.
	// It makes sure that if the tunnel is in the queue or being transferred,
	// the tunnel will not be enqueue into the tunnel queue.
	Deliver(*tunnel, transferType)

	// Count returns the number of tunnels in the queue.
	Count() int
}

type tunnelDeliver struct {
	logger *log.MOLogger
	queue  chan<- *tunnel
}

// newTunnelDeliver creates a new tunnel deliver.
func newTunnelDeliver(queue chan<- *tunnel, logger *log.MOLogger) TunnelDeliver {
	return &tunnelDeliver{
		logger: logger,
		queue:  queue,
	}
}

// Deliver implements the TunnelDeliver interface.
func (d *tunnelDeliver) Deliver(t *tunnel, typ transferType) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.mu.started {
		d.logger.Info("tunnel has not started, skip transfer", zap.Any("tunnel", t))
		return
	}
	if t.mu.inTransfer {
		d.logger.Info("tunnel is already in transfer, skip transfer",
			zap.Uint32("conn ID", t.cc.ConnID()))
		return
	}

	preDeliver := func() {
		// Set the transfer type according to the parameter.
		t.setTransferType(typ)
		// Before enqueue the entry, set the transfer state to true.
		t.mu.inTransfer = true
	}

	// If the queue is full, we reset transfer state and transfer type.
	queueFullAction := func() {
		t.mu.inTransfer = false
		t.setTransferType(transferByRebalance)
		d.logger.Info("rebalance queue is full",
			zap.Uint32("conn ID", t.cc.ConnID()))
	}

	preDeliver()

	select {
	case d.queue <- t:
	default:
		queueFullAction()
	}
}

// Count implements the TunnelDeliver interface.
func (d *tunnelDeliver) Count() int {
	return len(d.queue)
}
