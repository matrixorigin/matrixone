// Copyright 2021 - 2023 Matrix Origin
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
	"context"
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap"
)

const (
	// The default rebalancer queue size is 128.
	defaultQueueSize = 128
)

type rebalancer struct {
	stopper *stopper.Stopper
	logger  *log.MOLogger
	// mc is MO-Cluster instance, which is used to get CN servers.
	mc clusterservice.MOCluster
	// connManager is used to track the connections on the CN servers.
	connManager *connManager
	// queue takes the tunnels which need to do migration.
	queue chan *tunnel
	// If disabled is true, rebalance does nothing.
	disabled bool
	// interval indicates that how often the rebalance is act.
	interval time.Duration
	// tolerance is the tolerance that is used to calculate tunnels need
	// to migrate to other CN servers.  For example, if tolerance is 0.3,
	// and the average of tunnels is 10, then if there are 15 tunnels on
	// a CN server, 2 tunnels will do migration.
	tolerance float64
}

// rebalancerOption defines the function to set options of rebalancer.
type rebalancerOption func(*rebalancer)

// withRebalancerDisabled sets if rebalancer is disabled.
func withRebalancerDisabled() rebalancerOption {
	return func(r *rebalancer) {
		r.disabled = true
	}
}

// withRebalancerInterval sets the interval
func withRebalancerInterval(interval time.Duration) rebalancerOption {
	return func(r *rebalancer) {
		r.interval = interval
	}
}

// withRebalancerTolerance sets the tolerance of rebalancer.
func withRebalancerTolerance(tolerance float64) rebalancerOption {
	return func(r *rebalancer) {
		r.tolerance = tolerance
	}
}

// newRebalancer creates a new rebalancer.
func newRebalancer(
	stopper *stopper.Stopper, logger *log.MOLogger, mc clusterservice.MOCluster, opts ...rebalancerOption,
) (*rebalancer, error) {
	r := &rebalancer{
		stopper:     stopper,
		logger:      logger,
		connManager: newConnManager(),
		mc:          mc,
		queue:       make(chan *tunnel, defaultQueueSize),
	}
	for _, opt := range opts {
		opt(r)
	}

	// Starts the transfer go-routine to handle the transfer request.
	if err := r.stopper.RunNamedTask("rebalaner-transfer", r.handleTransfer); err != nil {
		return nil, err
	}
	// Starts the runner go-routine to check the tunnels that need to transfer.
	if err := r.stopper.RunNamedTask("rebalancer-runner", r.run); err != nil {
		return nil, err
	}
	return r, nil
}

// run begins the loop to check if there are any connections need to
// be rebalanced.
func (r *rebalancer) run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.doRebalance()
		case <-ctx.Done():
			r.logger.Info("rebalancer runner ended")
			return
		}
	}
}

// doRebalance do the real rebalance work by tenants.
func (r *rebalancer) doRebalance() {
	// Re-balance is disabled, nothing to do.
	if r.disabled {
		return
	}
	hashes := r.connManager.getLabelHashes()
	for _, h := range hashes {
		r.rebalanceByHash(h)
	}
}

func (r *rebalancer) rebalanceByHash(hash LabelHash) {
	// Collect the tunnels that need to migrate.
	tuns := r.collectTunnels(hash)

	// Put the tunnels to the queue.
	for _, t := range tuns {
		select {
		case r.queue <- t:
		default:
			r.logger.Info("rebalance queue is full")
		}
	}
}

func (r *rebalancer) collectTunnels(hash LabelHash) []*tunnel {
	// get CN servers from mocluster for this label.
	li := r.connManager.getLabelInfo(hash)
	var cns []*CNServer
	r.mc.GetCNService(li.genSelector(), func(s metadata.CNService) bool {
		cns = append(cns, &CNServer{
			hash:     hash,
			reqLabel: li,
			cnLabel:  s.Labels,
			uuid:     s.ServiceID,
			addr:     s.SQLAddress,
		})
		return true
	})
	cnCount := len(cns)
	if cnCount == 0 {
		return nil
	}

	// Here we get the tunnels on each CN server for the tenant.
	tuns := r.connManager.getCNTunnels(hash)
	if tuns == nil {
		return nil
	}

	// Calculate the upper limit of tunnels that each CN server could take.
	r.connManager.Lock()
	defer r.connManager.Unlock()
	tunnelCount := tuns.count()
	avg := float64(tunnelCount) / float64(cnCount)
	upperLimit := int(math.Max(1, math.Ceil(avg*(1+r.tolerance))))

	var ret []*tunnel
	// For each CN server, pick the tunnels that need to move to other
	// CN servers.
	for _, ts := range tuns {
		if ts.count() > upperLimit {
			ret = append(ret, pickTunnels(ts, ts.count()-upperLimit)...)
		}
	}
	return ret
}

// handlerTransfer gets the tunnel transfer request from queue and handles it.
func (r *rebalancer) handleTransfer(ctx context.Context) {
	for {
		select {
		case tun := <-r.queue:
			if err := tun.transfer(ctx); err != nil {
				r.logger.Error("failed to do transfer", zap.Error(err))
			}
		case <-ctx.Done():
			r.logger.Info("rebalancer transfer ended.")
			return
		}
	}
}
