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
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

const defaultScalingInterval = 5 * time.Second

type scaling struct {
	logger *log.MOLogger
	// How often we check the scaling state, the default value
	// is defaultScalingInterval.
	interval time.Duration
	// disabled is the scaling worker state. It is the same as rebalancer.
	disabled bool
	// we get connection information from it.
	connManager *connManager
	// mc is MO-Cluster instance, which is used to get CN servers.
	mc clusterservice.MOCluster
	// tunnelDeliver is used to deliver tunnel to the queue.
	tunnelDeliver TunnelDeliver
}

func newScaling(
	cm *connManager, tunnelDeliver TunnelDeliver, mc clusterservice.MOCluster, logger *log.MOLogger, disabled bool,
) *scaling {
	return &scaling{
		interval:      defaultScalingInterval,
		logger:        logger,
		disabled:      disabled,
		connManager:   cm,
		mc:            mc,
		tunnelDeliver: tunnelDeliver,
	}
}

func (s *scaling) run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.doScaling()
		case <-ctx.Done():
			s.logger.Info("scaling runner ended")
			return
		}
	}
}

func (s *scaling) doScaling() {
	if s.disabled {
		return
	}
	drainingCNs := make([]string, 0, 100)
	s.mc.GetCNService(clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
		if isDraining(s) {
			drainingCNs = append(drainingCNs, s.ServiceID)
		}
		return true
	})
	v2.ProxyDrainCounter.Add(float64(len(drainingCNs)))
	for _, cn := range drainingCNs {
		tuns := s.connManager.getTunnelsByCNID(cn)
		tunNum := len(tuns)
		if tunNum == 0 {
			continue
		}
		s.logger.Info("transferring tunnels on CN",
			zap.Int("tunnel number", len(tuns)),
			zap.String("CN ID", cn),
		)
		for _, t := range tuns {
			s.tunnelDeliver.Deliver(t, transferByScaling)
		}
	}
}

func isDraining(s metadata.CNService) bool {
	return s.WorkState == metadata.WorkState_Draining
}
