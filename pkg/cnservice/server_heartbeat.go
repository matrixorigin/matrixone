// Copyright 2021 - 2022 Matrix Origin
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

package cnservice

import (
	"context"
	"time"

	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

func (s *service) startCNStoreHeartbeat() error {
	// TODO: always enable heartbeat task
	if s._hakeeperClient == nil {
		return nil
	}
	return s.stopper.RunNamedTask("cnservice-heartbeat", s.heartbeatTask)
}

func (s *service) heartbeatTask(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HAKeeper.HeatbeatDuration.Duration)
	defer ticker.Stop()

	s.logger.Info("CNStore heartbeat started")

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("CNStore heartbeat stopped")
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HAKeeper.HeatbeatTimeout.Duration)
			err := s._hakeeperClient.SendCNHeartbeat(ctx, logservicepb.CNStoreHeartbeat{
				UUID:           s.cfg.UUID,
				ServiceAddress: s.cfg.ListenAddress,
				Role:           s.metadata.Role,
			})
			cancel()

			if err != nil {
				s.logger.Error("send DNShard heartbeat request failed",
					zap.Error(err))
			}
		}
	}
}
