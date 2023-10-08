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

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

func (s *Server) heartbeat(ctx context.Context) {
	if s.config.HAKeeper.HeartbeatInterval.Duration == 0 {
		panic("invalid heartbeat interval")
	}
	ticker := time.NewTicker(s.config.HAKeeper.HeartbeatInterval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.doHeartbeat(ctx)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func (s *Server) doHeartbeat(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, s.config.HAKeeper.HeartbeatTimeout.Duration)
	defer cancel()
	_, err := s.haKeeperClient.SendProxyHeartbeat(ctx, pb.ProxyHeartbeat{
		UUID:          s.config.UUID,
		ListenAddress: s.config.ListenAddress,
		ConfigData:    s.configData.GetData(),
	})
	if err != nil {
		s.runtime.Logger().Error("failed to send heartbeat", zap.Error(err))
	}
	s.configData.DecrCount()
}
