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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
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
	ctx, cancel := context.WithTimeoutCause(ctx, s.config.HAKeeper.HeartbeatTimeout.Duration, moerr.CauseDoHeartbeat)
	defer cancel()
	if err := s.sendHeartbeat(ctx); err != nil {
		err = moerr.AttachCause(ctx, err)
		s.runtime.Logger().Error("failed to send heartbeat", zap.Error(err))
	}
}

func (s *Server) renewServingLease() {
	duration := s.config.HAKeeper.HeartbeatInterval.Duration +
		s.config.HAKeeper.HeartbeatTimeout.Duration
	deadline := time.Now().Add(duration)
	s.servingLeaseDeadline.Store(&deadline)
}

func (s *Server) revokeServingLease() {
	s.servingLeaseDeadline.Store(nil)
}

func (s *Server) canAcceptNewConnections() bool {
	deadline := s.servingLeaseDeadline.Load()
	return deadline != nil && time.Now().Before(*deadline)
}

func (s *Server) sendHeartbeat(ctx context.Context) error {
	hb := pb.ProxyHeartbeat{
		UUID:                   s.config.UUID,
		ListenAddress:          s.config.ListenAddress,
		ConfigData:             s.configData.GetData(),
		GlobalSysVarGeneration: s.globalSysVarGeneration,
		ProtocolVersion:        defines.MORPCLatestVersion,
	}
	if s.handler != nil {
		hb.GlobalSysVarCommitTS = clusterservice.GlobalSysVarCommitTS(s.handler.moCluster)
	}
	_, err := s.haKeeperClient.SendProxyHeartbeat(ctx, hb)
	s.configData.DecrCount()
	if err != nil {
		s.revokeServingLease()
		return err
	}
	if err := ctx.Err(); err != nil {
		s.revokeServingLease()
		return err
	}
	s.renewServingLease()
	return nil
}

func (s *Server) initializeGlobalSysVarRouteBarrier(ctx context.Context) error {
	refresher, ok := s.handler.moCluster.(clusterservice.AuthoritativeRefresher)
	if !ok {
		return moerr.NewInternalError(ctx,
			"proxy cluster service does not support authoritative refresh")
	}
	if err := refresher.Refresh(ctx); err != nil {
		return err
	}
	return s.sendHeartbeat(ctx)
}
