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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"go.uber.org/zap"
)

func (s *service) startCNStoreHeartbeat() error {
	if s._hakeeperClient == nil {
		if _, err := s.getHAKeeperClient(); err != nil {
			return err
		}
	}
	return s.stopper.RunNamedTask("cnservice-heartbeat", s.heartbeatTask)
}

func (s *service) heartbeatTask(ctx context.Context) {
	defer logutil.LogAsyncTask(s.logger, "cnservice/heartbeat-task")()

	ticker := time.NewTicker(s.cfg.HAKeeper.HeatbeatDuration.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HAKeeper.HeatbeatTimeout.Duration)
			batch, err := s._hakeeperClient.SendCNHeartbeat(ctx, logservicepb.CNStoreHeartbeat{
				UUID:               s.cfg.UUID,
				ServiceAddress:     s.cfg.ServiceAddress,
				SQLAddress:         s.cfg.SQLAddress,
				Role:               s.metadata.Role,
				TaskServiceCreated: s.GetTaskRunner() != nil,
			})
			cancel()
			if err != nil {
				s.logger.Error("send DNShard heartbeat request failed",
					zap.Error(err))
				break
			}
			for _, command := range batch.Commands {
				if command.ServiceType != logservicepb.CNService {
					panic(fmt.Sprintf("received a Non-CN Schedule Command: %s", command.ServiceType.String()))
				}
				s.logger.Info("applying schedule command", zap.String("command", command.LogString()))
				if command.CreateTaskService != nil {
					s.createTaskService(command.CreateTaskService)
				}
			}
			s.logger.Debug("send DNShard heartbeat request completed")
		}
	}
}
