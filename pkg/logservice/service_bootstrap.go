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

package logservice

import (
	"context"
	"time"
)

func (s *Service) BootstrapHAKeeper(ctx context.Context, cfg Config) error {
	members, err := cfg.GetInitHAKeeperMembers()
	if err != nil {
		return err
	}
	replicaID := cfg.BootstrapConfig.HAKeeperReplicaID
	if err := s.store.startHAKeeperReplica(replicaID, members, false); err != nil {
		plog.Errorf("failed to start hakeeper replica, %v", err)
		return err
	}
	numOfLogShards := cfg.BootstrapConfig.NumOfLogShards
	numOfDNShards := cfg.BootstrapConfig.NumOfDNShards
	numOfLogReplicas := cfg.BootstrapConfig.NumOfLogShardReplicas
	for i := 0; i < checkBootstrapCycles; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		plog.Infof("trying to set initial cluster info")
		if err := s.store.setInitialClusterInfo(numOfLogShards,
			numOfDNShards, numOfLogReplicas); err != nil {
			plog.Errorf("failed to set initial cluster info, %v", err)
			time.Sleep(time.Second)
			continue
		}
		plog.Infof("initial cluster info set")
		break
	}
	return nil
}
