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
	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

type WrappedService struct {
	svc *Service
}

func NewWrappedService(
	c Config,
	fileService fileservice.FileService,
	shutdownC chan struct{},
	opts ...Option,
) (*WrappedService, error) {
	svc, err := NewService(c, fileService, shutdownC, opts...)
	if err != nil {
		return nil, err
	}
	return &WrappedService{svc: svc}, nil
}

func (w *WrappedService) Start() error {
	return nil
}

func (w *WrappedService) Close() error {
	return w.svc.Close()
}

func (w *WrappedService) ID() string {
	return w.svc.ID()
}

func (w *WrappedService) IsLeaderHakeeper() (bool, error) {
	isLeader, _, err := w.svc.store.isLeaderHAKeeper()
	return isLeader, err
}

func (w *WrappedService) GetClusterState() (*pb.CheckerState, error) {
	return w.svc.store.getCheckerState()
}

func (w *WrappedService) SetInitialClusterInfo(
	logShardNum, tnShartnum, logReplicaNum uint64,
) error {
	return w.svc.store.setInitialClusterInfo(
		logShardNum, tnShartnum, logReplicaNum, 0, nil, nil,
	)
}

func (w *WrappedService) GetTaskService() (taskservice.TaskService, bool) {
	w.svc.task.RLock()
	defer w.svc.task.RUnlock()
	if w.svc.task.holder == nil {
		return nil, false
	}
	return w.svc.task.holder.Get()
}

// StartHAKeeperReplica
// TODO: start hakeeper with specified log store, specified by caller
func (w *WrappedService) StartHAKeeperReplica(
	replicaID uint64, replicas map[uint64]dragonboat.Target, join bool,
) error {
	return w.svc.store.startHAKeeperReplica(replicaID, replicas, join)
}
