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
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/bootstrap"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const (
	minIDAllocCapacity uint64 = 1024
	defaultIDBatchSize uint64 = 1024 * 10

	hakeeperDefaultTimeout = 2 * time.Second
	checkBootstrapCycles   = 100
)

type idAllocator struct {
	// [nextID, lastID] is the range of IDs that can be assigned.
	// the next ID to be assigned is nextID
	nextID uint64
	lastID uint64
}

var _ hakeeper.IDAllocator = (*idAllocator)(nil)

func newIDAllocator() hakeeper.IDAllocator {
	return &idAllocator{nextID: 1, lastID: 0}
}

func (a *idAllocator) Next() (uint64, bool) {
	if a.nextID <= a.lastID {
		v := a.nextID
		a.nextID++
		return v, true
	}
	return 0, false
}

func (a *idAllocator) Set(next uint64, last uint64) {
	// make sure that this id allocator never emit any id smaller than
	// K8SIDRangeEnd
	if next < hakeeper.K8SIDRangeEnd {
		panic("invalid id allocator range")
	}
	a.nextID = next
	a.lastID = last
}

func (a *idAllocator) Capacity() uint64 {
	if a.nextID <= a.lastID {
		return (a.lastID - a.nextID) + 1
	}
	return 0
}

func (l *store) setInitialClusterInfo(numOfLogShards uint64,
	numOfDNShards uint64, numOfLogReplicas uint64) error {
	cmd := hakeeper.GetInitialClusterRequestCmd(numOfLogShards,
		numOfDNShards, numOfLogReplicas)
	ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
	defer cancel()
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("failed to propose initial cluster info", zap.Error(err))
		return err
	}
	if result.Value == uint64(pb.HAKeeperBootstrapFailed) {
		panic("bootstrap failed")
	}
	if result.Value != uint64(pb.HAKeeperCreated) {
		l.logger.Error("initial cluster info already set")
	}
	return nil
}

func (l *store) updateIDAlloc(count uint64) error {
	cmd := hakeeper.GetGetIDCmd(count)
	ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
	defer cancel()
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("propose get id failed", zap.Error(err))
		return err
	}
	// TODO: add a test for this
	l.alloc.Set(result.Value, result.Value+count-1)
	return nil
}

func (l *store) hakeeperCheck() {
	isLeader, term, err := l.isLeaderHAKeeper()
	if err != nil {
		l.logger.Error("failed to get HAKeeper Leader ID", zap.Error(err))
		return
	}

	if !isLeader {
		l.taskScheduler.StopScheduleCronTask()
		return
	}
	state, err := l.getCheckerState()
	if err != nil {
		// TODO: check whether this is temp error
		l.logger.Error("failed to get checker state", zap.Error(err))
		return
	}
	switch state.State {
	case pb.HAKeeperCreated:
		l.logger.Warn("waiting for initial cluster info to be set, check skipped")
		return
	case pb.HAKeeperBootstrapping:
		l.bootstrap(term, state)
	case pb.HAKeeperBootstrapCommandsReceived:
		l.checkBootstrap(state)
	case pb.HAKeeperBootstrapFailed:
		l.handleBootstrapFailure()
	case pb.HAKeeperRunning:
		l.healthCheck(term, state)
		l.taskSchedule(state)
	default:
		panic("unknown HAKeeper state")
	}
}

func (l *store) assertHAKeeperState(s pb.HAKeeperState) {
	state, err := l.getCheckerState()
	if err != nil {
		// TODO: check whether this is temp error
		l.logger.Error("failed to get checker state", zap.Error(err))
		return
	}
	if state.State != s {
		l.logger.Panic("unexpected state",
			zap.String("expected", s.String()),
			zap.String("got", state.State.String()))
	}
}

func (l *store) handleBootstrapFailure() {
	panic("failed to bootstrap the cluster")
}

func (l *store) healthCheck(term uint64, state *pb.CheckerState) {
	l.assertHAKeeperState(pb.HAKeeperRunning)
	defer l.assertHAKeeperState(pb.HAKeeperRunning)
	cmds, err := l.getScheduleCommand(true, term, state)
	if err != nil {
		l.logger.Error("failed to get check schedule commands", zap.Error(err))
		return
	}
	l.logger.Debug(fmt.Sprintf("cluster health check generated %d schedule commands", len(cmds)))
	if len(cmds) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
		defer cancel()
		for _, cmd := range cmds {
			l.logger.Debug("adding schedule command to hakeeper", zap.String("command", cmd.LogString()))
		}
		if err := l.addScheduleCommands(ctx, term, cmds); err != nil {
			// TODO: check whether this is temp error
			l.logger.Debug("failed to add schedule commands", zap.Error(err))
			return
		}
	}
}

func (l *store) taskSchedule(state *pb.CheckerState) {
	l.assertHAKeeperState(pb.HAKeeperRunning)
	defer l.assertHAKeeperState(pb.HAKeeperRunning)

	switch state.TaskState {
	case pb.TaskInitNotStart:
		l.registerTaskUser()
	case pb.TaskInitStarted:
		l.taskScheduler.StartScheduleCronTask()
		l.taskScheduler.Schedule(state.CNState, state.Tick)
	}
}

func (l *store) registerTaskUser() {
	user, ok := getTaskTableUserFromEnv()
	if !ok {
		user = randomUser()
	}

	// TODO: rename TaskTableUser to moadmin
	if err := l.setTaskTableUser(user); err != nil {
		l.logger.Error("failed to set task table user", zap.Error(err))
	}
}

func (l *store) bootstrap(term uint64, state *pb.CheckerState) {
	cmds, err := l.getScheduleCommand(false, term, state)
	if err != nil {
		l.logger.Error("failed to get bootstrap schedule commands", zap.Error(err))
		return
	}
	if len(cmds) > 0 {
		for _, c := range cmds {
			l.logger.Debug("bootstrap cmd", zap.String("cmd", c.LogString()))
		}
		ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
		defer cancel()
		if err := l.addScheduleCommands(ctx, term, cmds); err != nil {
			// TODO: check whether this is temp error
			l.logger.Debug("failed to add schedule commands", zap.Error(err))
			return
		}
		l.bootstrapCheckCycles = checkBootstrapCycles
		l.bootstrapMgr = bootstrap.NewBootstrapManager(state.ClusterInfo, nil)
		l.assertHAKeeperState(pb.HAKeeperBootstrapCommandsReceived)
	}
}

func (l *store) checkBootstrap(state *pb.CheckerState) {
	if l.bootstrapCheckCycles == 0 {
		if err := l.setBootstrapState(false); err != nil {
			panic(err)
		}
		l.assertHAKeeperState(pb.HAKeeperBootstrapFailed)
	}

	if l.bootstrapMgr == nil {
		l.bootstrapMgr = bootstrap.NewBootstrapManager(state.ClusterInfo, nil)
	}
	if !l.bootstrapMgr.CheckBootstrap(state.LogState) {
		l.bootstrapCheckCycles--
	} else {
		if err := l.setBootstrapState(true); err != nil {
			panic(err)
		}
		l.assertHAKeeperState(pb.HAKeeperRunning)
	}
}

func (l *store) setBootstrapState(success bool) error {
	state := pb.HAKeeperRunning
	if !success {
		state = pb.HAKeeperBootstrapFailed
	}
	cmd := hakeeper.GetSetStateCmd(state)
	ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
	defer cancel()
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	_, err := l.propose(ctx, session, cmd)
	return err
}

func (l *store) getCheckerState() (*pb.CheckerState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
	defer cancel()
	s, err := l.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.StateQuery{})
	if err != nil {
		return &pb.CheckerState{}, err
	}
	return s.(*pb.CheckerState), nil
}

func (l *store) getScheduleCommand(check bool,
	term uint64, state *pb.CheckerState) ([]pb.ScheduleCommand, error) {
	if l.alloc.Capacity() < minIDAllocCapacity {
		if err := l.updateIDAlloc(defaultIDBatchSize); err != nil {
			return nil, err
		}
	}

	if check {
		return l.checker.Check(l.alloc, *state), nil
	}
	m := bootstrap.NewBootstrapManager(state.ClusterInfo, nil)
	return m.Bootstrap(l.alloc, state.DNState, state.LogState)
}

func (l *store) setTaskTableUser(user pb.TaskTableUser) error {
	cmd := hakeeper.GetTaskTableUserCmd(user)
	ctx, cancel := context.WithTimeout(context.Background(), hakeeperDefaultTimeout)
	defer cancel()
	session := l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID)
	result, err := l.propose(ctx, session, cmd)
	if err != nil {
		l.logger.Error("failed to propose task user info", zap.Error(err))
		return err
	}
	if result.Value == uint64(pb.TaskInitFailed) {
		panic("failed to set task user")
	}
	if result.Value != uint64(pb.TaskInitNotStart) {
		l.logger.Error("task user info already set")
	}
	return nil
}

const (
	moAdminUser     = "mo_admin_user"
	moAdminPassword = "mo_admin_password"
)

func getTaskTableUserFromEnv() (pb.TaskTableUser, bool) {
	username, ok := os.LookupEnv(moAdminUser)
	if !ok {
		return pb.TaskTableUser{}, false
	}
	password, ok := os.LookupEnv(moAdminPassword)
	if !ok {
		return pb.TaskTableUser{}, false
	}
	if username == "" || password == "" {
		return pb.TaskTableUser{}, false
	}
	return pb.TaskTableUser{Username: username, Password: password}, true
}

func randomUser() pb.TaskTableUser {
	return pb.TaskTableUser{
		Username: uuid.NewString(),
		Password: uuid.NewString(),
	}
}
