// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/shard"
	"go.uber.org/zap"
)

type ServerOption func(*server)

type server struct {
	cfg                Config
	initReplicaVersion uint64
	r                  *rt
	env                Env
	schedulers         []scheduler
	filters            []filter
	stopper            *stopper.Stopper
	rpc                morpc.MethodBasedServer[*pb.Request, *pb.Response]
}

func NewShardServer(
	cfg Config,
	opts ...ServerOption,
) ShardServer {
	cfg.Validate()
	env := NewEnv(cfg.SelectCNLabel)
	s := &server{
		cfg: cfg,
		env: env,
		r:   newRuntime(env),
		stopper: stopper.NewStopper(
			"shard-server",
			stopper.WithLogger(getLogger().RawLogger()),
		),
		initReplicaVersion: uint64(time.Now().UnixNano()),
	}
	for _, opt := range opts {
		opt(s)
	}

	s.initRemote()
	s.initSchedulers()

	if err := s.stopper.RunTask(s.schedule); err != nil {
		panic(err)
	}

	if err := s.rpc.Start(); err != nil {
		panic(err)
	}
	return s
}

func (s *server) Close() error {
	if err := s.rpc.Close(); err != nil {
		return err
	}
	s.stopper.Stop()
	return nil
}

func (s *server) initSchedulers() {
	freezeFilter := newFreezeFilter(s.cfg.FreezeCNTimeout.Duration)
	s.schedulers = append(
		s.schedulers,
		newDownScheduler(),
		newAllocateScheduler(),
		newBalanceScheduler(
			s.cfg.MaxScheduleTables,
			freezeFilter,
		),
		newReplicaScheduler(
			freezeFilter,
		),
	)
}

func (s *server) initRemote() {
	pool := morpc.NewMessagePool(
		func() *pb.Request {
			return &pb.Request{}
		},
		func() *pb.Response {
			return &pb.Response{}
		},
	)

	rpc, err := morpc.NewMessageHandler(
		"shard-server",
		s.cfg.ListenAddress,
		s.cfg.RPC,
		pool,
	)
	if err != nil {
		panic(err)
	}
	s.rpc = rpc

	s.initHandlers()
}

func (s *server) initHandlers() {
	s.rpc.RegisterMethod(
		uint32(pb.Method_Heartbeat),
		s.handleHeartbeat,
		true,
	)
	s.rpc.RegisterMethod(
		uint32(pb.Method_CreateShards),
		s.handleCreateShards,
		true,
	)
	s.rpc.RegisterMethod(
		uint32(pb.Method_DeleteShards),
		s.handleDeleteShards,
		true,
	)
	s.rpc.RegisterMethod(
		uint32(pb.Method_GetShards),
		s.handleGetShards,
		true,
	)
	s.rpc.RegisterMethod(
		uint32(pb.Method_PauseCN),
		s.handlePauseCN,
		true,
	)
}

func (s *server) handleHeartbeat(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
	buffer *morpc.Buffer,
) error {
	resp.Heartbeat.Operators = s.r.heartbeat(
		req.Heartbeat.CN,
		req.Heartbeat.Shards,
	)
	return nil
}

func (s *server) handleCreateShards(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
	buffer *morpc.Buffer,
) error {
	id := req.CreateShards.ID
	v := req.CreateShards.Metadata
	if v.Policy == pb.Policy_None {
		return nil
	}
	if v.ShardsCount == 0 {
		panic("shards count is 0")
	}

	s.doCreate(id, v)
	return nil
}

func (s *server) handleDeleteShards(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
	buffer *morpc.Buffer,
) error {
	id := req.DeleteShards.ID
	s.r.delete(id)
	return nil
}

func (s *server) handleGetShards(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
	buffer *morpc.Buffer,
) error {
	shards := s.r.get(req.GetShards.ID)
	if len(shards) == 0 {
		s.doCreate(
			req.GetShards.ID,
			req.GetShards.Metadata,
		)
		return nil
	}
	resp.GetShards.Shards = shards
	return nil
}

func (s *server) handlePauseCN(
	ctx context.Context,
	req *pb.Request,
	resp *pb.Response,
	buffer *morpc.Buffer,
) error {
	s.r.Lock()
	defer s.r.Unlock()

	cn, ok := s.r.cns[req.PauseCN.ID]
	if !ok {
		return moerr.NewNotFoundNoCtx()
	}
	cn.pause()
	return nil
}

func (s *server) doCreate(
	id uint64,
	metadata pb.ShardsMetadata,
) {
	s.r.add(
		newTable(
			id,
			metadata,
			s.initReplicaVersion,
		))
}

func (s *server) schedule(ctx context.Context) {
	timer := time.NewTimer(s.cfg.ScheduleDuration.Duration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.doSchedule()
			timer.Reset(s.cfg.ScheduleDuration.Duration)
		}
	}
}

func (s *server) doSchedule() {
	for _, scheduler := range s.schedulers {
		if err := scheduler.schedule(s.r, s.filters...); err != nil {
			getLogger().Error("schedule shards failed",
				zap.Error(err))
		}
	}
}
