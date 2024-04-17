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

package gossip

import (
	"context"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"go.uber.org/zap"
)

const (
	defaultGossipNodes = 6
	// We do not need to exchange the entire data of node.
	defaultPushPullInterval  = 0
	defaultGossipInterval    = time.Second
	defaultUDPBufferSize     = 4 * 1024 * 1024 // 4MB
	defaultHandoffQueueDepth = 4096
)

type Node struct {
	ctx context.Context
	// if the gossip node is created, set it to true.
	created           bool
	nid               string
	logger            *zap.Logger
	list              *memberlist.Memberlist
	delegate          *delegate
	joined            atomic.Bool
	listenAddrFn      func() string
	serviceAddrFn     func() string
	cacheServerAddrFn func() string
}

func NewNode(ctx context.Context, nid string, opts ...Option) (*Node, error) {
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger := rt.Logger().Named("gossip")
	n := &Node{ctx: ctx, nid: nid, logger: logger.RawLogger()}
	for _, opt := range opts {
		opt(n)
	}
	return n, nil
}

func (n *Node) Create() error {
	if n.cacheServerAddrFn == nil {
		return moerr.NewInternalErrorNoCtx("cache service address not set")
	}
	n.delegate = newDelegate(n.logger, n.cacheServerAddrFn())
	cfg := memberlist.DefaultWANConfig()
	cfg.Delegate = n.delegate
	cfg.Events = n.delegate
	cfg.Name = n.nid
	cfg.PushPullInterval = defaultPushPullInterval
	cfg.GossipInterval = defaultGossipInterval
	cfg.GossipNodes = defaultGossipNodes
	cfg.UDPBufferSize = defaultUDPBufferSize
	cfg.HandoffQueueDepth = defaultHandoffQueueDepth
	// Discard the gossip logs.
	cfg.Logger = nil
	cfg.LogOutput = io.Discard

	listenAddr := n.listenAddrFn()
	if len(listenAddr) == 0 {
		n.logger.Error("cannot create gossip node, because listen address is empty")
		return nil
	}

	serviceAddr := n.serviceAddrFn()
	if len(serviceAddr) == 0 {
		n.logger.Error("cannot create gossip node, because service address is empty")
		return nil
	}

	// Set address that gossip uses.
	bindAddr, bindPort, err := parseAddress(n.listenAddrFn())
	if err != nil {
		return err
	}
	cfg.BindAddr = bindAddr
	cfg.BindPort = bindPort

	aAddr, aPort, err := parseAddress(serviceAddr)
	if err != nil {
		return err
	}
	cfg.AdvertiseAddr = aAddr
	cfg.AdvertisePort = aPort

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return moerr.NewInternalError(n.ctx, "CN gossip create node failed: %s", err)
	}
	n.list = ml
	n.created = true
	return nil
}

func (n *Node) Join(existing []string) error {
	if !n.created {
		return moerr.NewInternalErrorNoCtx("cannot join gossip cluster, because node has not been created")
	}
	m, err := n.list.Join(existing)
	if err != nil {
		n.logger.Error("node failed to join cluster",
			zap.String("node ID", n.nid), zap.Error(err))
		return err
	}
	n.logger.Info("node join cluster successfully",
		zap.String("node ID", n.nid),
		zap.Int("joined nodes", m))
	return nil
}

func (n *Node) Created() bool {
	return n.created
}

func (n *Node) SetJoined() {
	n.joined.Store(true)
}

func (n *Node) UnsetJoined() {
	n.joined.Store(false)
}

func (n *Node) Joined() bool {
	return n.joined.Load()
}

func (n *Node) Leave(timeout time.Duration) error {
	if !n.created {
		return nil
	}
	n.logger.Info("leaving gossip cluster",
		zap.String("node ID", n.nid))
	if err := n.list.Leave(timeout); err != nil {
		n.logger.Error("failed to leave gossip cluster",
			zap.String("node ID", n.nid),
			zap.Error(err))
	}
	if err := n.list.Shutdown(); err != nil {
		n.logger.Error("failed to shutdown gossip node",
			zap.String("node ID", n.nid),
			zap.Error(err))
		return err
	}
	return nil
}

func (n *Node) DistKeyCacheGetter() fileservice.KeyRouterFactory[query.CacheKey] {
	return func() client.KeyRouter[query.CacheKey] {
		if n.delegate != nil {
			return n.delegate.getDataCacheKey()
		}
		return nil
	}
}

func (n *Node) StatsKeyRouter() client.KeyRouter[pb.StatsInfoKey] {
	if n != nil && n.delegate != nil {
		return n.delegate.statsInfoKey
	}
	return nil
}

func (n *Node) NumMembers() int {
	return n.list.NumMembers()
}

func parseAddress(addr string) (string, int, error) {
	host, sp, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.ParseUint(sp, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return host, int(port), nil
}
