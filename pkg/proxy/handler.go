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

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"go.uber.org/zap"
)

// handler is the proxy service handler.
type handler struct {
	ctx     context.Context
	logger  *log.MOLogger
	config  Config
	stopper *stopper.Stopper
	// moCluster is the CN server cache, and is used to filter
	// CN servers by label.
	moCluster clusterservice.MOCluster
	// router select the best CN server and connects to it.
	router Router
}

var ErrNoAvailableCNServers = moerr.NewInternalErrorNoCtx("no available CN servers")

// newProxyHandler creates a new proxy handler.
func newProxyHandler(
	ctx context.Context, runtime runtime.Runtime, cfg Config, stopper *stopper.Stopper,
) (*handler, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	c, err := logservice.NewProxyHAKeeperClient(ctx, cfg.HAKeeper.ClientConfig)
	if err != nil {
		return nil, err
	}

	// Create the MO cluster.
	mc := clusterservice.NewMOCluster(c, cfg.Cluster.RefreshInterval.Duration)

	// Create the rebalancer.
	var opts []rebalancerOption
	opts = append(opts,
		withRebalancerInterval(cfg.RebalanceInterval.Duration),
		withRebalancerTolerance(cfg.RebalanceToerance),
	)
	if cfg.RebalanceDisabled {
		opts = append(opts, withRebalancerDisabled())
	}

	re, err := newRebalancer(stopper, runtime.Logger(), mc, opts...)
	if err != nil {
		return nil, err
	}

	return &handler{
		ctx:       context.Background(),
		logger:    runtime.Logger(),
		config:    cfg,
		stopper:   stopper,
		moCluster: mc,
		router:    newRouter(mc, re, false),
	}, nil
}

// handle handles the incoming connection.
func (h *handler) handle(c goetty.IOSession) error {
	// Create a new tunnel to manage client connection and server connection.
	t := newTunnel(h.ctx, h.logger)
	defer func() {
		_ = t.Close()
	}()

	cc, err := newClientConn(h.ctx, h.logger, c, h.moCluster, h.router, t)
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()

	// TODO(volgariver6): white list handling.

	// client builds connections with a best CN server and returns
	// the server connection.
	sc, err := cc.BuildConnWithServer(true)
	if err != nil {
		cc.SendErrToClient(err.Error())
		return err
	}
	defer func() { _ = sc.Close() }()

	h.logger.Debug("build connection successfully",
		zap.String("client", cc.RawConn().RemoteAddr().String()),
		zap.String("server", sc.RawConn().RemoteAddr().String()),
	)

	// TODO(volgariver6): Is the handshake packet available for long time?
	if err := t.run(cc, sc); err != nil {
		return err
	}

	select {
	case <-h.ctx.Done():
		return h.ctx.Err()
	case err := <-t.errC:
		h.logger.Error("proxy handle error", zap.Error(err))
		return err
	}
}

// Close closes the handler.
func (h *handler) Close() error {
	if h != nil {
		h.moCluster.Close()
	}
	return nil
}
