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
	// counterSet counts the events in proxy.
	counterSet *counterSet
	// haKeeperClient is the client to communicate with HAKeeper.
	haKeeperClient logservice.ClusterHAKeeperClient
}

var ErrNoAvailableCNServers = moerr.NewInternalErrorNoCtx("no available CN servers")

// newProxyHandler creates a new proxy handler.
func newProxyHandler(
	ctx context.Context,
	rt runtime.Runtime,
	cfg Config,
	st *stopper.Stopper,
	cs *counterSet,
	testHAKeeperClient logservice.ClusterHAKeeperClient,
) (*handler, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var err error
	c := testHAKeeperClient
	if c == nil {
		c, err = logservice.NewProxyHAKeeperClient(ctx, cfg.HAKeeper.ClientConfig)
		if err != nil {
			return nil, err
		}
	}

	// Create the MO cluster.
	mc := clusterservice.NewMOCluster(c, cfg.Cluster.RefreshInterval.Duration)
	rt.SetGlobalVariables(runtime.ClusterService, mc)

	// Create the rebalancer.
	var opts []rebalancerOption
	opts = append(opts,
		withRebalancerInterval(cfg.RebalanceInterval.Duration),
		withRebalancerTolerance(cfg.RebalanceToerance),
	)
	if cfg.RebalanceDisabled {
		opts = append(opts, withRebalancerDisabled())
	}

	re, err := newRebalancer(st, rt.Logger(), mc, opts...)
	if err != nil {
		return nil, err
	}

	ru := newRouter(mc, re, false)
	// Decorate the router if plugin is enabled
	if cfg.Plugin != nil {
		p, err := newRPCPlugin(cfg.Plugin.Backend, cfg.Plugin.Timeout)
		if err != nil {
			return nil, err
		}
		ru = newPluginRouter(ru, p)
	}
	return &handler{
		ctx:            context.Background(),
		logger:         rt.Logger(),
		config:         cfg,
		stopper:        st,
		moCluster:      mc,
		counterSet:     cs,
		router:         ru,
		haKeeperClient: c,
	}, nil
}

// handle handles the incoming connection.
func (h *handler) handle(c goetty.IOSession) error {
	h.counterSet.connAccepted.Add(1)
	h.counterSet.connTotal.Add(1)
	defer h.counterSet.connTotal.Add(-1)

	// Create a new tunnel to manage client connection and server connection.
	t := newTunnel(h.ctx, h.logger, h.counterSet)
	defer func() {
		_ = t.Close()
	}()

	cc, err := newClientConn(
		h.ctx, &h.config, h.logger, h.counterSet, c, h.haKeeperClient, h.moCluster, h.router, t,
	)
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()

	// client builds connections with a best CN server and returns
	// the server connection.
	sc, err := cc.BuildConnWithServer(true)
	if err != nil {
		h.counterSet.updateWithErr(err)
		cc.SendErrToClient(err.Error())
		return err
	}
	defer func() { _ = sc.Close() }()

	h.logger.Debug("build connection successfully",
		zap.String("client", cc.RawConn().RemoteAddr().String()),
		zap.String("server", sc.RawConn().RemoteAddr().String()),
	)

	if err := t.run(cc, sc); err != nil {
		return err
	}

	st := stopper.NewStopper("proxy-conn-handle", stopper.WithLogger(h.logger.RawLogger()))
	defer st.Stop()
	// Starts the event handler go-routine to handle the events comes from tunnel data flow,
	// such as, kill connection event.
	if err := st.RunNamedTask("event-handler", func(ctx context.Context) {
		for {
			select {
			case e := <-t.reqC:
				if err := cc.HandleEvent(ctx, e, t.respC); err != nil {
					h.logger.Error("failed to handle event",
						zap.Any("event", e), zap.Error(err))
				}
			case r := <-t.respC:
				if len(r) > 0 {
					t.mu.Lock()
					// We must call this method because it locks writeMu.
					if err := t.mu.serverConn.writeDataDirectly(cc.RawConn(), r); err != nil {
						h.logger.Error("failed to write event response",
							zap.Any("response", r), zap.Error(err))
					}
					t.mu.Unlock()
				}
			case <-ctx.Done():
				h.logger.Info("event handler stopped.")
				return
			}
		}
	}); err != nil {
		return err
	}

	select {
	case <-h.ctx.Done():
		return h.ctx.Err()
	case err := <-t.errC:
		h.counterSet.updateWithErr(err)
		h.logger.Error("proxy handle error", zap.Error(err))
		return err
	}
}

// Close closes the handler.
func (h *handler) Close() error {
	if h != nil {
		h.moCluster.Close()
		_ = h.haKeeperClient.Close()
	}
	return nil
}
