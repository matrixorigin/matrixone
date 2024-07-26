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
	"fmt"
	"net"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
	// rebalancer is the global rebalancer.
	rebalancer *rebalancer
	// counterSet counts the events in proxy.
	counterSet *counterSet
	// haKeeperClient is the client to communicate with HAKeeper.
	haKeeperClient logservice.ProxyHAKeeperClient
	// ipNetList is the list of ip net, which is parsed from CIDRs.
	ipNetList []*net.IPNet
	// SQLWorker works for the SQL selection. It connects to some
	// CN server and query for some information.
	sqlWorker SQLWorker
	// queryClient is the client which could send RPC request to query server.
	queryClient client.QueryClient
	// connCache is the cache of server connections.
	connCache ConnCache
}

var ErrNoAvailableCNServers = moerr.NewInternalErrorNoCtx("no available CN servers")

// newProxyHandler creates a new proxy handler.
func newProxyHandler(
	ctx context.Context,
	rt runtime.Runtime,
	cfg Config,
	st *stopper.Stopper,
	cs *counterSet,
	haKeeperClient logservice.ProxyHAKeeperClient,
	test bool,
) (*handler, error) {
	// Create the MO cluster.
	mc := clusterservice.NewMOCluster(cfg.UUID, haKeeperClient, cfg.Cluster.RefreshInterval.Duration)
	rt.SetGlobalVariables(runtime.ClusterService, mc)

	// Create the rebalancer.
	var opts []rebalancerOption
	opts = append(opts,
		withRebalancerInterval(cfg.RebalanceInterval.Duration),
		withRebalancerTolerance(cfg.RebalanceTolerance),
	)
	if cfg.RebalanceDisabled {
		opts = append(opts, withRebalancerDisabled())
	}

	re, err := newRebalancer(cfg.UUID, st, rt.Logger(), mc, opts...)
	if err != nil {
		return nil, err
	}

	// The SQL worker is mainly used in router currently.
	sw := newSQLWorker()

	var ru Router
	if test {
		ru = newRouter(mc, re, re.connManager, false)
	} else {
		ru = newRouter(mc, re, sw, false,
			withConnectTimeout(cfg.ConnectTimeout.Duration),
			withAuthTimeout(cfg.AuthTimeout.Duration),
		)
	}

	// Decorate the router if plugin is enabled
	if cfg.Plugin != nil {
		p, err := newRPCPlugin(cfg.UUID, cfg.Plugin.Backend, cfg.Plugin.Timeout)
		if err != nil {
			return nil, err
		}
		ru = newPluginRouter(cfg.UUID, ru, p)
	}

	var ipNetList []*net.IPNet
	for _, cidr := range cfg.InternalCIDRs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			rt.Logger().Error("failed to parse CIDR",
				zap.String("CIDR", cidr),
				zap.Error(err))
		} else {
			ipNetList = append(ipNetList, ipNet)
		}
	}
	qc, err := client.NewQueryClient(cfg.UUID, morpc.Config{})
	if err != nil {
		return nil, err
	}
	h := &handler{
		ctx:            ctx,
		logger:         rt.Logger(),
		config:         cfg,
		stopper:        st,
		moCluster:      mc,
		counterSet:     cs,
		router:         ru,
		rebalancer:     re,
		haKeeperClient: haKeeperClient,
		ipNetList:      ipNetList,
		sqlWorker:      sw,
		queryClient:    qc,
	}
	if h.config.ConnCacheEnabled {
		h.connCache = newConnCache(ctx, cfg.UUID, rt.Logger(), withQueryClient(qc))
	}
	return h, nil
}

// handle handles the incoming connection.
func (h *handler) handle(c goetty.IOSession) error {
	h.logger.Info("new connection comes", zap.Uint64("session ID", c.ID()))
	v2.ProxyConnectAcceptedCounter.Inc()
	h.counterSet.connAccepted.Add(1)
	h.counterSet.connTotal.Add(1)
	defer func() {
		v2.ProxyConnectCurrentCounter.Inc()
		h.counterSet.connTotal.Add(-1)
	}()

	// Create a new tunnel to manage client connection and server connection.
	t := newTunnel(h.ctx, h.logger, h.counterSet,
		withRealConn(),
		withRebalancePolicy(RebalancePolicyMapping[h.config.RebalancePolicy]),
		withRebalancer(h.rebalancer),
		withConnCacheEnabled(h.connCache != nil),
	)
	defer func() {
		_ = t.Close()
	}()

	cc, err := newClientConn(
		h.ctx,
		&h.config,
		h.logger,
		h.counterSet,
		c,
		h.haKeeperClient,
		h.moCluster,
		h.router,
		t,
		h.ipNetList,
		h.queryClient,
		h.connCache,
	)
	if err != nil {
		h.logger.Error("failed to create client conn", zap.Error(err))
		return err
	}
	h.logger.Debug("client conn created")
	defer func() { _ = cc.Close() }()

	// client builds connections with a best CN server and returns
	// the server connection.
	sc, err := cc.BuildConnWithServer("")
	if err != nil {
		if isConnEndErr(err) {
			return nil
		}
		h.logger.Error("failed to create server conn", zap.Error(err))
		h.counterSet.updateWithErr(err)
		cc.SendErrToClient(err)
		return err
	}
	h.logger.Debug("server conn created")
	defer func() {
		// This Close() function just disconnect from connManager,
		// but do not close the real raw connection. The raw connection
		// is closed if the server connection could not be pushed into
		// the connection cache, which is in (*clientConn).handleQuitEvent()
		// function.
		_ = sc.Close()
	}()

	h.logger.Info("build connection",
		zap.String("client->proxy", fmt.Sprintf("%s -> %s", cc.RawConn().RemoteAddr(), cc.RawConn().LocalAddr())),
		zap.String("proxy->server", fmt.Sprintf("%s -> %s", sc.RawConn().LocalAddr(), sc.RawConn().RemoteAddr())),
		zap.Uint32("conn ID", cc.ConnID()),
		zap.Uint64("session ID", c.ID()),
	)

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
				h.logger.Debug("event handler stopped.")
				return
			}
		}
	}); err != nil {
		return err
	}

	if err := t.run(cc, sc); err != nil {
		return err
	}

	select {
	case <-h.ctx.Done():
		return h.ctx.Err()
	case err := <-t.errC:
		if isEOFErr(err) || isConnEndErr(err) {
			h.logger.Info("connection closed",
				zap.Uint32("Conn ID", cc.ConnID()),
				zap.Uint64("session ID", c.ID()),
			)
			return nil
		}
		h.counterSet.updateWithErr(err)
		h.logger.Error("proxy handle error",
			zap.Uint32("Conn ID", cc.ConnID()),
			zap.Uint64("session ID", c.ID()),
			zap.Error(err),
		)
		return err
	}
}

// Close closes the handler.
func (h *handler) Close() error {
	if h != nil {
		h.moCluster.Close()
		_ = h.haKeeperClient.Close()
		if h.queryClient != nil {
			_ = h.queryClient.Close()
		}
		if h.connCache != nil {
			_ = h.connCache.Close()
		}
	}
	return nil
}
