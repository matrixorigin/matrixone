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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
)

const (
	tenantLabelKey = "account"
)

var (
	// noCNServerErr indicates that there are no available CN servers.
	noCNServerErr = moerr.NewInternalErrorNoCtx("no available CN server")
)

// Router is an interface to select CN server and connects to it.
type Router interface {
	// Route selects the best CN server according to the clientInfo.
	// This is the only method that allocate *CNServer, and other
	// SelectXXX method in this interface select CNServer from the
	// ones it allocated.
	// filter is a function which is used to do more checks whether the
	// CN server is a proper one. If it returns true, means that CN
	// server is not a valid one.
	Route(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error)

	// SelectByConnID selects the CN server which has the connection ID.
	SelectByConnID(connID uint32) (*CNServer, error)

	// Connect connects to the CN server and returns the connection.
	// It should take a handshakeResp as a parameter, which is the auth
	// request from client including tenant, username, database and others.
	Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error)
}

// RefreshableRouter is a router that can be refreshed to get latest route strategy
type RefreshableRouter interface {
	Router

	Refresh(sync bool)
}

// router can route the client connections to backend CN servers.
// Also, it can balance the load between CN servers so there is a
// rebalancer in it.
type router struct {
	rebalancer *rebalancer
	moCluster  clusterservice.MOCluster
	sqlRouter  SQLRouter
	test       bool

	// timeout configs.
	connectTimeout time.Duration
	authTimeout    time.Duration
}

type routeOption func(*router)

var _ Router = (*router)(nil)

func withConnectTimeout(t time.Duration) routeOption {
	return func(r *router) {
		r.connectTimeout = t
	}
}

func withAuthTimeout(t time.Duration) routeOption {
	return func(r *router) {
		r.authTimeout = t
	}
}

// newCNConnector creates a Router.
func newRouter(
	mc clusterservice.MOCluster,
	r *rebalancer,
	sqlRouter SQLRouter,
	test bool,
	opts ...routeOption,
) Router {
	rt := &router{
		rebalancer: r,
		moCluster:  mc,
		sqlRouter:  sqlRouter,
		test:       test,
	}
	for _, opt := range opts {
		opt(rt)
	}
	if rt.authTimeout == 0 { // for test
		rt.authTimeout = defaultAuthTimeout / 3
	}
	return rt
}

// SelectByConnID implements the Router interface.
func (r *router) SelectByConnID(connID uint32) (*CNServer, error) {
	cn, err := r.sqlRouter.GetCNServerByConnID(connID)
	if err != nil {
		logutil.Errorf("failed to get cn server by conn id %d: %v", connID, err)
		return nil, err
	}
	if cn == nil {
		return nil, noCNServerErr
	}
	// Return a new CNServer instance for temporary connection.
	return &CNServer{
		connID: cn.connID,
		salt:   cn.salt,
		uuid:   cn.uuid,
		addr:   cn.addr,
	}, nil
}

// selectForSuperTenant is used to select CN servers for sys tenant.
// For more detail, see route.RouteForSuperTenant.
func (r *router) selectForSuperTenant(sid string, c clientInfo, filter func(string) bool) []*CNServer {
	var cns []*CNServer
	route.RouteForSuperTenant(
		sid,
		c.labelInfo.genSelector(clusterservice.EQ_Globbing), c.username, filter,
		func(s *metadata.CNService) {
			cns = append(cns, &CNServer{
				reqLabel: c.labelInfo,
				cnLabel:  s.Labels,
				uuid:     s.ServiceID,
				addr:     s.SQLAddress,
			})
		})
	return cns
}

// selectForCommonTenant is used to select CN servers for common tenant.
// For more detail, see route.RouteForCommonTenant.
func (r *router) selectForCommonTenant(
	sid string,
	c clientInfo,
	filter func(string) bool,
) []*CNServer {
	var cns []*CNServer
	route.RouteForCommonTenant(
		sid,
		c.labelInfo.genSelector(clusterservice.EQ_Globbing), filter, func(s *metadata.CNService) {
			cns = append(cns, &CNServer{
				reqLabel: c.labelInfo,
				cnLabel:  s.Labels,
				uuid:     s.ServiceID,
				addr:     s.SQLAddress,
			})
		},
	)
	return cns
}

// Route implements the Router interface.
func (r *router) Route(ctx context.Context, sid string, c clientInfo, filter func(string) bool) (*CNServer, error) {
	var cns []*CNServer
	if c.isSuperTenant() {
		cns = r.selectForSuperTenant(sid, c, filter)
	} else {
		cns = r.selectForCommonTenant(sid, c, filter)
	}
	cnCount := len(cns)
	v2.ProxyAvailableBackendServerNumGauge.
		WithLabelValues(string(c.Tenant)).Set(float64(cnCount))

	if cnCount == 0 {
		return nil, noCNServerErr
	} else if cnCount == 1 {
		cns[0].hash = c.hash
		return cns[0], nil
	}

	s := r.rebalancer.connManager.selectOne(c.hash, cns)
	if s == nil {
		return nil, ErrNoAvailableCNServers
	}
	// Set the label hash for the select one.
	s.hash = c.hash
	return s, nil
}

// Connect implements the CNConnector interface.
func (r *router) Connect(
	cn *CNServer, handshakeResp *frontend.Packet, t *tunnel,
) (_ ServerConn, _ []byte, e error) {
	// Creates a server connection.
	sc, err := newServerConn(cn, t, r.rebalancer, r.connectTimeout)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if e != nil {
			_ = sc.Close()
		}
	}()

	// For test, ignore handshake phase.
	if r.test {
		r.rebalancer.connManager.connect(cn, t)
		// The second value should be recognized OK packet.
		return sc, makeOKPacket(8), nil
	}

	// Use the handshakeResp, which is auth request from client, to communicate
	// with CN server.
	resp, err := sc.HandleHandshake(handshakeResp, r.authTimeout)
	if err != nil {
		r.rebalancer.connManager.disconnect(cn, t)
		return nil, nil, err
	}
	// After handshake with backend CN server, set the connID of serverConn.
	cn.connID = sc.ConnID()

	// After connect succeed, track the connection.
	r.rebalancer.connManager.connect(cn, t)

	return sc, packetToBytes(resp), nil
}

// Refresh refreshes the router
func (r *router) Refresh(sync bool) {
	r.moCluster.ForceRefresh(sync)
}
