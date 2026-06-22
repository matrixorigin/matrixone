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

	"github.com/petermattis/goid"

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
	// allCNServersBusyErr indicates that all CN servers are busy, possibly due to too many active transactions
	allCNServersBusyErr = moerr.NewAllCNServersBusyNoCtx()
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

	// AllServers returns all CN servers. Note that the request user have to be
	// sys tenant.
	AllServers(sid string) ([]*CNServer, error)

	// Connect connects to the CN server and returns the connection.
	// It should take a handshakeResp as a parameter, which is the auth
	// request from client including tenant, username, database and others.
	Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error)
}

// routeSelectedConnector is implemented by routers that distinguish
// Route-selected new-session connects from internal/admin connects. The
// former should feed success/failure back into the CN health breaker; the
// latter must not mutate breaker state because they bypass the Route/probe
// state machine.
type routeSelectedConnector interface {
	ConnectRouteSelected(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error)
}

// transferRouter is implemented by routers that want session transfer /
// migration traffic to select a target CN without consuming a breaker-managed
// recovery probe. The transfer path is control-plane traffic, not a new
// client-session connect.
type transferRouter interface {
	RouteForTransfer(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error)
}

// cacheReuseChecker is implemented by routers that can decide whether a cached
// backend connection is still eligible for a fresh client login. Cached reuse
// must honor the same CN health policy as a new session route.
type cacheReuseChecker interface {
	CanReuseCachedCN(cn *CNServer) bool
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

	// health is the per-CN circuit breaker used to skip CN servers that are
	// temporarily unable to accept new connections. It may be nil, which
	// disables the feature.
	health *cnHealthChecker
	// healthDisabled, when true, leaves health nil after construction.
	healthDisabled bool
	// healthOpts are applied when the default health checker is built.
	healthOpts []cnHealthOption
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

// withCNHealthCheckDisabled disables the CN health circuit breaker.
func withCNHealthCheckDisabled() routeOption {
	return func(r *router) {
		r.healthDisabled = true
	}
}

// withCNHealthCheckCooldown sets the base and max cooldown of the CN health
// circuit breaker.
func withCNHealthCheckCooldown(base, max time.Duration) routeOption {
	return func(r *router) {
		r.healthOpts = append(r.healthOpts, withCNHealthCooldown(base, max))
	}
}

// withCNHealthCheckFailThreshold sets how many consecutive connect failures
// trip a CN's health breaker.
func withCNHealthCheckFailThreshold(threshold int) routeOption {
	return func(r *router) {
		r.healthOpts = append(r.healthOpts, withCNHealthFailThreshold(threshold))
	}
}

// withCNHealthCheckProbeWindow sets the half-open probe slot lifetime.
func withCNHealthCheckProbeWindow(d time.Duration) routeOption {
	return func(r *router) {
		r.healthOpts = append(r.healthOpts, withCNHealthProbeWindow(d))
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
	if !rt.healthDisabled {
		rt.health = newCNHealthChecker(rt.healthOpts...)
	}
	return rt
}

// SelectByConnID implements the Router interface.
func (r *router) SelectByConnID(connID uint32) (*CNServer, error) {
	cn, err := r.sqlRouter.GetCNServerByConnID(connID)
	if err != nil {
		logutil.Errorf("failed to get cn server by conn id %d goId %d: %v", connID, goid.Get(), err)
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

// AllServers implements the Router interface.
func (r *router) AllServers(sid string) ([]*CNServer, error) {
	var cns []*CNServer
	route.RouteForSuperTenant(
		sid,
		clusterservice.NewSelectAll(), "dump", nil,
		func(s *metadata.CNService) {
			cns = append(cns, &CNServer{
				uuid: s.ServiceID,
				addr: s.SQLAddress,
			})
		})
	return cns, nil
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

func (r *router) routeCandidates(sid string, c clientInfo, filter func(string) bool) []*CNServer {
	var cns []*CNServer
	if c.isSuperTenant() {
		cns = r.selectForSuperTenant(sid, c, filter)
	} else {
		cns = r.selectForCommonTenant(sid, c, filter)
	}
	return cns
}

// Route implements the Router interface.
func (r *router) Route(ctx context.Context, sid string, c clientInfo, filter func(string) bool) (*CNServer, error) {
	cns := r.routeCandidates(sid, c, filter)
	cnCount := len(cns)
	v2.ProxyAvailableBackendServerNumGauge.
		WithLabelValues(string(c.Tenant)).Set(float64(cnCount))

	if cnCount == 0 {
		return nil, noCNServerErr
	}

	// Apply the CN health circuit breaker. This skips CN servers that are
	// temporarily unable to accept connections (e.g. overloaded), so new
	// connections do not keep paying the auth timeout on a known-bad CN.
	candidates, allBusy := r.health.pick(cns)
	if allBusy {
		// Every candidate CN is temporarily unhealthy. Fast-fail with a
		// busy error instead of hanging on a doomed connection or returning
		// a misleading "no available CN server" error. The breaker will let
		// a half-open probe through once a cooldown expires, so the cluster
		// recovers automatically.
		return nil, allCNServersBusyErr
	}

	if len(candidates) == 1 {
		// A single candidate is either the only healthy CN or a half-open
		// probe. In both cases return it directly: routing it through
		// selectOne is unnecessary, and for a probe it would risk dropping
		// the reserved probe slot (see pick's invariants).
		candidates[0].hash = c.hash
		return candidates[0], nil
	}

	s := r.rebalancer.connManager.selectOne(c.hash, candidates)
	if s == nil {
		return nil, ErrNoAvailableCNServers
	}
	// Set the label hash for the select one.
	s.hash = c.hash
	return s, nil
}

// RouteForTransfer selects a CN for session transfer / migration. It uses the
// same label/filter selection as Route, but intentionally bypasses the
// breaker/probe gate so transfer traffic does not consume or mutate
// breaker-managed recovery probes.
func (r *router) RouteForTransfer(
	ctx context.Context, sid string, c clientInfo, filter func(string) bool,
) (*CNServer, error) {
	cns := r.routeCandidates(sid, c, filter)
	cnCount := len(cns)
	v2.ProxyAvailableBackendServerNumGauge.
		WithLabelValues(string(c.Tenant)).Set(float64(cnCount))

	if cnCount == 0 {
		return nil, noCNServerErr
	}
	if cnCount == 1 {
		cns[0].hash = c.hash
		return cns[0], nil
	}

	s := r.rebalancer.connManager.selectOne(c.hash, cns)
	if s == nil {
		return nil, ErrNoAvailableCNServers
	}
	s.hash = c.hash
	return s, nil
}

// CanReuseCachedCN implements cacheReuseChecker. Cached connections must only
// be reused when the breaker is closed/absent; otherwise they would bypass the
// new-session health gate.
func (r *router) CanReuseCachedCN(cn *CNServer) bool {
	if cn == nil {
		return true
	}
	return r.health.canReuseCachedCN(cn.uuid)
}

func (r *router) connect(
	cn *CNServer, handshakeResp *frontend.Packet, t *tunnel, accountHealth bool,
) (ServerConn, []byte, error) {
	// Creates a server connection.
	sc, err := newServerConn(cn, t, r.rebalancer, r.connectTimeout)
	if err != nil {
		// Connection failed, remove the placeholder that was added in selectOne.
		r.rebalancer.connManager.selectOneFailed(cn.hash, cn.uuid)
		// Only Route-selected session connects feed back into the breaker, and
		// only timeout/overload-like failures should be classified as
		// temporarily unhealthy. Hard dial failures (ECONNREFUSED, etc.) keep
		// their old semantics instead of being globally reclassified as
		// "busy".
		if accountHealth && isTimeoutErr(err) {
			r.health.reportFailure(cn.uuid, cn.addr)
		}
		return nil, nil, err
	}

	// For test, ignore handshake phase.
	if r.test {
		r.rebalancer.connManager.connect(cn, t)
		if accountHealth {
			r.health.reportSuccess(cn.uuid, cn.addr)
		}
		// The second value should be recognized OK packet.
		return sc, makeOKPacket(8), nil
	}

	// Use the handshakeResp, which is auth request from client, to communicate
	// with CN server.
	resp, err := sc.HandleHandshake(handshakeResp, r.authTimeout)
	if err != nil {
		_ = sc.Close()
		// Handshake failed, remove the placeholder that was added in selectOne.
		r.rebalancer.connManager.selectOneFailed(cn.hash, cn.uuid)
		if accountHealth && isTimeoutErr(err) {
			// A handshake timeout (most commonly an overloaded CN stuck on
			// login/session creation) marks the CN temporarily unhealthy.
			r.health.reportFailure(cn.uuid, cn.addr)
		}
		return nil, nil, err
	}
	// After handshake with backend CN server, set the connID of serverConn.
	cn.connID = sc.ConnID()

	// After connect succeed, track the connection.
	r.rebalancer.connManager.connect(cn, t)
	if accountHealth {
		// The Route-selected CN accepted a new session: close its breaker if
		// it was open.
		r.health.reportSuccess(cn.uuid, cn.addr)
	}

	return sc, packetToBytes(resp), nil
}

// Connect implements the Router interface. It is used by internal/admin paths
// (kill, upgrade, temp exec) and therefore does NOT mutate the CN health
// breaker.
func (r *router) Connect(
	cn *CNServer, handshakeResp *frontend.Packet, t *tunnel,
) (ServerConn, []byte, error) {
	return r.connect(cn, handshakeResp, t, false)
}

// ConnectRouteSelected is used only for Route-selected new-session connects.
// Those connects feed timeout/overload failures and successes back into the
// CN health breaker.
func (r *router) ConnectRouteSelected(
	cn *CNServer, handshakeResp *frontend.Packet, t *tunnel,
) (ServerConn, []byte, error) {
	return r.connect(cn, handshakeResp, t, true)
}

// Refresh refreshes the router
func (r *router) Refresh(sync bool) {
	r.moCluster.ForceRefresh(sync)
}
