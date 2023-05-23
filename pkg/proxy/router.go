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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const (
	tenantLabelKey        = "account"
	superTenant           = "sys"
	superUserRoot         = "root"
	superUserDump         = "dump"
	defaultConnectTimeout = 3 * time.Second
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
	Route(ctx context.Context, client clientInfo, filter func(string) bool) (*CNServer, error)

	// SelectByConnID selects the CN server which has the connection ID.
	SelectByConnID(connID uint32) (*CNServer, error)

	// SelectByTenant selects the CN servers belongs to the tenant.
	SelectByTenant(tenant Tenant) ([]*CNServer, error)

	// Connect connects to the CN server and returns the connection.
	// It should take a handshakeResp as a parameter, which is the auth
	// request from client including tenant, username, database and others.
	Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error)
}

// CNServer represents the backend CN server, including salt, tenant, uuid and address.
// When there is a new client connection, a new CNServer will be created.
type CNServer struct {
	// backendConnID is the backend CN server's connection ID, which is global unique
	// and is tracked in connManager.
	backendConnID uint32
	// clientConnID is the connection ID in proxy side.
	proxyConnID uint32
	// salt is generated in proxy module and will be sent to backend
	// server when build connection.
	salt []byte
	// reqLabel is the client requests, but not the label which CN server really has.
	reqLabel labelInfo
	// cnLabel is the labels that CN server has.
	cnLabel map[string]metadata.LabelList
	// hash keep the hash in it.
	hash LabelHash
	// uuid of the CN server.
	uuid string
	// addr is the net address of CN server.
	addr string
}

// Connect connects to backend server and returns IOSession.
func (s *CNServer) Connect() (goetty.IOSession, error) {
	c := goetty.NewIOSession(goetty.WithSessionCodec(frontend.NewSqlCodec()))
	err := c.Connect(s.addr, defaultConnectTimeout)
	if err != nil {
		return nil, newConnectErr(err)
	}
	// When build connection with backend server, proxy send its salt
	// to make sure the backend server uses the same salt to do authentication.
	if err := c.Write(s.salt, goetty.WriteOptions{Flush: true}); err != nil {
		return nil, err
	}
	return c, nil
}

// router can route the client connections to backend CN servers.
// Also, it can balance the load between CN servers so there is a
// rebalancer in it.
type router struct {
	rebalancer *rebalancer
	moCluster  clusterservice.MOCluster
	test       bool
}

var _ Router = (*router)(nil)

// newCNConnector creates a Router.
func newRouter(
	mc clusterservice.MOCluster,
	r *rebalancer,
	test bool,
) Router {
	return &router{
		rebalancer: r,
		moCluster:  mc,
		test:       test,
	}
}

// SelectByConnID implements the Router interface.
func (r *router) SelectByConnID(connID uint32) (*CNServer, error) {
	cn := r.rebalancer.connManager.getCNServerByConnID(connID)
	if cn == nil {
		return nil, moerr.NewInternalErrorNoCtx("no available CN server.")
	}
	// Return a new CNServer instance for temporary connection.
	return &CNServer{
		backendConnID: cn.backendConnID,
		salt:          cn.salt,
		uuid:          cn.uuid,
		addr:          cn.addr,
	}, nil
}

// SelectByTenant implements the Router interface.
func (r *router) SelectByTenant(tenant Tenant) ([]*CNServer, error) {
	return r.rebalancer.connManager.getCNServersByTenant(tenant), nil
}

// selectForSuperTenant is used to select CN servers for sys tenant.
// For sys tenant, there are some special strategies to select CN servers.
// The following strategies are listed in order of priority：
//  1. The CN servers which are configured as sys account.
//  2. The CN servers which are configured as some labels whose key is not account.
//  3. The CN servers which are configured as no labels.
//  4. At last, if no CN servers are selected,
//     4.1 If the username is dump or root, we just select one randomly.
//     4.2 Else, no servers are selected.
func (r *router) selectForSuperTenant(c clientInfo, filter func(string) bool) []*CNServer {
	var cns, emptyCNs, allCNs []*CNServer

	// S1: Select servers that configured as sys account.
	r.moCluster.GetCNService(c.labelInfo.genSelector(), func(s metadata.CNService) bool {
		if filter != nil && filter(s.ServiceID) {
			return true
		}
		cn := &CNServer{
			reqLabel: c.labelInfo,
			cnLabel:  s.Labels,
			uuid:     s.ServiceID,
			addr:     s.SQLAddress,
		}
		// At this phase, only append non-empty servers.
		if len(s.Labels) == 0 {
			emptyCNs = append(emptyCNs, cn)
		} else {
			cns = append(cns, cn)
		}
		return true
	})
	if len(cns) > 0 {
		return cns
	}

	// S2: If there are no servers that are configured as sys account.
	// There may be some performance issues, but we need to do this still.
	// Select all CN servers and select ones which are not configured as
	// label with key "account".
	r.moCluster.GetCNService(clusterservice.NewSelector(), func(s metadata.CNService) bool {
		if filter != nil && filter(s.ServiceID) {
			return true
		}
		cn := &CNServer{
			reqLabel: c.labelInfo,
			cnLabel:  s.Labels,
			uuid:     s.ServiceID,
			addr:     s.SQLAddress,
		}
		// Append CN servers that are not configured as label with key "account".
		if _, ok := s.Labels[tenantLabelKey]; len(s.Labels) > 0 && !ok {
			cns = append(cns, cn)
		}
		allCNs = append(allCNs, cn)
		return true
	})
	if len(cns) > 0 {
		return cns
	}

	// S3: Select CN servers which has no labels.
	if len(emptyCNs) > 0 {
		return emptyCNs
	}

	// S4.1: If the root is super, return all servers.
	if c.isSuperUser() {
		return allCNs
	}

	// S4.2: No servers are returned.
	return nil
}

func (r *router) selectForCommonTenant(c clientInfo, filter func(string) bool) []*CNServer {
	var cns []*CNServer
	var cnNotEmpty, cnEmpty bool
	r.moCluster.GetCNService(c.labelInfo.genSelector(), func(s metadata.CNService) bool {
		if filter != nil && filter(s.ServiceID) {
			return true
		}
		cns = append(cns, &CNServer{
			reqLabel: c.labelInfo,
			cnLabel:  s.Labels,
			uuid:     s.ServiceID,
			addr:     s.SQLAddress,
		})
		if len(s.Labels) > 0 {
			cnNotEmpty = true
		} else {
			cnEmpty = true
		}
		return true
	})

	// The result may contain some CN servers whose label is empty.
	// If there are empty and non-empty ones, we should exclude the empty ones.
	if cnNotEmpty && cnEmpty {
		i := 0
		for _, cn := range cns {
			if len(cn.cnLabel) != 0 {
				cns[i] = cn
				i++
			}
		}
		return cns[:i]
	}
	return cns
}

// Route implements the Router interface.
func (r *router) Route(ctx context.Context, c clientInfo, filter func(string) bool) (*CNServer, error) {
	var cns []*CNServer
	if c.isSuperTenant() {
		cns = r.selectForSuperTenant(c, filter)
	} else {
		cns = r.selectForCommonTenant(c, filter)
	}

	if len(cns) == 0 {
		return nil, noCNServerErr
	} else if len(cns) == 1 {
		return cns[0], nil
	}

	// getHash returns same hash for same labels.
	hash, err := c.labelInfo.getHash()
	if err != nil {
		return nil, err
	}

	s := r.rebalancer.connManager.selectOne(hash, cns)
	if s == nil {
		return nil, ErrNoAvailableCNServers
	}
	// Set the label hash for the select one.
	s.hash = hash
	return s, nil
}

// Connect implements the CNConnector interface.
func (r *router) Connect(
	cn *CNServer, handshakeResp *frontend.Packet, t *tunnel,
) (_ ServerConn, _ []byte, e error) {
	// Creates a server connection.
	sc, err := newServerConn(cn, t, r.rebalancer)
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
		return sc, makeOKPacket(), nil
	}

	// Use the handshakeResp, which is auth request from client, to communicate
	// with CN server.
	resp, err := sc.HandleHandshake(handshakeResp)
	if err != nil {
		r.rebalancer.connManager.disconnect(cn, t)
		return nil, nil, err
	}
	// After handshake with backend CN server, set the connID of serverConn.
	cn.backendConnID = sc.ConnID()

	// After connect succeed, track the connection.
	r.rebalancer.connManager.connect(cn, t)

	return sc, packetToBytes(resp), nil
}
