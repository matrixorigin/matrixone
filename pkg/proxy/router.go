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
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

const (
	tenantLabelKey        = "account"
	defaultConnectTimeout = 3 * time.Second
)

// Router is an interface to select CN server and connects to it.
type Router interface {
	// SelectByConnID selects the CN server which has the connection ID.
	SelectByConnID(connID uint32) (*CNServer, error)

	// SelectByLabel selects the best CN server with the label.
	SelectByLabel(label labelInfo) (*CNServer, error)

	// Connect connects to the CN server and returns the connection.
	// It should take a handshakeResp as a parameter, which is the auth
	// request from client including tenant, username, database and others.
	Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error)
}

// CNServer represents the backend CN server, including salt, tenant, uuid and address.
type CNServer struct {
	// connID is the backend CN server's connection ID, which is global unique
	// and is tracked in connManager.
	connID uint32
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
		return nil, err
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

// SelectByConnID implements the CNConnector interface.
func (r *router) SelectByConnID(connID uint32) (*CNServer, error) {
	cn := r.rebalancer.connManager.getCNServer(connID)
	if cn == nil {
		return nil, moerr.NewInternalErrorNoCtx("no available CN server.")
	}
	// Return a new CNServer instance for temporary connection.
	return &CNServer{
		salt: cn.salt,
		uuid: cn.uuid,
		addr: cn.addr,
	}, nil
}

// SelectByLabel implements the CNConnector interface.
func (r *router) SelectByLabel(label labelInfo) (*CNServer, error) {
	var cns []*CNServer
	var cnEmpty, cnNotEmpty bool
	r.moCluster.GetCNService(label.genSelector(), func(s metadata.CNService) bool {
		cns = append(cns, &CNServer{
			reqLabel: label,
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
	if len(cns) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("no available CN server.")
	} else if len(cns) == 1 {
		return cns[0], nil
	}

	// getHash returns same hash for same labels.
	hash, err := label.getHash()
	if err != nil {
		return nil, err
	}

	s := r.rebalancer.connManager.selectOne(hash, cns, cnEmpty && cnNotEmpty)
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
	cn.connID = sc.ConnID()

	// After connect succeed, track the connection.
	r.rebalancer.connManager.connect(cn, t)

	return sc, packetToBytes(resp), nil
}
