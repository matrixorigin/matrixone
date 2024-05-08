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
	"math"
	"sync"
)

// Tenant defines alias tenant name type of string.
type Tenant string

// EmptyTenant is an empty tenant.
var EmptyTenant Tenant = ""

// tunnelSet defines the tunnels map type. map is the container
// to contain all tunnels. It must be used within cnTunnels.
type tunnelSet map[*tunnel]struct{}

// add adds a new tunnel to tunnelSet. This method needs lock in connManager.
func (s tunnelSet) add(t *tunnel) {
	if _, ok := s[t]; !ok {
		s[t] = struct{}{}
	}
}

// del deletes a tunnel from a tunnelSet. This method needs locked.
func (s tunnelSet) del(t *tunnel) {
	delete(s, t)
}

// exists checks if the tunnel exist in tunnelSet. This method needs lock in connManager.
func (s tunnelSet) exists(t *tunnel) bool {
	_, ok := s[t]
	return ok
}

// count returns the number of tunnels in tunnelSet. This method needs lock in connManager.
func (s tunnelSet) count() int {
	return len(s)
}

// countWithoutIntent returns the number of tunnels in tunnelSet whose transfer intent is false.
// This method needs lock in connManager.
func (s tunnelSet) countWithoutIntent() int {
	var r int
	for t := range s {
		if !t.transferIntent.Load() {
			r++
		}
	}
	return r
}

// cnTunnels defines the type of map cn-uuid => tunnelSet
type cnTunnels map[string]tunnelSet

// newCNTunnels creates new cnTunnels.
func newCNTunnels() cnTunnels {
	return make(cnTunnels)
}

// add adds a new tunnel to the CN server. This method needs lock in connManager.
func (t cnTunnels) add(uuid string, tun *tunnel) {
	if tun == nil {
		return
	}
	if t[uuid] == nil {
		t[uuid] = make(tunnelSet)
	}
	t[uuid].add(tun)
}

// add adds a new tunnel to the CN server. This method needs lock in connManager.
func (t cnTunnels) del(uuid string, tun *tunnel) {
	if tun == nil {
		return
	}
	tunnels, ok := t[uuid]
	if !ok {
		return
	}
	if tunnels.exists(tun) {
		tunnels.del(tun)
	}
}

// count returns number of all tunnels. This method needs lock in connManager.
func (t cnTunnels) count() int {
	var r int
	for _, ts := range t {
		r += ts.count()
	}
	return r
}

// connInfo contains label info and CN tunnels.
type connInfo struct {
	label     labelInfo
	cnTunnels cnTunnels
}

// newConnInfo creates a new connection info.
func newConnInfo(label labelInfo) *connInfo {
	return &connInfo{
		label:     label,
		cnTunnels: newCNTunnels(),
	}
}

// count returns the size of CN tunnels. This method needs lock in connManager.
func (ci *connInfo) count() int {
	if ci == nil {
		return 0
	}
	return ci.cnTunnels.count()
}

// connManager tracks the connections to backend CN servers.
type connManager struct {
	sync.Mutex
	// LabelHash => connInfo
	// The hash is a hashed value from labelInfo.
	conns map[LabelHash]*connInfo

	// cn uuid => tunnels
	// It is mainly used in CN server draining.
	cnTunnels cnTunnels

	// Map from connection ID to CN server.
	connIDServers map[uint32]*CNServer

	// transferredConnIDs keeps the connection IDs that are transferred.
	// We don't remove these IDs when close connection.
	transferredConnIDs map[uint32]struct{}
}

// newConnManager creates a new connManager.
func newConnManager() *connManager {
	m := &connManager{
		conns:              make(map[LabelHash]*connInfo),
		connIDServers:      make(map[uint32]*CNServer),
		transferredConnIDs: make(map[uint32]struct{}),
		cnTunnels:          make(cnTunnels),
	}
	return m
}

// selectOne select the most suitable CN server according the connection count
// on each CN server. The least count CN server is returned.
func (m *connManager) selectOne(hash LabelHash, cns []*CNServer) *CNServer {
	m.Lock()
	defer m.Unlock()

	var ret *CNServer
	var minCount = math.MaxInt
	for _, cn := range cns {
		ci, ok := m.conns[hash]
		// There are no connections yet on all CN servers of this tenant.
		// Means that no CN server has been connected for this tenant.
		// So return any of it.
		if !ok {
			return cn
		}
		tunnels, ok := ci.cnTunnels[cn.uuid]
		// There are no connections on this CN server.
		if !ok {
			return cn
		}
		// Choose the CNServer that has the least connections on it.
		if tunnels.count() < minCount {
			ret = cn
			minCount = tunnels.count()
		}
	}
	return ret
}

// connect adds a new connection to connection manager.
func (m *connManager) connect(cn *CNServer, t *tunnel) {
	m.Lock()
	defer m.Unlock()
	_, ok := m.conns[cn.hash]
	if !ok {
		m.conns[cn.hash] = newConnInfo(cn.reqLabel)
	}
	m.conns[cn.hash].cnTunnels.add(cn.uuid, t)
	if _, ok := m.connIDServers[cn.connID]; ok {
		// this connection is transferred to a new CN server.
		m.transferredConnIDs[cn.connID] = struct{}{}
	} else {
		m.connIDServers[cn.connID] = cn
	}

	if _, ok := m.cnTunnels[cn.uuid]; !ok {
		m.cnTunnels[cn.uuid] = make(tunnelSet)
	}
	m.cnTunnels[cn.uuid][t] = struct{}{}
}

// disconnect removes a connection from connection manager.
func (m *connManager) disconnect(cn *CNServer, t *tunnel) {
	m.Lock()
	defer m.Unlock()
	ci, ok := m.conns[cn.hash]
	if ok {
		ci.cnTunnels.del(cn.uuid, t)
	}
	delete(m.cnTunnels[cn.uuid], t)
	if _, ok := m.transferredConnIDs[cn.connID]; !ok {
		delete(m.connIDServers, cn.connID)
	} else {
		delete(m.transferredConnIDs, cn.connID)
	}
}

// count returns the total connection count.
func (m *connManager) count() int {
	m.Lock()
	defer m.Unlock()
	var total int
	for _, c := range m.conns {
		total += c.count()
	}
	return total
}

// getTenants get all label hashes that have connections currently.
func (m *connManager) getLabelHashes() []LabelHash {
	m.Lock()
	defer m.Unlock()
	hashes := make([]LabelHash, 0, len(m.conns))
	for h, ci := range m.conns {
		if ci.count() > 0 {
			hashes = append(hashes, h)
		}
	}
	return hashes
}

// getCNTunnels get all CN tunnels belongs to this label.
func (m *connManager) getCNTunnels(hash LabelHash) cnTunnels {
	m.Lock()
	defer m.Unlock()
	ci, ok := m.conns[hash]
	if !ok {
		return nil
	}
	return ci.cnTunnels
}

// getLabelInfo gets the label info in connManager.
func (m *connManager) getLabelInfo(hash LabelHash) labelInfo {
	m.Lock()
	defer m.Unlock()
	ci, ok := m.conns[hash]
	if !ok {
		return labelInfo{}
	}
	return ci.label
}

// getCNServerByConnID returns a CN server which has the connection ID.
func (m *connManager) getCNServerByConnID(connID uint32) *CNServer {
	m.Lock()
	defer m.Unlock()
	cn, ok := m.connIDServers[connID]
	if ok {
		return cn
	}
	return nil
}

func (m *connManager) getTunnelsByCNID(id string) []*tunnel {
	m.Lock()
	defer m.Unlock()
	tunMap, ok := m.cnTunnels[id]
	if !ok {
		return nil
	}
	tuns := make([]*tunnel, 0, len(tunMap))
	for tun := range tunMap {
		tuns = append(tuns, tun)
	}
	return tuns
}
