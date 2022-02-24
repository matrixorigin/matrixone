// Copyright 2021 Matrix Origin
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

package main

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/transport"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

type NewInterceptableTransport func(
	t transport.Trans,
	nodes func() fz.Nodes,
) transport.Trans

type BlockNetwork func(from, to fz.NodeID)

func (_ Def) NewInterceptableTransport() (
	newTransport NewInterceptableTransport,
	block BlockNetwork,
) {

	var l sync.Mutex
	blocked := make(map[[2]fz.NodeID]bool)

	newTransport = func(upstream transport.Trans, getNodes func() fz.Nodes) transport.Trans {

		msgIsBlocked := func(msg meta.RaftMessage) bool {

			fromStoreID := msg.From.ContainerID
			toStoreID := msg.To.ContainerID
			var fromNodeID fz.NodeID = -1
			var toNodeID fz.NodeID = -1
			nodes := getNodes()
			for i, node := range nodes {
				if node.(*Node).RaftStore.Meta().ID == fromStoreID {
					fromNodeID = fz.NodeID(i)
				}
				if node.(*Node).RaftStore.Meta().ID == toStoreID {
					toNodeID = fz.NodeID(i)
				}
			}
			if fromNodeID == -1 {
				if len(nodes) == 0 {
					panic(fmt.Errorf("no such store: %v", fromStoreID))
				} else {
					return false
				}
			}
			if toNodeID == -1 {
				if len(nodes) == 0 {
					panic(fmt.Errorf("no such store: %v", toStoreID))
				} else {
					return false
				}
			}

			l.Lock()
			defer l.Unlock()
			return blocked[[2]fz.NodeID{fromNodeID, toNodeID}]
		}

		return &TransportProxy{

			send: func(msg meta.RaftMessage) bool {
				if msgIsBlocked(msg) {
					return false
				}
				return upstream.Send(msg)
			},

			sendSnapshot: func(msg meta.RaftMessage) bool {
				if msgIsBlocked(msg) {
					return false
				}
				return upstream.SendSnapshot(msg)
			},

			setFilter: func(fn func(meta.RaftMessage) bool) {
				upstream.SetFilter(fn)
			},

			sendingSnapshotCount: func() uint64 {
				return upstream.SendingSnapshotCount()
			},

			start: func() error {
				return upstream.Start()
			},

			close: func() error {
				return upstream.Close()
			},
		}
	}

	block = func(
		from, to fz.NodeID,
	) {
		l.Lock()
		defer l.Unlock()
		blocked[[2]fz.NodeID{from, to}] = true
	}

	return
}

type TransportProxy struct {
	send                 func(meta.RaftMessage) bool
	sendSnapshot         func(meta.RaftMessage) bool
	setFilter            func(fn func(meta.RaftMessage) bool)
	sendingSnapshotCount func() uint64
	start                func() error
	close                func() error
}

var _ transport.Trans = new(TransportProxy)

func (t *TransportProxy) Send(msg meta.RaftMessage) bool {
	return t.send(msg)
}

func (t *TransportProxy) SendSnapshot(msg meta.RaftMessage) bool {
	return t.sendSnapshot(msg)
}

func (t *TransportProxy) SetFilter(fn func(meta.RaftMessage) bool) {
	t.setFilter(fn)
}

func (t *TransportProxy) SendingSnapshotCount() uint64 {
	return t.sendingSnapshotCount()
}

func (t *TransportProxy) Start() error {
	return t.start()
}

func (t *TransportProxy) Close() error {
	return t.close()
}
