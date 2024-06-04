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

package tnservice

type PortSlot int

// New service should add before the last one.
const (
	TxnService PortSlot = iota
	LogtailService
	LockService
	QueryService
	ShardService
	MaxService
)

// String implements the fmt.Stringer interface.
func (s PortSlot) String() string {
	switch s {
	case TxnService:
		return "Txn service"
	case LogtailService:
		return "Logtail service"
	case LockService:
		return "Lock service"
	case QueryService:
		return "Query service"
	case ShardService:
		return "Shard service"
	default:
		return "Unknown service"
	}
}

// newPortStrategy returns true only if the port-base is not configured.
func (s *store) newPortStrategy() bool {
	return s.cfg.PortBase != 0
}

func (s *store) registerServices() {
	for slot := 0; slot < int(MaxService); slot++ {
		s.addressMgr.Register(slot)
	}
}

// The following methods mainly consider configuration compatibility.
// If there are no compatibility issues anymore, the methods could
// be removed.
func (s *store) txnServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(TxnService))
	}
	return s.cfg.ServiceAddress
}

func (s *store) txnServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(TxnService))
	}
	return s.cfg.ListenAddress
}

func (s *store) logtailServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(LogtailService))
	}
	return s.cfg.LogtailServer.ServiceAddress
}

func (s *store) logtailServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(LogtailService))
	}
	return s.cfg.LogtailServer.ListenAddress
}

func (s *store) lockServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(LockService))
	}
	return s.cfg.LockService.ServiceAddress
}

func (s *store) lockServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(LockService))
	}
	return s.cfg.LockService.ListenAddress
}

func (s *store) queryServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(QueryService))
	}
	return ""
}

func (s *store) queryServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(QueryService))
	}
	return ""
}

func (s *store) shardServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(ShardService))
	}
	return s.cfg.ShardService.ServiceAddress
}

func (s *store) shardServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(ShardService))
	}
	return s.cfg.ShardService.ListenAddress
}
