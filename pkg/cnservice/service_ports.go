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

package cnservice

type PortSlot int

// New service should add before the last one.
const (
	PipelineService PortSlot = iota
	LockService
	QueryService
	Gossip
	MaxService
)

// String implements the fmt.Stringer interface.
func (s PortSlot) String() string {
	switch s {
	case PipelineService:
		return "Pipeline service"
	case LockService:
		return "Lock service"
	case QueryService:
		return "Query service"
	case Gossip:
		return "Gossip"
	default:
		return "Unknown service"
	}
}

// newPortStrategy returns true only if the port-base is not configured.
func (s *service) newPortStrategy() bool {
	return s.cfg.PortBase != 0
}

func (s *service) registerServices() {
	for slot := 0; slot < int(MaxService); slot++ {
		s.addressMgr.Register(slot)
	}
}

// The following methods mainly consider configuration compatibility.
// If there are no compatibility issues anymore, the methods could
// be removed.
func (s *service) pipelineServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(PipelineService))
	}
	return s.cfg.ServiceAddress
}

func (s *service) pipelineServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(PipelineService))
	}
	return s.cfg.ListenAddress
}

func (s *service) lockServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(LockService))
	}
	return s.cfg.LockService.ServiceAddress
}

func (s *service) lockServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(LockService))
	}
	return s.cfg.LockService.ListenAddress
}

func (s *service) queryServiceServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(QueryService))
	}
	return s.cfg.QueryServiceConfig.Address.ServiceAddress
}

func (s *service) queryServiceListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(QueryService))
	}
	return s.cfg.QueryServiceConfig.Address.ListenAddress
}

func (s *service) gossipServiceAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ServiceAddress(int(Gossip))
	}
	return ""
}

func (s *service) gossipListenAddr() string {
	if s.newPortStrategy() {
		return s.addressMgr.ListenAddress(int(Gossip))
	}
	return ""
}
