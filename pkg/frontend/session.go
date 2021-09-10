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

package frontend

import (
	"matrixone/pkg/config"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

type Session struct {
	//cmd from the client
	Cmd int

	//for test
	Mrs *MysqlResultSet

	GuestMmu *guest.Mmu
	Mempool  *mempool.Mempool

	sessionVars config.SystemVariables

	Pu *config.ParameterUnit
}

func NewSession()*Session {
	return &Session{
		GuestMmu: guest.New(config.GlobalSystemVariables.GetGuestMmuLimitation(), config.HostMmu),
		Mempool: mempool.New(int(config.GlobalSystemVariables.GetMempoolMaxSize()),
			int(config.GlobalSystemVariables.GetMempoolFactor())),
	}
}

func NewSessionWithParameterUnit(pu *config.ParameterUnit) *Session {
	return &Session{
		GuestMmu: guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu),
		Mempool:  pu.Mempool,
		Pu:       pu,
	}
}
