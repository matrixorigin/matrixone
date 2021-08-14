package frontend

import (
	"matrixone/pkg/config"
	"matrixone/pkg/defines"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
)

type Session struct {
	//cmd from the client
	Cmd int

	//for test
	Mrs *defines.MysqlResultSet

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
