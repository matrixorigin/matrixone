package config

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
)

var GlobalSystemVariables SystemVariables

//host memory
var HostMmu *host.Mmu = nil

//mempool
var Mempool  *mempool.Mempool = nil

//Storage Engine
var StorageEngine engine.Engine

//Cluster Nodes
var ClusterNodes metadata.Nodes

type ParameterUnit struct {
	SV *SystemVariables

	//host memory
	HostMmu *host.Mmu

	//mempool
	Mempool  *mempool.Mempool

	//Storage Engine
	StorageEngine engine.Engine

	//Cluster Nodes
	ClusterNodes metadata.Nodes
}

func NewParameterUnit(sv *SystemVariables, hostMmu *host.Mmu, mempool *mempool.Mempool, storageEngine engine.Engine, clusterNodes metadata.Nodes) *ParameterUnit {
	return &ParameterUnit{
		SV:            sv,
		HostMmu:      hostMmu,
		Mempool: mempool,
		StorageEngine: storageEngine,
		ClusterNodes:  clusterNodes,
	}
}