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

/**
check if x in a slice
*/
func isInSlice(x string, arr []string) bool {
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

/**
check if x in a slice
*/
func isInSliceBool(x bool, arr []bool) bool {
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

/**
check if x in a slice
*/
func isInSliceInt64(x int64, arr []int64) bool {
	for _, y := range arr {
		if x == y {
			return true
		}
	}
	return false
}

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