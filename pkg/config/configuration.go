package config

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/metadata"
	"matrixone/pkg/vm/mmu/host"
)

var GlobalSystemVariables SystemVariables

//host memory
var HostMmu *host.Mmu = nil

//Storage Engine
var StorageEngine engine.Engine

//Cluster Nodes
var ClusterNodes metadata.Nodes