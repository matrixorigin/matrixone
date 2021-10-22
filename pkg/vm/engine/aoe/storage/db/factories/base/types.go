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

package base

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type FactoryType uint16

const (
	INVALID FactoryType = iota
	NORMAL
	MUTABLE
)

type CollectionFactory = func(iface.ITableData) imem.ICollection

type MutFactory interface {
	GetNodeFactroy(interface{}) NodeFactory
	GetType() FactoryType
	// GetCollectionFactory() CollectionFactory
}

type NodeFactory interface {
	CreateNode(base.ISegmentFile, *metadata.Block, *mb.MockSize) bb.INode
	GetManager() bb.INodeManager
}
