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

package table

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	fb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type altBlockFactory struct {
	nodeFactory base.NodeFactory
}

func newAltBlockFactory(mutFactory fb.MutFactory, tabledata iface.ITableData) *altBlockFactory {
	f := &altBlockFactory{
		nodeFactory: mutFactory.GetNodeFactroy(tabledata),
	}
	return f
}

func (af *altBlockFactory) CreateBlock(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	if meta.CommitInfo.Op < metadata.OpUpgradeFull {
		return newTBlock(host, meta, af.nodeFactory, nil)
	}
	return newBlock(host, meta)
}
