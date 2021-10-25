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

package factories

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type mutFactory struct {
	flusher mb.BlockFlusher
	mgr     bb.INodeManager
	// collectionFactory base.CollectionFactory
}

func NewMutFactory(mgr bb.INodeManager, flusher mb.BlockFlusher) *mutFactory {
	f := &mutFactory{
		mgr:     mgr,
		flusher: flusher,
	}
	return f
}

func (f *mutFactory) GetNodeFactroy(tdata interface{}) base.NodeFactory {
	return newMutNodeFactory(f, tdata.(iface.ITableData))
}

func (f *mutFactory) GetType() base.FactoryType {
	return base.MUTABLE
}

// func (f *mutFactory) GetCollectionFactory() base.CollectionFactory {
// 	return f.collectionFactory
// }

type normalFactory struct {
}

func NewNormalFactory() *normalFactory {
	f := &normalFactory{}
	return f
}

func (f *normalFactory) GetNodeFactroy(tdata interface{}) base.NodeFactory {
	return nil
}

func (f *normalFactory) GetType() base.FactoryType {
	return base.NORMAL
}
