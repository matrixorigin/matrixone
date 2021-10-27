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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
)

type MutFactory interface {
	GetNodeFactroy(interface{}) NodeFactory
}

type NodeFactory interface {
	CreateNode(base.ISegmentFile, *metadata.Block, *mb.MockSize) bb.INode
	GetManager() bb.INodeManager
}
