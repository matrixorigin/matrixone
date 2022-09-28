// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package evictable

import (
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	ConstPinDuration = 16 * time.Second
)

func EncodeColMetaKey(id *common.ID) string {
	return fmt.Sprintf("colMeta-%d-%d", id.BlockID, id.Idx)
}

func EncodeColBfKey(id *common.ID) string {
	return fmt.Sprintf("colBf-%d-%d", id.BlockID, id.Idx)
}

func EncodeColDataKey(id *common.ID) string {
	return fmt.Sprintf("colData-%d-%d", id.BlockID, id.Idx)
}

type EvictableNodeFactory = func() (base.INode, error)

func PinEvictableNode(mgr base.INodeManager, key string, factory EvictableNodeFactory) (base.INodeHandle, error) {
	var h base.INodeHandle
	var err error
	h, err = mgr.TryPinByKey(key, ConstPinDuration)
	if err == base.ErrNotFound {
		node, newerr := factory()
		if newerr != nil {
			return nil, newerr
		}
		// Ignore duplicate node and TODO: maybe handle no space
		mgr.Add(node)
		h, err = mgr.TryPinByKey(key, ConstPinDuration)
	}
	return h, err
}
