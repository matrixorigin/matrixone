// Copyright 2021 - 2023 Matrix Origin
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

package client

import "github.com/matrixorigin/matrixone/pkg/pb/gossip"

// KeyRouter is an interface manages the remote cache information.
type KeyRouter[T comparable] interface {
	// Target returns the remote cache server service address of
	// the cache key. If the cache do not exist in any node, it
	// returns empty string.
	Target(k T) string

	// AddItem pushes a item into a queue with a local
	// cache server service address in the item. Gossip will take
	// all the items and send them to other nodes in gossip cluster.
	AddItem(item gossip.CommonItem)
}
