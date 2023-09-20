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

package gossip

type Option func(*Node)

func WithListenAddrFn(f func() string) Option {
	return func(n *Node) {
		n.listenAddrFn = f
	}
}

func WithServiceAddrFn(f func() string) Option {
	return func(n *Node) {
		n.serviceAddrFn = f
	}
}

func WithCacheServerAddrFn(f func() string) Option {
	return func(n *Node) {
		n.cacheServerAddrFn = f
	}
}

func (n *Node) SetListenAddrFn(f func() string) {
	n.listenAddrFn = f
}

func (n *Node) SetServiceAddrFn(f func() string) {
	n.serviceAddrFn = f
}

func (n *Node) SetCacheServerAddrFn(f func() string) {
	n.cacheServerAddrFn = f
}
