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
// limitations under the License.

package fz

type NumNodes int

func (_ Def) NumNodes() NumNodes {
	return 3
}

func (_ Def) NumNodesConfigItem(
	num NumNodes,
) ConfigItems {
	return ConfigItems{num}
}

type NodeID int

type Node interface{}

type CloseNode func(id NodeID) error

func (_ Def) CloseNode() CloseNode {
	return func(id NodeID) error {
		return nil
	}
}
