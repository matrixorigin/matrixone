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

package main

import (
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) MainConfig(
	numNodes fz.NumNodes,
) fz.MainAction {
	return fz.MainAction{

		// random tree
		Action: fz.RandomActionTree([]fz.ActionMaker{

			// set / get pair
			func() fz.Action {
				id := rand.Int63()
				nodeID := fz.NodeID(rand.Intn(int(numNodes)))
				key := rand.Intn(1024)
				value := rand.Intn(1024)
				return fz.Seq(
					ActionSet{
						ID:     id,
						NodeID: nodeID,
						Key:    key,
						Value:  value,
					},
					ActionGet{
						ID:     id,
						NodeID: nodeID,
						Key:    key,
					},
				)
			},
		}, 64),
	}
}
