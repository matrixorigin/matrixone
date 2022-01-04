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
	"sync/atomic"

	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) MainAction(
	numNodes fz.NumNodes,
) fz.MainAction {

	var nextID int64

	const num = 8

	// action maker for specific client
	makers := func(clientID int) []fz.ActionMaker {
		return []fz.ActionMaker{

			// set
			func() fz.Action {
				id := atomic.AddInt64(&nextID, 1)
				key := rand.Intn(num / 2)
				value := rand.Intn(num / 2)
				return ActionSet{
					ID:       id,
					ClientID: clientID,
					Key:      key,
					Value:    value,
				}
			},

			// get
			func() fz.Action {
				id := atomic.AddInt64(&nextID, 1)
				key := rand.Intn(num / 2)
				return ActionGet{
					ID:       id,
					ClientID: clientID,
					Key:      key,
				}
			},

			// set / get pair
			func() fz.Action {
				id := atomic.AddInt64(&nextID, 1)
				key := rand.Intn(num / 2)
				value := rand.Intn(num / 2)
				return fz.Seq(
					ActionSet{
						ID:       id,
						ClientID: clientID,
						Key:      key,
						Value:    value,
					},
					ActionGet{
						ID:       id,
						ClientID: clientID,
						Key:      key,
					},
				)
			},
		}
	}

	// parallel client actions
	var action fz.ParallelAction
	for i := fz.NumNodes(0); i < numNodes; i++ {
		action.Actions = append(
			action.Actions,
			fz.RandSeq(makers(int(i)), num),
			//fz.RandomActionTree(makers(int(i)), num),
		)
	}

	return fz.MainAction{
		Action: action,
	}

}
