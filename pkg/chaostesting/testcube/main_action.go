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

	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) MainAction(
	numNodes fz.NumNodes,
) fz.MainAction {

	const num = 8

	// action maker for specific client
	makers := func(clientID int) []fz.ActionMaker {
		return []fz.ActionMaker{

			// set
			func() fz.Action {
				key := rand.Intn(num / 2)
				value := rand.Intn(num / 2)
				return ActionSet{
					ClientID: clientID,
					Key:      key,
					Value:    value,
				}
			},

			// get
			func() fz.Action {
				key := rand.Intn(num / 2)
				return ActionGet{
					ClientID: clientID,
					Key:      key,
				}
			},

			// set / get pair
			func() fz.Action {
				key := rand.Intn(num / 2)
				value := rand.Intn(num / 2)
				return fz.Seq(
					ActionSet{
						ClientID: clientID,
						Key:      key,
						Value:    value,
					},
					ActionGet{
						ClientID: clientID,
						Key:      key,
					},
				)
			},
		}
	}

	// parallel client actions
	var action fz.ParallelAction
	numStopInserted := 0
	maxNumStop := int(numNodes - (numNodes/2 + 1))
	for i := fz.NumNodes(0); i < numNodes; i++ {

		seq := fz.RandSeq(makers(int(i)), num)

		// ActionStopNode
		if numStopInserted < maxNumStop {
			if rand.Intn(10) == 0 {
				numStopInserted++
				pos := rand.Intn(len(seq.Actions) + 1)
				var newActions []fz.Action
				newActions = append(newActions, seq.Actions[:pos]...)
				newActions = append(newActions, ActionStopNode{
					NodeID: fz.NodeID(i),
				})
				newActions = append(newActions, seq.Actions[pos:]...)
				seq.Actions = newActions
			}
		}

		action.Actions = append(
			action.Actions,
			seq,
		)
	}

	return fz.MainAction{
		Action: action,
	}

}
