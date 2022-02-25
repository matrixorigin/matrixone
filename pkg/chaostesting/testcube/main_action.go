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
	faultMakers FaultActionMakers,
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

	var nodeActions fz.ParallelAction

	numFaults := 0
	maxFaults := int(numNodes - (numNodes/2 + 1))
	for i := fz.NumNodes(0); i < numNodes; i++ {

		seq := fz.RandSeq(makers(int(i)), num)
		randomInsert := func(action fz.Action) {
			numFaults++
			pos := rand.Intn(len(seq.Actions) + 1)
			var newActions []fz.Action
			newActions = append(newActions, seq.Actions[:pos]...)
			newActions = append(newActions, action)
			newActions = append(newActions, seq.Actions[pos:]...)
			seq.Actions = newActions
		}

		if numFaults < maxFaults {
			if rand.Intn(2) == 0 {
				action := faultMakers[rand.Intn(len(faultMakers))](fz.NodeID(i))
				if action != nil {
					randomInsert(action)
				}
			}
		}

		nodeActions.Actions = append(
			nodeActions.Actions,
			seq,
		)
	}

	return fz.MainAction{
		Action: nodeActions,
	}

}

type FaultActionMakers []func(id fz.NodeID) fz.Action

func (_ Def) DefaultFaults(
	makeActionStopNode makeActionStopNode,
	makeActionRestartNode makeActionRestartNode,
	makeActionIsolateNode makeActionIsolateNode,
	makeActionCrashNode makeActionCrashNode,
	makeActionFullyIsolateNode makeActionFullyIsolateNode,
) FaultActionMakers {
	return []func(id fz.NodeID) fz.Action{
		makeActionStopNode,
		makeActionRestartNode,
		makeActionIsolateNode,
		makeActionCrashNode,
		makeActionFullyIsolateNode,
	}
}

type (
	makeActionStopNode         func(id fz.NodeID) fz.Action
	makeActionRestartNode      func(id fz.NodeID) fz.Action
	makeActionIsolateNode      func(id fz.NodeID) fz.Action
	makeActionCrashNode        func(id fz.NodeID) fz.Action
	makeActionFullyIsolateNode func(id fz.NodeID) fz.Action
)

func (_ Def) ActionMakers(
	numNodes fz.NumNodes,
) (
	makeActionStopNode makeActionStopNode,
	makeActionRestartNode makeActionRestartNode,
	makeActionIsolateNode makeActionIsolateNode,
	makeActionCrashNode makeActionCrashNode,
	makeActionFullyIsolateNode makeActionFullyIsolateNode,
) {

	makeActionStopNode = func(id fz.NodeID) fz.Action {
		return ActionStopNode{
			NodeID: id,
		}
	}

	makeActionRestartNode = func(id fz.NodeID) fz.Action {
		return ActionRestartNode{
			NodeID: id,
		}
	}

	makeActionIsolateNode = func(id fz.NodeID) fz.Action {
		if numNodes < 2 {
			return nil
		}
		action := ActionIsolateNode{
			NodeID: id,
			Between: func() (nodes []fz.NodeID) {
				for between := 0; between < int(numNodes); between++ {
					if between == int(id) {
						continue
					}
					if rand.Intn(2) == 0 {
						nodes = append(nodes, fz.NodeID(between))
					}
				}
				return nodes
			}(),
		}
		return action
	}

	makeActionCrashNode = func(id fz.NodeID) fz.Action {
		return ActionCrashNode{
			NodeID: id,
		}
	}

	makeActionFullyIsolateNode = func(id fz.NodeID) fz.Action {
		return ActionFullyIsolateNode{
			NodeID: id,
		}
	}

	return
}
