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

import "math/rand"

func RandomActionTree(
	makers []ActionMaker,
	numActions int,
) Action {

	var split func([]Action) Action
	split = func(actions []Action) Action {
		if len(actions) == 0 {
			return nil
		}
		if len(actions) <= 10 {
			return leafRandTreeFuncs[rand.Intn(len(leafRandTreeFuncs))](actions)
		}
		nSplit := 3
		step := len(actions) / nSplit
		var actions2 []Action
		for i := step; i < len(actions); i += step {
			action := split(actions[:i])
			if action != nil {
				actions2 = append(actions2, action)
			}
			actions = actions[i:]
		}
		action := split(actions)
		if action != nil {
			actions2 = append(actions2, action)
		}
		return split(actions2)
	}

	var actions []Action
	for i := 0; i < numActions; i++ {
		actions = append(actions, makers[rand.Intn(len(makers))]())
	}

	return split(actions)
}

var leafRandTreeFuncs = []func([]Action) Action{
	func(actions []Action) Action {
		return Seq(actions...)
	},
	func(actions []Action) Action {
		return Par(actions...)
	},
}
