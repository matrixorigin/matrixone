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

import (
	"time"

	"github.com/anishathalye/porcupine"
)

type NewPorcupineChecker func(
	model porcupine.Model,
	getOps func() []porcupine.Operation,
	events *[]porcupine.Event,
	timeout time.Duration,
) Operator

func (_ Def) NewPorcupineChecker(
	genVisual PorcupineAlwaysGenerateVisualization,
	write WriteTestDataFile,
	clear ClearTestDataFile,
	report AddReport,
) NewPorcupineChecker {

	return func(
		model porcupine.Model,
		getOps func() []porcupine.Operation,
		events *[]porcupine.Event,
		timeout time.Duration,
	) Operator {

		return Operator{

			BeforeDo: func() {
				ce(clear("porcupine", "html"))
			},

			AfterClose: func() {

				if getOps != nil {
					res, info := porcupine.CheckOperationsVerbose(model, getOps(), timeout)
					if res != porcupine.Ok {
						report(Report{
							Kind: "porcupine",
							Desc: "porcupine check failed",
						})
					}
					if res != porcupine.Ok || genVisual {
						f, err, done := write("porcupine", "html")
						ce(err)
						ce(porcupine.Visualize(model, info, f))
						ce(done())
					}
				}

				if events != nil {
					res, info := porcupine.CheckEventsVerbose(model, *events, timeout)
					if res != porcupine.Ok {
						report(Report{
							Kind: "porcupine",
							Desc: "porcupine check failed",
						})
					}
					if res != porcupine.Ok || genVisual {
						f, err, done := write("porcupine", "html")
						ce(err)
						ce(porcupine.Visualize(model, info, f))
						ce(done())
					}
				}

			},
		}

	}

}

type PorcupineAlwaysGenerateVisualization bool

func (_ Def) PorcupineAlwaysGenerateVisualization() PorcupineAlwaysGenerateVisualization {
	return false
}

type kvResultNotFound struct{}

var KVResultNotFound = kvResultNotFound{}

func (_ kvResultNotFound) String() string {
	return "not-found"
}

type kvResultTimeout struct{}

var KVResultTimeout = kvResultTimeout{}

func (_ kvResultTimeout) String() string {
	return "timeout"
}

var PorcupineKVModel = porcupine.Model{
	Partition:      porcupine.NoPartition,
	PartitionEvent: porcupine.NoPartitionEvent,

	Init: func() any {
		// copy-on-write map
		return make(map[any]any)
	},

	Step: func(state any, input any, output any) (ok bool, newState any) {
		m := state.(map[any]any)
		arg := input.([2]any)
		op := arg[0].(string)
		key := arg[1]

		switch op {

		case "get":
			if output == KVResultNotFound {
				_, ok := m[key]
				return !ok, state
			} else if output == KVResultTimeout {
				return true, state
			}
			return m[key] == output, state

		case "set":
			if output == KVResultTimeout {
				return true, state
			}
			newMap := make(map[any]any, len(m)+1)
			for k, v := range m {
				newMap[k] = v
			}
			newMap[key] = output
			return true, newMap

		}

		panic("impossible")
	},

	Equal: func(state1, state2 any) bool {
		m1 := state1.(map[any]any)
		m2 := state2.(map[any]any)
		for k, v := range m1 {
			if v != m2[k] {
				return false
			}
		}
		for k, v := range m2 {
			if v != m1[k] {
				return false
			}
		}
		return true
	},
}
