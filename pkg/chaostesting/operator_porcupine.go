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
	"github.com/google/uuid"
)

type PorcupineReport string

type NewPorcupineChecker func(
	model porcupine.Model,
	getOps func() []porcupine.Operation,
	events *[]porcupine.Event,
	timeout time.Duration,
) Operator

func (_ Def) NewPorcupineChecker(
	genVisual PorcupineAlwaysGenerateVisualization,
	write WriteTestDataFile,
) NewPorcupineChecker {

	return func(
		model porcupine.Model,
		getOps func() []porcupine.Operation,
		events *[]porcupine.Event,
		timeout time.Duration,
	) Operator {

		return Operator{

			AfterStop: func(
				report AddReport,
				uuid uuid.UUID,
			) {

				if getOps != nil {
					res, info := porcupine.CheckOperationsVerbose(model, getOps(), timeout)
					if res != porcupine.Ok {
						report(PorcupineReport("porcupine check failed"))
					}
					if res != porcupine.Ok || genVisual {
						f, err, done := write(uuid, "porcupine", "html")
						ce(err)
						ce(porcupine.Visualize(model, info, f))
						ce(done())
					}
				}

				if events != nil {
					res, info := porcupine.CheckEventsVerbose(model, *events, timeout)
					if res != porcupine.Ok {
						report(PorcupineReport("porcupine check failed"))
					}
					if res != porcupine.Ok || genVisual {
						f, err, done := write(uuid, "porcupine", "html")
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
		value := output

		switch op {

		case "get":
			return m[key] == value, state

		case "set":
			newMap := make(map[any]any, len(m)+1)
			for k, v := range m {
				newMap[k] = v
			}
			newMap[key] = value
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
