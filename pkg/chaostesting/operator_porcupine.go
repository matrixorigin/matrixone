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

func PorcupineChecker(
	model porcupine.Model,
	operations *[]porcupine.Operation,
	events *[]porcupine.Event,
) Operator {

	return Operator{

		AfterStop: func(
			report AddReport,
		) {

			if operations != nil {
				res, info := porcupine.CheckOperationsVerbose(model, *operations, time.Minute*10)
				_ = info
				if res != porcupine.Ok {
					report("porcupine check failed")
				}
			}

			if events != nil {
				res, info := porcupine.CheckEventsVerbose(model, *events, time.Minute*10)
				_ = info
				if res != porcupine.Ok {
					report("porcupine check failed")
				}
			}

		},
	}

}
