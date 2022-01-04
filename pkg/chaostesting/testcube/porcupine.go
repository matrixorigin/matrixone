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
	"fmt"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

type (
	LogPorcupineOp func(
		fn func() (clientID int, input any, output any, err error),
	) error
	GetPorcupineOps func() []porcupine.Operation
)

func (_ Def) Porcupine() (
	log LogPorcupineOp,
	get GetPorcupineOps,
) {

	var ops []porcupine.Operation
	var lock sync.Mutex
	tStart := time.Now()

	log = func(
		fn func() (clientID int, input any, output any, err error),
	) error {
		t0 := time.Now()
		clientID, input, output, err := fn()
		if err != nil {
			return err
		}
		t1 := time.Now()
		lock.Lock()
		defer lock.Unlock()
		ops = append(ops, porcupine.Operation{
			ClientId: clientID,
			Input:    input,
			Output:   output,
			Call:     int64(t0.Sub(tStart)),
			Return:   int64(t1.Sub(tStart)),
		})
		return nil
	}

	get = func() []porcupine.Operation {
		lock.Lock()
		defer lock.Unlock()
		return ops
	}

	return
}

func (_ Def) PorcupineReport(
	get GetPorcupineOps,
	newChecker fz.NewPorcupineChecker,
	testDataDir fz.TestDataDir,
) fz.Operators {

	return fz.Operators{

		// checker
		newChecker(
			fz.PorcupineKVModel,
			get,
			nil,
			time.Second*10,
		),

		// write log to file
		fz.Operator{
			AfterReport: func(
				getReport fz.GetReport,
				write fz.WriteTestDataFile,
			) {

				//var report fz.PorcupineReport
				//if !getReport(&report) {
				//	// no error
				//	return
				//}

				f, err, done := write("porcupine", "log")
				ce(err)
				for _, op := range get() {
					fmt.Fprintf(f, "%+v\n", op)
				}
				ce(done())

			},
		},
	}

}

func (_ Def2) PorcupineOptions() (
	genVisual fz.PorcupineAlwaysGenerateVisualization,
) {
	genVisual = true
	return
}
