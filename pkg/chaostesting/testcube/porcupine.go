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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/google/uuid"
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

const reportFilesDir = "reports"

func (_ Def) PorcupineReport(
	get GetPorcupineOps,
) fz.Operators {

	// ensure output dir
	_, err := os.Stat(reportFilesDir)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
		ce(os.Mkdir(reportFilesDir, 0755))
	}
	ce(err)

	return fz.Operators{

		// checker
		fz.PorcupineChecker(
			fz.PorcupineKVModel,
			get,
			nil,
			time.Second*10,
		),

		// write log to file
		fz.Operator{
			AfterReport: func(
				uuid uuid.UUID,
				getReport fz.GetReport,
			) {

				var report fz.PorcupineReport
				if !getReport(&report) {
					// no error
					return
				}

				f, err := os.CreateTemp(reportFilesDir, "*.tmp")
				ce(err)
				for _, op := range get() {
					fmt.Fprintf(f, "%+v\n", op)
				}
				ce(f.Close())

				filePath := filepath.Join(
					reportFilesDir,
					uuid.String()+"-porcupine.log",
				)
				ce(os.Rename(
					f.Name(),
					filePath,
				))

				pt("porcupine log written to %s\n", filePath)

			},
		},
	}

}
