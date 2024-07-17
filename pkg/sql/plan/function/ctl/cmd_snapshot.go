// Copyright 2023 Matrix Origin
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

package ctl

import (
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	tspb "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleGetSnapshotTS(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	rt := runtime.ServiceRuntime(proc.GetService())
	now, _ := rt.Clock().Now()
	return Result{
		Method: GetSnapshotMethod,
		Data:   now.DebugString(),
	}, nil
}

func handleUseSnapshotTS(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender,
) (Result, error) {
	var options []client.TxnOption
	rt := runtime.ServiceRuntime(proc.GetService())
	if parameter != "" {
		ts, err := tspb.ParseTimestamp(parameter)
		if err != nil {
			return Result{}, err
		}
		options = append(options, client.WithSnapshotTS(ts))
	}

	rt.SetGlobalVariables(runtime.TxnOptions, options)
	return Result{
		Method: UseSnapshotMethod,
		Data:   "OK",
	}, nil
}
