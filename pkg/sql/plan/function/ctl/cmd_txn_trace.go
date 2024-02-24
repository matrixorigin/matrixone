// Copyright 2024 Matrix Origin
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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// select mo_ctl('cn', 'txn-trace', 'enable|disable|clear|add|decode-complex')
func handleTxnTrace(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	params := strings.Split(parameter, " ")
	if len(params) == 0 {
		return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s", parameter)
	}

	switch strings.ToLower(params[0]) {
	case "enable":
		err := trace.GetService().Enable()
		if err != nil {
			return Result{}, err
		}
		return Result{Data: "OK"}, nil
	case "disable":
		err := trace.GetService().Disable()
		if err != nil {
			return Result{}, err
		}
		return Result{Data: "OK"}, nil
	case "clear":
		err := trace.GetService().ClearFilters()
		if err != nil {
			return Result{}, err
		}
		return Result{Data: "OK"}, nil
	case "add":
		// add table [column1, column2, ...]
		if len(params) < 2 {
			return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s", parameter)
		}
		var columns []string
		table := params[1]
		if len(params) > 2 {
			columns = params[2:]
		}

		err := trace.GetService().AddEntryFilter(table, columns)
		if err != nil {
			return Result{}, err
		}
		return Result{Data: "OK"}, nil
	case "decode-complex":
		// decode complex pk
		if len(params) != 2 {
			return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s", parameter)
		}
		value, err := trace.GetService().DecodeHexComplexPK(params[1])
		if err != nil {
			return Result{}, err
		}
		return Result{Data: value}, nil
	default:
		return Result{}, moerr.NewInvalidInputNoCtx("invalid parameter %s", parameter)
	}
}
