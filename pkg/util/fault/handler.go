// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fault

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

const (
	EnableFault  = "ENABLE_FAULT_INJECTION"
	DisableFault = "DISABLE_FAULT_INJECTION"
	StatusFault  = "STATUS_FAULT_POINT"
	ListFault    = "LIST_FAULT_POINT"
	AddFault     = "ADD_FAULT_POINT"
	RemoveFault  = "REMOVE_FAULT_POINT"
)

func HandleFaultInject(
	ctx context.Context,
	name string,
	parameter string,
) (res string) {
	switch name {
	case EnableFault:
		res = handleEnableFaultInjection()
	case DisableFault:
		res = handleDisableFaultInjection()
	case StatusFault:
		res = handleStatusFaultPoint()
	case ListFault:
		res = handleListFaultPoint()
	case AddFault:
		res = handleAddFaultPoint(ctx, parameter)
	case RemoveFault:
		res = handleRemoveFaultPoint(ctx, parameter)
	default:
		res = "unknown fault injection command"
	}
	return
}

func handleEnableFaultInjection() string {
	previousStatus := "enabled"
	if Enable() {
		previousStatus = "disabled"
	}
	return fmt.Sprintf("Fault injection enabled. Previous status: %s", previousStatus)
}

func handleDisableFaultInjection() string {
	previousStatus := "disabled"
	if Disable() {
		previousStatus = "enabled"
	}
	return fmt.Sprintf("Fault injection disabled. Previous status: %s", previousStatus)
}

func handleStatusFaultPoint() string {
	status := "disabled"
	if Status() {
		status = "enabled"
	}
	return fmt.Sprintf("Fault injection is %s", status)
}

func handleListFaultPoint() string {
	return ListAllFaultPoints()
}

func handleRemoveFaultPoint(ctx context.Context, parameter string) string {
	res, err := RemoveFaultPoint(ctx, parameter)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("Fault point '%s' successfully removed. Previously existed: %v", parameter, res)
}

func handleAddFaultPoint(ctx context.Context, parameter string) string {
	// parameter like "name.freq.action.iarg.sarg.constant"
	parameters := strings.Split(parameter, ".")
	if len(parameters) != 6 {
		return "Invalid argument! Expected format: name.freq.action.iarg.sarg.constant"
	}

	name := parameters[0]
	freq := parameters[1]
	action := parameters[2]

	iarg, err := strconv.Atoi(parameters[3])
	if err != nil {
		return fmt.Sprintf("Invalid argument for iarg: %s. It should be an integer.", parameters[3])
	}
	sarg := parameters[4]
	constant := parameters[5] == "true"

	err = AddFaultPoint(ctx, name, freq, action, int64(iarg), sarg, constant)
	if err != nil {
		return err.Error()
	}
	return "OK"
}
