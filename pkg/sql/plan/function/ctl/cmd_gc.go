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
	"runtime/debug"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleCNGC(*process.Process, serviceType, string, requestSender) (Result, error) {
	debug.FreeOSMemory()
	runtime.ProcessLevelRuntime().Logger().Info("force free memory completed")
	return Result{
		Method: ForceGCMethod,
		Data:   "OK",
	}, nil
}
