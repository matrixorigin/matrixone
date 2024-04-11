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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// select mo_ctl("dn", "deduplicate", "incremental_dedup")
// select mo_ctl("dn", "deduplicate", "full_skip_workspace_dedup")
// select mo_ctl("dn", "deduplicate", "full_dedup")
// select mo_ctl("dn", "deduplicate", "skip_all_dedup")
// select mo_ctl("dn", "deduplicate", "invalid_dedup_value")
func handleDeduplicate(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {

	if _, ok := db.DedupName2Type[parameter]; !ok {
		return Result{}, moerr.NewInvalidArgNoCtx(parameter, "no such cmd")
	}

	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payloadFn := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		req := db.Deduplicate{
			Cmd: parameter,
		}
		return req.MarshalBinary()
	}

	repsonseUnmarshaler := func(b []byte) (interface{}, error) {
		return string(b[:]), nil
	}

	return GetTNHandlerFunc(
		api.OpCode_OpTNDeduplicate, whichTN, payloadFn, repsonseUnmarshaler,
	)(proc, service, parameter, sender)

}
