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

package dispatch

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) waitRemoteReceiversReady(proc *process.Process) error {
	cnt := len(arg.RemoteRegs)
	for cnt > 0 {
		ctx, cancel := context.WithTimeout(context.TODO(), waitNotifyTimeout)
		select {
		case <-ctx.Done():
			cancel()
			return moerr.NewInternalErrorNoCtx("wait notify message timeout")

		case <-proc.Ctx.Done():
			cancel()
			return moerr.NewInternalErrorNoCtx("process has done")

		case receiver := <-proc.DispatchNotifyCh:
			cancel()
			arg.ctr.remoteReceivers = append(arg.ctr.remoteReceivers, receiver)
			cnt--
		}
	}

	return nil
}
