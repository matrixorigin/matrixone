// Copyright 2021 - 2022 Matrix Origin
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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/vm/process"

	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

type serviceType string

var (
	dn serviceType = "DN"
	cn serviceType = "CN"

	supportedServiceTypes = map[serviceType]struct{}{
		dn: {},
		cn: {},
	}
)

var (
	// register all supported debug command here
	supportedCmds = map[string]handleFunc{
		strings.ToUpper(pb.CmdMethod_Ping.String()):          handlePing(),
		strings.ToUpper(pb.CmdMethod_Flush.String()):         handleFlush(),
		strings.ToUpper(pb.CmdMethod_Task.String()):          handleTask,
		strings.ToUpper(pb.CmdMethod_UseSnapshot.String()):   handleUseSnapshotTS,
		strings.ToUpper(pb.CmdMethod_GetSnapshot.String()):   handleGetSnapshotTS,
		strings.ToUpper(pb.CmdMethod_Checkpoint.String()):    handleCheckpoint(),
		strings.ToUpper(pb.CmdMethod_ForceGC.String()):       handleCNGC,
		strings.ToUpper(pb.CmdMethod_Inspect.String()):       handleInspectDN(),
		strings.ToUpper(pb.CmdMethod_Label.String()):         handleSetLabel,
		strings.ToUpper(pb.CmdMethod_SyncCommit.String()):    handleSyncCommit,
		strings.ToUpper(pb.CmdMethod_AddFaultPoint.String()): handleAddFaultPoint(),
	}
)

type requestSender = func(context.Context, []txn.CNOpRequest) ([]txn.CNOpResponse, error)

type handleFunc func(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error)
