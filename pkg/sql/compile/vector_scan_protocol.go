// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Legacy distributed vector scans always execute partition zero locally.
// Nonzero remote partitions have the same interpretation on both versions;
// a remote zero partition requires explicit execution-route ownership.
func hasRemoteVectorPartitionZero(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	if p.Node != nil && p.Node.CnCnt > 1 && p.Node.CnIdx == 0 &&
		p.DataSource != nil && p.DataSource.Node != nil &&
		p.DataSource.Node.NodeType == plan.Node_VECTOR_INDEX_SCAN {
		return true
	}
	for _, child := range p.Children {
		if hasRemoteVectorPartitionZero(child) {
			return true
		}
	}
	return false
}

func validateRemoteVectorPartitionProtocol(proc *process.Process, p *pipeline.Pipeline) error {
	if !hasRemoteVectorPartitionZero(p) {
		return nil
	}
	if proc != nil {
		version, known := remoteMORPCProtocolVersion(proc.GetService())
		if known && version >= defines.MORPCVersion96 {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx("remote vector partition zero requires MORPC protocol version 96")
}

func validateVectorPartitionDestination(proc *process.Process, p *pipeline.Pipeline) error {
	return validateVectorPartitionDestinationWithResult(proc, p, nil)
}

func validateVectorPartitionDestinationWithResult(proc *process.Process, p *pipeline.Pipeline, requiresBoundProtocol *bool) error {
	required := hasRemoteVectorPartitionZero(p)
	if requiresBoundProtocol != nil {
		*requiresBoundProtocol = required
	}
	if !required {
		return nil
	}
	// Use the actual transport destination, including for scans nested below
	// a merge. Re-probe before sending: compilation may predate a rollback.
	if p.Node != nil {
		supported, err := remoteWorkersSupportProtocol(proc,
			engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion96)
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx("remote destination does not support vector partition zero (MORPC version 96)")
}
