// Copyright 2026 Matrix Origin
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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateStatementHashDestination rechecks the actual worker selected for
// this serialized pipeline. The coordinator's protocol version is not enough:
// a worker can be downgraded or replaced after compile-time placement, and an
// older worker cannot construct the MO_STATEMENT_HASH function (ID 583).
func validateStatementHashDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"MO_STATEMENT_HASH remote execution requires a versioned remote destination",
		)
	}
	expectedBuildCommitID, err := proc.StatementHashBuildCommitIDForRemote()
	if err != nil {
		return err
	}
	supported, err := remoteWorkersSupportProtocolAndBuild(proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion94, expectedBuildCommitID)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtx(
			"remote destination does not support MO_STATEMENT_HASH with the same build commit (MORPC version 93)",
		)
	}
	return nil
}
