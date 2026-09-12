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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Strict writes need complete reporting even when bounded diagnostics omit a
// cut. Apply the same placement policy to internal SQL inheriting that intent.
// The terminal completeness marker remains the final fail-closed safeguard.
func (c *Compile) constrainStrictWriteWorkers() error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	required, err := c.strictWriteGroupConcatPromotionEnabled()
	if err != nil {
		return err
	}
	required = required || requiresGroupConcatCutReporting(c.proc.GetWarningSink())
	if !required {
		return nil
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, defines.MORPCVersion67)
	if err != nil {
		return err
	}
	if supported {
		return nil
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}

func validateStrictWriteDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if proc == nil || !requiresGroupConcatCutReporting(proc.GetWarningSink()) {
		return nil
	}
	if p != nil && p.Node != nil {
		supported, err := remoteWorkersSupportProtocol(proc, engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, defines.MORPCVersion67)
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
	}
	return moerr.NewNotSupportedNoCtx("remote strict writes require complete GROUP_CONCAT cut reporting (MORPC version 67)")
}
