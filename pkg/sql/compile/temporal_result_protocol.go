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

func temporalExpressionProtocolVersion(features plan.RemoteExpressionFeatures) int64 {
	if features.TemporalResultContracts || features.NormalizedIntervalUnits || features.WeekSessionDefault {
		return defines.MORPCVersion98
	}
	return 0
}

func validateTemporalResultDestination(proc *process.Process, p *pipeline.Pipeline, required int64) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx("temporal result contracts require a versioned remote destination")
	}
	supported, err := remoteWorkersSupportProtocol(proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, required)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtxf("remote destination does not support temporal result contracts (MORPC version %d)", required)
	}
	return nil
}
