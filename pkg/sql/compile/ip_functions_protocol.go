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

// validateIPFunctionDestination rechecks the actual serialized destination at
// send time. A worker can be downgraded or replaced after compile-time
// placement, so a coordinator-only version check is insufficient.
func validateIPFunctionDestination(proc *process.Process, p *pipeline.Pipeline) error {
	if p == nil || p.Node == nil {
		return moerr.NewNotSupportedNoCtx(
			"versioned expression semantics require a remote destination",
		)
	}
	features, err := plan.RequiredRemoteExpressionFeatures(p)
	if err != nil {
		return err
	}
	required := requiredExpressionContractProtocolVersion(features)
	supported, err := remoteWorkersSupportProtocol(proc,
		engine.Nodes{{Id: p.Node.Id, Addr: p.Node.Addr}}, required)
	if err != nil {
		return err
	}
	if !supported {
		return moerr.NewNotSupportedNoCtxf(
			"remote destination does not support versioned expression semantics (MORPC version %d)",
			required,
		)
	}
	return nil
}

func requiredExpressionContractProtocolVersion(features plan.RemoteExpressionFeatures) int64 {
	if features.JSONScalarLiteralContracts {
		return defines.MORPCVersion104
	}
	if features.JSONInputContracts || features.YearBitCast {
		return defines.MORPCVersion101
	}
	if features.ExpressionResultMetadataContracts || features.TOBase64ResultContracts || features.IPFunctionResultContracts {
		return defines.MORPCVersion86
	}
	if features.IPFunctionSemantics {
		return defines.MORPCVersion72
	}
	return 0
}
