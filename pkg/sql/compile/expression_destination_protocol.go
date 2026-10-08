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
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateRemoteExpressionDestination checks independent expression floors in
// their diagnostic order. One send observes its actual destination once; no
// observation survives a later send, placement, or receiver validation.
func validateRemoteExpressionDestination(proc *process.Process, p *pipeline.Pipeline, features plan.RemoteExpressionFeatures) error {
	integerArgumentsVersion := defines.MORPCVersion85
	if features.SpecialIntegerConsumers {
		integerArgumentsVersion = defines.MORPCVersion98
	}
	gates := [...]struct {
		enabled              bool
		version              int64
		missing, unsupported string
	}{
		{features.IntegerArithmeticDomains, defines.MORPCVersion71,
			"checked integer arithmetic requires a versioned remote destination",
			"remote destination does not support checked integer arithmetic (MORPC version %d)"},
		{features.RowDependentConvBases, defines.MORPCVersion70,
			"row-dependent CONV bases requires a versioned remote destination",
			"remote destination does not support row-dependent CONV bases (MORPC version %d)"},
		{features.IntegerParameterCoercion || features.SpecialIntegerConsumers, integerArgumentsVersion,
			"integer parameter coercion requires a versioned remote destination",
			"remote destination does not support integer parameter execution (MORPC version %d)"},
		{features.PreparedPrecisionScalar, defines.MORPCVersion95,
			"prepared scalar precision requires a versioned remote destination",
			"remote destination does not support prepared scalar precision (MORPC version %d)"},
		{features.DecimalDivisionSemantics, defines.MORPCVersion97,
			"decimal division requires a versioned remote destination",
			"remote destination does not support decimal division (MORPC version %d)"},
		{temporalExpressionProtocolVersion(features) != 0, temporalExpressionProtocolVersion(features),
			"temporal result contracts require a versioned remote destination",
			"remote destination does not support temporal result contracts (MORPC version %d)"},
		{features.IPFunctionSemantics || features.TOBase64ResultContracts || features.IPFunctionResultContracts || features.ExpressionResultMetadataContracts || features.JSONInputContracts || features.YearBitCast || features.JSONScalarLiteralContracts, requiredExpressionContractProtocolVersion(features),
			"versioned expression semantics require a remote destination",
			"remote destination does not support versioned expression semantics (MORPC version %d)"},
		{features.StringNumericResultContracts, defines.MORPCVersion80,
			"corrected string numeric result contracts require a versioned remote destination",
			"remote destination does not support corrected string numeric result contracts (MORPC version %d)"},
		{features.BoundedConditionalStringDomains, defines.MORPCVersion83,
			"bounded conditional string domains require a versioned remote destination",
			"remote destination does not support bounded conditional string domains (MORPC version %d)"},
		{features.SpatialDistanceSemantics, defines.MORPCVersion90,
			"geodetic spatial-distance semantics require a versioned remote destination",
			"remote destination does not support geodetic spatial-distance semantics (MORPC version %d)"},
		{features.DecimalLiteralSemantics, defines.MORPCVersion89,
			"exact DECIMAL256 literal semantics require a versioned remote destination",
			"remote destination does not support exact DECIMAL256 literal semantics (MORPC protocol version %d)"},
		{pipelineRequiresPartitionFulltextRoute(p), defines.MORPCVersion107,
			"partitioned FULLTEXT routing requires a versioned remote destination",
			"remote destination does not support partitioned FULLTEXT routing (MORPC protocol version %d)"},
	}
	var parent context.Context
	var observed int64
	var known, probed bool
	for _, gate := range gates {
		if !gate.enabled {
			continue
		}
		if p == nil || p.Node == nil {
			return moerr.NewNotSupportedNoCtx(gate.missing)
		}
		if proc == nil {
			return moerr.NewNotSupportedNoCtxf(gate.unsupported, gate.version)
		}
		if parent == nil {
			parent = proc.Ctx
			if parent == nil {
				parent = context.Background()
			}
		}
		if err := parent.Err(); err != nil {
			return err
		}
		// The rollout floor can change during the probe. Read it for every gate
		// just as the previous independent validators did.
		current, currentKnown := remoteMORPCProtocolVersion(proc.GetService())
		if !currentKnown || current < gate.version {
			return moerr.NewNotSupportedNoCtxf(gate.unsupported, gate.version)
		}
		if !probed {
			ctx, cancel := context.WithTimeoutCause(parent, 5*time.Second, errRemoteCapabilityProbeTimeout)
			version, versionKnown, err := remoteWorkerProtocolVersion(ctx, proc, engine.Node{Id: p.Node.Id, Addr: p.Node.Addr})
			cancel()
			if err != nil && parent.Err() != nil {
				return parent.Err()
			}
			observed, known, probed = version, versionKnown && err == nil, true
		}
		if !known || observed < gate.version {
			return moerr.NewNotSupportedNoCtxf(gate.unsupported, gate.version)
		}
	}
	return nil
}

// Lowered children can carry routing metadata without an expression marker.
// Check the whole wire tree before a send: an older receiver ignores unknown
// protobuf fields and cannot enforce the new receiver-side admission check.
func pipelineRequiresPartitionFulltextRoute(p *pipeline.Pipeline) bool {
	if p == nil {
		return false
	}
	for _, instruction := range p.InstructionList {
		if preInsert := instruction.GetPreInsert(); preInsert != nil && preInsert.PreserveInput {
			return true
		}
		if update := instruction.GetMultiUpdate(); update != nil {
			for _, target := range update.UpdateCtxList {
				if target != nil && target.PartitionIndexCtx != nil {
					return true
				}
			}
		}
	}
	for _, child := range p.Children {
		if pipelineRequiresPartitionFulltextRoute(child) {
			return true
		}
	}
	return false
}
