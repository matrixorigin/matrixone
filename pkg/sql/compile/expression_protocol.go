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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// constrainRemoteExpressionWorkers analyzes the current expression generation
// once for placement. A worker must satisfy every independent feature floor;
// their maximum needs only one capability probe per selected worker. Send-time
// validation still checks the actual pipeline and destination independently.
func (c *Compile) constrainRemoteExpressionWorkers(qry *plan.Query) error {
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil {
		return err
	}
	// This rebind requirement also applies to local execution. Do not hide it
	// behind the remote-placement fast path or an earlier worker fallback.
	if features.LegacyIntervalUnits {
		return moerr.NewNotSupportedNoCtx("legacy interval unit contract requires rebinding")
	}
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	required := remoteExpressionProtocolVersion(features)
	if requiresStringNumericCompatibilityProtocol(c.proc, features) {
		if c.proc != nil && c.proc.Base != nil && c.proc.GetSessionInfo().LegacyNumericCompatibilityMode {
			return moerr.NewNotSupportedNoCtx(
				"string numeric compatibility cannot run with a legacy session contract",
			)
		}
		required = max(required, defines.MORPCVersion107)
	}
	if required == 0 {
		return nil
	}
	supported, err := remoteWorkersSupportProtocol(c.proc, c.cnList, required)
	if err != nil || supported {
		return err
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}

func remoteExpressionProtocolVersion(features plan.RemoteExpressionFeatures) int64 {
	required := max(requiredExpressionContractProtocolVersion(features), temporalExpressionProtocolVersion(features))
	if features.IntegerArithmeticDomains {
		required = max(required, defines.MORPCVersion71)
	}
	if features.RowDependentConvBases {
		required = max(required, defines.MORPCVersion70)
	}
	if features.IntegerParameterCoercion {
		required = max(required, defines.MORPCVersion85)
	}
	if features.SpecialIntegerConsumers {
		required = max(required, defines.MORPCVersion98)
	}
	if features.PreparedPrecisionScalar {
		required = max(required, defines.MORPCVersion95)
	}
	if features.DecimalDivisionSemantics {
		required = max(required, defines.MORPCVersion97)
	}
	if features.StringNumericResultContracts {
		required = max(required, defines.MORPCVersion80)
	}
	if features.BoundedConditionalStringDomains {
		required = max(required, defines.MORPCVersion83)
	}
	if features.SpatialDistanceSemantics {
		required = max(required, defines.MORPCVersion90)
	}
	if features.DecimalLiteralSemantics {
		required = max(required, defines.MORPCVersion89)
	}
	return required
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

func temporalExpressionProtocolVersion(features plan.RemoteExpressionFeatures) int64 {
	if features.TemporalResultContracts || features.NormalizedIntervalUnits || features.WeekSessionDefault {
		return defines.MORPCVersion98
	}
	return 0
}

// A timeout cause must not report an error when a probe succeeds.
var errRemoteCapabilityProbeTimeout = moerr.NewInternalError(
	moerr.NoReportContext(), "remote capability probe timed out")

// Probe the selected workers as well as the coordinator's rollout gate.
// Capabilities are not cached across executions or sender checks.
func remoteWorkersSupportProtocol(proc *process.Process, workers engine.Nodes, minimum int64) (bool, error) {
	if proc == nil {
		return false, nil
	}
	parent := proc.Ctx
	if parent == nil {
		parent = context.Background()
	}
	if err := parent.Err(); err != nil {
		return false, err
	}
	version, known := remoteMORPCProtocolVersion(proc.GetService())
	if !known || version < minimum {
		return false, nil
	}
	ctx, cancel := context.WithTimeoutCause(parent, 5*time.Second, errRemoteCapabilityProbeTimeout)
	defer cancel()
	for _, worker := range workers {
		version, known, err := remoteWorkerProtocolVersion(ctx, proc, worker)
		if err != nil {
			return false, parent.Err()
		}
		if !known || version < minimum {
			return false, nil
		}
	}
	return true, nil
}

// remoteWorkerProtocolVersion resolves the actual pipeline endpoint and owns
// the response lifetime. Callers keep their coordinator floor, timeout, and
// transient-error policy; the observed worker version is local to one boundary.
func remoteWorkerProtocolVersion(ctx context.Context, proc *process.Process, worker engine.Node) (int64, bool, error) {
	if worker.Addr == "" || proc.GetQueryClient() == nil {
		return 0, false, nil
	}
	cluster, err := clusterservice.GetMOClusterWithContext(ctx, proc.GetService())
	if err != nil {
		return 0, false, err
	}
	var addr, workerID string
	selector := clusterservice.NewSelector()
	if worker.Id != "" {
		selector = clusterservice.NewServiceIDSelector(worker.Id)
	}
	err = clusterservice.GetCNServiceWithoutWorkingStateWithContext(ctx, cluster,
		selector, func(cn metadata.CNService) bool {
			if cn.PipelineServiceAddress == worker.Addr && (worker.Id == "" || cn.ServiceID == worker.Id) {
				addr, workerID = cn.QueryAddress, cn.ServiceID
				return false
			}
			return true
		})
	if err != nil {
		return 0, false, err
	}
	if addr == "" {
		return 0, false, nil
	}
	if workerID != "" && workerID == proc.GetService() {
		version, known := remoteMORPCProtocolVersion(proc.GetService())
		return version, known, nil
	}
	client := proc.GetQueryClient()
	req := client.NewRequest(querypb.CmdMethod_GetProtocolVersion)
	req.GetProtocolVersion = &querypb.GetProtocolVersionRequest{}
	resp, err := client.SendMessage(ctx, addr, req)
	if resp != nil {
		defer client.Release(resp)
	}
	if err != nil {
		return 0, false, err
	}
	if resp == nil || resp.GetProtocolVersion == nil {
		return 0, false, nil
	}
	return resp.GetProtocolVersion.Version, true, nil
}
