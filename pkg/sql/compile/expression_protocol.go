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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	if features.InvalidDecimalStatisticalResultContract {
		return moerr.NewNotSupportedNoCtx("decimal statistical result contract requires rebinding")
	}
	if features.InvalidTemporalResultContract {
		return moerr.NewNotSupportedNoCtx("temporal result vector contract mismatch requires rebinding")
	}
	if features.LegacyIntervalUnits {
		return moerr.NewNotSupportedNoCtx("legacy interval unit contract requires rebinding")
	}
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	required := executionRemoteExpressionProtocolVersion(c.proc, features)
	if required == 0 {
		return nil
	}
	supported, err := remoteWorkersSupportProtocolForPlacement(c.proc, c.cnList, required)
	if err != nil || supported {
		return err
	}
	c.execType = plan2.ExecTypeAP_ONECN
	c.cnList, err = c.scheduleQueryWorkers()
	return err
}

// Diagnostics depend on the outer statement and current session mode, unlike
// expression result contracts. Keep this floor out of persisted-plan admission.
func executionRemoteExpressionProtocolVersion(proc *process.Process, features plan.RemoteExpressionFeatures) int64 {
	required := remoteExpressionProtocolVersion(features)
	if features.ScalarDivision && proc.DivisionByZeroWarningsEnabled() {
		required = max(required, defines.MORPCVersion116)
	}
	return required
}

func remoteExpressionProtocolVersion(features plan.RemoteExpressionFeatures) int64 {
	required := max(requiredExpressionContractProtocolVersion(features), temporalExpressionProtocolVersion(features))
	if features.LoadFKIgnoreCheck {
		required = max(required, defines.MORPCVersion125)
	}
	if features.TimestampDatetimeResultContracts || features.DynamicTimeResultContracts {
		required = max(required, defines.MORPCVersion122)
	}
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
	if features.DecimalIntegerDivisionSemantics {
		required = max(required, defines.MORPCVersion121)
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
	if features.MediumIntAssignmentBounds {
		required = max(required, defines.MORPCVersion110)
	}
	if features.DateNumericResultContracts {
		required = max(required, defines.MORPCVersion112)
	}
	if features.LeastGreatestCollationSemantics {
		required = max(required, defines.MORPCVersion117)
	}
	if features.DecimalStatisticalResultContracts {
		required = max(required, defines.MORPCVersion113)
	}
	if features.TableCollationFunction {
		required = max(required, defines.MORPCVersion118)
	}
	if features.CalendarIntervalNormalization {
		required = max(required, defines.MORPCVersion115)
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
	if features.TimestampAddSecondIntervals {
		return defines.MORPCVersion124
	}
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
// A false result without an error means support could not be verified;
// observation failures retain their cause for required gates and send checks.
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
		version, known, probeErr := remoteWorkerProtocolVersion(ctx, proc, worker)
		if err := parent.Err(); err != nil {
			return false, err
		}
		if probeErr != nil {
			return false, probeErr
		}
		if !known || version < minimum {
			return false, nil
		}
	}
	return true, nil
}

// Optional placement may safely fall back locally when remote support cannot
// be verified. Only statement cancellation is fatal here, even when a probe's
// own error is context.Canceled or context.DeadlineExceeded.
func remoteWorkersSupportProtocolForPlacement(proc *process.Process, workers engine.Nodes, minimum int64) (bool, error) {
	supported, err := remoteWorkersSupportProtocol(proc, workers, minimum)
	if proc != nil && proc.Ctx != nil {
		if parentErr := proc.Ctx.Err(); parentErr != nil {
			return false, parentErr
		}
	}
	return supported && err == nil, nil
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

// Cached compilations can survive a cluster capability rollback. Reject changed
// result contracts and LOAD FK admission here, before Prepare or statement
// effects; placement/receiver owners retain the other expression policies.
func validateTemporalResultProtocol(proc *process.Process, owner any) error {
	var required int64
	if queryPlan, ok := owner.(*plan.Plan); ok {
		if create := createTableForNameAdmission(queryPlan); create != nil {
			required = create.CtasExecutionProtocolVersion
		}
	}
	var version int64
	var known bool
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			value, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			var valid bool
			version, valid = value.(int64)
			known = exists && valid
		}
	}
	if required < 0 {
		return moerr.NewNotSupportedNoCtx("invalid CTAS population protocol requirement requires rebinding")
	}
	if required > 0 && (!known || version < required) {
		return moerr.NewNotSupportedf(context.Background(), "CTAS population requires MORPC protocol version %d", required)
	}
	if known && version >= defines.MORPCVersion125 {
		return nil
	}
	features, err := plan.RequiredRemoteExpressionFeatures(owner)
	if err != nil {
		return err
	}
	if features.LoadFKIgnoreCheck && (!known || version < defines.MORPCVersion125) {
		return moerr.NewNotSupportedNoCtx("LOAD IGNORE foreign-key filtering requires MORPC protocol version 125")
	}
	if features.InvalidTemporalResultContract {
		return moerr.NewNotSupportedNoCtx("temporal result vector contract mismatch requires rebinding")
	}
	if (features.TimestampDatetimeResultContracts || features.DynamicTimeResultContracts) && (!known || version < defines.MORPCVersion122) {
		return moerr.NewNotSupportedNoCtx("temporal result contracts require MORPC protocol version 122")
	}
	return nil
}
