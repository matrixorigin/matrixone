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

package plan

import (
	"errors"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type updatePlannerRoute string

const (
	updatePlannerModern      updatePlannerRoute = "modern"
	updatePlannerLegacy      updatePlannerRoute = "legacy"
	updatePlannerSpecialized updatePlannerRoute = "specialized"
	updatePlannerRejected    updatePlannerRoute = "rejected"
	updatePlannerUnknown     updatePlannerRoute = "unknown"
)

type updatePlannerRouteReason string

const (
	updateRouteReasonNone            updatePlannerRouteReason = "none"
	updateRouteReasonMultiTarget     updatePlannerRouteReason = "multi_target"
	updateRouteReasonForeignKey      updatePlannerRouteReason = "foreign_key"
	updateRouteReasonIrregularIndex  updatePlannerRouteReason = "irregular_index"
	updateRouteReasonAutoIncrementFK updatePlannerRouteReason = "auto_increment_foreign_key"
	updateRouteReasonIceberg         updatePlannerRouteReason = "iceberg"
	updateRouteReasonExternalTable   updatePlannerRouteReason = "external_table"
	updateRouteReasonTableForm       updatePlannerRouteReason = "unsupported_table_form"
	updateRouteReasonEmptyTableName  updatePlannerRouteReason = "empty_table_name"
	updateRouteReasonBinderError     updatePlannerRouteReason = "binder_error"
	updateRouteReasonUnknown         updatePlannerRouteReason = "unknown"
)

type updatePlannerRouteError struct {
	route  updatePlannerRoute
	reason updatePlannerRouteReason
	err    error
}

func (e *updatePlannerRouteError) Error() string {
	return e.err.Error()
}

func (e *updatePlannerRouteError) Unwrap() error {
	return e.err
}

func newUpdatePlannerRouteError(
	route updatePlannerRoute,
	reason updatePlannerRouteReason,
	err error,
) error {
	return &updatePlannerRouteError{
		route:  route,
		reason: reason,
		err:    err,
	}
}

func newLegacyUpdatePlannerRouteError(reason updatePlannerRouteReason, err error) error {
	return newUpdatePlannerRouteError(updatePlannerLegacy, reason, err)
}

func classifyUpdatePlannerError(err error) (updatePlannerRoute, updatePlannerRouteReason, error) {
	var routeErr *updatePlannerRouteError
	if errors.As(err, &routeErr) {
		return routeErr.route, routeErr.reason, routeErr.err
	}
	if moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML) {
		return updatePlannerUnknown, updateRouteReasonUnknown, err
	}
	return updatePlannerRejected, updateRouteReasonBinderError, err
}

func recordUpdatePlannerRoute(
	route updatePlannerRoute,
	reason updatePlannerRouteReason,
	result string,
) {
	v2.UpdatePlannerRouteCounter.WithLabelValues(string(route), string(reason), result).Inc()

	if route == updatePlannerModern {
		return
	}

	fields := []zap.Field{
		zap.String("planner", string(route)),
		zap.String("reason", string(reason)),
		zap.String("result", result),
	}
	if route == updatePlannerUnknown {
		logutil.Warn("update planner route", fields...)
		return
	}
	logutil.Info("update planner route", fields...)
}
