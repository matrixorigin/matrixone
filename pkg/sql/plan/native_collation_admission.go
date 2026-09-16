package plan

import (
	"context"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const native0900AdmissionError = "versioned collation semantics and key formats are disabled until the cluster activation gate is complete"

// This switch is set only by the package's _test.go file.  Production builds
// have no test escape hatch; in particular, an empty Process service identity
// is not treated as evidence that a native plan is safe to execute.
var native0900TestAdmission atomic.Bool

func tableUsesNative0900(table *TableDef) bool {
	return tableUsesVersionedCollation(table)
}

// tableUsesVersionedCollation covers all corrected text identities and their
// persisted key layouts. The legacy helper name is retained for existing
// callers and tests, but admission must not let general-ci or utf8mb4_bin V1
// objects bypass the same cluster gate as native 0900.
func tableUsesVersionedCollation(table *TableDef) bool {
	if table == nil {
		return false
	}
	if table.KeyFormat != uint32(types.LegacyKeyFormat) || table.CollationVersion != uint32(types.CollationVersionLegacy) {
		return true
	}
	for _, col := range table.Cols {
		if col != nil && isVersionedCollationType(col.Typ) {
			return true
		}
	}
	for _, index := range table.Indexes {
		if index != nil && index.KeyFormat != uint32(types.LegacyKeyFormat) {
			return true
		}
	}
	return false
}

// native0900AdmissionAllowed is deliberately test-only in this phase.  The
// protocol number is not a durable catalog/recovery gate, so a production
// service must not enable native 0900 merely by advertising a version.
func native0900AdmissionAllowed(proc *process.Process) bool {
	return native0900TestAdmission.Load()
}

func requireNative0900Admission(ctx context.Context, proc *process.Process, table *TableDef) error {
	if !tableUsesNative0900(table) || native0900AdmissionAllowed(proc) {
		return nil
	}
	return moerr.NewNotSupportedNoCtx(native0900AdmissionError)
}

// requireNative0900PlanAdmission is the final local planner fence.  DDL
// checks protect new catalog objects, while this check also rejects a query
// which only carries an explicit COLLATE expression or reaches a relation
// whose native key format survives optimization without a string expression.
func requireNative0900PlanAdmission(ctx context.Context, proc *process.Process, p *planpb.Plan) error {
	if p == nil || native0900AdmissionAllowed(proc) {
		return nil
	}
	features, err := planpb.RequiredRemoteExpressionFeatures(p)
	if err != nil {
		return err
	}
	if !features.CollationKeyV1 &&
		!features.NativeCollationV1 && !features.NativeCollationSchemaV1 {
		return nil
	}
	return moerr.NewNotSupportedNoCtx(native0900AdmissionError)
}
