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

package external

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// maxKafkaControlValue bounds start_id/size: far above any real Kafka
	// offset, far below the +1/arithmetic overflow line.
	maxKafkaControlValue = int64(1) << 62
	// maxKafkaTimeoutSeconds keeps time.Duration(v)*time.Second in range.
	maxKafkaTimeoutSeconds = int64(1) << 31
)

// DeriveKafkaReadControl resolves the read-control pseudo-columns of a Kafka
// external table scan (__mo_read_start_id / __mo_read_size /
// __mo_read_timeout) from the scan's filter conjuncts into the KafkaScan and
// CONSUMES the generating conjuncts (they position the read; they are not
// row filters). Only a top-level `<control> = <column-free expr>` conjunct
// generates a value; any other appearance of a control column stays a
// row-level filter over the effective value the scan used.
//
// Semantics (issue #27518): start_id is the last-consumed offset — reading
// begins at start_id+1. With autocommit=false a start_id is REQUIRED and the
// read offset is committed at that position before reading (exactly-once
// chaining with LAST_KAFKA_MESSAGE_ID()); -1 means "from the earliest". With
// autocommit=true, 0 (the default) means earliest-inclusive and -1 means
// latest. size caps the record count (0 = unlimited). timeout ends the read
// when no new message arrives within that many seconds (0 = block forever).
func DeriveKafkaReadControl(ctx context.Context, node *plan.Node, proc *process.Process) error {
	ks := node.ExternScan.GetKafkaScan()
	if ks == nil {
		return moerr.NewInternalError(ctx, "kafka scan is missing its metadata")
	}
	seen := map[string]int64{}
	residual := node.FilterList[:0]
	for _, conjunct := range node.FilterList {
		name, val, generated, err := deriveKafkaControl(ctx, node, proc, conjunct)
		if err != nil {
			return err
		}
		if !generated {
			residual = append(residual, conjunct)
			continue
		}
		if prev, dup := seen[name]; dup && prev != val {
			return moerr.NewInvalidInputf(ctx,
				"contradictory %s predicates: %d and %d", name, prev, val)
		}
		seen[name] = val
		switch name {
		case catalog.KafkaReadStartID:
			if val < -1 {
				return moerr.NewInvalidInputf(ctx, "%s must be >= -1, got %d", name, val)
			}
			// bound so StartId+1 cannot overflow (MaxInt64 would wrap to a
			// negative offset and read from the wrong position)
			if val > maxKafkaControlValue {
				return moerr.NewInvalidInputf(ctx, "%s value out of range: %d", name, val)
			}
			ks.StartId = val
			ks.HasStartId = true
		case catalog.KafkaReadSize:
			if val <= 0 {
				return moerr.NewInvalidInputf(ctx, "%s must be positive, got %d", name, val)
			}
			if val > maxKafkaControlValue {
				return moerr.NewInvalidInputf(ctx, "%s value out of range: %d", name, val)
			}
			ks.Size = val
		case catalog.KafkaReadTimeout:
			if val < 0 {
				return moerr.NewInvalidInputf(ctx, "%s must be >= 0, got %d", name, val)
			}
			// bound so time.Duration(val)*time.Second cannot overflow into a
			// negative duration (which would expire the poll instantly and
			// silently return 0 rows)
			if val > maxKafkaTimeoutSeconds {
				return moerr.NewInvalidInputf(ctx,
					"%s must be <= %d seconds (0 blocks forever), got %d", name, int64(maxKafkaTimeoutSeconds), val)
			}
			ks.TimeoutSeconds = val
		}
	}
	node.FilterList = residual
	if !ks.Autocommit && !ks.HasStartId {
		return moerr.NewInvalidInput(ctx,
			"kafka external table with autocommit=false requires a __mo_read_start_id = <last consumed offset> predicate (-1 for the earliest)")
	}
	return nil
}

// deriveKafkaControl inspects one conjunct: if it is `<control col> = <expr>`
// with a column-free expr, it returns (control name, value, true).
func deriveKafkaControl(ctx context.Context, node *plan.Node, proc *process.Process, expr *plan.Expr) (string, int64, bool, error) {
	fn, ok := expr.Expr.(*plan.Expr_F)
	if !ok || fn.F == nil || fn.F.Func == nil || fn.F.Func.ObjName != "=" || len(fn.F.Args) != 2 {
		return "", 0, false, nil
	}
	for i := 0; i < 2; i++ {
		col, ok := fn.F.Args[i].Expr.(*plan.Expr_Col)
		if !ok || col.Col == nil {
			continue
		}
		name := kafkaControlColName(node, col.Col)
		if name == "" {
			continue
		}
		if !isColumnFreeExpr(node, fn.F.Args[1-i]) {
			return "", 0, false, nil
		}
		val, isNull, err := evalKafkaControlValue(ctx, proc, name, fn.F.Args[1-i])
		if err != nil {
			return "", 0, false, err
		}
		if isNull {
			// NULL never equals anything; the conjunct is an ordinary
			// (always-false) row filter, not a control.
			return "", 0, false, nil
		}
		return name, val, true, nil
	}
	return "", 0, false, nil
}

// kafkaControlColName resolves a column reference to a read-control column
// name, "" otherwise. Scoped by reserved ColId so only the synthetic columns
// qualify.
func kafkaControlColName(node *plan.Node, col *plan.ColRef) string {
	if node.TableDef == nil || col.ColPos < 0 || int(col.ColPos) >= len(node.TableDef.Cols) {
		return ""
	}
	def := node.TableDef.Cols[col.ColPos]
	switch def.Name {
	case catalog.KafkaReadStartID, catalog.KafkaReadSize, catalog.KafkaReadTimeout:
		if catalog.IsKafkaHiddenCol(def.Name, def.ColId) {
			return def.Name
		}
	}
	return ""
}

// evalKafkaControlValue evaluates a column-free expression to the int64
// control value.
func evalKafkaControlValue(ctx context.Context, proc *process.Process, name string, expr *plan.Expr) (int64, bool, error) {
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return 0, false, err
	}
	defer executor.Free()
	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return 0, false, err
	}
	if vec == nil || vec.IsConstNull() {
		return 0, true, nil
	}
	if vec.GetNulls().Contains(0) {
		return 0, true, nil
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		return int64(vector.GetFixedAtWithTypeCheck[int8](vec, 0)), false, nil
	case types.T_int16:
		return int64(vector.GetFixedAtWithTypeCheck[int16](vec, 0)), false, nil
	case types.T_int32:
		return int64(vector.GetFixedAtWithTypeCheck[int32](vec, 0)), false, nil
	case types.T_int64:
		return vector.GetFixedAtWithTypeCheck[int64](vec, 0), false, nil
	case types.T_uint8:
		return int64(vector.GetFixedAtWithTypeCheck[uint8](vec, 0)), false, nil
	case types.T_uint16:
		return int64(vector.GetFixedAtWithTypeCheck[uint16](vec, 0)), false, nil
	case types.T_uint32:
		return int64(vector.GetFixedAtWithTypeCheck[uint32](vec, 0)), false, nil
	case types.T_uint64:
		v := vector.GetFixedAtWithTypeCheck[uint64](vec, 0)
		if v > uint64(1)<<62 {
			return 0, false, moerr.NewInvalidInputf(ctx, "%s value out of range", name)
		}
		return int64(v), false, nil
	default:
		return 0, false, moerr.NewInvalidInputf(ctx,
			"%s must compare against an integer value, not %s", name, vec.GetType().String())
	}
}
