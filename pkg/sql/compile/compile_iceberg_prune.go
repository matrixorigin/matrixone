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
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	icebergapi "github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func icebergPrunePredicatesFromNode(node *plan.Node) []icebergapi.PrunePredicate {
	fieldIDs := icebergFieldIDsByColumnPosition(node)
	if len(fieldIDs) == 0 {
		return nil
	}
	return icebergPrunePredicatesFromNodeWithFieldIDs(node, fieldIDs)
}

func icebergPrunePredicatesFromNodeWithFieldIDs(node *plan.Node, fieldIDs []int) []icebergapi.PrunePredicate {
	if len(fieldIDs) == 0 {
		return nil
	}
	var out []icebergapi.PrunePredicate
	for _, filter := range node.GetFilterList() {
		out = append(out, icebergPrunePredicatesFromExpr(filter, fieldIDs)...)
	}
	return out
}

func icebergResidualFilterDigestFromNode(node *plan.Node) string {
	if node == nil || len(node.FilterList) == 0 {
		return ""
	}
	hash := sha256.New()
	for _, filter := range node.FilterList {
		if filter == nil {
			continue
		}
		data, err := proto.Marshal(filter)
		if err != nil {
			return ""
		}
		hash.Write([]byte{0})
		hash.Write(data)
	}
	sum := hash.Sum(nil)
	return hex.EncodeToString(sum[:8])
}

func icebergFieldIDsByColumnPosition(node *plan.Node) []int {
	if node == nil || node.TableDef == nil || node.ExternScan == nil || node.ExternScan.IcebergScan == nil {
		return nil
	}
	cols := node.TableDef.GetCols()
	projectedFieldIDs := node.ExternScan.IcebergScan.GetProjectedFieldIds()
	if len(cols) == 0 || len(projectedFieldIDs) != len(cols) {
		return nil
	}
	out := make([]int, len(cols))
	for i, fieldID := range projectedFieldIDs {
		if fieldID <= 0 {
			return nil
		}
		out[i] = int(fieldID)
	}
	return out
}

func icebergFieldIDsByColumnMapping(node *plan.Node, mappings []icebergapi.IcebergColumnMapping) []int {
	if node == nil || node.TableDef == nil || len(node.TableDef.Cols) == 0 || len(mappings) == 0 {
		return nil
	}
	fieldsByName := make(map[string]int, len(mappings))
	for _, mapping := range mappings {
		if mapping.FieldID <= 0 {
			continue
		}
		name := icebergNormalizeColumnName(mapping.ColumnName)
		if name == "" {
			continue
		}
		if prev, ok := fieldsByName[name]; ok && prev != mapping.FieldID {
			return nil
		}
		fieldsByName[name] = mapping.FieldID
	}
	out := make([]int, len(node.TableDef.Cols))
	seenCols := make(map[string]struct{}, len(node.TableDef.Cols))
	for i, col := range node.TableDef.Cols {
		name := icebergNormalizeColumnName(col.GetName())
		if name == "" {
			return nil
		}
		if _, ok := seenCols[name]; ok {
			return nil
		}
		seenCols[name] = struct{}{}
		fieldID, ok := fieldsByName[name]
		if !ok || fieldID <= 0 {
			return nil
		}
		out[i] = fieldID
	}
	return out
}

func icebergNormalizeColumnName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func icebergPrunePredicatesFromExpr(expr *plan.Expr, fieldIDs []int) []icebergapi.PrunePredicate {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return nil
	}
	op, ok := icebergPruneOpFromFuncName(fn.Func.ObjName)
	if !ok {
		if fn.Func.ObjName == "and" {
			var out []icebergapi.PrunePredicate
			for _, arg := range fn.Args {
				out = append(out, icebergPrunePredicatesFromExpr(arg, fieldIDs)...)
			}
			return out
		}
		return nil
	}
	if len(fn.Args) != 2 {
		return nil
	}
	if predicate, ok := icebergPrunePredicateFromBinary(fn.Args[0], fn.Args[1], op, fieldIDs); ok {
		return []icebergapi.PrunePredicate{predicate}
	}
	if predicate, ok := icebergPrunePredicateFromBinary(fn.Args[1], fn.Args[0], icebergFlipPruneOp(op), fieldIDs); ok {
		return []icebergapi.PrunePredicate{predicate}
	}
	return nil
}

func icebergPrunePredicateFromBinary(colExpr, litExpr *plan.Expr, op icebergapi.PruneOp, fieldIDs []int) (icebergapi.PrunePredicate, bool) {
	col := colExpr.GetCol()
	lit := litExpr.GetLit()
	if col == nil || lit == nil {
		return icebergapi.PrunePredicate{}, false
	}
	colPos := int(col.GetColPos())
	if colPos < 0 || colPos >= len(fieldIDs) {
		return icebergapi.PrunePredicate{}, false
	}
	literal, ok := icebergPruneLiteralFromPlanLiteral(lit)
	if !ok {
		return icebergapi.PrunePredicate{}, false
	}
	return icebergapi.PrunePredicate{FieldID: fieldIDs[colPos], Op: op, Literal: literal}, true
}

func icebergPruneOpFromFuncName(name string) (icebergapi.PruneOp, bool) {
	switch name {
	case "=":
		return icebergapi.PruneOpEQ, true
	case "<":
		return icebergapi.PruneOpLT, true
	case "<=":
		return icebergapi.PruneOpLTE, true
	case ">":
		return icebergapi.PruneOpGT, true
	case ">=":
		return icebergapi.PruneOpGTE, true
	default:
		return "", false
	}
}

func icebergFlipPruneOp(op icebergapi.PruneOp) icebergapi.PruneOp {
	switch op {
	case icebergapi.PruneOpLT:
		return icebergapi.PruneOpGT
	case icebergapi.PruneOpLTE:
		return icebergapi.PruneOpGTE
	case icebergapi.PruneOpGT:
		return icebergapi.PruneOpLT
	case icebergapi.PruneOpGTE:
		return icebergapi.PruneOpLTE
	default:
		return op
	}
}

func icebergPruneLiteralFromPlanLiteral(lit *plan.Literal) (icebergapi.PruneLiteral, bool) {
	if lit == nil {
		return icebergapi.PruneLiteral{}, false
	}
	if lit.GetIsnull() {
		return icebergapi.PruneLiteral{IsNull: true}, true
	}
	switch value := lit.GetValue().(type) {
	case *plan.Literal_I8Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.I8Val)}, true
	case *plan.Literal_I16Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.I16Val)}, true
	case *plan.Literal_I32Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.I32Val)}, true
	case *plan.Literal_I64Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: value.I64Val}, true
	case *plan.Literal_U8Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.U8Val)}, true
	case *plan.Literal_U16Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.U16Val)}, true
	case *plan.Literal_U32Val:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.U32Val)}, true
	case *plan.Literal_U64Val:
		if value.U64Val > math.MaxInt64 {
			return icebergapi.PruneLiteral{}, false
		}
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeLong, Int64: int64(value.U64Val)}, true
	case *plan.Literal_Dateval:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeDate, Int64: int64(value.Dateval)}, true
	case *plan.Literal_Dval:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeDouble, Float64: value.Dval}, true
	case *plan.Literal_Fval:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeFloat, Float64: float64(value.Fval)}, true
	case *plan.Literal_Sval:
		return icebergapi.PruneLiteral{Kind: icebergapi.TypeString, String: value.Sval}, true
	case *plan.Literal_Datetimeval:
		return icebergapi.PruneLiteral{
			Kind:       icebergapi.TypeTimestamp,
			Int64:      types.Datetime(value.Datetimeval).ConvertToGoTime(time.UTC).UnixMicro(),
			Normalized: true,
		}, true
	case *plan.Literal_Timestampval:
		// Timestamp literals are normalized by the binder/session timezone before
		// they reach the plan. Convert the internal MO timestamp instant to the
		// Iceberg UTC microsecond bound used by metadata stats.
		return icebergapi.PruneLiteral{
			Kind:       icebergapi.TypeTimestampTZ,
			Int64:      types.Timestamp(value.Timestampval).ToDatetime(time.UTC).ConvertToGoTime(time.UTC).UnixMicro(),
			Normalized: true,
		}, true
	default:
		return icebergapi.PruneLiteral{}, false
	}
}
