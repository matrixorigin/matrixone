// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package hashbuild

import (
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// SpillKeyMemory describes allocations owned by key evaluation, independently
// of the borrowed input vectors and of the hashmap's own allocator contract.
type SpillKeyMemory struct {
	EncodedUnit     int64
	ExpressionBytes int64
	// ResultBytes bounds the root result vectors for all requested rows,
	// including flattened headers, area capacity and null/grouping words.
	// Borrowed column results count here but not in ExpressionBytes.
	ResultBytes int64
	// LogicalResultBytes bounds referenced logical headers and payload. It
	// excludes allocation-class rounding, bitmap backing and retained stores,
	// so a producer must not feed ResultBytes back as the next logical row.
	LogicalResultBytes int64
	StringKeys         bool
}

// A payload is affine in the referenced source columns. Their combined bytes
// cannot exceed inputBytes; summing coefficients before taking the maximum
// preserves that fact for distinct columns and accounts repeated projections.
type keyPayload struct {
	fixed   int64
	columns map[int32]int64
}
type keyContract struct {
	payload    keyPayload
	scratch    int64
	width      int64
	varlen     bool
	expression bool
}

func keySum(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}
func keyProduct(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	return a * b, true
}
func (p *keyPayload) add(other keyPayload, multiplier int64) bool {
	n, ok := keyProduct(other.fixed, multiplier)
	if !ok {
		return false
	}
	p.fixed, ok = keySum(p.fixed, n)
	if !ok {
		return false
	}
	if p.columns == nil {
		p.columns = make(map[int32]int64)
	}
	for column, weight := range other.columns {
		n, ok = keyProduct(weight, multiplier)
		if !ok {
			return false
		}
		p.columns[column], ok = keySum(p.columns[column], n)
		if !ok {
			return false
		}
	}
	return true
}
func (p keyPayload) bytes(inputBytes int64, rows int, columnBytes []int64) (int64, bool) {
	fixed, ok := keyProduct(p.fixed, int64(rows))
	if !ok {
		return 0, false
	}
	var weight int64
	for _, n := range p.columns {
		weight = max(weight, n)
	}
	variable, aggregateOK := keyProduct(weight, inputBytes)
	if columnBytes != nil {
		var selected int64
		selectedOK := true
		for column, coefficient := range p.columns {
			if column < 0 || int(column) >= len(columnBytes) || columnBytes[column] < 0 {
				return 0, false
			}
			n, ok := keyProduct(coefficient, columnBytes[column])
			if !ok {
				selectedOK = false
				break
			}
			selected, ok = keySum(selected, n)
			if !ok {
				selectedOK = false
				break
			}
		}
		// Either independent proof can establish a finite bound. A large
		// unrelated input need not overflow a small referenced-column proof.
		if selectedOK && (!aggregateOK || selected < variable) {
			variable, aggregateOK = selected, true
		}
	}
	if !aggregateOK {
		return 0, false
	}
	return keySum(fixed, variable)
}

// Full old and complete new vector allocations coexist. The mpool growth rule
// returns at most twice the rounded required capacity (including its small
// 4096-byte transition); the two live generations and rounding are explicit.
func keyVectorPeak(width, payload int64, rows int) (int64, bool) {
	data, ok := keyProduct(width, int64(rows))
	if !ok || data > int64(mpool.CapLimit) || payload > int64(mpool.CapLimit) {
		return 0, false
	}
	data = mpool.GrowCapacity(0, data)
	area := mpool.GrowCapacity(0, payload)
	total, ok := keySum(data, area)
	if !ok {
		return 0, false
	}
	total, ok = keyProduct(total, 4)
	if !ok {
		return 0, false
	}
	bitmap := int64((uint64(rows)+64)/64) * 8
	total, ok = keySum(total, bitmap*4)
	if !ok {
		return 0, false
	}
	return keySum(total, 16384)
}
func keyExpressionContract(expr *plan.Expr, inputBytes int64, rows int, columnBytes []int64) (keyContract, bool) {
	if expr == nil || rows <= 0 || inputBytes < 0 {
		return keyContract{}, false
	}
	typ := types.T(expr.Typ.Id).ToType()
	result := keyContract{width: int64(typ.TypeSize()), varlen: typ.IsVarlen()}
	if col := expr.GetCol(); col != nil {
		if col.ColPos < 0 || columnBytes != nil && int(col.ColPos) >= len(columnBytes) {
			return keyContract{}, false
		}
		if result.varlen {
			result.payload.columns = map[int32]int64{col.ColPos: 1}
		} else {
			result.payload.fixed = result.width
		}
		return result, true
	}
	if literal := expr.GetLit(); literal != nil {
		literalBytes := int64(0)
		if result.varlen && !literal.Isnull {
			// These Sval types are copied as bytes by the constant executor.
			// Arrays and datalinks perform parsing, so their scratch is not
			// bounded merely by the encoded literal's length.
			switch typ.Oid {
			case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_geometry:
			default:
				return keyContract{}, false
			}
			value, ok := literal.Value.(*plan.Literal_Sval)
			if !ok {
				return keyContract{}, false
			}
			literalBytes = int64(len(value.Sval))
		}
		result.payload.fixed = result.width
		if result.varlen {
			result.payload.fixed = literalBytes
		}
		if literal.Isnull {
			result.payload = keyPayload{}
		}
		result.expression = true
		var ok bool
		result.scratch, ok = keyVectorPeak(result.width, literalBytes, rows)
		return result, ok
	}
	function := expr.GetF()
	if function == nil || function.Func == nil {
		return keyContract{}, false
	}
	name := strings.ToLower(function.Func.ObjName)
	serial := name == "serial" || name == "serial_full"
	nullPredicate := name == "isnull" || name == "isnotnull"
	if nullPredicate && (typ.Oid != types.T_bool || len(function.Args) != 1 || function.Args[0] == nil || function.Args[0].GetT() != nil) {
		return keyContract{}, false
	}
	if !serial {
		// Result width alone is not an allocation contract (JSON predicates, for
		// example, parse a variable-sized tree before producing one BOOL).
		switch name {
		case "+", "-", "*", "/", "div", "%", "mod", "=", "<>", "!=", "<", "<=", ">", ">=", "and", "or", "xor", "not", "cast", "isnull", "isnotnull":
		default:
			return keyContract{}, false
		}
		for _, arg := range function.Args {
			if arg == nil {
				return keyContract{}, false
			}
			if arg.GetT() != nil {
				continue
			}
			typ := types.T(arg.Typ.Id).ToType()
			// These two predicates read only IsConstNull/GetNulls. Their BOOL
			// result does not inspect, parse or copy a varlen argument's payload.
			// The child executor still needs its own recursive contract below.
			if !nullPredicate && !typ.IsFixedLen() {
				return keyContract{}, false
			}
		}
	}
	if result.varlen && !serial {
		return keyContract{}, false
	}
	result.expression = true
	for _, arg := range function.Args {
		// CAST target types are executor metadata, not an evaluated vector.
		if arg != nil && arg.GetT() != nil {
			continue
		}
		child, ok := keyExpressionContract(arg, inputBytes, rows, columnBytes)
		if !ok {
			return keyContract{}, false
		}
		result.scratch, ok = keySum(result.scratch, child.scratch)
		if !ok {
			return keyContract{}, false
		}
		if serial {
			// Packer string escaping doubles each zero byte; type/length/terminator
			// consume at most three extra bytes per argument. Twice fixed TypeSize
			// also covers integer length prefixes and all supported fixed encodings.
			if !result.payload.add(child.payload, 2) {
				return keyContract{}, false
			}
			result.payload.fixed, ok = keySum(result.payload.fixed, 3)
			if !ok {
				return keyContract{}, false
			}
		}
	}
	if !serial {
		result.payload.fixed = result.width
	}
	payload := int64(0)
	var ok bool
	if result.varlen {
		payload, ok = result.payload.bytes(inputBytes, rows, columnBytes)
		if !ok {
			return keyContract{}, false
		}
	}
	vectorPeak, ok := keyVectorPeak(result.width, payload, rows)
	if !ok {
		return keyContract{}, false
	}
	result.scratch, ok = keySum(result.scratch, vectorPeak)
	if !ok {
		return keyContract{}, false
	}
	if serial {
		// ensureSizeSlow requests old capacity+n. At a growth old capacity is
		// smaller than the final row size, hence request<2*row. ClassAllocator's
		// complete new class is <=2*ClassAllocationSize(row); old<that class.
		// Bound one row by the full payload to include skewed singleton values.
		size, ok := malloc.ClassAllocationSize(uint64(max(payload, 4096)))
		if !ok || size > math.MaxInt64/3 {
			return keyContract{}, false
		}
		result.scratch, ok = keySum(result.scratch, int64(size)*3)
		if !ok {
			return keyContract{}, false
		}
	}
	return result, true
}

// SupportsBoundedSpillKeys is checked on the entire import DAG before admission
// or evaluation. Unknown varlen functions keep their legacy execution path.
func SupportsBoundedSpillKeys(conditions []*plan.Expr) bool {
	if len(conditions) == 0 {
		return false
	}
	for _, expr := range conditions {
		if _, ok := keyExpressionContract(expr, 1, 1, nil); !ok {
			return false
		}
	}
	return true
}

// SpillKeyMemoryBound is shared by normal prehash D, skew I, and the compiler's
// input envelope. inputBytes bounds all referenced input payload together;
// rows is the largest batch evaluated by one executor generation.
func SpillKeyMemoryBound(conditions []*plan.Expr, inputBytes int64, rows int) (SpillKeyMemory, bool) {
	return SpillKeyMemoryBoundForColumns(conditions, inputBytes, rows, nil)
}

// SpillKeyMemoryBoundForColumns additionally accepts logical referenced bytes
// for each input column over the same rows. Call with row bounds for one-row
// derivation and batch bounds for a batch. Headers may be included in a column
// bound, conservatively. A nil slice retains the aggregate-only contract.
// Affine payloads use both the aggregate correlation and the actual referenced
// columns, so an unrelated JSON/array column cannot inflate a small serial key.
func SpillKeyMemoryBoundForColumns(conditions []*plan.Expr, inputBytes int64, rows int, columnBytes []int64) (SpillKeyMemory, bool) {
	if len(conditions) == 0 || inputBytes < 0 || rows <= 0 {
		return SpillKeyMemory{}, false
	}
	for _, n := range columnBytes {
		if n < 0 {
			return SpillKeyMemory{}, false
		}
	}
	contracts := make([]keyContract, 0, len(conditions))
	var encoded keyPayload
	result := SpillKeyMemory{}
	var physicalWidth int64
	for _, expr := range conditions {
		contract, ok := keyExpressionContract(expr, inputBytes, rows, columnBytes)
		if !ok {
			return SpillKeyMemory{}, false
		}
		contracts = append(contracts, contract)
		data, ok := keyProduct(contract.width, int64(rows))
		if !ok || data > int64(mpool.CapLimit) {
			return SpillKeyMemory{}, false
		}
		area := int64(0)
		if contract.varlen {
			area, ok = contract.payload.bytes(inputBytes, rows, columnBytes)
			if !ok || area > int64(mpool.CapLimit) {
				return SpillKeyMemory{}, false
			}
		}
		logical, ok := keySum(data, area)
		if !ok {
			return SpillKeyMemory{}, false
		}
		result.LogicalResultBytes, ok = keySum(result.LogicalResultBytes, logical)
		if !ok {
			return SpillKeyMemory{}, false
		}
		backing, ok := keySum(mpool.GrowCapacity(0, data), mpool.GrowCapacity(0, area))
		if !ok {
			return SpillKeyMemory{}, false
		}
		backing, ok = keySum(backing, int64((uint64(rows)+64)/64)*16)
		if !ok {
			return SpillKeyMemory{}, false
		}
		result.ResultBytes, ok = keySum(result.ResultBytes, backing)
		if !ok {
			return SpillKeyMemory{}, false
		}
		result.ExpressionBytes, ok = keySum(result.ExpressionBytes, contract.scratch)
		if !ok {
			return SpillKeyMemory{}, false
		}
		width := contract.width
		if contract.varlen {
			width = 128
		}
		physicalWidth, ok = keySum(physicalWidth, width)
		if !ok {
			return SpillKeyMemory{}, false
		}
		if !encoded.add(contract.payload, 1) {
			return SpillKeyMemory{}, false
		}
		prefix := int64(1)
		if contract.varlen {
			prefix = 3
		}
		encoded.fixed, ok = keySum(encoded.fixed, prefix)
		if !ok {
			return SpillKeyMemory{}, false
		}
	}
	result.StringKeys = physicalWidth > 8
	var ok bool
	result.EncodedUnit, ok = encoded.bytes(inputBytes, min(rows, hashmap.UnitLimit), columnBytes)
	if !ok {
		return SpillKeyMemory{}, false
	}
	// One expression makes evalBatch duplicate all root key vectors. Their full
	// new storage is separate from function results and borrowed column vectors.
	duplicates := false
	for _, contract := range contracts {
		duplicates = duplicates || contract.expression
	}
	if duplicates {
		for _, contract := range contracts {
			payload := int64(0)
			if contract.varlen {
				payload, ok = contract.payload.bytes(inputBytes, rows, columnBytes)
				if !ok {
					return SpillKeyMemory{}, false
				}
			}
			peak, ok := keyVectorPeak(contract.width, payload, rows)
			if !ok {
				return SpillKeyMemory{}, false
			}
			result.ExpressionBytes, ok = keySum(result.ExpressionBytes, peak)
			if !ok {
				return SpillKeyMemory{}, false
			}
		}
	}
	return result, true
}
