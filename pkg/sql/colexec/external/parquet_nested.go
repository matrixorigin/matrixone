// Copyright 2024 Matrix Origin
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
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/parquet-go/parquet-go"
)

// isNestedTargetTypeSupported checks if nested type can map to target type
func isNestedTargetTypeSupported(t types.T) bool {
	switch t {
	case types.T_json, types.T_text, types.T_varchar, types.T_char:
		return true
	default:
		return false
	}
}

// getNestedMapper creates mapper for nested type column
func (h *ParquetHandler) getNestedMapper(col *parquet.Column, dt plan.Type) *columnMapper {
	return &columnMapper{
		srcNull:            col.Optional(),
		dstNull:            !dt.NotNullable,
		maxDefinitionLevel: byte(col.MaxDefinitionLevel()),
		mapper:             nil, // nested columns use row mode
	}
}

// getDataByRow reads data row by row (used when has nested columns)
func (h *ParquetHandler) getDataByRow(bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	if h.offset > 0 {
		if err := h.rowReader.SeekToRow(h.offset); err != nil {
			return moerr.ConvertGoError(param.Ctx, err)
		}
	}

	rowBuf := make([]parquet.Row, int(h.batchCnt))
	n, err := h.rowReader.ReadRows(rowBuf)
	if err != nil && !errors.Is(err, io.EOF) {
		return moerr.ConvertGoError(param.Ctx, err)
	}
	rowBuf = rowBuf[:n]

	for _, row := range rowBuf {
		if err := h.processRow(row, bat, param, proc); err != nil {
			return err
		}
	}

	bat.SetRowCount(n)
	h.offset += int64(n)

	finish := n == 0 || h.offset >= h.file.NumRows()
	if finish {
		h.cleanup()
		param.parqh = nil
		param.Fileparam.FileFin++
		if param.Fileparam.FileFin >= param.Fileparam.FileCnt {
			param.Fileparam.End = true
			// Save parquet schema for nested columns
			h.saveNestedSchemas(param, proc)
		}
	}

	return nil
}

// cleanup releases resources
func (h *ParquetHandler) cleanup() {
	if h.rowReader != nil {
		h.rowReader.Close()
		h.rowReader = nil
	}
}

// processRow processes a single row
func (h *ParquetHandler) processRow(row parquet.Row, bat *batch.Batch, param *ExternalParam, proc *process.Process) error {
	for colIdx, col := range h.cols {
		if col == nil || param.Cols[colIdx].Hidden || h.mappers[colIdx] == nil {
			continue
		}
		vec := bat.Vecs[colIdx]
		def := param.Cols[colIdx]

		if !col.Leaf() {
			if err := h.processNestedValue(row, col, vec, def, proc); err != nil {
				return err
			}
		} else {
			if err := h.processLeafValue(row, col, vec, def, proc); err != nil {
				return err
			}
		}
	}
	return nil
}

// processLeafValue processes a leaf column value
func (h *ParquetHandler) processLeafValue(
	row parquet.Row,
	col *parquet.Column,
	vec *vector.Vector,
	def *plan.ColDef,
	proc *process.Process,
) error {
	colIndex := col.Index()
	var value parquet.Value
	found := false
	for _, v := range row {
		if v.Column() == colIndex {
			value = v
			found = true
			break
		}
	}

	if !found || value.IsNull() {
		return appendNull(vec, def, proc)
	}

	return appendLeafValue(value, col, vec, def, proc)
}

// appendNull appends a NULL value to vector
func appendNull(vec *vector.Vector, def *plan.ColDef, proc *process.Process) error {
	if def.NotNull {
		return moerr.NewConstraintViolation(proc.Ctx, "NULL value not allowed")
	}
	return vector.AppendAny(vec, nil, true, proc.Mp())
}

// appendLeafValue appends a leaf value to vector
func appendLeafValue(
	v parquet.Value,
	col *parquet.Column,
	vec *vector.Vector,
	def *plan.ColDef,
	proc *process.Process,
) error {
	targetType := types.T(def.Typ.Id)
	st := col.Type()

	switch targetType {
	case types.T_bool:
		return vector.AppendFixed(vec, v.Boolean(), false, proc.Mp())
	case types.T_int8:
		return vector.AppendFixed(vec, int8(v.Int32()), false, proc.Mp())
	case types.T_int16:
		return vector.AppendFixed(vec, int16(v.Int32()), false, proc.Mp())
	case types.T_int32:
		return vector.AppendFixed(vec, v.Int32(), false, proc.Mp())
	case types.T_int64:
		if st.Kind() == parquet.Int32 {
			return vector.AppendFixed(vec, int64(v.Int32()), false, proc.Mp())
		}
		return vector.AppendFixed(vec, v.Int64(), false, proc.Mp())
	case types.T_uint8:
		return vector.AppendFixed(vec, uint8(v.Int32()), false, proc.Mp())
	case types.T_uint16:
		return vector.AppendFixed(vec, uint16(v.Int32()), false, proc.Mp())
	case types.T_uint32:
		if st.Kind() == parquet.Int32 {
			return vector.AppendFixed(vec, uint32(v.Int32()), false, proc.Mp())
		}
		return vector.AppendFixed(vec, uint32(v.Int64()), false, proc.Mp())
	case types.T_uint64:
		return vector.AppendFixed(vec, uint64(v.Int64()), false, proc.Mp())
	case types.T_float32:
		return vector.AppendFixed(vec, v.Float(), false, proc.Mp())
	case types.T_float64:
		if st.Kind() == parquet.Float {
			return vector.AppendFixed(vec, float64(v.Float()), false, proc.Mp())
		}
		return vector.AppendFixed(vec, v.Double(), false, proc.Mp())
	case types.T_char, types.T_varchar, types.T_text, types.T_blob,
		types.T_binary, types.T_varbinary:
		return vector.AppendBytes(vec, v.ByteArray(), false, proc.Mp())
	default:
		return moerr.NewNYIf(proc.Ctx, "row mode convert to %s", targetType.String())
	}
}

// processNestedValue processes a nested column value
func (h *ParquetHandler) processNestedValue(
	row parquet.Row,
	col *parquet.Column,
	vec *vector.Vector,
	def *plan.ColDef,
	proc *process.Process,
) error {
	colValues := extractNestedColumnValues(row, col)

	if isNestedColumnNull(colValues, col) {
		return appendNull(vec, def, proc)
	}

	nested, err := reconstructNestedValue(proc.Ctx, col, colValues)
	if err != nil {
		return moerr.NewInternalErrorf(proc.Ctx,
			"failed to reconstruct nested column %s: %v", col.Name(), err)
	}

	targetType := types.T(def.Typ.Id)
	return writeNestedToVector(nested, targetType, vec, proc)
}

// extractNestedColumnValues extracts all values for a nested column from row
func extractNestedColumnValues(row parquet.Row, col *parquet.Column) []parquet.Value {
	startIdx, endIdx := getNestedColumnIndexRange(col)
	var values []parquet.Value
	for _, v := range row {
		colIdx := v.Column()
		if colIdx >= startIdx && colIdx < endIdx {
			values = append(values, v)
		}
	}
	return values
}

// getNestedColumnIndexRange gets leaf column index range for nested column
func getNestedColumnIndexRange(col *parquet.Column) (start, end int) {
	leaves := collectLeafColumns(col)
	if len(leaves) == 0 {
		return -1, -1
	}
	start = leaves[0].Index()
	end = leaves[len(leaves)-1].Index() + 1
	return
}

// collectLeafColumns recursively collects all leaf columns
func collectLeafColumns(col *parquet.Column) []*parquet.Column {
	if col.Leaf() {
		return []*parquet.Column{col}
	}
	var leaves []*parquet.Column
	for _, child := range col.Columns() {
		leaves = append(leaves, collectLeafColumns(child)...)
	}
	return leaves
}

// isNestedColumnNull checks if nested column is NULL
func isNestedColumnNull(values []parquet.Value, col *parquet.Column) bool {
	if len(values) == 0 {
		return true
	}
	if col != nil && col.Optional() && len(values) > 0 {
		return values[0].DefinitionLevel() == 0
	}
	return false
}

// reconstructNestedValue reconstructs nested structure to Go type
func reconstructNestedValue(ctx context.Context, col *parquet.Column, values []parquet.Value) (any, error) {
	// Check LogicalType first
	logicalType := col.Type().LogicalType()
	if logicalType != nil {
		if logicalType.List != nil {
			return reconstructList(ctx, col, values)
		}
		if logicalType.Map != nil {
			// Map with LogicalType annotation has structure:
			// metadata (Map) -> key_value (repeated) -> key, value
			// We need to pass the key_value child to reconstructMap
			children := col.Columns()
			if len(children) == 1 {
				return reconstructMap(ctx, children[0], values)
			}
			return reconstructMap(ctx, col, values)
		}
	}

	// Check for Parquet 3-level List pattern (without LogicalType annotation)
	// Pattern: group (optional) -> repeated group "list" -> element
	if isParquetListPattern(col) {
		return reconstructListFromPattern(ctx, col, values)
	}

	// Check for Parquet Map pattern (without LogicalType annotation)
	// Pattern: group (optional) -> repeated group "key_value" -> key, value
	if isParquetMapPattern(col) {
		return reconstructMapFromPattern(ctx, col, values)
	}

	return reconstructStruct(ctx, col, values)
}

// isParquetListPattern checks if column matches Parquet 3-level List pattern
// Pattern: group (optional) -> repeated group "list" -> element
func isParquetListPattern(col *parquet.Column) bool {
	if col.Leaf() {
		return false
	}
	children := col.Columns()
	if len(children) != 1 {
		return false
	}
	listChild := children[0]
	// Check if child is named "list" and is repeated
	return listChild.Name() == "list" && listChild.Repeated()
}

// isParquetMapPattern checks if column matches Parquet Map pattern
// Pattern: group (optional) -> repeated group "key_value" -> key, value
func isParquetMapPattern(col *parquet.Column) bool {
	if col.Leaf() {
		return false
	}
	children := col.Columns()
	if len(children) != 1 {
		return false
	}
	kvChild := children[0]
	// Check if child is named "key_value" and is repeated
	return kvChild.Name() == "key_value" && kvChild.Repeated()
}

// reconstructListFromPattern reconstructs List from 3-level pattern
func reconstructListFromPattern(ctx context.Context, col *parquet.Column, values []parquet.Value) ([]any, error) {
	// Extract values from the "list.element" path
	listChild := col.Columns()[0]
	if len(listChild.Columns()) == 0 {
		return nil, moerr.NewInternalErrorf(ctx, "list child has no element column")
	}
	elementCol := listChild.Columns()[0]

	// Check if element is a leaf or nested structure
	if elementCol.Leaf() {
		// Simple case: list of primitive values
		return reconstructList(ctx, listChild, values)
	} else {
		// Complex case: list of structs/maps/lists
		return reconstructListOfNested(ctx, elementCol, values)
	}
}

// reconstructMapFromPattern reconstructs Map from pattern
func reconstructMapFromPattern(ctx context.Context, col *parquet.Column, values []parquet.Value) (map[string]any, error) {
	// Extract values from the "key_value.key" and "key_value.value" paths
	kvChild := col.Columns()[0]
	return reconstructMap(ctx, kvChild, values)
}

// reconstructListOfNested reconstructs List of nested structures (Struct/Map/List)
func reconstructListOfNested(ctx context.Context, elementCol *parquet.Column, values []parquet.Value) ([]any, error) {
	result := make([]any, 0)
	if len(values) == 0 {
		return result, nil
	}

	leafCols := collectLeafColumns(elementCol)
	if len(leafCols) == 0 {
		return result, nil
	}

	// Check if this is list<list> (single leaf column) or list<struct> (multiple leaf columns)
	if len(leafCols) == 1 {
		// list<list<T>> case: group by RepetitionLevel
		// rep=0 means new row, rep=1 means new outer list element, rep=2 means continue inner list
		var groups [][]parquet.Value
		currentGroup := make([]parquet.Value, 0)

		for _, v := range values {
			rep := v.RepetitionLevel()
			// rep <= 1 means new element in outer list
			if rep <= 1 && len(currentGroup) > 0 {
				groups = append(groups, currentGroup)
				currentGroup = make([]parquet.Value, 0)
			}
			currentGroup = append(currentGroup, v)
		}
		if len(currentGroup) > 0 {
			groups = append(groups, currentGroup)
		}

		for _, group := range groups {
			nested, err := reconstructNestedByType(ctx, elementCol, group)
			if err != nil {
				return nil, err
			}
			result = append(result, nested)
		}
		return result, nil
	}

	// list<struct> case: values are stored column-wise
	valuesByCol := make(map[int][]parquet.Value)
	for _, v := range values {
		colIdx := v.Column()
		valuesByCol[colIdx] = append(valuesByCol[colIdx], v)
	}

	firstLeafIdx := leafCols[0].Index()
	firstColValues := valuesByCol[firstLeafIdx]
	if len(firstColValues) == 0 {
		return result, nil
	}

	numElements := len(firstColValues)

	for i := 0; i < numElements; i++ {
		group := make([]parquet.Value, 0, len(leafCols))
		for _, leaf := range leafCols {
			colValues := valuesByCol[leaf.Index()]
			if i < len(colValues) {
				group = append(group, colValues[i])
			}
		}
		if len(group) > 0 {
			nested, err := reconstructNestedByType(ctx, elementCol, group)
			if err != nil {
				return nil, err
			}
			result = append(result, nested)
		}
	}

	return result, nil
}

func reconstructList(ctx context.Context, col *parquet.Column, values []parquet.Value) ([]any, error) {
	result := make([]any, 0)
	// Empty list case: no values at all
	if len(values) == 0 {
		return result, nil
	}
	// Empty list case: single NULL value with low definition level
	// This indicates an empty list, not a list with a NULL element
	if len(values) == 1 && values[0].IsNull() && values[0].RepetitionLevel() == 0 {
		return result, nil
	}

	// Check if this is a list of nested types (list<list>, list<struct>, list<map>)
	// For List with LogicalType, structure is: col (List) -> list (repeated) -> element
	children := col.Columns()
	if len(children) > 0 {
		listChild := children[0]
		// Check if list has element child that is nested
		elementChildren := listChild.Columns()
		if len(elementChildren) > 0 {
			elementCol := elementChildren[0]
			// If element is not a leaf, it's a nested type - use special handling
			if !elementCol.Leaf() {
				return reconstructListOfNested(ctx, elementCol, values)
			}
			// Check if element itself is a List/Map (has LogicalType)
			elemLogicalType := elementCol.Type().LogicalType()
			if elemLogicalType != nil && (elemLogicalType.List != nil || elemLogicalType.Map != nil) {
				return reconstructListOfNested(ctx, elementCol, values)
			}
		}
	}

	// Simple list of primitive values
	for _, v := range values {
		if v.IsNull() {
			result = append(result, nil)
		} else {
			result = append(result, parquetValueToGo(v))
		}
	}
	return result, nil
}

// reconstructMap reconstructs Map type
func reconstructMap(ctx context.Context, col *parquet.Column, values []parquet.Value) (map[string]any, error) {
	result := make(map[string]any)

	// Handle empty map: no values or only NULL placeholder values
	if len(values) == 0 {
		return result, nil
	}
	// Check if all values are NULL with low definition level (empty map indicator)
	allNull := true
	for _, v := range values {
		if !v.IsNull() || v.DefinitionLevel() > 1 {
			allNull = false
			break
		}
	}
	if allNull {
		return result, nil
	}

	// For Map with LogicalType, structure is: map -> key_value -> key/value
	// We need to navigate to the key_value group first
	searchCol := col
	children := col.Columns()
	if len(children) == 1 {
		// This is likely the key_value group
		kvChild := children[0]
		if kvChild.Name() == "key_value" || kvChild.Name() == "entries" {
			searchCol = kvChild
		}
	}

	// Find key and value columns
	var keyCol, valueCol *parquet.Column
	for _, child := range searchCol.Columns() {
		if child.Name() == "key" {
			keyCol = child
		} else if child.Name() == "value" {
			valueCol = child
		}
	}

	if keyCol == nil {
		return result, nil
	}

	keyColIdx := keyCol.Index()

	// Simple case: both key and value are leaf columns
	if keyCol.Leaf() && (valueCol == nil || valueCol.Leaf()) {
		valColIdx := -1
		if valueCol != nil {
			valColIdx = valueCol.Index()
		}

		var keys, vals []parquet.Value
		for _, v := range values {
			// Skip NULL keys (they indicate empty entries or padding)
			if v.Column() == keyColIdx && !v.IsNull() {
				keys = append(keys, v)
			} else if v.Column() == valColIdx {
				vals = append(vals, v)
			}
		}

		for i := 0; i < len(keys); i++ {
			keyStr := stringifyMapKey(keys[i])
			if _, exists := result[keyStr]; exists {
				return nil, moerr.NewInternalErrorf(ctx, "duplicate map key: %s", keyStr)
			}
			if i < len(vals) {
				if vals[i].IsNull() {
					result[keyStr] = nil
				} else {
					result[keyStr] = parquetValueToGo(vals[i])
				}
			} else {
				result[keyStr] = nil
			}
		}
		return result, nil
	}

	// Complex case: value is nested (List/Struct/Map)
	if valueCol != nil && !valueCol.Leaf() {
		valueLeafCols := collectLeafColumns(valueCol)

		// Collect all keys with their positions
		var keys []parquet.Value
		var keyPositions []int // position in the values slice
		for i, v := range values {
			if v.Column() == keyColIdx {
				keys = append(keys, v)
				keyPositions = append(keyPositions, i)
			}
		}

		numKeys := len(keys)
		if numKeys == 0 {
			return result, nil
		}

		// Collect value column values
		var valueColValues []parquet.Value
		for _, v := range values {
			colIdx := v.Column()
			for _, leaf := range valueLeafCols {
				if colIdx == leaf.Index() {
					valueColValues = append(valueColValues, v)
					break
				}
			}
		}

		// For map<K, list<T>>, we need to group values by RepetitionLevel
		// rep=0 means new row, rep=1 means new map entry, rep=2 means continue list
		if len(valueLeafCols) == 1 {
			// Single leaf column (e.g., map<string, list<int>>)
			// Group values by key using RepetitionLevel
			valueGroups := make([][]parquet.Value, numKeys)
			currentKeyIdx := -1

			for _, v := range valueColValues {
				rep := v.RepetitionLevel()
				// rep <= 1 means new map entry (new key)
				if rep <= 1 {
					currentKeyIdx++
					if currentKeyIdx >= numKeys {
						break
					}
					valueGroups[currentKeyIdx] = make([]parquet.Value, 0)
				}
				if currentKeyIdx >= 0 && currentKeyIdx < numKeys {
					valueGroups[currentKeyIdx] = append(valueGroups[currentKeyIdx], v)
				}
			}

			for i := 0; i < numKeys; i++ {
				keyStr := stringifyMapKey(keys[i])
				if _, exists := result[keyStr]; exists {
					return nil, moerr.NewInternalErrorf(ctx, "duplicate map key: %s", keyStr)
				}

				if i < len(valueGroups) && len(valueGroups[i]) > 0 {
					nested, err := reconstructNestedByType(ctx, valueCol, valueGroups[i])
					if err != nil {
						return nil, err
					}
					result[keyStr] = nested
				} else {
					result[keyStr] = nil
				}
			}
		} else {
			// Multiple leaf columns (e.g., map<string, struct>)
			// Group values by column for struct values
			valuesByCol := make(map[int][]parquet.Value)
			for _, v := range values {
				colIdx := v.Column()
				for _, leaf := range valueLeafCols {
					if colIdx == leaf.Index() {
						valuesByCol[colIdx] = append(valuesByCol[colIdx], v)
						break
					}
				}
			}

			// Build value groups by interleaving
			for i := 0; i < numKeys; i++ {
				keyStr := stringifyMapKey(keys[i])
				if _, exists := result[keyStr]; exists {
					return nil, moerr.NewInternalErrorf(ctx, "duplicate map key: %s", keyStr)
				}

				group := make([]parquet.Value, 0, len(valueLeafCols))
				for _, leaf := range valueLeafCols {
					colValues := valuesByCol[leaf.Index()]
					if i < len(colValues) {
						group = append(group, colValues[i])
					}
				}

				if len(group) == 0 {
					result[keyStr] = nil
				} else {
					nested, err := reconstructNestedByType(ctx, valueCol, group)
					if err != nil {
						return nil, err
					}
					result[keyStr] = nested
				}
			}
		}
	}

	return result, nil
}

// stringifyMapKey converts map key to string
func stringifyMapKey(v parquet.Value) string {
	if v.IsNull() {
		return "null"
	}
	switch v.Kind() {
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return string(v.ByteArray())
	default:
		return fmt.Sprintf("%v", parquetValueToGo(v))
	}
}

// reconstructStruct reconstructs Struct type
func reconstructStruct(ctx context.Context, col *parquet.Column, values []parquet.Value) (map[string]any, error) {
	result := make(map[string]any)
	for _, child := range col.Columns() {
		fieldName := child.Name()
		if child.Leaf() {
			for _, v := range values {
				if v.Column() == child.Index() {
					if v.IsNull() {
						result[fieldName] = nil
					} else {
						result[fieldName] = parquetValueToGo(v)
					}
					break
				}
			}
		} else {
			childValues := filterValuesByColumn(values, child)
			childResult, err := reconstructNestedByType(ctx, child, childValues)
			if err != nil {
				return nil, err
			}
			result[fieldName] = childResult
		}
	}
	return result, nil
}

// filterValuesByColumn filters values belonging to nested column
func filterValuesByColumn(values []parquet.Value, col *parquet.Column) []parquet.Value {
	startIdx, endIdx := getNestedColumnIndexRange(col)
	var result []parquet.Value
	for _, v := range values {
		colIdx := v.Column()
		if colIdx >= startIdx && colIdx < endIdx {
			result = append(result, v)
		}
	}
	return result
}

// reconstructNestedByType reconstructs nested structure by type
func reconstructNestedByType(ctx context.Context, col *parquet.Column, values []parquet.Value) (any, error) {
	logicalType := col.Type().LogicalType()
	if logicalType != nil {
		if logicalType.List != nil {
			return reconstructList(ctx, col, values)
		}
		if logicalType.Map != nil {
			return reconstructMap(ctx, col, values)
		}
	}

	// Check for Parquet 3-level List pattern (without LogicalType annotation)
	if isParquetListPattern(col) {
		return reconstructListFromPattern(ctx, col, values)
	}

	// Check for Parquet Map pattern (without LogicalType annotation)
	if isParquetMapPattern(col) {
		return reconstructMapFromPattern(ctx, col, values)
	}

	return reconstructStruct(ctx, col, values)
}

// parquetValueToGo converts parquet.Value to Go type
func parquetValueToGo(v parquet.Value) any {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		return int64(v.Int32())
	case parquet.Int64:
		return v.Int64()
	case parquet.Float:
		return float64(v.Float())
	case parquet.Double:
		return v.Double()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		data := v.ByteArray()
		// Check if this is valid UTF-8 string
		if isValidUTF8(data) {
			return string(data)
		}
		// For binary data, encode as base64 to ensure valid JSON
		return base64.StdEncoding.EncodeToString(data)
	default:
		return nil
	}
}

// isValidUTF8 checks if byte slice is valid UTF-8 string
func isValidUTF8(data []byte) bool {
	for i := 0; i < len(data); {
		if data[i] < 0x80 {
			// ASCII
			i++
			continue
		}
		// Multi-byte sequence
		if data[i]&0xE0 == 0xC0 {
			// 2-byte sequence
			if i+1 >= len(data) || data[i+1]&0xC0 != 0x80 {
				return false
			}
			i += 2
		} else if data[i]&0xF0 == 0xE0 {
			// 3-byte sequence
			if i+2 >= len(data) || data[i+1]&0xC0 != 0x80 || data[i+2]&0xC0 != 0x80 {
				return false
			}
			i += 3
		} else if data[i]&0xF8 == 0xF0 {
			// 4-byte sequence
			if i+3 >= len(data) || data[i+1]&0xC0 != 0x80 || data[i+2]&0xC0 != 0x80 || data[i+3]&0xC0 != 0x80 {
				return false
			}
			i += 4
		} else {
			return false
		}
	}
	return true
}

// writeNestedToVector writes nested structure to vector
func writeNestedToVector(nested any, targetType types.T, vec *vector.Vector, proc *process.Process) error {
	bj, err := bytejson.CreateByteJSON(nested)
	if err != nil {
		return moerr.NewInternalErrorf(proc.Ctx, "failed to create JSON: %v", err)
	}

	switch targetType {
	case types.T_json:
		jsonBytes, err := types.EncodeJson(bj)
		if err != nil {
			return moerr.NewInternalErrorf(proc.Ctx, "failed to encode JSON: %v", err)
		}
		return vector.AppendBytes(vec, jsonBytes, false, proc.Mp())
	case types.T_text, types.T_varchar, types.T_char:
		jsonStr := bj.String()
		return vector.AppendBytes(vec, []byte(jsonStr), false, proc.Mp())
	default:
		return moerr.NewInternalErrorf(proc.Ctx, "unsupported target type: %s", targetType.String())
	}
}
