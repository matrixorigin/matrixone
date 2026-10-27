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

package table_function

import (
	"encoding/json"
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
)

// MemoryColumnBound describes referenced payload, rather than a declared SQL
// width: lenient ingestion can store a value wider than VARCHAR(n).
type MemoryColumnBound struct {
	Type       types.Type
	RowBytes   int64
	BatchBytes int64
	Constant   bool
}

type FunctionMemoryBound struct {
	Bytes   int64
	Columns []MemoryColumnBound
}

func memoryBoundSum(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}
func memoryBoundProduct(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	return a * b, true
}

// MemoryVectorCapacity covers complete old/new and retained executor vector
// storage. rowBytes may include a header; including it again as varlen area is
// conservative. No allocation is performed to obtain this bound.
func MemoryVectorCapacity(typ types.Type, rowBytes int64, rows int) (int64, bool) {
	return MemoryBatchVectorCapacity(typ, rowBytes, math.MaxInt64, rows)
}

// MemoryBatchVectorCapacity also accepts the producer's enforced complete
// batch payload limit, so a wide singleton is never multiplied by 8192.
func MemoryBatchVectorCapacity(typ types.Type, rowBytes, batchBytes int64, rows int) (int64, bool) {
	if rowBytes < 0 || rows <= 0 || typ.TypeSize() <= 0 {
		return 0, false
	}
	data, ok := memoryBoundProduct(int64(typ.TypeSize()), int64(rows))
	if !ok || data > int64(mpool.CapLimit) {
		return 0, false
	}
	area := int64(0)
	if typ.IsVarlen() {
		area, ok = memoryBoundProduct(rowBytes, int64(rows))
		if !ok {
			return 0, false
		}
		area = min(area, batchBytes)
		if area > int64(mpool.CapLimit) {
			return 0, false
		}
	}
	capacity, ok := memoryBoundSum(mpool.GrowCapacity(0, data), mpool.GrowCapacity(0, area))
	if !ok {
		return 0, false
	}
	capacity, ok = memoryBoundProduct(capacity, 4)
	if !ok {
		return 0, false
	}
	return memoryBoundSum(capacity, int64((uint64(rows)+64)/64)*8*8+16384)
}

// SupportsMemoryBound is owned by the function implementation, not by a SQL
// compiler switch on index or parser names. Datalink fetches and the other
// parsers have independent allocation graphs and retain their legacy path.
func (tableFunction *TableFunction) SupportsMemoryBound() bool {
	if tableFunction == nil || tableFunction.FuncName != "fulltext_index_tokenize" || len(tableFunction.Args) < 2 {
		return false
	}
	var param fulltext.FullTextParserParam
	if len(tableFunction.Params) > 0 && json.Unmarshal(tableFunction.Params, &param) != nil {
		return false
	}
	if param.Parser != "" && param.Parser != "default" && param.Parser != "ngram" {
		return false
	}
	for i, arg := range tableFunction.Args {
		if arg == nil || types.T(arg.Typ.Id) == types.T_datalink {
			return false
		}
		if i > 0 {
			switch types.T(arg.Typ.Id) {
			case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
			default:
				return false
			}
		}
	}
	return true
}

// MemoryBound covers tokenizeState's complete retained document, transient
// content construction, and its 8192-row result batch. SimpleTokenizer yields
// at most one token per input byte, followed by one DOC_LEN entry. Every copied
// token is at most MAX_TOKEN_SIZE bytes. Apply's output is charged by its caller
// using the real Result positions; unrelated source columns are never copied
// into this function's allocation table.
func (tableFunction *TableFunction) MemoryBound(args []MemoryColumnBound, argumentRowBytes, unit int64) (FunctionMemoryBound, bool) {
	if unit <= 0 || !tableFunction.SupportsMemoryBound() || len(args) != len(tableFunction.Args) {
		return FunctionMemoryBound{}, false
	}
	var content int64
	for _, arg := range args[1:] {
		var ok bool
		content, ok = memoryBoundSum(content, arg.RowBytes)
		if !ok {
			return FunctionMemoryBound{}, false
		}
	}
	content = min(content, argumentRowBytes)
	content, ok := memoryBoundSum(content, int64(len(args)-2))
	if !ok {
		return FunctionMemoryBound{}, false
	}
	tokens, ok := memoryBoundSum(content, 1)
	if !ok {
		return FunctionMemoryBound{}, false
	}
	// Slice append: old and new backing can coexist, capacity rounding and the
	// initial 512 entries are explicit. Resetting length retains old strings;
	// include their generation alongside replacement strings.
	entries, ok := memoryBoundSum(tokens, 512)
	if !ok {
		return FunctionMemoryBound{}, false
	}
	entries, ok = memoryBoundProduct(entries, 4*int64(unsafe.Sizeof(FullTextEntry{})))
	if !ok {
		return FunctionMemoryBound{}, false
	}
	strings, ok := memoryBoundProduct(tokens, 2*32) // 32-byte class contains a 23-byte token.
	if !ok {
		return FunctionMemoryBound{}, false
	}
	buffer, ok := memoryBoundProduct(content, 4) // bytes.Buffer old/new and string conversion.
	if !ok {
		return FunctionMemoryBound{}, false
	}
	result := FunctionMemoryBound{Columns: []MemoryColumnBound{
		{Type: args[0].Type, RowBytes: args[0].RowBytes},
		{Type: types.T_int32.ToType(), RowBytes: 4},
		{Type: types.T_varchar.ToType(), RowBytes: int64(types.T_varchar.ToType().TypeSize() + tokenizer.MAX_TOKEN_SIZE)},
	}}
	var outputRow int64
	for _, col := range result.Columns {
		outputRow, ok = memoryBoundSum(outputRow, col.RowBytes)
		if !ok {
			return FunctionMemoryBound{}, false
		}
	}
	batchBytes := max(unit, outputRow)
	idCopy, ok := memoryBoundProduct(args[0].RowBytes, 2)
	if !ok {
		return FunctionMemoryBound{}, false
	}
	// GetAny(...,true) copies one document ID. Its rounded byte storage and
	// slice/interface box are separate from the shared interfaces in Words.
	idCopy, ok = memoryBoundSum(idCopy, int64(unsafe.Sizeof([]byte{})+unsafe.Sizeof(any(nil))))
	if !ok {
		return FunctionMemoryBound{}, false
	}
	for _, part := range []int64{entries, strings, buffer, idCopy, 32768} {
		result.Bytes, ok = memoryBoundSum(result.Bytes, part)
		if !ok {
			return FunctionMemoryBound{}, false
		}
	}
	for i, col := range result.Columns {
		peak, ok := MemoryBatchVectorCapacity(col.Type, col.RowBytes, batchBytes, colexec.DefaultBatchSize)
		if !ok {
			return FunctionMemoryBound{}, false
		}
		result.Bytes, ok = memoryBoundSum(result.Bytes, peak)
		if !ok {
			return FunctionMemoryBound{}, false
		}
		result.Columns[i].BatchBytes, ok = memoryBoundProduct(col.RowBytes, colexec.DefaultBatchSize)
		if !ok {
			return FunctionMemoryBound{}, false
		}
		result.Columns[i].BatchBytes = min(result.Columns[i].BatchBytes, batchBytes)
	}
	return result, true
}

// SetMemoryOutputLimit sizes a tokenizer chunk for the actual document and
// actual Apply projection. Both the function's result and copied source columns
// fit the common byte unit, except for a single legal wide row. It is called
// only after ArgsEval and before starting this document; no partial TVF batch is
// dropped or replayed.
func (tableFunction *TableFunction) SetMemoryOutputLimit(unit int64, input *batch.Batch, row int, result []colexec.ResultPos) {
	tableFunction.memoryOutputRows = 0
	if unit <= 0 || !tableFunction.SupportsMemoryBound() || len(tableFunction.ctr.argVecs) == 0 {
		return
	}
	vectorRow := func(bat *batch.Batch, pos int) int64 {
		vec := bat.Vecs[pos]
		n := int64(vec.GetType().TypeSize())
		if vec.GetType().IsVarlen() && !vec.IsNull(uint64(row)) {
			n += int64(len(vec.GetBytesAt(row)))
		}
		return n
	}
	id := tableFunction.ctr.argVecs[0]
	idBytes := int64(id.GetType().TypeSize())
	if id.GetType().IsVarlen() && !id.IsNull(uint64(row)) {
		idBytes += int64(len(id.GetBytesAt(row)))
	}
	functionRows := []int64{idBytes, 4, int64(types.T_varchar.ToType().TypeSize() + tokenizer.MAX_TOKEN_SIZE)}
	functionRow := functionRows[0] + functionRows[1] + functionRows[2]
	var projectedRow int64
	for _, pos := range result {
		if pos.Rel == 0 {
			projectedRow += vectorRow(input, int(pos.Pos))
		} else {
			projectedRow += functionRows[pos.Pos]
		}
	}
	tableFunction.memoryOutputRows = int(max(int64(1), min(int64(colexec.DefaultBatchSize), unit/max(int64(1), functionRow, projectedRow))))
}
