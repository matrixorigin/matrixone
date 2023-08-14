// Copyright 2022 Matrix Origin
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

package agg

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type GroupConcat struct {
	result    []bytes.Buffer
	separator string
}

const group_concat_max_len = 1024

func newGroupConcat(separator string) *GroupConcat {
	return &GroupConcat{
		separator: separator,
	}
}
func GroupConcatReturnType(parameters []types.Type) types.Type {
	for _, p := range parameters {
		if p.Oid == types.T_binary || p.Oid == types.T_varbinary || p.Oid == types.T_blob {
			return types.T_blob.ToType()
		}
	}
	return types.T_text.ToType()
}

func (g *GroupConcat) Grows(cnt int) {
	for i := 0; i < cnt; i++ {
		buffer := bytes.Buffer{}
		buffer.Grow(group_concat_max_len)
		g.result = append(g.result, buffer)
	}
}

func (g *GroupConcat) Eval(vs [][]byte, err error) ([][]byte, error) {
	result := make([][]byte, 0, len(g.result))

	for i := 0; i < len(g.result); i++ {
		result = append(result, g.result[i].Bytes())
	}

	return result, nil
}

func (g *GroupConcat) Fill(groupIndex int64, input []byte, oldValue []byte, z int64, isEmpty bool, isNull bool) ([]byte, bool, error) {

	if isNull || g.result[groupIndex].Len() > group_concat_max_len {
		return nil, isEmpty, nil
	}

	tuple, err := types.Unpack(input)

	if err != nil {
		return nil, false, err
	}

	tupleStr := tupleToString(tuple)

	if !isEmpty {
		g.result[groupIndex].WriteString(g.separator)
	}
	g.result[groupIndex].WriteString(tupleStr)

	return []byte{}, false, nil
}

func (g *GroupConcat) Merge(xIndex int64, yIndex int64, x []byte, y []byte, xIsEmpty bool, yIsEmpty bool, yGroupConcat any) ([]byte, bool, error) {

	if yIsEmpty || g.result[xIndex].Len() > group_concat_max_len {
		return []byte{}, xIsEmpty && yIsEmpty, nil
	}

	if !xIsEmpty {
		g.result[xIndex].WriteString(g.separator)
	}
	g.result[xIndex].Write(yGroupConcat.(*GroupConcat).result[yIndex].Bytes())

	return []byte{}, xIsEmpty && yIsEmpty, nil
}

func (g *GroupConcat) MarshalBinary() ([]byte, error) {

	bytes := g.result
	strings := make([]string, 0, len(bytes))

	for i := range bytes {
		strings = append(strings, bytes[i].String())
	}
	res := types.EncodeStringSlice(strings)

	return res, nil
}

func (g *GroupConcat) UnmarshalBinary(odata []byte) error {

	data := make([]byte, len(odata))
	copy(data, data)

	strings := types.DecodeStringSlice(data)
	result := make([]bytes.Buffer, len(strings))

	for i := range result {
		result[i].WriteString(strings[i])
	}
	g.result = result

	return nil
}

func tupleToString(tp types.Tuple) string {
	res := ""
	for _, t := range tp {
		switch t := t.(type) {
		case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
			res += fmt.Sprintf("%v", t)
		case []byte:
			res += *(*string)(unsafe.Pointer(&t))
		case types.Date:
			res += fmt.Sprintf("%v", t.String())
		case types.Time:
			res += fmt.Sprintf("%v", t.String())
		case types.Datetime:
			res += fmt.Sprintf("%v", t.String())
		case types.Timestamp:
			res += fmt.Sprintf("%v", t.String())
		case types.Decimal64:
			res += fmt.Sprintf("%v", t.Format(0))
		case types.Decimal128:
			res += fmt.Sprintf("%v", t.Format(0))
		default:
			res += fmt.Sprintf("%v", t)
		}
	}
	return res
}
