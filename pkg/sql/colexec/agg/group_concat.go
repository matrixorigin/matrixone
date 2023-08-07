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
	"fmt"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type GroupConcat1 struct {
	result    []strings.Builder
	separator string
}

func newGroupConcat1(separator string) *GroupConcat1 {
	return &GroupConcat1{
		separator: separator,
	}
}
func GroupConcat1ReturnType(_ []types.Type) types.Type {
	return types.T_varchar.ToType()
}

func (g *GroupConcat1) Grows(cnt int) {
	logutil.Infof("group_concat grows")
	for len(g.result) < cnt {
		g.result = append(g.result, strings.Builder{})
	}

	logutil.Infof("cnt %d  len result %d", cnt, len(g.result))
}

func (g *GroupConcat1) Eval(vs [][]byte, err error) ([][]byte, error) {
	result := make([][]byte, 0, len(g.result))
	for i := range g.result {
		result = append(result, []byte(g.result[i].String()))
	}
	return result, nil
}

func (g *GroupConcat1) Fill(groupIndex int64, input []byte, oldValue []byte, z int64, isEmpty bool, isNull bool) ([]byte, bool, error) {

	if isNull {
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

func (g *GroupConcat1) Merge(xIndex int64, yIndex int64, x []byte, y []byte, xIsEmpty bool, yIsEmpty bool, yGroupConcat1 any) ([]byte, bool, error) {
	logutil.Infof("group_concat merge")
	if yIsEmpty {
		return []byte{}, xIsEmpty && yIsEmpty, nil
	}

	if !xIsEmpty {
		g.result[xIndex].WriteString(g.separator)
	}
	g.result[xIndex].WriteString(yGroupConcat1.(*GroupConcat1).result[yIndex].String())

	return []byte{}, xIsEmpty && yIsEmpty, nil
}

func (g *GroupConcat1) MarshalBinary() ([]byte, error) {

	sbuilders := g.result
	strings := make([]string, 0, len(sbuilders))

	for i := range sbuilders {
		strings = append(strings, sbuilders[i].String())
	}
	res := types.EncodeStringSlice(strings)

	return res, nil
}

func (g *GroupConcat1) UnmarshalBinary(odata []byte) error {

	data := make([]byte, len(odata))
	copy(data, data)

	strs := types.DecodeStringSlice(data)
	result := make([]strings.Builder, len(strs))

	for i := range result {
		result[i].WriteString(strs[i])
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
