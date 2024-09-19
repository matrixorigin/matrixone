// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func fullTextMatch(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moerr.NewNotSupported(proc.Ctx, "function fulltext_match not supported")

	/*
		p1 := vector.GenerateFunctionStrParameter(ivecs[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
		rs := vector.MustFunctionResult[bool](result)

		if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
			return moerr.NewInvalidArg(proc.Ctx, "fullTextMatch search pattern", "not scalar")
		}

		if !ivecs[1].IsConst() || ivecs[1].IsConstNull() {
			return moerr.NewInvalidArg(proc.Ctx, "fullTextMatch search mode", "not scalar")
		}

		// pattern
		pattern, _ := p1.GetStrValue(0)
		mode, _ := p2.GetValue(0)

		logutil.Infof("pattern %s, mode %d", pattern, mode)
		ncols := len(ivecs)

		recs := make([]vector.FunctionParameterWrapper[types.Varlena], ncols-2)

		for i := 2; i < ncols; i++ {
			recs[i-2] = vector.GenerateFunctionStrParameter(ivecs[i])
		}

		for i := uint64(0); i < uint64(length); i++ {
			for j := 0; j < len(recs); j++ {
				s, null := recs[j].GetStrValue(i)
				logutil.Infof("[%d] col %d str=%s, null=%d", i, j, s, null)
			}

			rs.Append(true, false)
		}

		return nil
	*/
}

func fullTextMatchScore(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return moerr.NewNotSupported(proc.Ctx, "function fulltext_match_score not supported")

	/*
		p1 := vector.GenerateFunctionStrParameter(ivecs[0])
		p2 := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
		rs := vector.MustFunctionResult[float32](result)

		if !ivecs[0].IsConst() || ivecs[0].IsConstNull() {
			return moerr.NewInvalidArg(proc.Ctx, "fullTextMatch search pattern", "not scalar")
		}

		if !ivecs[1].IsConst() || ivecs[1].IsConstNull() {
			return moerr.NewInvalidArg(proc.Ctx, "fullTextMatch search mode", "not scalar")
		}

		// pattern
		pattern, _ := p1.GetStrValue(0)
		mode, _ := p2.GetValue(0)

		logutil.Infof("pattern %s, mode %d", pattern, mode)
		ncols := len(ivecs)

		recs := make([]vector.FunctionParameterWrapper[types.Varlena], ncols-2)

		for i := 2; i < ncols; i++ {
			recs[i-2] = vector.GenerateFunctionStrParameter(ivecs[i])
		}

		for i := uint64(0); i < uint64(length); i++ {
			for j := 0; j < len(recs); j++ {
				s, null := recs[j].GetStrValue(i)
				logutil.Infof("[%d] col %d str=%s, null=%d", i, j, s, null)
			}

			rs.Append(float32(1.33), false)
		}

		return nil
	*/
}
