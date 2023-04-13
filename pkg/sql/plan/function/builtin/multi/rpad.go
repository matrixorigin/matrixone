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

package multi

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Rpad(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVec, tgtLenVec, padVec := ivecs[0], ivecs[1], ivecs[2]

	rtyp := *srcVec.GetType()
	if srcVec.IsConstNull() || tgtLenVec.IsConstNull() || padVec.IsConstNull() {
		return vector.NewConstNull(rtyp, srcVec.Length(), proc.Mp()), nil
	}

	srcVals := vector.MustStrCol(srcVec)
	tgtLenVals := vector.MustFixedCol[int64](tgtLenVec)
	padVals := vector.MustStrCol(padVec)
	if srcVec.IsConst() && tgtLenVec.IsConst() && padVec.IsConst() {
		rval, isNull := doRpad(srcVals[0], tgtLenVals[0], padVals[0])
		if isNull {
			return vector.NewConstNull(rtyp, srcVec.Length(), proc.Mp()), nil
		}
		return vector.NewConstBytes(rtyp, []byte(rval), srcVec.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, srcVec.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Set(rvec.GetNulls(), srcVec.GetNulls())
		nulls.Set(rvec.GetNulls(), tgtLenVec.GetNulls())
		nulls.Set(rvec.GetNulls(), padVec.GetNulls())

		srcIdx, tgtLenIdx, padIdx := 0, 0, 0
		srcInc, tgtLenInc, padInc := 1, 1, 1
		if srcVec.IsConst() {
			srcInc = 0
		}
		if tgtLenVec.IsConst() {
			tgtLenInc = 0
		}
		if padVec.IsConst() {
			padInc = 0
		}

		for i := 0; i < srcVec.Length(); i++ {
			if !rvec.GetNulls().Contains(uint64(i)) {
				rval, isNull := doRpad(srcVals[srcIdx], tgtLenVals[tgtLenIdx], padVals[padIdx])
				if isNull {
					rvec.GetNulls().Set(uint64(i))
				} else {
					vector.SetBytesAt(rvec, i, []byte(rval), proc.Mp())
				}
			}
			srcIdx += srcInc
			tgtLenIdx += tgtLenInc
			padIdx += padInc
		}
		return rvec, nil
	}
}

func doRpad(src string, tgtLen int64, pad string) (string, bool) {
	const MaxTgtLen = int64(16 * 1024 * 1024)

	srcRune, padRune := []rune(string(src)), []rune(string(pad))
	srcLen, padLen := len(srcRune), len(padRune)

	if tgtLen < 0 || tgtLen > MaxTgtLen {
		return "", true
	} else if int(tgtLen) < srcLen {
		return string(srcRune[:tgtLen]), false
	} else if int(tgtLen) == srcLen {
		return src, false
	} else if padLen == 0 {
		return "", false
	} else {
		r := int(tgtLen) - srcLen
		p, m := r/padLen, r%padLen
		return src + strings.Repeat(pad, p) + string(padRune[:m]), false
	}
}
