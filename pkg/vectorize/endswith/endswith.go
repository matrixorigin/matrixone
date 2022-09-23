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

package endswith

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func isEqualSuffix(b1, b2 string) uint8 {
	if len(b1) >= len(b2) && bytes.Equal([]byte(b1)[len(b1)-len(b2):], []byte(b2)) {
		return 1
	}
	return 0
}

func EndsWith(lv, rv []string, rs []uint8) error {
	for i := range lv {
		if lv[i] == "" || rv[i] == ""{
			return moerr.NewInvalidArg("Endswith input", "empty string")
		}
		rs[i] = isEqualSuffix(lv[i], rv[i])
	}
	return nil
}

func EndsWithRightConst(lv, rv []string, rs []uint8) error {
	if rv[0] == ""{
		return moerr.NewInvalidArg("Endswith input", "empty string")
	}
	for i := range lv {
		if lv[i] == ""{
			return moerr.NewInvalidArg("Endswith input", "empty string")
		}
		rs[i] = isEqualSuffix(lv[i], rv[0])
	}
	return nil
}

func EndsWithLeftConst(lv, rv []string, rs []uint8) error{
	if lv[0] == ""{
		return moerr.NewInvalidArg("Endswith input", "empty string")
	}
	for i := range rv {
		if rv[i] == ""{
			return moerr.NewInvalidArg("Endswith input", "empty string")
		}
		rs[i] = isEqualSuffix(lv[0], rv[i])
	}

	return nil
}

func EndsWithAllConst(lv, rv []string, rs []uint8) error {
	if lv[0] == "" || rv[0] == ""{
		return moerr.NewInvalidArg("Endswith input", "empty string")
	}
	rs[0] = isEqualSuffix(lv[0], rv[0])
	return nil
}
