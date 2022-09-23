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

package startswith

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func hasPrefix(b1, b2 []byte) uint8 {
	if len(b1) >= len(b2) && bytes.Equal(b1[:len(b2)], b2) {
		return 1
	}
	return 0
}

func StartsWith(lv, rv []string, rs []uint8) error {
	for i := range lv {
		if lv[i] == "" || rv[i] == "" {
			return moerr.NewInvalidArg("StartsWith input", "empty string")
		}
		rs[i] = hasPrefix([]byte(lv[i]), []byte(rv[i]))
	}
	return nil
}

func StartsWithRightConst(lv []string, rv string, rs []uint8) error {
	if rv == "" {
		return moerr.NewInvalidArg("StartsWith input", "empty string")
	}
	for i := range lv {
		if lv[i] == "" {
			return moerr.NewInvalidArg("StartsWith input", "empty string")
		}
		rs[i] = hasPrefix([]byte(lv[i]), []byte(rv))
	}
	return nil
}

func StartsWithLeftConst(lv string, rv []string, rs []uint8) error {
	if lv == "" {
		return moerr.NewInvalidArg("StartsWith input", "empty string")
	}
	for i := range rv {
		if rv[i] == "" {
			return moerr.NewInvalidArg("StartsWith input", "empty string")
		}
		rs[i] = hasPrefix([]byte(lv), []byte(rv[i]))
	}
	return nil
}

func StartsWithAllConst(lv, rv string, rs []uint8) error {
	if rv == "" || lv == "" {
		return moerr.NewInvalidArg("StartsWith input", "empty string")
	}
	rs[0] = hasPrefix([]byte(lv), []byte(rv))
	return nil
}
